package com.enttribe.topology.ingestion.utils;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Phase 1 of the topology createMultipleVertices flow.
 *
 * <p>For each row in a batch:
 * <ul>
 *   <li>Composite key is {@code neName|relation} ({@code |relation} omitted if relation is blank).</li>
 *   <li>Existing vertices are fetched in one query via {@code V().has(neName, within(...))}.</li>
 *   <li>If a vertex with the same composite key exists, its properties are updated.</li>
 *   <li>Otherwise a new vertex is added with label {@code neType}.</li>
 *   <li>Properties that belong on edges only (see
 *       {@link GraphConstants#isSourceDestinationProperty(String)}) are skipped.</li>
 *   <li>Null values are skipped on writes (matches the topology service).</li>
 *   <li>The whole batch is one JanusGraph transaction — committed on success,
 *       rolled back on any unrecoverable exception.</li>
 * </ul>
 */
public final class VertexWriter {

    private static final Logger logger = LoggerFactory.getLogger(VertexWriter.class);

    private final JanusGraph graph;

    public VertexWriter(JanusGraph graph) {
        this.graph = graph;
    }

    public WriteOutcome writeBatch(String stageName, List<Map<String, Object>> batch) {
        if (batch == null || batch.isEmpty()) {
            return new WriteOutcome(true, 0, 0, 0);
        }

        int created = 0;
        int updated = 0;
        int skipped = 0;

        JanusGraphTransaction tx = graph.newTransaction();
        try {
            GraphTraversalSource gtx = tx.traversal();

            // 1. Collect all distinct neNames in this batch (skipping rows with no neName).
            Set<String> neNames = new HashSet<>(batch.size());
            for (Map<String, Object> row : batch) {
                Object neName = row.get(GraphConstants.NENAME);
                if (neName != null && !neName.toString().isBlank()) {
                    neNames.add(neName.toString());
                }
            }
            if (neNames.isEmpty()) {
                logger.warn("[{}] vertex batch contained 0 rows with a usable neName — skipping", stageName);
                tx.commit();
                return new WriteOutcome(true, 0, 0, batch.size());
            }

            // 2. Batch-fetch existing vertices and bucket them by composite key.
            Map<String, JanusGraphVertex> existingByKey = batchFetchExistingVertices(gtx, neNames);

            // 3. For each row, decide whether to update or create.
            for (Map<String, Object> data : batch) {
                try {
                    Object neNameObj = data.get(GraphConstants.NENAME);
                    Object neTypeObj = data.get(GraphConstants.NETYPE);
                    if (neNameObj == null || neNameObj.toString().isBlank()
                            || neTypeObj == null || neTypeObj.toString().isBlank()) {
                        skipped++;
                        logger.debug("[{}] skipping vertex row — missing neName/neType: {}", stageName, data);
                        continue;
                    }

                    String neName = neNameObj.toString();
                    String neType = neTypeObj.toString();
                    String relationStr = stringOrNull(data.get(GraphConstants.RELATION));
                    String key = compositeKey(neName, relationStr);

                    JanusGraphVertex existing = existingByKey.get(key);
                    // Defensive: ensure the existing vertex's relation matches; otherwise treat as new.
                    if (existing != null && !relationsEqual(relationStr, vertexRelation(existing))) {
                        existing = null;
                    }

                    Vertex target;
                    if (existing != null) {
                        target = existing;
                        updated++;
                    } else {
                        target = gtx.addV(neType).next();
                        created++;
                    }

                    setVertexProperties(target, data);
                } catch (Exception rowEx) {
                    skipped++;
                    logger.warn("[{}] vertex row failed (continuing batch) — neName={}, err={}",
                            stageName, data.get(GraphConstants.NENAME), rowEx.getMessage());
                }
            }

            tx.commit();
            logger.info("[{}] vertices: created={}, updated={}, skipped={}, batchSize={}",
                    stageName, created, updated, skipped, batch.size());
            return new WriteOutcome(true, created, updated, skipped);
        } catch (Exception batchEx) {
            safeRollback(tx);
            logger.error("[{}] vertex batch rolled back — {}", stageName, batchEx.getMessage(), batchEx);
            return new WriteOutcome(false, 0, 0, batch.size());
        }
    }

    private Map<String, JanusGraphVertex> batchFetchExistingVertices(GraphTraversalSource gtx,
                                                                     Set<String> neNames) {
        Map<String, JanusGraphVertex> byKey = new HashMap<>(neNames.size() * 2);
        if (neNames.isEmpty()) return byKey;

        List<Vertex> vertices = gtx.V()
                .has(GraphConstants.NENAME, P.within(new ArrayList<>(neNames)))
                .toList();

        for (Vertex v : vertices) {
            VertexProperty<Object> neNameProp = v.property(GraphConstants.NENAME);
            if (!neNameProp.isPresent()) continue;
            String neName = String.valueOf(neNameProp.value());
            String relation = vertexRelation(v);
            String key = compositeKey(neName, relation);
            // First-write wins (matches the topology service's behaviour)
            byKey.putIfAbsent(key, (JanusGraphVertex) v);
        }
        return byKey;
    }

    private static void setVertexProperties(Vertex vertex, Map<String, Object> data) {
        for (Map.Entry<String, Object> e : data.entrySet()) {
            String key = e.getKey();
            Object value = e.getValue();
            if (value == null) continue;
            if (GraphConstants.isSourceDestinationProperty(key)) continue;
            vertex.property(key, value);
        }
    }

    private static String compositeKey(String neName, String relation) {
        if (relation != null && !relation.isBlank()) {
            return neName + "|" + relation;
        }
        return neName;
    }

    private static boolean relationsEqual(String a, String b) {
        if (a == null || a.isBlank()) return b == null || b.isBlank();
        if (b == null || b.isBlank()) return false;
        return a.equals(b);
    }

    private static String vertexRelation(Vertex v) {
        VertexProperty<Object> p = v.property(GraphConstants.RELATION);
        return p.isPresent() ? String.valueOf(p.value()) : null;
    }

    private static String stringOrNull(Object v) {
        if (v == null) return null;
        String s = v.toString().trim();
        return s.isEmpty() ? null : s;
    }

    private static void safeRollback(JanusGraphTransaction tx) {
        if (tx == null) return;
        try {
            if (tx.isOpen()) tx.rollback();
        } catch (Exception e) {
            logger.warn("Rollback failed: {}", e.getMessage());
        }
    }

    public static final class WriteOutcome {
        public final boolean success;
        public final int created;
        public final int updated;
        public final int skipped;

        public WriteOutcome(boolean success, int created, int updated, int skipped) {
            this.success = success;
            this.created = created;
            this.updated = updated;
            this.skipped = skipped;
        }
    }
}
