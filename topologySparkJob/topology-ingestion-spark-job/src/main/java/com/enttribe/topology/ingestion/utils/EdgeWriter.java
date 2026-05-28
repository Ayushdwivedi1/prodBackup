package com.enttribe.topology.ingestion.utils;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphTransaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Phase 2 of the topology createMultipleVertices flow.
 *
 * <p>For each row in a batch where {@code neName}, {@code neighbourNEName} and
 * {@code relation} are all present:
 * <ul>
 *   <li>Looks up the source and destination vertices (already created in Phase 1).</li>
 *   <li>Searches for an existing edge with the same
 *       {@code srcInterface, destInterface, relation} — and additionally {@code linkType}
 *       when {@code relation == "ISIS"}.</li>
 *   <li>If found, updates the edge's properties (matches
 *       {@code updateExistingEdgePropertiesForMap} in the service).</li>
 *   <li>Otherwise, creates a new edge with all the properties the service writes
 *       (matches {@code createNewEdgeForMap}).</li>
 *   <li>Whole batch is one JanusGraph transaction.</li>
 * </ul>
 */
public final class EdgeWriter {

    private static final Logger logger = LoggerFactory.getLogger(EdgeWriter.class);

    private final JanusGraph graph;

    public EdgeWriter(JanusGraph graph) {
        this.graph = graph;
    }

    public WriteOutcome writeBatch(String stageName, List<Map<String, Object>> batch) {
        if (batch == null || batch.isEmpty()) {
            return new WriteOutcome(true, 0, 0, 0);
        }

        // Filter rows that actually describe an edge.
        int created = 0;
        int updated = 0;
        int skipped = 0;

        Set<String> requiredVertexNames = new HashSet<>();
        for (Map<String, Object> data : batch) {
            String src = stringOrNull(data.get(GraphConstants.NENAME));
            String dst = stringOrNull(data.get(GraphConstants.NEIGHBOUR_NENAME));
            String rel = stringOrNull(data.get(GraphConstants.RELATION));
            if (src == null || dst == null || rel == null) continue;
            requiredVertexNames.add(src);
            requiredVertexNames.add(dst);
        }
        if (requiredVertexNames.isEmpty()) {
            // No edge-shaped rows in this batch — that's fine, nothing to do.
            return new WriteOutcome(true, 0, 0, batch.size());
        }

        JanusGraphTransaction tx = graph.newTransaction();
        try {
            GraphTraversalSource gtx = tx.traversal();

            // 1. Batch-fetch all vertices we may need (matches batchFetchAllRequiredVertices in service).
            Map<String, Vertex> verticesByName = new HashMap<>(requiredVertexNames.size() * 2);
            List<Vertex> vertices = gtx.V()
                    .has(GraphConstants.NENAME, P.within(new java.util.ArrayList<>(requiredVertexNames)))
                    .toList();
            for (Vertex v : vertices) {
                VertexProperty<Object> p = v.property(GraphConstants.NENAME);
                if (p.isPresent()) {
                    verticesByName.putIfAbsent(String.valueOf(p.value()), v);
                }
            }

            // 2. Process each row.
            for (Map<String, Object> data : batch) {
                try {
                    String src = stringOrNull(data.get(GraphConstants.NENAME));
                    String dst = stringOrNull(data.get(GraphConstants.NEIGHBOUR_NENAME));
                    String relation = stringOrNull(data.get(GraphConstants.RELATION));
                    String neType = stringOrNull(data.get(GraphConstants.NETYPE));
                    if (src == null || dst == null || relation == null || neType == null) {
                        skipped++;
                        continue;
                    }

                    Vertex sourceVertex = verticesByName.get(src);
                    Vertex destVertex = verticesByName.get(dst);
                    if (sourceVertex == null || destVertex == null) {
                        skipped++;
                        logger.debug("[{}] edge skipped — missing vertex(es). src={} (found={}), dst={} (found={})",
                                stageName, src, sourceVertex != null, dst, destVertex != null);
                        continue;
                    }

                    String linkType = stringOrNull(data.get(GraphConstants.LINK_TYPE));
                    Edge existing = findExistingEdge(gtx, src, dst, relation, linkType);

                    if (existing != null) {
                        updateExistingEdge(existing, data);
                        updated++;
                    } else {
                        createNewEdge(destVertex, sourceVertex, neType, relation, data);
                        created++;
                    }
                } catch (Exception rowEx) {
                    skipped++;
                    logger.warn("[{}] edge row failed (continuing batch) — neName={}, err={}",
                            stageName, data.get(GraphConstants.NENAME), rowEx.getMessage());
                }
            }

            tx.commit();
            logger.info("[{}] edges: created={}, updated={}, skipped={}, batchSize={}",
                    stageName, created, updated, skipped, batch.size());
            return new WriteOutcome(true, created, updated, skipped);
        } catch (Exception batchEx) {
            safeRollback(tx);
            logger.error("[{}] edge batch rolled back — {}", stageName, batchEx.getMessage(), batchEx);
            return new WriteOutcome(false, 0, 0, batch.size());
        }
    }

    /** Mirrors {@code findExistingEdgeWithLinkType} in {@code JanusgraphServiceImpl}. */
    private static Edge findExistingEdge(GraphTraversalSource gtx, String src, String dst,
                                         String relation, String linkType) {
        GraphTraversal<Edge, Edge> t = gtx.E()
                .has(GraphConstants.SRC_INTERFACE, src)
                .has(GraphConstants.DEST_INTERFACE, dst)
                .has(GraphConstants.RELATION, relation);

        if (GraphConstants.RELATION_ISIS.equalsIgnoreCase(relation)) {
            if (linkType != null && !linkType.isEmpty()) {
                t = t.has(GraphConstants.LINK_TYPE, linkType);
            } else {
                t = t.hasNot(GraphConstants.LINK_TYPE);
            }
        }
        return t.tryNext().orElse(null);
    }

    /** Mirrors {@code createNewEdgeForMap} in {@code JanusgraphServiceImpl}. */
    private static void createNewEdge(Vertex destinationVertex, Vertex sourceVertex,
                                      String edgeLabel, String relation, Map<String, Object> data) {
        String src = stringOrNull(data.get(GraphConstants.NENAME));
        String dst = stringOrNull(data.get(GraphConstants.NEIGHBOUR_NENAME));
        String sourceNode = stringOrNull(data.get(GraphConstants.PARENT_NE));
        String destinationNode = stringOrNull(data.get(GraphConstants.NEIGHBOUR_PARENT_NENAME));
        String cktId = data.get(GraphConstants.CKTID) != null ? data.get(GraphConstants.CKTID).toString() : null;
        String hostname = stringOrNull(data.get(GraphConstants.HOSTNAME));
        String sourceCategory = stringOrNull(data.get(GraphConstants.SRC_CATEGORY));
        String destinationCategory = stringOrNull(data.get(GraphConstants.DEST_CATEGORY));
        String sourceId = GraphConstants.extractingId(src);
        String destinationId = GraphConstants.extractingId(dst);
        String sourceNodeId = stringOrNull(data.get(GraphConstants.NE_ID));
        String destinationNodeId = stringOrNull(data.get(GraphConstants.NEIGHBOUR_NE_ID));

        // addEdge with varargs — gremlin allows null values, they just won't be stored.
        // Order and keys must match createNewEdgeForMap exactly.
        Edge edge = destinationVertex.addEdge(edgeLabel, sourceVertex,
                GraphConstants.CKTID, cktId,
                GraphConstants.LINE_NAME, hostname,
                GraphConstants.SRC_CATEGORY, sourceCategory,
                GraphConstants.DEST_CATEGORY, destinationCategory,
                GraphConstants.RELATION, relation,
                GraphConstants.CREATED_AT, System.currentTimeMillis(),
                GraphConstants.STATUS, GraphConstants.ACTIVE,
                GraphConstants.SRC_INTERFACE, data.get(GraphConstants.NENAME),
                GraphConstants.DEST_INTERFACE, data.get(GraphConstants.NEIGHBOUR_NENAME),
                GraphConstants.SRC_LAT, data.get(GraphConstants.LAT),
                GraphConstants.SRC_LONG, data.get(GraphConstants.LNG),
                GraphConstants.DEST_LAT, data.get(GraphConstants.NEIGHBOUR_PARENT_NE_LAT),
                GraphConstants.DEST_LONG, data.get(GraphConstants.NEIGHBOUR_PARENT_NE_LNG),
                GraphConstants.SRC_TYPE, data.get(GraphConstants.SRC_TYPE),
                GraphConstants.NEIGHBOUR_VENDOR, data.get(GraphConstants.NEIGHBOUR_VENDOR),
                GraphConstants.NEIGHBOUR_DOMAIN, data.get(GraphConstants.NEIGHBOUR_DOMAIN),
                GraphConstants.START_ID, sourceId,
                GraphConstants.END_ID, destinationId,
                GraphConstants.START_NEID, sourceNodeId,
                GraphConstants.END_NEID, destinationNodeId,
                GraphConstants.NEIGHBOUR_TECH, data.get(GraphConstants.NEIGHBOUR_TECHNOLOGY),
                GraphConstants.ISDEL, data.get(GraphConstants.ISDEL),
                GraphConstants.SRC_BANDWIDTH, data.get(GraphConstants.SRC_BANDWIDTH),
                GraphConstants.DEST_BANDWIDTH, data.get(GraphConstants.DEST_BANDWIDTH),
                GraphConstants.LINK_TYPE, data.get(GraphConstants.LINK_TYPE),
                GraphConstants.AREA_IP_ADDRESS, data.get(GraphConstants.AREA_IP_ADDRESS),
                GraphConstants.TAG, data.get(GraphConstants.TAG),
                GraphConstants.SRC_VENDOR, data.get(GraphConstants.SRC_VENDOR),
                GraphConstants.DEST_VENDOR, data.get(GraphConstants.DEST_VENDOR),
                GraphConstants.SRC_TECHNOLOGY, data.get(GraphConstants.SRC_TECHNOLOGY),
                GraphConstants.DEST_TECHNOLOGY, data.get(GraphConstants.DEST_TECHNOLOGY),
                GraphConstants.DEST_TYPE, data.get(GraphConstants.DEST_TYPE),
                GraphConstants.SRC_DOMAIN, data.get(GraphConstants.SRC_DOMAIN),
                GraphConstants.DEST_DOMAIN, data.get(GraphConstants.DEST_DOMAIN),
                GraphConstants.LSP_NAME, data.get(GraphConstants.LSP_NAME),
                GraphConstants.HOP_SEQUENCE, data.get(GraphConstants.HOP_SEQUENCE),
                GraphConstants.LSP_ID, data.get(GraphConstants.LSP_ID));

        // start/end are only set when parent values are present (service does the same).
        if (sourceNode != null) {
            edge.property(GraphConstants.START, sourceNode);
        }
        if (destinationNode != null) {
            edge.property(GraphConstants.END, destinationNode);
        }
        edge.property(GraphConstants.LAST_DISCOVERY_TIME, data.get(GraphConstants.LAST_DISCOVERY_TIME));
    }

    /** Mirrors {@code updateExistingEdgePropertiesForMap} in {@code JanusgraphServiceImpl}. */
    private static void updateExistingEdge(Edge existing, Map<String, Object> data) {
        String sourceNode = stringOrNull(data.get(GraphConstants.PARENT_NE));
        String destinationNode = stringOrNull(data.get(GraphConstants.NEIGHBOUR_PARENT_NENAME));

        if (sourceNode != null) {
            existing.property(GraphConstants.START, sourceNode);
        }
        if (destinationNode != null) {
            existing.property(GraphConstants.END, destinationNode);
        }

        existing.property(GraphConstants.TAG, data.get(GraphConstants.TAG));
        existing.property(GraphConstants.AREA_IP_ADDRESS, data.get(GraphConstants.AREA_IP_ADDRESS));
        existing.property(GraphConstants.SRC_BANDWIDTH, data.get(GraphConstants.SRC_BANDWIDTH));
        existing.property(GraphConstants.DEST_BANDWIDTH, data.get(GraphConstants.DEST_BANDWIDTH));
        existing.property(GraphConstants.DEST_LAT, data.get(GraphConstants.NEIGHBOUR_PARENT_NE_LAT));
        existing.property(GraphConstants.DEST_LONG, data.get(GraphConstants.NEIGHBOUR_PARENT_NE_LNG));
        existing.property(GraphConstants.NEIGHBOUR_VENDOR, data.get(GraphConstants.NEIGHBOUR_VENDOR));
        existing.property(GraphConstants.NEIGHBOUR_DOMAIN, data.get(GraphConstants.NEIGHBOUR_DOMAIN));
        existing.property(GraphConstants.NEIGHBOUR_TECH, data.get(GraphConstants.NEIGHBOUR_TECHNOLOGY));
        existing.property(GraphConstants.SRC_LAT, data.get(GraphConstants.LAT));
        existing.property(GraphConstants.SRC_LONG, data.get(GraphConstants.LNG));
        existing.property(GraphConstants.LAST_DISCOVERY_TIME, data.get(GraphConstants.LAST_DISCOVERY_TIME));
        existing.property(GraphConstants.ISDEL, data.get(GraphConstants.ISDEL));

        existing.property(GraphConstants.SRC_VENDOR, data.get(GraphConstants.SRC_VENDOR));
        existing.property(GraphConstants.DEST_VENDOR, data.get(GraphConstants.DEST_VENDOR));
        existing.property(GraphConstants.SRC_TECHNOLOGY, data.get(GraphConstants.SRC_TECHNOLOGY));
        existing.property(GraphConstants.DEST_TECHNOLOGY, data.get(GraphConstants.DEST_TECHNOLOGY));
        existing.property(GraphConstants.SRC_TYPE, data.get(GraphConstants.SRC_TYPE));
        existing.property(GraphConstants.DEST_TYPE, data.get(GraphConstants.DEST_TYPE));
        existing.property(GraphConstants.SRC_DOMAIN, data.get(GraphConstants.SRC_DOMAIN));
        existing.property(GraphConstants.DEST_DOMAIN, data.get(GraphConstants.DEST_DOMAIN));
        existing.property(GraphConstants.SRC_CATEGORY, data.get(GraphConstants.SRC_CATEGORY));
        existing.property(GraphConstants.DEST_CATEGORY, data.get(GraphConstants.DEST_CATEGORY));
        existing.property(GraphConstants.LSP_NAME, data.get(GraphConstants.LSP_NAME));
        existing.property(GraphConstants.HOP_SEQUENCE, data.get(GraphConstants.HOP_SEQUENCE));
        existing.property(GraphConstants.LSP_ID, data.get(GraphConstants.LSP_ID));

        // Refresh interface keys in case neName/neighbourNEName moved (service does this too).
        existing.property(GraphConstants.SRC_INTERFACE, data.get(GraphConstants.NENAME));
        existing.property(GraphConstants.DEST_INTERFACE, data.get(GraphConstants.NEIGHBOUR_NENAME));

        // Refresh start/end NE ids only when present (service does conditional update).
        Object srcNeId = data.get(GraphConstants.NE_ID);
        Object dstNeId = data.get(GraphConstants.NEIGHBOUR_NE_ID);
        if (srcNeId != null && !srcNeId.toString().isBlank()) {
            existing.property(GraphConstants.START_NEID, srcNeId);
        }
        if (dstNeId != null && !dstNeId.toString().isBlank()) {
            existing.property(GraphConstants.END_NEID, dstNeId);
        }
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
