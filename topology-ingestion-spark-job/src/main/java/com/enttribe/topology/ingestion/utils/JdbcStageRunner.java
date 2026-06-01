package com.enttribe.topology.ingestion.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Streams an inventory SQL query from MySQL and writes the results into
 * JanusGraph using {@link VertexWriter} (Phase 1) and {@link EdgeWriter}
 * (Phase 2). Mirrors the two-phase {@code createMultipleVertices} flow in
 * the topology service.
 */
public final class JdbcStageRunner {

    private static final Logger logger = LoggerFactory.getLogger(JdbcStageRunner.class);

    static {
        // Force-load JDBC drivers so their static initializers register them with
        // DriverManager. Required when running under classloader scopes where
        // DriverManager's TCCL-based ServiceLoader scan does not see the drivers
        // bundled in this JAR (e.g. spark-on-k8s with image-bundled jars taking
        // precedence on the AppClassLoader).
        String[] driverClasses = {"org.mariadb.jdbc.Driver", "com.mysql.cj.jdbc.Driver"};
        for (String dc : driverClasses) {
            try {
                Class.forName(dc);
                logger.info("Registered JDBC driver: {}", dc);
            } catch (ClassNotFoundException e) {
                logger.warn("JDBC driver not on classpath (skipping): {}", dc);
            }
        }
    }

    private final String jdbcUrl;
    private final String dbUser;
    private final String dbPassword;
    private final int fetchSize;
    private final int vertexBatchSize;
    private final int edgeBatchSize;
    private final VertexWriter vertexWriter;
    private final EdgeWriter edgeWriter;

    public JdbcStageRunner(VertexWriter vertexWriter, EdgeWriter edgeWriter) {
        this.jdbcUrl = ConfigLoader.get("LCM_DB_URL");
        this.dbUser = ConfigLoader.get("DB_USER");
        this.dbPassword = ConfigLoader.get("DB_PASSWORD");
        this.fetchSize = ConfigLoader.getInt("JDBC_FETCH_SIZE", 1000);
        this.vertexBatchSize = ConfigLoader.getInt("VERTEX_BATCH_SIZE", GraphConstants.VERTEX_BATCH_SIZE);
        this.edgeBatchSize = ConfigLoader.getInt("EDGE_BATCH_SIZE", GraphConstants.EDGE_BATCH_SIZE);
        this.vertexWriter = vertexWriter;
        this.edgeWriter = edgeWriter;

        if (jdbcUrl == null || dbUser == null || dbPassword == null) {
            throw new IllegalStateException(
                    "LCM_DB_URL / DB_USER / DB_PASSWORD must be set (env var or config.properties)");
        }
    }

    public StageResult run(String stageName, String query) {
        long startMs = System.currentTimeMillis();
        logger.info("[{}] starting", stageName);

        List<Map<String, Object>> allRows;
        try {
            allRows = readQuery(stageName, query);
        } catch (SQLException e) {
            long elapsed = System.currentTimeMillis() - startMs;
            logger.error("[{}] SQL failure after {} ms: {}", stageName, elapsed, e.getMessage(), e);
            return StageResult.failed(elapsed);
        }

        int totalRows = allRows.size();
        if (totalRows == 0) {
            long elapsed = System.currentTimeMillis() - startMs;
            logger.warn("[{}] returned 0 rows in {} ms", stageName, elapsed);
            return StageResult.empty(elapsed);
        }

        // Phase 1 — vertices, in batches of vertexBatchSize.
        VertexAcc vAcc = new VertexAcc();
        for (int i = 0; i < totalRows; i += vertexBatchSize) {
            int end = Math.min(i + vertexBatchSize, totalRows);
            List<Map<String, Object>> sub = new ArrayList<>(allRows.subList(i, end));
            VertexWriter.WriteOutcome o = vertexWriter.writeBatch(stageName, sub);
            vAcc.add(o);
        }

        // Phase 2 — edges, in batches of edgeBatchSize.
        EdgeAcc eAcc = new EdgeAcc();
        for (int i = 0; i < totalRows; i += edgeBatchSize) {
            int end = Math.min(i + edgeBatchSize, totalRows);
            List<Map<String, Object>> sub = new ArrayList<>(allRows.subList(i, end));
            EdgeWriter.WriteOutcome o = edgeWriter.writeBatch(stageName, sub);
            eAcc.add(o);
        }

        long elapsed = System.currentTimeMillis() - startMs;
        boolean overall = vAcc.failedBatches == 0 && eAcc.failedBatches == 0;
        logger.info("[{}] done: rows={}, vCreated={}, vUpdated={}, vSkipped={}, vFailedBatches={}, "
                        + "eCreated={}, eUpdated={}, eSkipped={}, eFailedBatches={}, elapsed={} ms",
                stageName, totalRows,
                vAcc.created, vAcc.updated, vAcc.skipped, vAcc.failedBatches,
                eAcc.created, eAcc.updated, eAcc.skipped, eAcc.failedBatches, elapsed);

        return new StageResult(overall, totalRows,
                vAcc.created, vAcc.updated, vAcc.skipped, vAcc.failedBatches,
                eAcc.created, eAcc.updated, eAcc.skipped, eAcc.failedBatches,
                elapsed);
    }

    private List<Map<String, Object>> readQuery(String stageName, String query) throws SQLException {
        List<Map<String, Object>> rows = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(jdbcUrl, dbUser, dbPassword);
             Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            stmt.setFetchSize(fetchSize);
            try (ResultSet rs = stmt.executeQuery(query)) {
                ResultSetMetaData md = rs.getMetaData();
                int cols = md.getColumnCount();
                while (rs.next()) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    for (int i = 1; i <= cols; i++) {
                        String name = md.getColumnLabel(i);
                        Object value;
                        if (GraphConstants.LAST_DISCOVERY_TIME.equalsIgnoreCase(name)) {
                            Timestamp ts = rs.getTimestamp(i);
                            value = (ts == null) ? null : ts.toLocalDateTime().toString().replace("T", " ");
                        } else if (GraphConstants.isEpochMillisProperty(name)) {
                            Timestamp ts = rs.getTimestamp(i);
                            value = (ts == null) ? null : ts.getTime();
                        } else {
                            value = rs.getObject(i);
                            if (value instanceof java.math.BigDecimal) {
                                value = ((java.math.BigDecimal) value).doubleValue();
                            } else if (value instanceof java.math.BigInteger) {
                                value = ((java.math.BigInteger) value).longValue();
                            }
                        }
                        row.put(name, value);
                    }
                    rows.add(row);
                }
            }
        }
        logger.info("[{}] loaded {} rows from MySQL", stageName, rows.size());
        return rows;
    }

    private static final class VertexAcc {
        int created;
        int updated;
        int skipped;
        int failedBatches;
        void add(VertexWriter.WriteOutcome o) {
            created += o.created;
            updated += o.updated;
            skipped += o.skipped;
            if (!o.success) failedBatches++;
        }
    }

    private static final class EdgeAcc {
        int created;
        int updated;
        int skipped;
        int failedBatches;
        void add(EdgeWriter.WriteOutcome o) {
            created += o.created;
            updated += o.updated;
            skipped += o.skipped;
            if (!o.success) failedBatches++;
        }
    }

    public static final class StageResult {
        public final boolean success;
        public final int rows;
        public final int vCreated;
        public final int vUpdated;
        public final int vSkipped;
        public final int vFailedBatches;
        public final int eCreated;
        public final int eUpdated;
        public final int eSkipped;
        public final int eFailedBatches;
        public final long elapsedMs;

        public StageResult(boolean success, int rows,
                           int vCreated, int vUpdated, int vSkipped, int vFailedBatches,
                           int eCreated, int eUpdated, int eSkipped, int eFailedBatches,
                           long elapsedMs) {
            this.success = success;
            this.rows = rows;
            this.vCreated = vCreated;
            this.vUpdated = vUpdated;
            this.vSkipped = vSkipped;
            this.vFailedBatches = vFailedBatches;
            this.eCreated = eCreated;
            this.eUpdated = eUpdated;
            this.eSkipped = eSkipped;
            this.eFailedBatches = eFailedBatches;
            this.elapsedMs = elapsedMs;
        }

        static StageResult empty(long elapsed) {
            return new StageResult(true, 0, 0, 0, 0, 0, 0, 0, 0, 0, elapsed);
        }

        static StageResult failed(long elapsed) {
            return new StageResult(false, 0, 0, 0, 0, 1, 0, 0, 0, 1, elapsed);
        }
    }
}
