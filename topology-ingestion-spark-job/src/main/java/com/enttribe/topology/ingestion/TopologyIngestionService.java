package com.enttribe.topology.ingestion;

import com.enttribe.commons.spark.SparkJob;
import com.enttribe.topology.ingestion.core.TopologyIngestionJob;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SparkRunner entry point — used when launching this job through the
 * Enttribe spark-runner platform (same pattern as BoomingCellsService).
 *
 * For local / spark-submit launches use {@link TopologyIngestionJobTrigger}.
 */
public class TopologyIngestionService extends SparkJob {

    private static final Logger logger = LoggerFactory.getLogger(TopologyIngestionService.class);
    private static String domain = "TOPOLOGY";

    @Override
    public void run() {
        try {
            logger.info("Topology Ingestion Service Started");
            scala.Option<SparkSession> activeSession = SparkSession.getActiveSession();
            SparkSession sparkSession = activeSession.isDefined() ? activeSession.get()
                    : SparkSession.builder().getOrCreate();

            boolean success = TopologyIngestionJob.startTopologyIngestionJob(sparkSession, domain);
            if (success) {
                logger.info("Topology Ingestion Service Completed Successfully");
            } else {
                logger.warn("Topology Ingestion Service Completed with one or more failed stages");
            }
        } catch (Exception e) {
            logger.error("Error running Topology Ingestion Service", e);
            throw new RuntimeException("Error running Topology Ingestion Service", e);
        }
    }

    public static void main(String[] args) throws Exception {
        enableDebugLoggingForJanusGraphAndDriver();
        logClasspathDiagnostics();
        if (args != null && args.length > 0 && args[0] != null && !args[0].isBlank()) {
            domain = args[0];
        }
        new TopologyIngestionService().start();
    }

    private static void enableDebugLoggingForJanusGraphAndDriver() {
        try {
            org.apache.logging.log4j.core.config.Configurator.setLevel(
                    "org.janusgraph", org.apache.logging.log4j.Level.DEBUG);
            org.apache.logging.log4j.core.config.Configurator.setLevel(
                    "com.datastax.oss.driver", org.apache.logging.log4j.Level.DEBUG);
            org.apache.logging.log4j.core.config.Configurator.setLevel(
                    "com.enttribe.shaded.datastax.oss.driver", org.apache.logging.log4j.Level.DEBUG);
            logger.info("DEBUG logging enabled for org.janusgraph and DataStax driver packages");
        } catch (Throwable t) {
            logger.warn("Failed to enable DEBUG logging: {}", t.getMessage());
        }
    }

    private static void logClasspathDiagnostics() {
        try {
            String cls = "org.janusgraph.diskstorage.cql.CQLStoreManager";
            String location;
            boolean loadable;
            try {
                Class<?> c = Class.forName(cls);
                loadable = true;
                java.security.CodeSource src = c.getProtectionDomain().getCodeSource();
                location = src != null && src.getLocation() != null
                        ? src.getLocation().toString() : "<unknown>";
            } catch (Throwable t) {
                loadable = false;
                location = t.getClass().getSimpleName() + ": " + t.getMessage();
            }
            logger.info("CLASSPATH DIAG | host={} | CQLStoreManager loadable={} | from={}",
                    System.getenv().getOrDefault("HOSTNAME", "?"), loadable, location);
            logger.info("CLASSPATH DIAG | java.class.path={}", System.getProperty("java.class.path"));
        } catch (Throwable t) {
            logger.warn("CLASSPATH DIAG failed: {}", t.getMessage());
        }
    }
}
