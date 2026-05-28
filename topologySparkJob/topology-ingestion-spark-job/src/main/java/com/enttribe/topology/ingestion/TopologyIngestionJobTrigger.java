package com.enttribe.topology.ingestion;

import com.enttribe.topology.ingestion.core.TopologyIngestionJob;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopologyIngestionJobTrigger {

    private static final Logger logger = LoggerFactory.getLogger(TopologyIngestionJobTrigger.class);

    public static void main(String[] args) {
        setLog4jPropertiesForSpark();
        logClasspathDiagnostics();
        SparkConf conf = getSparkConf();
        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();

        try {
            boolean success = TopologyIngestionJob.startTopologyIngestionJob(session, "TOPOLOGY");
            if (success) {
                logger.info("Topology ingestion job completed successfully");
            } else {
                logger.warn("Topology ingestion job completed with one or more failed stages");
            }
        } catch (Exception e) {
            logger.error("Topology ingestion job execution failed", e);
            throw new RuntimeException("Topology ingestion job execution failed", e);
        } finally {
            try {
                if (session != null) {
                    logger.info("Stopping Spark session...");
                    try {
                        org.apache.spark.SparkContext sc = session.sparkContext();
                        if (sc != null && !sc.isStopped()) {
                            sc.stop();
                            logger.info("SparkContext stopped");
                        }
                    } catch (IllegalStateException e) {
                        logger.debug("SparkContext already stopped: {}", e.getMessage());
                    } catch (Exception e) {
                        logger.debug("Error stopping SparkContext: {}", e.getMessage());
                    }
                    try {
                        org.apache.spark.SparkContext sc = session.sparkContext();
                        if (sc != null && !sc.isStopped()) {
                            session.stop();
                            logger.info("Spark session stopped successfully");
                        }
                    } catch (IllegalStateException e) {
                        logger.debug("Spark session already stopped: {}", e.getMessage());
                    } catch (Exception e) {
                        logger.error("Error stopping Spark session", e);
                        try {
                            session.stop();
                        } catch (Exception ignore) {
                        }
                    }
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            } catch (Exception e) {
                logger.error("Error during Spark session shutdown", e);
            }
            logger.info("Application shutdown complete");
            System.exit(0);
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

    private static void setLog4jPropertiesForSpark() {
        System.setProperty("log4j.logger.org.apache.spark.scheduler.TaskSetManager", "WARN");
        System.setProperty("log4j.logger.org.apache.spark.executor.Executor", "WARN");
        System.setProperty("log4j.logger.org.apache.spark.storage.BlockManager", "WARN");
        System.setProperty("log4j.logger.org.apache.spark.scheduler.TaskSchedulerImpl", "WARN");
        System.setProperty("log4j.logger.org.apache.spark.scheduler.DAGScheduler", "WARN");
        System.setProperty("log4j.logger.org.apache.spark.storage.BlockManagerMaster", "WARN");
    }

    private static SparkConf getSparkConf() {
        System.setProperty("user.timezone", "UTC");

        // Driver-only workload: queries + HTTP POSTs run in the driver.
        // Sized small and predictable; the topology service is the bottleneck, not Spark.
        return new SparkConf()
                .setAppName("TopologyIngestionJobTrigger")
                .setMaster("local[*]")
                .set("spark.driver.memory", "2g")
                .set("spark.driver.maxResultSize", "512m")
                .set("spark.memory.fraction", "0.6")
                .set("spark.memory.storageFraction", "0.3")
                .set("spark.sql.shuffle.partitions", "8")
                .set("spark.default.parallelism", "8")
                .set("spark.sql.adaptive.enabled", "true")
                .set("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .set("spark.sql.codegen.wholeStage", "false")
                .set("spark.local.dir", System.getProperty("java.io.tmpdir") + "/spark-local-dir")
                .set("spark.network.timeout", "600s")
                .set("spark.eventLog.enabled", "false")
                .set("spark.ui.enabled", "true")
                .set("spark.sql.broadcastTimeout", "300")
                .set("spark.task.maxResultSize", "512m")
                .set("spark.driver.extraJavaOptions",
                        "-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35 -XX:ConcGCThreads=2 "
                                + "--add-opens=java.base/java.lang=ALL-UNNAMED "
                                + "--add-opens=java.base/java.io=ALL-UNNAMED "
                                + "--add-opens=java.base/java.util=ALL-UNNAMED "
                                + "--add-opens=java.base/java.nio=ALL-UNNAMED "
                                + "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED")
                .set("spark.executor.extraJavaOptions",
                        "-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35 -XX:ConcGCThreads=2 "
                                + "--add-opens=java.base/java.lang=ALL-UNNAMED "
                                + "--add-opens=java.base/java.io=ALL-UNNAMED "
                                + "--add-opens=java.base/java.util=ALL-UNNAMED "
                                + "--add-opens=java.base/java.nio=ALL-UNNAMED "
                                + "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED")
                .set("spark.sql.session.timeZone", "UTC");
    }
}
