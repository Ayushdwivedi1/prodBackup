package com.enttribe.topology.ingestion.utils;

import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.ResultSet;

/**
 * Opens a JanusGraph instance using the same convention as the topology
 * service: read a JanusGraph {@code .properties} file from
 * {@code TOPOLOGY_PROPERTIES} (env var, {@code config.properties}, or the
 * bundled {@link #TOPOLOGY_PROPERTIES}), inject a unique
 * {@code graph.unique-instance-id}, then call {@code JanusGraphFactory.open()}.
 */
public final class GraphInitializer {

    private static final Logger logger = LoggerFactory.getLogger(GraphInitializer.class);

    /** Classpath resource and default value for {@code TOPOLOGY_PROPERTIES}. */
    public static final String TOPOLOGY_PROPERTIES = "topology.properties";

    private GraphInitializer() {
    }

    public static JanusGraph open() {
        String topologyPropertiesPath = ConfigLoader.get("TOPOLOGY_PROPERTIES", TOPOLOGY_PROPERTIES);
        return open(topologyPropertiesPath);
    }

    public static JanusGraph open(String topologyPropertiesPath) {
        Properties properties = loadJanusGraphProperties(topologyPropertiesPath);

        selfTestRuntimeClasses();
        preCheckCassandraConnection(properties);

        String uniqueInstanceId = buildRuntimeUniqueInstanceId();
        properties.setProperty("graph.unique-instance-id", uniqueInstanceId);
        logger.info("Opening JanusGraph with instance-id: {}", uniqueInstanceId);

        try {
            JanusGraphFactory.Builder builder = JanusGraphFactory.build();
            for (String key : properties.stringPropertyNames()) {
                builder.set(key, properties.getProperty(key));
            }
            JanusGraph graph = builder.open();
            logger.info("JanusGraph opened successfully from {}", topologyPropertiesPath);
            return graph;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to open JanusGraph", e);
        }
    }

    private static Properties loadJanusGraphProperties(String topologyPropertiesPath) {
        Properties properties = new Properties();
        Path sourcePath = Paths.get(topologyPropertiesPath);
        if (Files.exists(sourcePath)) {
            try (InputStream inputStream = Files.newInputStream(sourcePath)) {
                properties.load(inputStream);
            } catch (IOException e) {
                throw new IllegalStateException(
                        "Failed to read JanusGraph properties from: " + topologyPropertiesPath, e);
            }
            return properties;
        }

        try (InputStream inputStream = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream(topologyPropertiesPath)) {
            if (inputStream == null) {
                throw new IllegalStateException(
                        "TOPOLOGY_PROPERTIES does not point to a readable file or classpath resource: "
                                + topologyPropertiesPath);
            }
            properties.load(inputStream);
        } catch (IOException e) {
            throw new IllegalStateException(
                    "Failed to read JanusGraph properties from classpath: " + topologyPropertiesPath, e);
        }
        return properties;
    }

    private static void selfTestRuntimeClasses() {
        logger.info("===== CLASSPATH SELF-TEST =====");
        String[] testClasses = {
                "io.vavr.collection.Iterator",
                "io.vavr.Tuple",
                "org.janusgraph.diskstorage.cql.CQLStoreManager",
                "org.janusgraph.diskstorage.cql.function.mutate.AbstractCQLMutateManyFunction",
                "com.datastax.oss.driver.api.core.CqlSession",
        };
        for (String cls : testClasses) {
            try {
                Class<?> c = Class.forName(cls);
                String src = "<unknown>";
                if (c.getProtectionDomain() != null && c.getProtectionDomain().getCodeSource() != null
                        && c.getProtectionDomain().getCodeSource().getLocation() != null) {
                    src = c.getProtectionDomain().getCodeSource().getLocation().toString();
                }
                logger.info("CLASS-TEST | {} -> OK | classloader={} | from={}",
                        cls, c.getClassLoader(), src);
            } catch (Throwable t) {
                logger.error("CLASS-TEST | {} -> {} : {}", cls, t.getClass().getName(), t.getMessage());
            }
        }
        logger.info("CLASS-TEST | TCCL={} | AppCL={}",
                Thread.currentThread().getContextClassLoader(),
                ClassLoader.getSystemClassLoader());
        logger.info("===== CLASSPATH SELF-TEST DONE =====");
    }

    private static void preCheckCassandraConnection(Properties properties) {
        String hostnameProp = properties.getProperty("storage.hostname", "");
        int port = Integer.parseInt(properties.getProperty("storage.port", "9042"));
        String dc = properties.getProperty("storage.cql.local-datacenter");
        String username = properties.getProperty("storage.username");
        String password = properties.getProperty("storage.password");
        String keyspace = properties.getProperty("storage.cql.keyspace");

        logger.info("===== CASSANDRA PRE-CHECK =====");
        logger.info("PRECHECK | hostnames='{}' port={} dc='{}' user='{}' keyspace='{}'",
                hostnameProp, port, dc, username, keyspace);

        CqlSessionBuilder builder = CqlSession.builder().withLocalDatacenter(dc);
        for (String h : hostnameProp.split(",")) {
            String host = h.trim();
            if (!host.isEmpty()) {
                builder.addContactPoint(new InetSocketAddress(host, port));
            }
        }
        if (username != null && !username.isEmpty()) {
            builder.withAuthCredentials(username, password == null ? "" : password);
        }

        try (CqlSession session = builder.build()) {
            ResultSet rs = session.execute("SELECT cluster_name FROM system.local");
            String cluster = rs.one().getString("cluster_name");
            logger.info("PRECHECK | connected to cluster: {}", cluster);

            ResultSet ks = session.execute("SELECT keyspace_name FROM system_schema.keyspaces");
            List<String> keyspaces = new ArrayList<>();
            ks.forEach(row -> keyspaces.add(row.getString("keyspace_name")));
            logger.info("PRECHECK | available keyspaces: {}", keyspaces);
            logger.info("PRECHECK | target keyspace '{}' exists: {}", keyspace, keyspaces.contains(keyspace));

            ResultSet dcs = session.execute("SELECT data_center FROM system.local");
            String actualDc = dcs.one().getString("data_center");
            logger.info("PRECHECK | local node reports data_center='{}', we configured local-datacenter='{}'",
                    actualDc, dc);

            ResultSet peers = session.execute("SELECT data_center, rpc_address FROM system.peers");
            List<String> peerInfo = new ArrayList<>();
            peers.forEach(row -> peerInfo.add(row.getString("data_center") + "@" + row.getInetAddress("rpc_address")));
            logger.info("PRECHECK | peers: {}", peerInfo);

            logger.info("===== CASSANDRA PRE-CHECK PASSED =====");
        } catch (Throwable t) {
            logger.error("===== CASSANDRA PRE-CHECK FAILED =====", t);
            Throwable cur = t;
            int depth = 0;
            while (cur != null && depth < 10) {
                logger.error("PRECHECK cause[{}] {}: {}", depth, cur.getClass().getName(), cur.getMessage());
                cur = cur.getCause();
                depth++;
            }
        }
    }

    private static String buildRuntimeUniqueInstanceId() {
        String hostName = System.getenv("HOSTNAME");
        if (hostName == null || hostName.isBlank()) {
            hostName = "topology-ingestion-spark-job";
        }
        return hostName + "-" + UUID.randomUUID();
    }

    public static void close(JanusGraph graph) {
        if (graph == null) return;
        try {
            if (graph.isOpen()) {
                graph.close();
                logger.info("JanusGraph closed");
            }
        } catch (Exception e) {
            logger.error("Error closing JanusGraph", e);
        }
    }
}
