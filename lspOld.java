package com.enttribe.custom.processor;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.*;
import java.util.*;

public class ProcessorRailtelProduction42 implements Runnable {

    private static final Logger logger =  LoggerFactory.getLogger(ProcessorRailtelProduction42.class);

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFile Successfully Processed!")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFile Failed to Process!")
            .build();

    @Override
    public void run() {
    }

    // Replace the existing runSnippet(...) method with this version
    public FlowFile runSnippet(FlowFile flowFile, ProcessSession session, ProcessContext context,
            Connection connection, String cacheValue) throws SQLException {

        if (flowFile == null)
            return null;

        Connection connectionNew = null;
        try {
            connectionNew = ensureOpen(connection);

            connectionNew = ensureOpen(connectionNew);
            boolean neighbourInterfaceLldpSuccess = executeStage("neighbourInterfaceForLLDP", getNeighbourInterfaceForLLDP(), flowFile, session,
                    connectionNew);
            if (!neighbourInterfaceLldpSuccess)
                return flowFile;

            connectionNew = ensureOpen(connectionNew);
            boolean childInterfaceLldpSuccess = executeStage("childInterfaceForLLDP", getchildInterfaceForLLDP(), flowFile, session, connectionNew);
            if (!childInterfaceLldpSuccess)
                return flowFile;

            // All success
            flowFile = session.putAttribute(flowFile, "topology.status", "ALL_SUCCESS");
            session.transfer(flowFile, REL_SUCCESS);

        } catch (Exception e) {
            logger.error("Exception in DwdmProcessor: {}", e.getMessage(), e);
            flowFile = session.putAttribute(flowFile, "topology.status", "EXCEPTION");
            flowFile = session.putAttribute(flowFile, "topology.error", e.getMessage());
            session.transfer(flowFile, REL_FAILURE);
        } finally {
            try {
                if (connectionNew != null && !connectionNew.isClosed()) {
                    connectionNew.close();
                }
            } catch (SQLException ignore) {
            }
        }
        return flowFile;
    }

    // Add inside class
    private Connection ensureOpen(Connection current) {
        try {
            if (current == null || current.isClosed()) {
                return getJDBCConnection(current);
            }
            return current;
        } catch (SQLException e) {
            return getJDBCConnection(current);
        }
    }

    private static Connection getJDBCConnection(Connection connection) {
        String jdbcUrl = "jdbc:mysql://mysql-platform-cluster.platformdb.svc.cluster.local:6446/LCM?autoReconnect=true&permitMysqlScheme=true";
        String username = "LCM";
        String password = "lcm%lcm2";
        try {
            return DriverManager.getConnection(jdbcUrl, username, password);
        } catch (SQLException e) {
            return connection;
        }
    }

    private boolean executeStage(String stageName, String query, FlowFile flowFile,
            ProcessSession session, Connection connection) {

        Statement stmt = null;
        ResultSet rs = null;

        try {
            stmt = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setFetchSize(1000);
            rs = stmt.executeQuery(query);

            List<Map<String, Object>> dataList = getListOfMapFromResultSet(rs);
            flowFile = session.putAttribute(flowFile, stageName + ".record.count", String.valueOf(dataList.size()));

            if (dataList.isEmpty()) {
                flowFile = session.putAttribute(flowFile, stageName + ".status", "NO_RECORDS");
                session.transfer(flowFile, REL_FAILURE);
                logger.warn("{} stage returned 0 records.", stageName);
                return false;
            }

            // Batch send 1000
            ObjectMapper mapper = new ObjectMapper();
            int batchSize = 1000;
            for (int i = 0; i < dataList.size(); i += batchSize) {
                int end = Math.min(i + batchSize, dataList.size());
                List<Map<String, Object>> batch = dataList.subList(i, end);

                String payload = mapper.writeValueAsString(batch);
                logger.info("{} stage batch {} - {} records", stageName, (i / batchSize) + 1, batch.size());

                boolean apiSuccess = callAPI(payload);
                if (!apiSuccess) {
                    flowFile = session.putAttribute(flowFile, stageName + ".status", "API_FAILED");
                    session.transfer(flowFile, REL_FAILURE);
                    return false;
                }
            }

            flowFile = session.putAttribute(flowFile, stageName + ".status", "SUCCESS");
            return true;

        } catch (Exception e) {
            logger.error("{} stage exception: {}", stageName, e.getMessage(), e);
            flowFile = session.putAttribute(flowFile, stageName + ".status", "EXCEPTION");
            flowFile = session.putAttribute(flowFile, stageName + ".error", e.getMessage());
            session.transfer(flowFile, REL_FAILURE);
            return false;

        } finally {
            try {
                if (rs != null)
                    rs.close();
            } catch (SQLException ignore) {
            }
            try {
                if (stmt != null)
                    stmt.close();
            } catch (SQLException ignore) {
            }
        }
    }

    private boolean callAPI(String payload) {
        try {
            String apiUrl = "http://topology-service/topology/graphdb/createMultipleVertices";
            HttpClient client = HttpClient.newHttpClient();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(apiUrl))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(payload))
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            logger.info("API Response Code: {}, Body: {}", response.statusCode(), response.body());
            return response.statusCode() == 200;

        } catch (Exception e) {
            logger.error("API call exception: {}", e.getMessage(), e);
            return false;
        }
    }

    private List<Map<String, Object>> getListOfMapFromResultSet(ResultSet resultSet) throws SQLException {
        List<Map<String, Object>> list = new ArrayList<>();
        ResultSetMetaData metaData = resultSet.getMetaData();
        int cols = metaData.getColumnCount();
    
        while (resultSet.next()) {
            Map<String, Object> map = new LinkedHashMap<>();
    
            for (int i = 1; i <= cols; i++) {
                String columnName = metaData.getColumnLabel(i);
                Object value = resultSet.getObject(i);
    
                // Only convert lastDiscoveryDateTime
                if ("lastDiscoveryDateTime".equalsIgnoreCase(columnName)) {
                    Timestamp ts = resultSet.getTimestamp(i);
                    if (ts != null) {
                        String formatted = ts.toLocalDateTime().toString().replace("T", " ");
                        map.put(columnName, formatted);
                    } else {
                        map.put(columnName, null);
                    }
                } else {
                    map.put(columnName, value);
                }
            }
    
            list.add(map);
        }
    
        return list;
    }

    private String getNeighbourInterfaceForLLDP() { return "SELECT DISTINCT 'LSP' AS relation, src.ID AS id, src.NE_NAME AS neName, src.NE_TYPE AS neType, l.MODIFICATION_TIME AS lspModifyTime, src.LATITUDE AS latitude, src.LONGITUDE AS longitude, src.BANDWIDTH AS bandwidth, src.NE_SOURCE AS neSource, src.NE_STATUS AS neStatus, src.CKT_ID AS cktId, src.IS_DELETED AS isDeleted, src.NE_ID AS neId, src.PM_EMS_ID AS pmEmsId, src.FM_EMS_ID AS fmEmsId, src.CM_EMS_ID AS cmEmsId, src.IPV4 AS ipv4, src.IPV6 AS ipv6, src.MODEL AS model, src.MAC_ADDRESS AS macAddress, src.FRIENDLY_NAME AS friendlyName, src.UUID AS uuid, src.SERIAL_NUMBER AS serialNumber, parentSrc.NE_NAME AS parentNE FROM LSP l JOIN LSP_HOP h ON h.LSP_ID = l.ID JOIN NETWORK_ELEMENT src ON h.INTERFACE_NE_ID = src.ID LEFT JOIN NETWORK_ELEMENT parentSrc ON src.PARENT_NE_ID_FK = parentSrc.ID WHERE l.IS_DELETED = 0 AND h.IS_DELETED = 0 AND (parentSrc.IS_DELETED = 0 OR parentSrc.IS_DELETED IS NULL) AND (parentSrc.NE_TYPE IN ('ROUTER') OR parentSrc.NE_CATEGORY IN ('ROUTER'))"; }

    private String getchildInterfaceForLLDP() { return "SELECT 'LSP' AS relation, l.LSP_NAME AS lspName, h1.LSP_ID AS lspId, h1.HOP_SEQUENCE AS hopSequence, src.ID AS id, src.NE_NAME AS neName, src.LATITUDE AS latitude, src.LONGITUDE AS longitude, src.BANDWIDTH AS srcBandwidth, dest.BANDWIDTH AS destBandwidth, src.NE_SOURCE AS neSource, src.NE_STATUS AS neStatus, parentSrc.NE_TYPE AS srcType, parentDest.NE_TYPE AS destType, src.NE_TYPE AS neType, parentSrc.NE_CATEGORY AS srcCategory, parentDest.NE_CATEGORY AS destCategory, parentSrc.VENDOR AS srcVendor, parentDest.VENDOR AS destVendor, src.CKT_ID AS cktId, src.IS_DELETED AS isDeleted, parentSrc.TECHNOLOGY AS srcTechnology, parentDest.TECHNOLOGY AS destTechnology, parentSrc.DOMAIN AS srcDomain, parentDest.DOMAIN AS destDomain, src.NE_ID AS neId, src.PM_EMS_ID AS pmEmsId, src.FM_EMS_ID AS fmEmsId, src.CM_EMS_ID AS cmEmsId, src.IPV4 AS ipv4, src.IPV6 AS ipv6, src.MODEL AS model, src.MAC_ADDRESS AS macAddress, src.FRIENDLY_NAME AS friendlyName, src.UUID AS uuid, src.SERIAL_NUMBER AS serialNumber, dest.NE_ID AS neighbourNEId, dest.NE_NAME AS neighbourNEName, dest.LATITUDE AS neighbourNELat, dest.LONGITUDE AS neighbourNELong, parentSrc.NE_NAME AS parentNE, parentDest.NE_NAME AS neighbourParentNEName, parentDest.LONGITUDE AS neighbourParentNELong, parentDest.LATITUDE AS neighbourParentNELat FROM LSP l JOIN LSP_HOP h1 ON h1.LSP_ID = l.ID JOIN LSP_HOP h2 ON h2.LSP_ID = l.ID AND h2.HOP_SEQUENCE = h1.HOP_SEQUENCE + 1 JOIN NETWORK_ELEMENT src ON h1.INTERFACE_NE_ID = src.ID JOIN NETWORK_ELEMENT dest ON h2.INTERFACE_NE_ID = dest.ID LEFT JOIN NETWORK_ELEMENT parentSrc ON src.PARENT_NE_ID_FK = parentSrc.ID LEFT JOIN NETWORK_ELEMENT parentDest ON dest.PARENT_NE_ID_FK = parentDest.ID WHERE l.IS_DELETED = 0 AND h1.IS_DELETED = 0 AND h2.IS_DELETED = 0 AND src.IS_DELETED = 0 AND dest.IS_DELETED = 0"; }    
}
