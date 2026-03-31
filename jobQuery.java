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

public class ProcessorRailtelProduction3033 implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(ProcessorRailtelProduction3033.class);

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
            boolean parentVertexSuccess = executeStage("parentVertexForAll", getParentVertexForAll(), flowFile, session, connectionNew);
            if (!parentVertexSuccess)
                return flowFile;

            connectionNew = ensureOpen(connectionNew);
            boolean neighbourInterfaceLldpSuccess = executeStage("neighbourInterfaceForLLDP", getNeighbourInterfaceForLLDP(), flowFile, session,
                    connectionNew);
            if (!neighbourInterfaceLldpSuccess)
                return flowFile;

            connectionNew = ensureOpen(connectionNew);
            boolean childInterfaceLldpSuccess = executeStage("childInterfaceForLLDP", getchildInterfaceForLLDP(), flowFile, session, connectionNew);
            if (!childInterfaceLldpSuccess)
                return flowFile;

            connectionNew = ensureOpen(connectionNew);
            boolean neighbourInterfaceOspfSuccess = executeStage("neighbourInterfaceForOSPF", getNeighbourInterfaceForOSPF(), flowFile, session, connectionNew);
            if (!neighbourInterfaceOspfSuccess)
                return flowFile;
                
            connectionNew = ensureOpen(connectionNew);
            boolean childInterfaceOspfSuccess = executeStage("childInterfaceForOSPF", getchildInterfaceForOSPF(), flowFile, session, connectionNew);
            if (!childInterfaceOspfSuccess)
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
                session.transfer(flowFile, REL_SUCCESS);
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

    // SQL queries of DWDM AND ROUTER
    private String getParentVertexForAll() {return "SELECT 'VERTEX' AS relation, ne.ID AS id, ne.NE_NAME AS neName, ne.SOURCE AS source , ne.MODIFIED_TIME AS neModifyTime, ne.LAST_DISCOVERY_TIME AS lastDiscoveryDateTime, ne.LATITUDE AS latitude, ne.LONGITUDE AS longitude, ne.NE_SOURCE AS neSource, ne.NE_STATUS AS neStatus, ne.NE_TYPE AS neType, ne.NE_CATEGORY AS neCategory, ne.VENDOR AS vendor, ne.CKT_ID AS cktId, ne.IS_DELETED AS isDeleted, ne.TECHNOLOGY AS technology, pop.NEL_ID AS popId, pop.NEL_CODE AS popCode, pop.FRIENDLY_NAME AS popName, ne.DOMAIN AS domain, ne.NE_ID AS neId, ne.PM_EMS_ID AS pmEmsId, ne.FM_EMS_ID AS fmEmsId, ne.CM_EMS_ID AS cmEmsId, ne.IPV4 AS ipv4, ne.IPV6 AS ipv6, ne.MODEL AS model, ne.MAC_ADDRESS AS macAddress, ne.FRIENDLY_NAME AS friendlyName, ne.HOST_NAME AS hostname, ne.UUID AS uuid, ne.SERIAL_NUMBER AS serialNumber, l1.GEO_NAME AS geographyL1Name, l2.GEO_NAME AS geographyL2Name, l3.GEO_NAME AS geographyL3Name, l4.GEO_NAME AS geographyL4Name, neighbourNE.NE_ID AS neighbourNEId, neighbourNE.NE_NAME AS neighbourNEName, neighbourNE.LATITUDE AS neighbourNELat, neighbourNE.LONGITUDE AS neighbourNELong, parentNE.NE_NAME AS parentNE, neighbourParentNE.NE_NAME AS neighbourParentNEName, neighbourParentNE.LONGITUDE AS neighbourParentNELong, neighbourParentNE.LATITUDE AS neighbourParentNELat FROM NETWORK_ELEMENT ne LEFT JOIN NE_LOCATION pop ON ne.NE_LOCATION_ID_FK = pop.ID LEFT JOIN PRIMARY_GEO_L1 l1 ON pop.GEOGRAPHY_L1_ID_FK = l1.id LEFT JOIN PRIMARY_GEO_L2 l2 ON pop.GEOGRAPHY_L2_ID_FK = l2.id LEFT JOIN PRIMARY_GEO_L3 l3 ON pop.GEOGRAPHY_L3_ID_FK = l3.id LEFT JOIN PRIMARY_GEO_L4 l4 ON pop.GEOGRAPHY_L4_ID_FK = l4.id LEFT JOIN NETWORK_ELEMENT parentNE ON ne.PARENT_NE_ID_FK = parentNE.ID LEFT JOIN NETWORK_ELEMENT neighbourNE ON ne.NETWORK_ELEMENT_ID_FK = neighbourNE.ID LEFT JOIN NETWORK_ELEMENT neighbourParentNE ON neighbourNE.PARENT_NE_ID_FK = neighbourParentNE.ID WHERE ne.IS_DELETED = 0 AND DATE(ne.LAST_DISCOVERY_TIME) = CURRENT_DATE AND ne.NE_TYPE IN ('GNE','ILA','MDWDM','NIC','NODE','OADM','OTN','ROUTER') AND ne.NE_CATEGORY IN ('GATEWAY_INTERFACE','OPTICAL_AMPLIFIERS','DWDM','OTN','NODE','ROUTER')";}

    private String getNeighbourInterfaceForLLDP() { return "SELECT DISTINCT 'LLDP' AS relation, src.ID AS id, src.NE_NAME AS neName, src.NE_TYPE AS neType, l.MODIFIED_TIME AS lldpModifyTime, src.MODIFIED_TIME AS lldpInterfaceModifyTime, l.LAST_DISCOVERY_TIME AS lastDiscoveryDateTime, src.LATITUDE AS latitude, src.LONGITUDE AS longitude, src.CAPACITY AS bandwidth, src.NE_SOURCE AS neSource, src.NE_STATUS AS neStatus, src.CKT_ID AS cktId, src.IS_DELETED AS isDeleted, src.NE_ID AS neId, src.PM_EMS_ID AS pmEmsId, src.FM_EMS_ID AS fmEmsId, src.CM_EMS_ID AS cmEmsId, src.IPV4 AS ipv4, src.IPV6 AS ipv6, src.MODEL AS model, src.MAC_ADDRESS AS macAddress, src.FRIENDLY_NAME AS friendlyName, l.LLDP_LINK_NAME AS hostname, src.UUID AS uuid, src.SERIAL_NUMBER AS serialNumber, parentSrc.NE_NAME AS parentNE FROM LLDP_LINK l JOIN NETWORK_ELEMENT src ON l.SOURCE_INTERFACE_NE_ID = src.ID or l.DESTINATION_INTERFACE_NE_ID = src.ID LEFT JOIN NETWORK_ELEMENT parentSrc ON src.PARENT_NE_ID_FK = parentSrc.ID WHERE parentSrc.IS_DELETED = 0 AND DATE(l.LAST_DISCOVERY_TIME) = CURRENT_DATE AND (parentSrc.NE_TYPE IN ('ROUTER') OR parentSrc.NE_CATEGORY IN ('ROUTER'))"; }

    private String getchildInterfaceForLLDP() { return "SELECT 'LLDP' AS relation, src.ID AS id, src.NE_NAME AS neName, link.MODIFIED_TIME AS lldpModifyTime, src.MODIFIED_TIME AS lldpInterfaceModifyTime, link.LAST_DISCOVERY_TIME AS lastDiscoveryDateTime, parentSrc.LATITUDE AS latitude, parentSrc.LONGITUDE AS longitude, src.CAPACITY AS srcBandwidth, dest.CAPACITY AS destBandwidth, src.NE_SOURCE AS neSource, src.NE_STATUS AS neStatus, parentSrc.NE_TYPE AS srcType, parentDest.NE_TYPE AS destType, src.NE_TYPE AS neType, parentSrc.NE_CATEGORY AS srcCategory, parentDest.NE_CATEGORY AS destCategory, parentSrc.VENDOR AS srcVendor, parentDest.VENDOR AS destVendor, src.CKT_ID AS cktId, src.IS_DELETED AS isDeleted, parentSrc.TECHNOLOGY AS srcTechnology, parentDest.TECHNOLOGY AS destTechnology, parentSrc.DOMAIN AS srcDomain, parentDest.DOMAIN AS destDomain, src.NE_ID AS neId, src.PM_EMS_ID AS pmEmsId, src.FM_EMS_ID AS fmEmsId, src.CM_EMS_ID AS cmEmsId, src.IPV4 AS ipv4, src.IPV6 AS ipv6, src.MODEL AS model, src.MAC_ADDRESS AS macAddress, src.FRIENDLY_NAME AS friendlyName, link.LLDP_LINK_NAME AS hostname, src.UUID AS uuid, src.SERIAL_NUMBER AS serialNumber, dest.NE_ID AS neighbourNEId, dest.NE_NAME AS neighbourNEName, dest.LATITUDE AS neighbourNELat, dest.LONGITUDE AS neighbourNELong, parentSrc.NE_NAME AS parentNE, parentDest.NE_NAME AS neighbourParentNEName, parentDest.LONGITUDE AS neighbourParentNELong, parentDest.LATITUDE AS neighbourParentNELat FROM LLDP_LINK link JOIN NETWORK_ELEMENT src ON src.ID = link.SOURCE_INTERFACE_NE_ID JOIN NETWORK_ELEMENT dest ON dest.ID = link.DESTINATION_INTERFACE_NE_ID LEFT JOIN NETWORK_ELEMENT parentSrc ON src.PARENT_NE_ID_FK = parentSrc.ID LEFT JOIN NETWORK_ELEMENT parentDest ON dest.PARENT_NE_ID_FK = parentDest.ID WHERE link.IS_DELETED = 0 AND DATE(link.LAST_DISCOVERY_TIME) = CURRENT_DATE AND src.IS_DELETED = 0 AND dest.IS_DELETED = 0"; }

    private String getNeighbourInterfaceForOSPF() { return "SELECT DISTINCT 'OSPF' AS relation, src.ID AS id, src.NE_NAME AS neName, src.NE_TYPE AS neType, l.MODIFIED_TIME AS ospfModifyTime, src.MODIFIED_TIME AS ospfInterfaceModifyTime, l.LAST_DISCOVERY_TIME AS lastDiscoveryDateTime, src.LATITUDE AS latitude, src.LONGITUDE AS longitude, src.CAPACITY AS bandwidth, src.NE_SOURCE AS neSource, src.NE_STATUS AS neStatus, src.CKT_ID AS cktId, src.IS_DELETED AS isDeleted, src.NE_ID AS neId, src.PM_EMS_ID AS pmEmsId, src.FM_EMS_ID AS fmEmsId, src.CM_EMS_ID AS cmEmsId, src.IPV4 AS ipv4, src.IPV6 AS ipv6, src.MODEL AS model, src.MAC_ADDRESS AS macAddress, src.FRIENDLY_NAME AS friendlyName, src.UUID AS uuid, src.SERIAL_NUMBER AS serialNumber, parentSrc.NE_NAME AS parentNE FROM OSPF_LINK l JOIN NETWORK_ELEMENT src ON l.SOURCE_INTERFACE_NE_ID = src.ID or l.DESTINATION_INTERFACE_NE_ID = src.ID LEFT JOIN NETWORK_ELEMENT parentSrc ON src.PARENT_NE_ID_FK = parentSrc.ID WHERE parentSrc.IS_DELETED = 0 AND DATE(l.LAST_DISCOVERY_TIME) = CURRENT_DATE AND (parentSrc.NE_TYPE IN ('ROUTER') OR parentSrc.NE_CATEGORY IN ('ROUTER'))"; }

    private String getchildInterfaceForOSPF() { return "SELECT DISTINCT 'OSPF' AS relation, ospf.CKT_ID AS cktId, ne.ID AS id, ne.NE_NAME AS neName, COALESCE(src.LATITUDE, ne.LATITUDE) AS latitude, COALESCE(src.LONGITUDE, ne.LONGITUDE) AS longitude, ne.NE_TYPE AS neType, ospf.TAG AS tag, ne.MODIFIED_TIME AS ospfInterfaceModifyTime, ospf.MODIFIED_TIME AS ospfModifyTime, ospf.LAST_DISCOVERY_TIME AS lastDiscoveryDateTime, ne.CAPACITY AS srcBandwidth, neighbourNE.CAPACITY AS destBandwidth, ospf.AREA_IP_ADDRESS AS areaIpAddress, ne.NE_SOURCE AS neSource, ne.NE_STATUS AS neStatus, COALESCE(src.NE_TYPE, ne.NE_TYPE) AS srcType, COALESCE(rem.NE_TYPE, neighbourNE.NE_TYPE) AS destType, COALESCE(src.NE_CATEGORY, ne.NE_CATEGORY) AS srcCategory, COALESCE(rem.NE_CATEGORY, neighbourNE.NE_CATEGORY) AS destCategory, COALESCE(src.VENDOR, ne.VENDOR) AS srcVendor, COALESCE(rem.VENDOR, neighbourNE.VENDOR) AS destVendor, ne.IS_DELETED AS isDeleted, COALESCE(src.TECHNOLOGY, ne.TECHNOLOGY) AS srcTechnology, COALESCE(rem.TECHNOLOGY, neighbourNE.TECHNOLOGY) AS destTechnology, COALESCE(src.DOMAIN, ne.DOMAIN) AS srcDomain, COALESCE(rem.DOMAIN, neighbourNE.DOMAIN) AS destDomain, ne.NE_ID AS neId, ne.PM_EMS_ID AS pmEmsId, ne.FM_EMS_ID AS fmEmsId, ne.CM_EMS_ID AS cmEmsId, ne.IPV4 AS ipv4, ne.IPV6 AS ipv6, ne.MODEL AS model, ne.MAC_ADDRESS AS macAddress, ne.FRIENDLY_NAME AS friendlyName, ne.HOST_NAME AS hostname, ne.UUID AS uuid, ne.SERIAL_NUMBER AS serialNumber, neighbourNE.NE_ID AS neighbourNEId, neighbourNE.NE_NAME AS neighbourNEName, COALESCE(neighbourNE.LATITUDE, rem.LATITUDE) AS neighbourNELat, COALESCE(neighbourNE.LONGITUDE, rem.LONGITUDE) AS neighbourNELong, COALESCE(src.NE_NAME, ne.NE_NAME) AS parentNE, COALESCE(rem.NE_NAME, neighbourNE.NE_NAME) AS neighbourParentNEName, COALESCE(rem.LONGITUDE, neighbourNE.longitude) AS neighbourParentNELong, COALESCE(rem.LATITUDE, neighbourNE.LATITUDE) AS neighbourParentNELat FROM OSPF_LINK ospf INNER JOIN NETWORK_ELEMENT src ON src.ID = ospf.SOURCE_NE_ID INNER JOIN NETWORK_ELEMENT rem ON rem.ID = ospf.DESTINATION_NE_ID LEFT JOIN NETWORK_ELEMENT ne ON ne.ID = ospf.SOURCE_INTERFACE_NE_ID LEFT JOIN NETWORK_ELEMENT neighbourNE ON neighbourNE.ID = ospf.DESTINATION_INTERFACE_NE_ID WHERE ne.IS_DELETED = 0 AND DATE(ospf.LAST_DISCOVERY_TIME) = CURRENT_DATE AND neighbourNE.IS_DELETED = 0 AND ne.NE_TYPE = 'INTERFACE' AND neighbourNE.NE_TYPE = 'INTERFACE' ORDER BY src.ID ASC";
    }
}
