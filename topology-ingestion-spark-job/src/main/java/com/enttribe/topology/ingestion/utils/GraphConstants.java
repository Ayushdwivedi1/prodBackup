package com.enttribe.topology.ingestion.utils;

/**
 * Property and label names used by topology service. These are copied verbatim
 * from {@code AffectedNodesConstants} and {@code JanusGraphUtilsConstants} in
 * the topology backend so the Spark job writes graph data with the exact same
 * keys the service reads.
 *
 * <p>If the service ever renames a property, update it here too — the schema is
 * not auto-validated.
 */
public final class GraphConstants {

    private GraphConstants() {
    }

    // ---- vertex/edge common ----
    public static final String NENAME = "neName";
    public static final String NETYPE = "neType";
    public static final String RELATION = "relation";
    public static final String NE_ID = "neId";
    public static final String PARENT_NE = "parentNE";
    public static final String NEIGHBOUR_NENAME = "neighbourNEName";
    public static final String NEIGHBOUR_NE_ID = "neighbourNEId";
    public static final String NEIGHBOUR_PARENT_NENAME = "neighbourParentNEName";
    public static final String NEIGHBOUR_PARENT_NE_LAT = "neighbourParentNELat";
    public static final String NEIGHBOUR_PARENT_NE_LNG = "neighbourParentNELong";
    public static final String NEIGHBOUR_NE_LAT = "neighbourNELat";
    public static final String NEIGHBOUR_NE_LNG = "neighbourNELong";
    public static final String NEIGHBOUR_VENDOR = "neighbourVendor";
    public static final String NEIGHBOUR_DOMAIN = "neighbourDomain";
    public static final String NEIGHBOUR_TECHNOLOGY = "neighbourTechnology";
    public static final String NEIGHBOUR_TECH = "neighbourTechnology";

    public static final String LAT = "latitude";
    public static final String LNG = "longitude";
    public static final String HOSTNAME = "hostname";
    public static final String CATEGORY = "category";
    public static final String NECATEGORY = "neCategory";
    public static final String NE_STATUS = "neStatus";
    public static final String VENDOR = "vendor";
    public static final String TECHNOLOGY = "technology";
    public static final String DOMAIN = "domain";
    public static final String TYPE = "neType";
    public static final String ISDEL = "isDeleted";

    // ---- edge-only properties ----
    public static final String START = "start";
    public static final String END = "end";
    public static final String STATUS = "status";
    public static final String ACTIVE = "ACTIVE";
    public static final String CREATED_AT = "createdAt";

    public static final String SRC_INTERFACE = "srcInterface";
    public static final String DEST_INTERFACE = "destInterface";
    public static final String SRC_LAT = "srcLat";
    public static final String SRC_LONG = "srcLong";
    public static final String DEST_LAT = "destLat";
    public static final String DEST_LONG = "destLong";
    public static final String SRC_TYPE = "srcType";
    public static final String DEST_TYPE = "destType";
    public static final String SRC_VENDOR = "srcVendor";
    public static final String DEST_VENDOR = "destVendor";
    public static final String SRC_TECHNOLOGY = "srcTechnology";
    public static final String DEST_TECHNOLOGY = "destTechnology";
    public static final String SRC_DOMAIN = "srcDomain";
    public static final String DEST_DOMAIN = "destDomain";
    public static final String SRC_CATEGORY = "srcCategory";
    public static final String DEST_CATEGORY = "destCategory";
    public static final String SRC_BANDWIDTH = "srcBandwidth";
    public static final String DEST_BANDWIDTH = "destBandwidth";

    public static final String START_ID = "startId";
    public static final String END_ID = "endId";
    public static final String START_NEID = "startNeId";
    public static final String END_NEID = "endNeId";

    public static final String LINE_NAME = "lineName";
    public static final String CKTID = "cktId";
    public static final String LINK_TYPE = "linkType";
    public static final String AREA_IP_ADDRESS = "areaIpAddress";
    public static final String TAG = "tag";
    public static final String LSP_NAME = "lspName";
    public static final String HOP_SEQUENCE = "hopSequence";
    public static final String LSP_ID = "lspId";
    public static final String LAST_DISCOVERY_TIME = "lastDiscoveryDateTime";

    // ---- modification-time properties (always tracked when present) ----
    public static final String NE_MODIFY_TIME = "neModifyTime";
    public static final String LLDP_MODIFY_TIME = "lldpModifyTime";
    public static final String OSPF_MODIFY_TIME = "ospfModifyTime";
    public static final String LLDP_INTERFACE_MODIFY_TIME = "lldpInterfaceModifyTime";
    public static final String OSPF_INTERFACE_MODIFY_TIME = "ospfInterfaceModifyTime";

    /**
     * JanusGraph schema stores these as {@code Long} (epoch millis). JDBC returns
     * {@code java.sql.Timestamp} for {@code MODIFIED_TIME} columns — convert at read time.
     */
    public static boolean isEpochMillisProperty(String key) {
        if (key == null) return false;
        switch (key) {
            case NE_MODIFY_TIME:
            case LLDP_MODIFY_TIME:
            case OSPF_MODIFY_TIME:
            case LLDP_INTERFACE_MODIFY_TIME:
            case OSPF_INTERFACE_MODIFY_TIME:
                return true;
            default:
                return false;
        }
    }

    // ---- relation labels ----
    public static final String RELATION_VERTEX = "VERTEX";
    public static final String RELATION_ISIS = "ISIS";

    // ---- batch sizes (match topology service) ----
    public static final int VERTEX_BATCH_SIZE = 5000;
    public static final int EDGE_BATCH_SIZE = 2000;

    /**
     * Properties that must NOT be set on the vertex itself — they belong on
     * the edge only. Mirrors {@code isSourceDestinationProperty} in
     * {@code JanusgraphServiceImpl}.
     */
    public static boolean isSourceDestinationProperty(String key) {
        if (key == null) return false;
        switch (key) {
            case SRC_VENDOR:
            case DEST_VENDOR:
            case SRC_TECHNOLOGY:
            case DEST_TECHNOLOGY:
            case SRC_TYPE:
            case DEST_TYPE:
            case SRC_DOMAIN:
            case DEST_DOMAIN:
            case SRC_CATEGORY:
            case DEST_CATEGORY:
            case NEIGHBOUR_NENAME:
            case NEIGHBOUR_NE_ID:
            case NEIGHBOUR_PARENT_NENAME:
            case NEIGHBOUR_PARENT_NE_LAT:
            case NEIGHBOUR_PARENT_NE_LNG:
            case NEIGHBOUR_NE_LAT:
            case NEIGHBOUR_NE_LNG:
            case CKTID:
                return true;
            default:
                return false;
        }
    }

    /**
     * Extracts the trailing id segment of a hierarchical interface name.
     * "node1/cardA/eth_3" → "3", "foo" → "foo". Mirrors {@code extractingId}.
     */
    public static String extractingId(String name) {
        return (name != null) ? name.replaceAll("^.*_", "") : null;
    }
}
