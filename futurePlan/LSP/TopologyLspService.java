package com.enttribe.module.topology.core.corelation.service;

import java.util.List;
import java.util.Map;

/**
 * Service interface for retrieving MPLS LSP link data from PM Cassandra
 * keyspace (pm.mpls_lsp_link).
 * topology graph.
 *
 * <p>
 * This interface provides methods for retrieving MPLS LSP link data from PM
 * Cassandra keyspace (pm.mpls_lsp_link).
 * </p>
 *
 * @author Enttribe
 * @version 4.0.0
 * @since 1.0
 */
public interface TopologyLspService {

    /**
     * Retrieves MPLS LSP link data from PM Cassandra (pm.mpls_lsp_link).
     * When filter is "LSP", returns a single map with "nodes" and "links".
     * When filter is an LSP name, returns single object; optional start/end filter
     * by source/destination node (IP or name, case-insensitive contains).
     *
     * @param filter    optional; when "LSP" returns single object { nodes, links };
     *                  when LSP name filters by that name
     * @param start     optional; when filter is LSP name, include only where source
     *                  (IP or node name) contains this value
     * @param end       optional; when filter is LSP name, include only where
     *                  destination (IP or node name) contains this value
     * @return when filter=LSP: single Map with "nodes", "links" and "timestamp";
     *         when filter=LSP name: single Map with nodes, link details, metaInfo,
     *         timestamp; otherwise List of maps with sourceIp, destinationIp,
     *         lspName, links, nodes, timestamp
     */
    Object getTopologyLspData(String filter, String start, String end);

    /**
     * Returns only links for LSP data (filter=LSP). Optionally filters by start,
     * end, or lspName. Pagination via lLimit, uLimit (same as getTopologyDetail).
     *
     * @param start     optional; include only links whose start contains this value
     * @param end       optional; include only links whose end contains this value
     * @param lspName   optional; include only links whose lspName starts with this
     *                  value
     * @param lLimit    optional; lower limit (default 0)
     * @param uLimit    optional; upper limit (default 25)
     * @return map with "links" only (same as getTopologyDetail edge response), or
     *         "message" when LSP config data is unavailable
     */
    @SuppressWarnings("java:S107")
    Object getLspData(String start, String end, String lspName, Integer lLimit, Integer uLimit,
            String orderBy, String orderType, String metaInfoFilters);

    /**
     * Same as getLspDataCount with additional optional metaInfo comparison filters.
     * AND format: key<op>value,key<op>value
     * Example: mplsLspAge>100,mplsPathBandwidth>=1000
     */
    String getLspDataCount(String start, String end, String lspName, String metaInfoFilters);

    /**
     * Returns LSP status summary for a router:
     * totalLsp from topology LSP count and downLsp from FM active alarm count.
     *
     * @param entityName router name (used as both FM entityName and topology start)
     * @return map containing totalLsp, downLsp and activeLsp
     */
    Object getLspStatus(Map<String, Object> payload);

    /**
     * Generic LSP search backed by Cassandra; more modes may be added on this entry point.
     * Current mode: region + neTypes (GraphDB {@code geographyL1Name} + {@code neType} list),
     * then links/nodes from PM MPLS LSP data.
     *
     * @param neTypes comma-separated NE types (required for region mode)
     * @param region geography L1 name (required for region mode)
     * @param filter optional; same meaning as GraphDB {@code getFilterData} ({@code parent},
     *               {@code lldp}/{@code ospf}/{@code isis} on {@code relation}, or default)
     * @return empty list when nothing matches; otherwise one map with {@code links},
     *         {@code nodes}, and optional {@code timestamp}
     */
    List<Map<String, Object>> lspSearch(String neTypes, String region, String filter);

    /**
     * Cassandra-backed POST filter (GraphDB equivalent: {@code getFilterData}).
     * Subgraph is built only from MPLS LSP links in Cassandra (not LLDP/OSPF/ISIS topology).
     *
     * @param neNames parent NE names (same role as GraphDB payload)
     * @param filter optional {@code parent} or relation filters (same keywords as GraphDB {@code getFilterData})
     */
    List<Map<String, Object>> getLspFilterData(List<String> neNames, String filter);
}
