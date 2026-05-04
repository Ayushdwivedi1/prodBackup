package com.enttribe.module.topology.core.corelation.rest;

import com.enttribe.module.topology.core.corelation.constants.TopologyPermission;
import com.enttribe.core.generic.utils.ProductApiResponses;
import java.util.List;
import java.util.Map;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

/**
 * REST interface for retrieving MPLS LSP link data from PM Cassandra keyspace
 * (pm.mpls_lsp_link).
 * 
 * <p>
 * This interface provides endpoints for retrieving MPLS LSP link data from PM
 * Cassandra keyspace (pm.mpls_lsp_link).
 * Connection settings are read from conf/topologyConf.properties.
 * </p>
 *
 * <p>
 * Example usage:
 * </p>
 * 
 * <pre>
 * Object result = topologyLspRest.getTopologyLspData("LSP", null, null);
 * </pre>
 *
 * @author Enttribe
 * @version 4.0.0
 * @since 1.0
 */
@FeignClient(name = "TopologyLspService", url = "${topology-service.url}", path = "lsp", primary = false)
@Tag(name = "TopologyLspRest")
public interface TopologyLspRest {

    /**
     * Retrieves MPLS LSP link data from PM Cassandra (pm.mpls_lsp_link table).
     * Connection settings are read from conf/topologyConf.properties.
     * When filter=LSP, response is a single object { "nodes": [...], "links": [...]
     * }. When filter is an LSP name, returns single object with nodes, link details,
     * and optional start/end filtering.
     *
     * @param filter    optional; when "LSP" returns single object with nodes and
     *                  links; when an LSP name, filters by that name
     * @param start     optional; when filter is LSP name, include only data where
     *                  source node (IP or name) contains this value (case-insensitive)
     * @param end       optional; when filter is LSP name, include only data where
     *                  destination node (IP or name) contains this value (case-insensitive)
     * @return when filter=LSP: single map with "nodes", "links" and "timestamp";
     *         when filter=LSP name: single map with nodes, explicitRouteLinkDetails,
     *         recordRouteLinkDetails, metaInfo, timestamp (optionally filtered by start/end)
     */
    @GetMapping(path = "/getTopologyLspData", produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(summary = "Retrieves MPLS LSP link data from PM Cassandra", tags = "getTopologyLspData", security = @SecurityRequirement(name = "default", scopes = TopologyPermission.TOPOLOGY_LSP_READ_TOPOLOGY_LSP_DATA))
    @ProductApiResponses
    Object getTopologyLspData(
            @RequestParam(name = "filter", required = false) String filter,
            @RequestParam(name = "start", required = false) String start,
            @RequestParam(name = "end", required = false) String end);

    /**
     * Returns only links (and timestamp) for filter=LSP. Uses getTopologyLspData
     * internally.
     * Optional query params filter links by start, end, or lspName. Supports
     * pagination via lLimit, uLimit (same as getTopologyDetail).
     *
     * @param start     optional; include only links whose "start" contains this
     *                  value (e.g. Baroda_House)
     * @param end       optional; include only links whose "end" contains this value
     * @param lspName   optional; include only links whose "lspName" starts with
     *                  this value (e.g. BH-Mx204)
     * @param lLimit    optional; lower limit for pagination (default 0)
     * @param uLimit    optional; upper limit for pagination (default 25)
     * @param orderBy   optional; order by key
     * @param orderType optional; order type (asc or desc)
     * @param metaInfoFilters optional; comparison filters on metaInfo fields.
     *                        AND format: key<op>value,key<op>value
     *                        Example: mplsLspAge>100,mplsPathBandwidth>=1000
     * @return {"links": [...]} only (same shape as getTopologyDetail edge
     *         response), or {"message": "..."} when LSP config data is unavailable
     */
    @GetMapping(path = "/getLspData", produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(summary = "Returns LSP links only (with optional search and pagination)", tags = "getLspData", security = @SecurityRequirement(name = "default", scopes = TopologyPermission.TOPOLOGY_LSP_READ_TOPOLOGY_LSP))
    @ProductApiResponses
    Object getLspData(
            @RequestParam(name = "start", required = false) String start,
            @RequestParam(name = "end", required = false) String end,
            @RequestParam(name = "lspName", required = false) String lspName,
            @RequestParam(name = "lLimit", required = false, defaultValue = "0") Integer lLimit,
            @RequestParam(name = "uLimit", required = false, defaultValue = "24") Integer uLimit,
            @RequestParam(name = "orderBy", required = false) String orderBy,
            @RequestParam(name = "orderType", required = false) String orderType,
            @RequestParam(name = "metaInfoFilters", required = false) String metaInfoFilters);

    /**
     * Same query params as getLspData; returns only the count of matching links as
     * plain text (e.g. "20").
     *
     * @param start     optional; same as getLspData
     * @param end       optional; same as getLspData
     * @param lspName   optional; same as getLspData
     * @return plain text count, e.g. "20"
     */
    @GetMapping(path = "/getLspDataCount", produces = MediaType.TEXT_PLAIN_VALUE)
    @Operation(summary = "Returns count of LSP links (same params as getLspData), plain text", tags = "getLspDataCount", security = @SecurityRequirement(name = "default", scopes = TopologyPermission.TOPOLOGY_LSP_READ_TOPOLOGY_LSP_COUNT))
    @ProductApiResponses
    String getLspDataCount(
            @RequestParam(name = "start", required = false) String start,
            @RequestParam(name = "end", required = false) String end,
            @RequestParam(name = "lspName", required = false) String lspName,
            @RequestParam(name = "metaInfoFilters", required = false) String metaInfoFilters);

    /**
     * Returns summary count of total/down/active LSP for a router.
     * Request payload should contain entityName.
     */
    @PostMapping(path = "/getLspStatus", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(summary = "Returns total/down/active LSP count for router", tags = "getLspStatus", security = @SecurityRequirement(name = "default", scopes = TopologyPermission.TOPOLOGY_LSP_READ_TOPOLOGY_LSP_STATUS))
    @ProductApiResponses
    Object getLspStatus(@RequestBody Map<String, Object> payload);

    /**
     * Generic Cassandra LSP search endpoint; additional query modes may be added later.
     * Current behavior: region-wise search (GraphDB-aligned {@code geographyL1Name}),
     * requires {@code neTypes} and {@code region}.
     */
    @GetMapping(path = "/lspSearch", produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(summary = "LSP search from Cassandra (extensible; region + neTypes today)", tags = "lspSearch", security = @SecurityRequirement(name = "default", scopes = TopologyPermission.TOPOLOGY_LSP_READ_TOPOLOGY_LSP_DATA))
    @ProductApiResponses
    List<Map<String, Object>> lspSearch(
            @RequestParam String neTypes,
            @RequestParam String region,
            @RequestParam(name = "filter", required = false) String filter);

    /**
     * Cassandra equivalent of GraphDB {@code /getFilterData} for MPLS LSP topology only.
     */
    @PostMapping(path = "/getFilterLspData", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(summary = "LSP links and nodes for given NE names (Cassandra)", tags = "getLspFilterData", security = @SecurityRequirement(name = "default", scopes = TopologyPermission.TOPOLOGY_LSP_READ_TOPOLOGY_LSP_DATA))
    @ProductApiResponses
    List<Map<String, Object>> getLspFilterData(
            @RequestBody List<String> payload,
            @RequestParam(name = "filter", required = false) String filter);
}
