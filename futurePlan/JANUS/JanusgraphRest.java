package com.enttribe.module.topology.core.corelation.rest;

import java.util.List;
import java.util.Map;

import com.enttribe.module.topology.core.corelation.constants.TopologyPermission;
import com.enttribe.module.topology.core.wrapper.NetworkElementWrapper;
import com.enttribe.network.module.fm.core.wrapper.AlarmMonitoringRequestFilterWrapper;
import com.enttribe.core.generic.utils.ProductApiResponses;
import com.enttribe.lcm.util.wrapper.Filter;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * REST interface for creating and managing network element vertices in the
 * topology graph database.
 * 
 * <p>
 * This interface provides endpoints for adding vertices, linking them with
 * parent nodes,
 * and retrieving network elements from the JanusGraph database. It uses Spring
 * Cloud OpenFeign
 * for client-side load balancing and service discovery.
 * </p>
 *
 * <p>
 * Example usage:
 * </p>
 * 
 * <pre>
 * NetworkElementWrapper wrapper = new NetworkElementWrapper();
 * wrapper.setNetworkElements(Arrays.asList(networkElement));
 * String result = janusgraphRest.addVertexByNetworkElement(wrapper);
 * </pre>
 *
 * @author Enttribe
 * @version 4.0.0
 * @since 1.0
 */
@FeignClient(name = "JanusgraphService", url = "${topology-service.url}", path = "graphdb", primary = false)
@Tag(name = "JanusgraphRest")
public interface JanusgraphRest {

  /**
   * Adds a vertex to the graph database using NetworkElementWrapper data.
   * The vertex is created with properties extracted from the network element
   * wrapper.
   *
   * @param networkElement the network element wrapper containing vertex data,
   *                       must not be null
   * @return a string response indicating the result of vertex creation
   * @throws IllegalArgumentException if networkElement is null
   */
  @PostMapping(path = "/addVertexByNetworkElement", consumes = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Adds a vertex using NetworkElementWrapper data", tags = "addVertexByNetworkElement", security = @SecurityRequirement(name = "default", scopes = TopologyPermission.TOPOLOGY_GRAPHDB_WRITE_NETWORK_ELEMENT))
  @ProductApiResponses
  String addVertexByNetworkElement(@RequestBody NetworkElementWrapper networkElement);

  /**
   * Adds a vertex and creates a parent-child relationship with an existing
   * vertex.
   * The request data should contain both the new vertex properties and parent
   * linking information.
   *
   * @param requestData a map containing vertex and parent linking parameters,
   *                    must not be null
   * @return a string response indicating the result of vertex creation and
   *         linking
   * @throws IllegalArgumentException if requestData is null or missing required
   *                                  parameters
   */
  @PostMapping(path = "/addVertexWithParentLink", consumes = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Adds a vertex and links it with a parent vertex", tags = "addVertexWithParentLink", security = @SecurityRequirement(name = "default", scopes = TopologyPermission.TOPOLOGY_GRAPHDB_WRITE_PARENT_LINK))
  @ProductApiResponses
  String addVertexWithParentLink(@RequestBody Map<String, Object> requestData);

  /**
   * Retrieves network elements from the graph database based on filter criteria.
   * The filters are applied to find matching network element vertices.
   *
   * @param filters a list of Filter objects defining the search criteria, must
   *                not be null
   * @return a list of network elements matching the filter criteria
   * @throws IllegalArgumentException if filters is null
   */
  @PostMapping(path = "/getNetworkElements", consumes = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Retrieves network elements based on filter criteria", tags = "getNetworkElements", security = @SecurityRequirement(name = "default", scopes = TopologyPermission.TOPOLOGY_GRAPHDB_READ_NETWORK_ELEMENTS))
  @ProductApiResponses
  List<Map<String, Object>> getNetworkElement(@RequestBody List<Filter> filters);

  /**
   * Searches for network elements based on network element type and name.
   * The search is case-insensitive and supports partial matching.
   * 
   * neTypes is mandatory. Only one of neName, areaIpAddress, tag, popId, or popName can be provided at a time.
   * If areaIpAddress or tag is provided, the method will search by area IP address or tag instead
   * of performing a vertex search. If popId or popName is provided, it will search by POP ID or POP name.
   * The relation parameter is optional and filters results to only include vertices connected by edges with the specified relation.
   *
   * @param neTypes the network element types to search for (mandatory)
   * @param neName  the network element name to search for (optional, but only one search parameter allowed)
   * @param areaIpAddress the area IP address to search for (optional, but only one search parameter allowed).
   *                      If provided, the method will search by area IP address instead
   * @param tag the tag to search for (optional, but only one search parameter allowed).
   *            If provided, the method will search by tag instead
   * @param popId the POP ID to search for (optional, but only one search parameter allowed).
   *              If provided, the method will search by POP ID (exact match)
   * @param popName the POP name to search for (optional, but only one search parameter allowed).
   *                If provided, the method will search by POP name (regex pattern)
   * @param ipv4 the IPv4 address to search for (optional, but only one search parameter allowed).
   *             If provided, the method will search by IPv4 (exact match)
   * @param popCode the POP code to search for (optional, but only one search parameter allowed).
   *                If provided, the method will search by POP code (regex pattern)
   * @param relation the relation type to filter by (optional). If provided, only vertices connected by edges with this relation are returned
   * @return a list of matching network elements. If areaIpAddress or tag is provided, 
   *         returns list with links and nodes.
   * @throws InvalidParametersException if neTypes is empty, or if multiple search parameters are provided,
   *                                    or if none of the search parameters is provided
   */
  @GetMapping(path = "/searchByNeTypeAndNeName", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Searches for network elements based on network element type and name", tags = "searchByNeTypeAndNeName", security = @SecurityRequirement(name = "default", scopes = TopologyPermission.TOPOLOGY_GRAPHDB_READ_BASIS_OF_NENAME))
  @ProductApiResponses
  List<Map<String, Object>> searchByNeTypeAndNeName(@RequestParam(required = false) String neTypes, @RequestParam(required = false) String neName, @RequestParam(required = false) String areaIpAddress, @RequestParam(required = false) String tag, @RequestParam(required = false) String popId, @RequestParam(required = false) String popName, @RequestParam(required = false) String ipv4, @RequestParam(required = false) String popCode, @RequestParam(required = false) String region, @RequestParam(required = false) String relation);

  /**
   * Retrieves count of network elements grouped by geographical state.
   * The geography level parameter determines the granularity of the grouping.
   *
   * @param geographyLevel the geographic level for counting (e.g., "state",
   *                       "city"), must not be null
   * @return a list of maps containing state-wise counts with geography and count
   *         information
   * @throws IllegalArgumentException if geographyLevel is null
   */
  @GetMapping(path = "/stateWiseCount", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Retrieves count of network elements by geographical state", tags = "stateWiseCount", security = @SecurityRequirement(name = "default", scopes = TopologyPermission.TOPOLOGY_GRAPHDB_READ_STATE_WISE_COUNT))
  @ProductApiResponses
  List<Map<String, Object>> stateWiseCount(@RequestParam String geographyLevel);

  /**
   * Retrieves count of network elements grouped by network element type.
   * The request payload can contain additional filtering parameters.
   *
   * @param requestPayload a map containing parameters for counting by NE type,
   *                       must not be null
   * @return a list of maps containing NE type-wise counts with type and count
   *         information
   * @throws IllegalArgumentException if requestPayload is null
   */
  @PostMapping(path = "/neTypeWiseCount", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Retrieves count of network elements by network element type", tags = "neTypeWiseCount", security = @SecurityRequirement(name = "default", scopes = TopologyPermission.TOPOLOGY_GRAPHDB_READ_NETYPE_WISE_COUNT))
  @ProductApiResponses
  List<Map<String, Object>> neTypeWiseCount(@RequestBody Map<String, Object> requestPayload);

  /**
   * Creates multiple network element vertices in the graph database.
   *
   * <p>
   * This method processes a list of network elements and creates corresponding
   * vertices
   * in the graph database. For each network element, it also creates the
   * necessary edge
   * relationships. If a vertex already exists for a network element, it updates
   * the
   * properties instead of creating a new vertex.
   * </p>
   * 
   * @param payloadList a list of network element wrappers to create as vertices
   * @return a list of maps containing information about the created vertices
   * @throws InvalidInputException   if the network elements list is empty
   * @throws VertexCreationException if there is an error creating the vertices
   * @throws NullPointerException    if networkElements or any individual element
   *                                 is null
   */
  @PostMapping(path = "/createMultipleVertices", consumes = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Creates multiple network element vertices in the graph database", tags = "createMultipleVertices", security = @SecurityRequirement(name = "default", scopes = TopologyPermission.TOPOLOGY_GRAPHDB_WRITE_MULTIPLE_VERTEX))
  @ProductApiResponses
  List<Map<String, Object>> createMultipleVertices(@RequestBody List<Map<String, Object>> payloadList);

  /**
   * Downloads all vertex data in CSV, PDF, or JSON format.
   *
   * <p>
   * This method exports all vertex data from the graph database in the requested
   * format.
   * The data includes all properties and relationships for each vertex.
   * </p>
   *
   * @param format the format for data export, must be one of: "csv", "xml", or
   *               "json"
   * @param opmode the operation mode specifying what data to export, defaults to
   *               "both"
   * @return ResponseEntity containing the exported data as byte array with
   *         appropriate headers
   * @throws IllegalArgumentException if format is not supported
   * @throws DataProcessingException  if there is an error during data export
   */
  @GetMapping(path = "/downloadAllData", produces = { "text/csv", "application/xml", MediaType.APPLICATION_JSON_VALUE })
  @Operation(summary = "Downloads all vertex data in CSV, PDF, or JSON format", tags = "downloadAllData", security = @SecurityRequirement(name = "default", scopes = TopologyPermission.TOPOLOGY_GRAPHDB_READ_DOWNLOAD_ALL_DATA))
  @ProductApiResponses
  ResponseEntity<byte[]> downloadAllData(@RequestParam String format,
      @RequestParam(required = false, defaultValue = "both") String opmode);

  /**
   * Retrieves count of vertices or edges based on the operation mode with
   * filters.
   *
   * <p>
   * This method returns the total count of vertices or edges in the graph
   * database
   * based on the specified operation mode and filters. When opmode is "vertex",
   * it returns count of vertices with valid latitude and longitude values.
   * When opmode is "edge", it returns edge count.
   * </p>
   *
   * <p>
   * The request payload should contain:
   * </p>
   * <ul>
   * <li>"opmode": the operation mode ("vertex" or "edge")</li>
   * <li>"filters": a map containing filter properties and their values (e.g.,
   * {"neType": "ILA,DWDM,GNE", "vendor": "CISCO,ZTE,NOKIA"})</li>
   * </ul>
   *
   * @param requestPayload map containing "opmode" and "filters" for the count
   *                       operation
   * @return ResponseEntity containing the count as a number
   */
  @PostMapping(path = "/getTopologyDataCount", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Retrieves count of vertices or edges based on operation mode with filters", tags = "getTopologyDataCount", security = @SecurityRequirement(name = "default", scopes = TopologyPermission.TOPOLOGY_GRAPHDB_READ_GET_TOPOLOGY_DATA_COUNT))
  @ProductApiResponses
  ResponseEntity<Object> getTopologyDataCount(@RequestBody Map<String, Object> requestPayload);

  /**
   * Searches for edges and vertices based on network element names with optional
   * filter.
   * If filter is null or blank, behaves like {@link #getFilterData(List)}.
   * Currently supported filter values:
   * - "parent": return only nodes that exactly match the provided neNames and
   * links among them.
   */
  @PostMapping(path = "/getFilterData", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Searches for edges based on flexible start and end property matching", tags = "getFilterData", security = @SecurityRequirement(name = "default", scopes = TopologyPermission.TOPOLOGY_GRAPHDB_READ_FILTER_DATA))
  @ProductApiResponses
  List<Map<String, Object>> getFilterData(@RequestBody List<String> payload,
      @RequestParam(name = "filter", required = false) String filter);

  /**
   * Retrieves all links (edges) that have the given lspName and the nodes
   * corresponding to start/end of those links. Response shape matches getFilterData.
   *
   * @param lspName the LSP name to filter edges by (e.g. NDLS-MX204-GGN-NCS)
   * @return list containing one map with "links" (edges with lspName) and "nodes" (vertices for start/end)
   */
  @GetMapping(path = "/getFilterLspData", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Retrieves links and nodes filtered by LSP name", tags = "getFilterLspData", security = @SecurityRequirement(name = "default", scopes = TopologyPermission.TOPOLOGY_GRAPHDB_READ_FILTER_LSP_DATA))
  @ProductApiResponses
  List<Map<String, Object>> getFilterLspData(@RequestParam(name = "lspName") String lspName);

  /**
   * Returns the maximum modification timestamp across vertices and edges.
   *
   * @return maximum modification epoch millis, or 0 if none present
   */
  @GetMapping(path = "/maxModificationTime", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Returns the maximum modification timestamp across vertices and edges", tags = "maxModificationTime", security = @SecurityRequirement(name = "default", scopes = TopologyPermission.TOPOLOGY_GRAPHDB_READ_MAX_TIME))
  @ProductApiResponses
  Map<String, String> getMaxModificationTime();

  /**
   * Retrieves graph nodes linked to FM alarms using optional filter
   * and pagination (lower/upper limits).
   *
   * @param wrapper optional filter for alarms (nullable)
   * @return list of alarm-based node details from JanusGraph
   */
  @PostMapping(path = "/getAlarmBasedNodes", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Get graph nodes based on FM alarms", tags = "getAlarmBasedNodes", security = @SecurityRequirement(name = "default", scopes = TopologyPermission.TOPOLOGY_GRAPHDB_READ_ALARM_DATA))
  @ProductApiResponses
  List<Map<String, Object>> getAlarmBasedNodes(@RequestBody AlarmMonitoringRequestFilterWrapper wrapper);

  /**
   * Retrieves topology detail data (vertices and/or edges) based on operation
   * mode and filters.
   * <p>
   * This method supports flexible filtering with comma-separated values (OR
   * logic).
   * The operation mode determines what data to return:
   * - "vertex": returns only vertex data
   * - "edge": returns only edge data
   * </p>
   * 
   * <p>
   * The request payload should contain:
   * </p>
   * <ul>
   * <li>"opmode": the operation mode ("vertex" or "edge") - defaults to "vertex"
   * if not provided</li>
   * <li>"filters": a map containing filter properties and their values (e.g.,
   * {"neType": "ILA,DWDM,GNE", "vendor": "CISCO,ZTE,NOKIA"}) - optional</li>
   * </ul>
   * 
   * @param requestPayload map containing "opmode" and "filters" for the topology
   *                       detail operation
   * @param lLimit         lower limit for pagination (default: 0)
   * @param uLimit         upper limit for pagination (default: 25)
   * @return map containing "nodes" for vertex data or "links" for edge data
   */
  @PostMapping(path = "/getTopologyDetail", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Retrieves topology detail data (vertices and/or edges) based on operation mode and filters with pagination support", tags = "getTopologyDetail", security = @SecurityRequirement(name = "default", scopes = TopologyPermission.TOPOLOGY_GRAPHDB_READ_GET_TOPOLOGY_DETAIL))
  @ProductApiResponses
  Map<String, Object> getTopologyDetail(@RequestBody Map<String, Object> requestPayload,
      @RequestParam(defaultValue = "0") Integer lLimit,
      @RequestParam(defaultValue = "25") Integer uLimit);

  /**
   * Retrieves count of topology detail data (vertices and/or edges) based on
   * operation mode and filters.
   * <p>
   * This method supports the same flexible filtering logic as getTopologyDetail
   * but returns
   * only the count of matching records instead of the actual data.
   * </p>
   * 
   * <p>
   * The request payload should contain:
   * </p>
   * <ul>
   * <li>"opmode": the operation mode ("vertex" or "edge") - defaults to "vertex"
   * if not provided</li>
   * <li>"filters": a map containing filter properties and their values (e.g.,
   * {"neType": "ILA,DWDM,GNE", "vendor": "CISCO,ZTE,NOKIA"}) - optional</li>
   * </ul>
   * 
   * @param requestPayload map containing "opmode" and "filters" for the count
   *                       operation
   * @return the count as a Long value
   */
  @PostMapping(path = "/getTopologyDetailCount", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Retrieves count of topology detail data based on operation mode and filters", tags = "getTopologyDetailCount", security = @SecurityRequirement(name = "default", scopes = TopologyPermission.TOPOLOGY_GRAPHDB_READ_GET_TOPOLOGY_DETAIL_COUNT))
  @ProductApiResponses
  Long getTopologyDetailCount(@RequestBody Map<String, Object> requestPayload);

  /**
   * Downloads CSV file for batch processing results.
   * 
   * <p>
   * This method generates and downloads CSV files containing batch processing results
   * including new records, updated records, or failed records. The file name determines
   * the type of data to export.
   * </p>
   * 
   * <p>
   * Supported file types:
   * </p>
   * <ul>
   * <li>new_records_*.csv - Contains newly created records</li>
   * <li>updated_records_*.csv - Contains updated records</li>
   * <li>failed_records_*.csv - Contains failed records with error messages</li>
   * </ul>
   * 
   * @param fileName the name of the CSV file to download, must not be null or empty
   * @return ResponseEntity containing the CSV file as byte array with appropriate headers
   * @throws IllegalArgumentException if fileName is null or empty
   * @throws ResourceNotFoundException if the requested file type is not supported
   */
  @GetMapping(path = "/downloadCsvFile", produces = "text/csv")
  @Operation(summary = "Downloads CSV file for batch processing results", tags = "downloadCsvFile", security = @SecurityRequirement(name = "default", scopes = TopologyPermission.TOPOLOGY_GRAPHDB_READ_DOWNLOAD_ALL_DATA))
  @ProductApiResponses
  ResponseEntity<byte[]> downloadCsvFile(@RequestParam String fileName);

  /**
   * Retrieves all links (edges) and their connected nodes (vertices) based on area IP address or tag.
   * 
   * <p>
   * This method searches for all edges that have the specified areaIpAddress or tag property value,
   * and returns those edges along with their connected start and end vertices.
   * If areaIpAddress is provided, it filters by areaIpAddress. If tag is provided, it filters by tag.
   * If both parameters are null or empty, returns an empty response with empty links and nodes arrays.
   * </p>
   * 
   * @param areaIpAddress the area IP address to search for (optional, can be null)
   * @param tag the tag to search for (optional, can be null)
   * @return a list containing a single map with "links" (edges) and "nodes" (vertices) arrays.
   *         Returns empty arrays if both parameters are null or empty.
   * @throws DataProcessingException if there is an error processing the data
   */
  @GetMapping(path = "/getLinksByAreaIpAddress", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Retrieves all links and their connected nodes based on area IP address or tag", tags = "getLinksByAreaIpAddress", security = @SecurityRequirement(name = "default", scopes = TopologyPermission.TOPOLOGY_GRAPHDB_READ_AREA_IP))
  @ProductApiResponses
  List<Map<String, Object>> getLinksByAreaIpAddress(@RequestParam(required = false) String areaIpAddress, @RequestParam(required = false) String tag);

  /**
   * Deletes vertices and edges older than 7 days, keeping only the last 7 days of data.
   * 
   * <p>
   * This method automatically calculates the date 7 days ago from today and deletes all vertices and edges
   * that have a lastDiscoveryTime property value less than that date. This ensures only the last 7 days
   * of data are retained. Deletes vertices first, commits, then deletes edges and commits again.
   * </p>
   * 
   * <p>
   * Example: If current date is "2026-01-15", it will calculate "2026-01-08" (7 days ago) and delete
   * all vertices and edges where lastDiscoveryTime < "2026-01-08", keeping data from "2026-01-08" onwards.
   * </p>
   * 
   * @return a map containing the status of the deletion operation
   * @throws DataProcessingException if there is an error during deletion
   */
  @DeleteMapping(path = "/deleteByLastDiscoveryTime", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Deletes vertices and edges older than 7 days, keeping only the last 7 days of data", tags = "deleteByLastDiscoveryTime", security = @SecurityRequirement(name = "default", scopes = TopologyPermission.TOPOLOGY_GRAPHDB_DELETE_BY_LAST_DISCOVERY_TIME))
  @ProductApiResponses
  Map<String, Object> deleteByLastDiscoveryTime();
  
  
}
