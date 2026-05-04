package com.enttribe.module.topology.core.corelation.service;

import com.enttribe.module.topology.core.wrapper.NetworkElementWrapper;
import com.enttribe.network.module.fm.core.wrapper.AlarmMonitoringRequestFilterWrapper;
import com.enttribe.lcm.util.wrapper.Filter;

import java.util.List;
import java.util.Map;

/**
 * Service interface for vertex creation and management operations in the
 * topology graph.
 *
 * <p>
 * This interface provides methods for creating vertices representing network
 * elements,
 * linking vertices with parent nodes, and retrieving network element
 * information from the graph database.
 * All methods are designed to work with JanusGraph database for topology
 * management.
 * </p>
 *
 * @author Enttribe
 * @version 4.0.0
 * @since 1.0
 */
public interface JanusgraphService {

    /**
     * Creates a vertex and links it to a parent vertex in the graph database.
     *
     * @param requestData map containing the properties for the new vertex and
     *                    parent link information
     * @return a map containing information about the created vertex and link
     */
    Map<String, Object> addVertexWithParentLink(Map<String, Object> requestData);

    /**
     * Creates vertices in the graph database based on provided network elements.
     *
     * @param list wrapper containing network elements to be added as vertices
     * @return a list of maps containing information about the created vertices
     */
    List<Map<String, Object>> addVertexByNetworkElement(NetworkElementWrapper list);

    /**
     * Retrieves network elements from the graph database based on the provided
     * filters.
     *
     * @param wrapper list of filters to apply when retrieving network elements
     * @return a list of maps containing the properties of matching network elements
     */
    List<Map<String, Object>> getNetworkElement(List<Filter> wrapper);

    /**
     * Searches for network elements in the graph database based on network element
     * type and name.
     *
     * <p>
     * neTypes is mandatory. Only one of neName, areaIpAddress, tag, popId, or popName can be provided at a time.
     * If areaIpAddress or tag is provided, the method will search by area IP address or tag instead
     * of performing a vertex search. If popId or popName is provided, it will search by POP ID or POP name.
     * The relation parameter is optional and filters results to only include vertices connected by edges with the specified relation.
     * </p>
     *
     * @param neTypes the network element type to search for (mandatory, must not be null or empty)
     * @param neName  the network element name to search for (optional, can be null, but only one search parameter allowed)
     * @param areaIpAddress the area IP address to search for (optional, can be null, but only one search parameter allowed).
     *                      If provided, the method will search by area IP address instead
     * @param tag the tag to search for (optional, can be null, but only one search parameter allowed).
     *            If provided, the method will search by tag instead
     * @param popId the POP ID to search for (optional, can be null, but only one search parameter allowed).
     *              If provided, the method will search by POP ID (exact match)
     * @param popName the POP name to search for (optional, can be null, but only one search parameter allowed).
     *                If provided, the method will search by POP name (regex pattern)
     * @param relation the relation type to filter by (optional). If provided, only vertices connected by edges with this relation are returned
     * @return a list of maps containing the matching network elements and their
     *         properties. If areaIpAddress or tag is provided, returns list with links and nodes.
     * @throws InvalidParametersException if neTypes is empty, or if multiple search parameters are provided,
     *                                    or if none of the search parameters is provided
     */
    @SuppressWarnings("java:S107")
    List<Map<String, Object>> searchByNeTypeAndNeName(String neTypes, String neName, String areaIpAddress, String tag,
            String popId, String popName, String ipv4, String popCode, String region, String relation);

    /**
     * Retrieves count of vertices grouped by state based on the specified geography
     * level.
     *
     * @param geographyLevel the geographic level to group by (e.g., "state",
     *                       "district")
     * @return a list of maps containing state names and their corresponding vertex
     *         counts
     */
    List<Map<String, Object>> stateWiseCount(String geographyLevel);

    /**
     * Retrieves count of vertices grouped by network element type.
     *
     * @param requestPayload map containing parameters for the query, including
     *                       "neTypes" (list of network element types)
     *                       and "geographicType" (geographic level for grouping)
     * @return a list of maps containing network element types and their
     *         corresponding vertex counts
     */
    List<Map<String, Object>> neTypeWiseCount(Map<String, Object> requestPayload);

    /**
     * Creates multiple vertices in the graph database based on provided network
     * elements.
     *
     * @param networkElements a list of network elements to create as vertices
     * @return a list of maps containing information about the created vertices
     * @throws InvalidParametersException if the network elements list is empty or
     *                                    contains invalid elements
     * @throws VertexCreationException    if there is an error creating the vertices
     */
    List<Map<String, Object>> createMultipleVertices(List<Map<String, Object>> networkElements);

    /**
     * Downloads all data from the graph database in the specified format.
     *
     * @param format the format in which to export the data (e.g., "json", "csv")
     * @param opmode the operation mode specifying what data to export, defaults to
     *               "both"
     * @return byte array containing the exported data
     */
    byte[] downloadAllData(String format, String opmode);

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
     * @param opmode  the operation mode specifying what to count, must be either
     *                "vertex" or "edge"
     * @param filters map containing filter properties and their values
     * @return the count as a Long value
     * @throws IllegalArgumentException if opmode is not "vertex" or "edge"
     */
    Long getTopologyDataCount(String opmode, Map<String, Object> filters);

    /**
     * Searches for edges and vertices based on network element names with optional
     * filter.
     * If filter is null or blank, behaves like {@link #getFilterData(List)}.
     * Currently supported filter values:
     * - "parent": return only nodes that exactly match the provided neNames and
     * links among them.
     */
    List<Map<String, Object>> getFilterData(List<String> payload, String filter);

    /**
     * Retrieves all links (edges) that have the given lspName and the nodes
     * corresponding to start/end of those links. Response shape matches
     * getFilterData: list of one map with "links" and "nodes".
     *
     * @param lspName the LSP name to filter edges by (required)
     * @return list containing one map with "links" (edges with lspName) and "nodes" (vertices for start/end)
     */
    List<Map<String, Object>> getFilterLspData(String lspName);

    /**
     * Returns the maximum 'ModificationTime' for vertices.
     *
     * <p>
     * Keys in the returned map:
     * </p>
     * - "vertexMaxTime" -> max from vertices, formatted as yyyy-MM-dd HH:mm:ss
     * (UTC)
     * If none present, returns empty string.
     */
    Map<String, String> getMaxModificationTime();

    /**
     * Retrieves alarm-based nodes from JanusGraph with optional filtering and
     * pagination.
     * <p>
     * This method is typically used to fetch nodes that are linked with alarms in
     * the FM (Fault Management) service or graph database. The query can be refined
     * using the provided filter and bounded with lower and upper limits for
     * pagination.
     * </p>
     * 
     * @param wrapper optional filter for alarms (nullable)
     * @return list of alarm-based node details from JanusGraph
     */
    List<Map<String, Object>> getAlarmBasedNodes(AlarmMonitoringRequestFilterWrapper wrapper);

    /**
     * Retrieves topology detail data (vertices or edges) based on operation mode
     * and filters.
     * <p>
     * This method supports flexible filtering with comma-separated values (OR
     * logic).
     * For vertex queries, only vertices with valid latitude and longitude
     * coordinates are returned.
     * The operation mode determines what data to return:
     * - "vertex": returns only vertex data with valid coordinates
     * - "edge": returns only edge data
     * </p>
     * 
     * @param opmode  the operation mode specifying what data to retrieve ("vertex"
     *                or "edge")
     * @param filters map containing filter properties and their values
     * @param lLimit  lower limit for pagination (optional, null for no limit)
     * @param uLimit  upper limit for pagination (optional, null for no limit)
     * @return list of maps containing the topology detail data
     */
    List<Map<String, Object>> getTopologyDetail(String opmode, Map<String, Object> filters, Integer lLimit,
            Integer uLimit);

    /**
     * Retrieves count of topology detail data (vertices or edges) based on
     * operation mode and filters.
     * <p>
     * This method supports the same flexible filtering logic as getTopologyDetail
     * (OR logic with comma-separated values)
     * but returns only the count of matching records instead of the actual data.
     * For vertex queries, only vertices with valid latitude and longitude
     * coordinates are counted.
     * </p>
     * 
     * @param opmode  the operation mode specifying what data to count ("vertex" or
     *                "edge")
     * @param filters map containing filter properties and their values
     * @return the count as a Long value
     */
    Long getTopologyDetailCount(String opmode, Map<String, Object> filters);

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
     * @return the CSV file content as byte array
     * @throws IllegalArgumentException if fileName is null or empty
     * @throws ResourceNotFoundException if the requested file type is not supported
     */
    byte[] downloadCsvFile(String fileName);

    /**
     * Retrieves all links (edges) and their connected nodes (vertices) based on area IP address or tag.
     * 
     * <p>
     * This method searches for all edges that have the specified areaIpAddress or tag property value,
     * and returns those edges along with their connected start and end vertices.
     * If areaIpAddress is provided, it filters by areaIpAddress. If tag is provided, it filters by tag.
     * If both parameters are null or empty, returns null.
     * </p>
     * 
     * @param areaIpAddress the area IP address to search for (optional, can be null)
     * @param tag the tag to search for (optional, can be null)
     * @return a list containing a single map with "links" (edges) and "nodes" (vertices) arrays.
     *         Returns null if both parameters are null or empty.
     * @throws InvalidParametersException if both parameters are provided (only one allowed at a time)
     * @throws DataProcessingException if there is an error processing the data
     */
    List<Map<String, Object>> getLinksByAreaIpAddress(String areaIpAddress, String tag);

    /**
     * Deletes vertices and edges older than 7 days, keeping only the last 7 days of data.
     * 
     * <p>
     * This method automatically calculates the date 7 days ago from today and deletes all vertices and edges
     * that have a lastDiscoveryTime property value less than that date. This ensures only the last 7 days
     * of data are retained.
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
    Map<String, Object> deleteByLastDiscoveryTime();
}
