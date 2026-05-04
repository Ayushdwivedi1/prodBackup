package com.enttribe.module.topology.core.corelation.rest.impl;

import com.enttribe.core.generic.exceptions.application.ResourceNotFoundException;
import com.enttribe.module.topology.core.corelation.rest.JanusgraphRest;
import com.enttribe.module.topology.core.corelation.service.JanusgraphService;
import com.enttribe.module.topology.core.graphinstancemgmt.service.impl.JanusGraphInstanceManagerServiceImpl;
import com.enttribe.module.topology.core.janusgraphutils.constants.JanusGraphUtilsConstants;
import com.enttribe.module.topology.core.querybuilder.exceptions.NetworkElementFetchException;
import com.enttribe.module.topology.core.utils.exceptions.DataNotFoundException;
import com.enttribe.module.topology.core.utils.exceptions.DataProcessingException;
import com.enttribe.module.topology.core.utils.exceptions.EdgeSearchException;
import com.enttribe.module.topology.core.utils.exceptions.InvalidInputException;
import com.enttribe.module.topology.core.utils.exceptions.InvalidParametersException;
import com.enttribe.module.topology.core.utils.exceptions.VertexCreationException;
import com.enttribe.module.topology.core.utils.exceptions.VertexNotFoundException;
import com.enttribe.module.topology.core.wrapper.NetworkElementWrapper;
import com.enttribe.network.module.fm.core.wrapper.AlarmMonitoringRequestFilterWrapper;
import com.enttribe.lcm.util.wrapper.Filter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Primary;
import org.springframework.http.ContentDisposition;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * REST controller implementation for JanusGraph database operations.
 *
 * <p>
 * This controller provides endpoints for managing vertices and network elements
 * in the JanusGraph database. It handles vertex creation, network element
 * operations,
 * and data retrieval with proper error handling and validation.
 * </p>
 *
 * <p>
 * Example usage:
 * </p>
 * 
 * <pre>
 * NetworkElementWrapper wrapper = new NetworkElementWrapper();
 * wrapper.setNetworkElements(Arrays.asList(networkElement));
 * String result = controller.addVertexByNetworkElement(wrapper);
 * </pre>
 *
 * @author Enttribe
 * @version 4.0.0
 * @since 1.0
 */
@Primary
@RequestMapping(produces = MediaType.APPLICATION_JSON_VALUE, path = "graphdb")
@RestController
public class JanusgraphRestImpl implements JanusgraphRest {

    /**
     * Service for managing JanusGraph instances and connections.
     */
    @Autowired
    private JanusGraphInstanceManagerServiceImpl janusGraphInstanceManagerService;

    /**
     * Logger instance for this class.
     */
    private static final Logger logger = LoggerFactory.getLogger(JanusgraphRestImpl.class);

    /**
     * Service for JanusGraph operations and vertex management.
     */
    @Autowired
    private JanusgraphService janusgraphService;

    /**
     * Creates vertices in the graph database based on the provided network
     * elements.
     *
     * <p>
     * This method validates the input network element wrapper and delegates the
     * vertex
     * creation to the service layer. It handles various exceptions that might occur
     * during
     * the creation process and provides appropriate error responses.
     * </p>
     *
     * @param networkElements the network element wrapper containing data for vertex
     *                        creation
     * @return a string response indicating the result of the operation
     * @throws DataNotFoundException   if the network element is not found
     * @throws DataProcessingException if there is an error processing the data
     * @throws VertexCreationException if there is an error creating the vertex
     * @throws NullPointerException    if networkElements is null
     */
    @Override
    public String addVertexByNetworkElement(NetworkElementWrapper networkElements) {
        Objects.requireNonNull(networkElements, "Network elements wrapper cannot be null");
        logger.info("Received request to created vertices with data");
        try {
            List<Map<String, Object>> response = janusgraphService.addVertexByNetworkElement(networkElements);
            logger.info("Vertices successfully created: {} entries", response.size());
            return "Vertices created successfully: " + response;
        } catch (DataNotFoundException e) {
            logger.error(JanusGraphUtilsConstants.NOT_FOUND);
            throw e;
        } catch (DataProcessingException e) {
            logger.error("Data processing error while creating vertices");
            throw e;
        } catch (Exception e) {
            logger.error("Failed to create vertices");
            throw new VertexCreationException("Failed to create vertices: " + e.getMessage(), e);
        }
    }

    /**
     * Creates a vertex in the graph database and links it to a parent vertex.
     *
     * <p>
     * This method validates the input request data and delegates the vertex
     * creation
     * and linking to the service layer. It handles various exceptions that might
     * occur
     * during the process and provides appropriate error responses.
     * </p>
     *
     * @param requestData a map containing the properties for the vertex and parent
     *                    link information
     * @return a JSON string representation of the response map containing the
     *         operation results
     * @throws ResourceNotFoundException if a required resource is not found
     * @throws DataProcessingException   if there is an error processing the data
     * @throws VertexCreationException   if there is an error creating the vertex or
     *                                   link
     * @throws NullPointerException      if requestData is null
     * @throws InvalidInputException     if requestData is empty
     */
    @Override
    public String addVertexWithParentLink(Map<String, Object> requestData) {
        Objects.requireNonNull(requestData, "Request data cannot be null");
        if (requestData.isEmpty()) {
            throw new InvalidInputException("Request data cannot be empty");
        }
        logger.info("Received request to create vertex with parent link data");
        try {
            Map<String, Object> response = janusgraphService.addVertexWithParentLink(requestData);
            logger.info("Vertex successfully created and linked");
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.writeValueAsString(response);
        } catch (ResourceNotFoundException e) {
            logger.error("Resource not found while creating vertex with parent link");
            throw e;
        } catch (JsonProcessingException e) {
            logger.error("Error serializing response");
            throw new DataProcessingException("Failed to serialize response: " + e.getMessage(), e);
        } catch (Exception e) {
            logger.error("Failed to create vertex with parent link");
            throw new VertexCreationException("Failed to create vertex with parent link: " + e.getMessage(), e);
        }
    }

    /**
     * Retrieves network elements from the graph database based on the provided
     * filters.
     *
     * <p>
     * This method validates the input filter wrapper and delegates the retrieval
     * to the service layer. It handles various exceptions that might occur during
     * the process and provides appropriate error responses.
     *
     * @param wrapper a list of filters to apply when retrieving network elements
     * @return a list of maps containing the retrieved network elements and their
     *         properties
     * @throws ResourceNotFoundException if the requested network elements are not
     *                                   found
     * @throws DataProcessingException   if there is an error processing the data
     * @throws RuntimeException          if there is an unexpected error during
     *                                   retrieval
     * @throws NullPointerException      if wrapper is null
     */
    @Override
    public List<Map<String, Object>> getNetworkElement(List<Filter> wrapper) {
        Objects.requireNonNull(wrapper, "Filter wrapper cannot be null");
        logger.info("Received request to fetch network elements with filters, count: {}", wrapper.size());
        try {
            List<Map<String, Object>> networkElements = janusgraphService.getNetworkElement(wrapper);
            logger.info("Successfully fetched {} network elements.", networkElements.size());
            return networkElements;
        } catch (ResourceNotFoundException e) {
            logger.error(JanusGraphUtilsConstants.NOT_FOUND);
            throw e;
        } catch (DataProcessingException e) {
            logger.error("Data processing error while fetching network elements");
            throw e;
        } catch (Exception e) {
            logger.error("Failed to fetch network elements");
            throw new NetworkElementFetchException("Failed to fetch network elements: " + e.getMessage(), e);
        }
    }

    /**
     * Searches for network elements in the graph database based on network element
     * type and name.
     *
     * <p>
     * This method validates the input parameters and delegates the search operation
     * to the service layer. It handles various exceptions that might occur during
     * the search process and provides appropriate error responses.
     * 
     * If areaIpAddress or tag is provided, the method will search by area IP address or tag instead
     * of performing a vertex name search.
     *
     * @param neTypes the network element type to search for (optional if areaIpAddress or tag is provided),
     *                must not be null or empty if areaIpAddress and tag are not provided
     * @param neName  the network element name to search for (optional if areaIpAddress or tag is provided),
     *                must not be null or empty if areaIpAddress and tag are not provided
     * @param areaIpAddress the area IP address to search for (optional, can be null).
     *                      If provided, the method will search by area IP address instead
     * @param tag the tag to search for (optional, can be null).
     *            If provided, the method will search by tag instead
     * @return a list of maps containing the matching network elements and their
     *         properties. If areaIpAddress or tag is provided, returns list with links and nodes.
     * @throws NullPointerException      if neTypes or neName is null when areaIpAddress and tag are not provided
     * @throws InvalidInputException     if neTypes or neName is empty after
     *                                   trimming when areaIpAddress and tag are not provided
     * @throws ResourceNotFoundException if no matching network elements are found
     * @throws VertexNotFoundException   if there is an error during the search
     *                                   operation
     */
    @Override
    public List<Map<String, Object>> searchByNeTypeAndNeName(String neTypes, String neName, String areaIpAddress,
            String tag, String popId, String popName, String ipv4, String popCode, String region,
            String relation) {
        logger.info(
                "Received request to search vertices with neTypes: {}, neName: {}, areaIpAddress: {}, tag: {}, popId: {}, popName: {}, ipv4: {}, popCode: {}, region: {}, relation: {}",
                neTypes, neName, areaIpAddress, tag, popId, popName, ipv4, popCode, region, relation);

        try {
            List<Map<String, Object>> response = janusgraphService.searchByNeTypeAndNeName(neTypes, neName,
                    areaIpAddress, tag, popId, popName, ipv4, popCode, region, relation);
            logger.info("Vertices successfully fetched: {} entries", response.size());
            return response;

        } catch (InvalidParametersException e) {
            throw e;
        } catch (ResourceNotFoundException e) {
            logger.error(JanusGraphUtilsConstants.NOT_FOUND);
            throw e;
        } catch (InvalidInputException e) {
            logger.error("Invalid input for search");
            throw e;
        } catch (Exception e) {
            logger.error("Failed to fetch vertices");
            throw new VertexNotFoundException("Failed to fetch vertices: " + e.getMessage(), e);
        }
    }

    /**
     * Retrieves a count of vertices grouped by geographic state or region.
     *
     * <p>
     * This method validates the input geography level and delegates the counting
     * operation to the service layer.
     *
     * @param geographyLevel the geographic level to group by (e.g., "state",
     *                       "region")
     * @return a list of maps containing the count of vertices for each geographic
     *         area
     * @throws InvalidInputException   if the geography level is invalid
     * @throws DataProcessingException if there is an error processing the data
     * @throws VertexNotFoundException if there is an error retrieving the vertices
     * @throws NullPointerException    if geographyLevel is null
     */
    @Override
    public List<Map<String, Object>> stateWiseCount(String geographyLevel) {
        Objects.requireNonNull(geographyLevel, "Geography level cannot be null");
        if (geographyLevel.trim().isEmpty()) {
            throw new InvalidInputException("Geography level cannot be empty");
        }

        logger.info("Received request to stateWiseCount vertices with data");
        try {
            List<Map<String, Object>> response = janusgraphService.stateWiseCount(geographyLevel);
            logger.info("Vertices successfully fetched: {} entries", response.size());
            return response;
        } catch (InvalidInputException e) {
            logger.error("Invalid geography level");
            throw e;
        } catch (DataProcessingException e) {
            logger.error("Data processing error while fetching state-wise count");
            throw e;
        } catch (Exception e) {
            logger.error("Failed to fetch vertices");
            throw new VertexNotFoundException("Failed to fetch state-wise count: " + e.getMessage(), e);
        }
    }

    /**
     * Retrieves a count of vertices grouped by network element type and geography.
     *
     * <p>
     * This method validates the input request payload and delegates the counting
     * operation to the service layer.
     *
     * @param requestPayload a map containing the network element types and
     *                       geographic type
     * @return a list of maps containing the count of vertices for each network
     *         element type and geographic area
     * @throws InvalidInputException   if any input parameters are invalid
     * @throws DataProcessingException if there is an error processing the data
     * @throws VertexNotFoundException if there is an error retrieving the vertices
     * @throws NullPointerException    if requestPayload or its required fields are
     *                                 null
     */
    @Override
    public List<Map<String, Object>> neTypeWiseCount(Map<String, Object> requestPayload) {
        Objects.requireNonNull(requestPayload, "Request payload cannot be null");
        if (requestPayload.isEmpty()) {
            throw new InvalidInputException("Request payload cannot be empty");
        }

        List<String> neTypes = (List<String>) requestPayload.get("neTypes");
        String geographyLevel = (String) requestPayload.get("geographicType");

        Objects.requireNonNull(neTypes, "Network element types list cannot be null");
        Objects.requireNonNull(geographyLevel, "Geographic type cannot be null");

        if (neTypes.isEmpty()) {
            throw new InvalidInputException("Network element types list cannot be empty");
        }
        if (geographyLevel.trim().isEmpty()) {
            throw new InvalidInputException("Geographic type cannot be empty");
        }

        logger.info("Received request to fetch neTypeWiseCount with neTypes count: {}", neTypes.size());
        logger.info("Received request to fetch neTypeWiseCount with geography level");
        try {
            return janusgraphService.neTypeWiseCount(requestPayload);
        } catch (InvalidInputException e) {
            logger.error("Invalid input for NE type count");
            throw e;
        } catch (DataProcessingException e) {
            logger.error("Data processing error while fetching NE type count");
            throw e;
        } catch (Exception e) {
            logger.error("Failed to fetch NE type count");
            throw new VertexNotFoundException("Failed to fetch NE type count: " + e.getMessage(), e);
        }
    }

    /**
     * Creates multiple vertices in the graph database from a list of network
     * elements.
     *
     * <p>
     * This method validates the input network elements list and processes each
     * element
     * to create corresponding vertices. It aggregates the responses from individual
     * vertex creations.
     *
     * @param networkElements a list of network element wrappers to create as
     *                        vertices
     * @return a list of maps containing information about the created vertices
     * @throws InvalidInputException   if the network elements list is empty
     * @throws VertexCreationException if there is an error creating the vertices
     * @throws NullPointerException    if networkElements or any individual element
     *                                 is null
     */
    @Override
    public List<Map<String, Object>> createMultipleVertices(List<Map<String, Object>> networkElements) {
        Objects.requireNonNull(networkElements, "Network elements list cannot be null");

        if (networkElements.isEmpty()) {
            throw new InvalidInputException("Network elements list cannot be empty");
        }

        logger.info("Received request to process {} vertices", networkElements.size());

        try {
            List<Map<String, Object>> responseList = janusgraphService.createMultipleVertices(networkElements);

            logger.info("Vertices successfully processed: {}", responseList);
            return responseList;
        } catch (VertexCreationException e) {
            logger.error("Failed to process vertices");
            throw e;
        } catch (RuntimeException e) {
            logger.error("Unexpected error while processing vertices");
            throw new VertexCreationException("Failed to process multiple vertices: " + e.getMessage(), e);
        }
    }

    /**
     * Downloads all vertex data from the graph database in the specified format.
     *
     * <p>
     * This method validates the input format and delegates the download operation
     * to the service layer. It handles various exceptions that might occur during
     * the process and provides appropriate error responses.
     * </p>
     *
     * @param format the format for data export (csv, xml, json)
     * @param opmode the operation mode specifying what data to export, defaults to
     *               "both"
     * @return ResponseEntity containing the exported data
     * @throws IllegalArgumentException if format is not supported
     * @throws DataExportException      if there is an error during data export
     */
    @Override
    public ResponseEntity<byte[]> downloadAllData(String format, String opmode) {
        logger.info("Download request received for format: {}, opmode: {}", format, opmode);

        try {
            byte[] fileBytes = janusgraphService.downloadAllData(format, opmode);
            String fileName = "janusgraph_" + opmode.toLowerCase() + "." + format.toLowerCase();

            HttpHeaders headers = new HttpHeaders();
            headers.setContentDisposition(ContentDisposition.builder("attachment").filename(fileName).build());

            switch (format.toLowerCase()) {
                case "json":
                    headers.setContentType(MediaType.APPLICATION_JSON);
                    break;
                case "csv":
                    headers.setContentType(MediaType.valueOf("text/csv"));
                    break;
                case "pdf":
                    headers.setContentType(MediaType.APPLICATION_PDF);
                    break;
                default:
                    logger.warn("Unsupported format received: {}", format);
                    return ResponseEntity.status(HttpStatus.UNSUPPORTED_MEDIA_TYPE).build();
            }

            return new ResponseEntity<>(fileBytes, headers, HttpStatus.OK);

        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().build();
        } catch (Exception e) {
            logger.error("Download failed: {}", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    /**
     * Retrieves count of vertices or edges based on the operation mode.
     *
     * <p>
     * This method validates the operation mode parameter and delegates the count
     * operation to the service layer. It handles various exceptions that might
     * occur
     * during the process and provides appropriate error responses.
     * </p>
     *
     * @param requestPayload map containing "opmode" and "filters" for the count
     *                       operation
     * @return the count as a Long value
     * @throws IllegalArgumentException if opmode is not "vertex" or "edge"
     * @throws DataProcessingException  if there is an error during the count
     *                                  operation
     */
    @Override
    public ResponseEntity<Object> getTopologyDataCount(Map<String, Object> requestPayload) {
        logger.info("Received request to get count with payload: {}", requestPayload);

        try {
            String opmode = (String) requestPayload.get(JanusGraphUtilsConstants.OPMODE);
            @SuppressWarnings("unchecked")
            Map<String, Object> filters = (Map<String, Object>) requestPayload.get(JanusGraphUtilsConstants.FILTERS);

            if (opmode == null || opmode.trim().isEmpty()) {
                throw new IllegalArgumentException("Operation mode cannot be null or empty");
            }

            Long result = janusgraphService.getTopologyDataCount(opmode, filters);
            logger.info("Successfully retrieved count for opmode {}: {}", opmode, result);
            return ResponseEntity.ok(result);
        } catch (IllegalArgumentException e) {
            logger.error("Invalid operation mode provided: {}", e.getMessage());
            Map<String, String> errorResponse = new HashMap<>();
            errorResponse.put(JanusGraphUtilsConstants.ERROR, "Invalid operation mode: " + e.getMessage());
            errorResponse.put(JanusGraphUtilsConstants.STATUS, "400");
            return ResponseEntity.badRequest().body(errorResponse);
        } catch (DataProcessingException e) {
            logger.error("Data processing error while getting count: {}", e.getMessage());
            Map<String, String> errorResponse = new HashMap<>();
            errorResponse.put(JanusGraphUtilsConstants.ERROR, "Data processing error: " + e.getMessage());
            errorResponse.put(JanusGraphUtilsConstants.STATUS, "500");
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        } catch (Exception e) {
            logger.error("Unexpected error while getting count: {}", e.getMessage(), e);
            Map<String, String> errorResponse = new HashMap<>();
            errorResponse.put(JanusGraphUtilsConstants.ERROR, "Unexpected error occurred while processing request");
            errorResponse.put(JanusGraphUtilsConstants.STATUS, "500");
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }

    /**
     * Searches for edges based on flexible start and end property matching.
     *
     * @param payload a list of maps containing the start and end properties for
     *                edge search
     * @return a list of maps containing the matching edges and their properties
     */
    @Override
    public List<Map<String, Object>> getFilterData(List<String> payload, String filter) {
        Objects.requireNonNull(payload, "Payload cannot be null");

        if (payload.isEmpty()) {
            throw new InvalidInputException("Payload cannot be empty");
        }

        logger.info("Received request to search edges with payload: {} and filter: {}", payload, filter);

        try {
            List<Map<String, Object>> responseList = janusgraphService.getFilterData(payload, filter);

            logger.info("Edges successfully fetched: {}", responseList.size());
            return responseList;
        } catch (RuntimeException e) {
            logger.error("Unexpected error while searching edges");
            throw new EdgeSearchException("Failed to search edges: " + e.getMessage(), e);
        }
    }

    /**
     * Retrieves all links (edges) that have the given lspName and the nodes
     * corresponding to start/end of those links. Response shape matches
     * getFilterData: list of one map with "links" and "nodes".
     *
     * @param lspName the LSP name to filter edges by (required)
     * @return list containing one map with "links" (edges with lspName) and "nodes" (vertices for start/end)
     */
    @Override
    public List<Map<String, Object>> getFilterLspData(String lspName) {
        logger.info("Received request for LSP filtered data with lspName: {}", lspName);
        try {
            return janusgraphService.getFilterLspData(lspName);
        } catch (RuntimeException e) {
            logger.error("Unexpected error while fetching LSP filtered data");
            throw new EdgeSearchException("Failed to fetch LSP filtered data: " + e.getMessage(), e);
        }
    }

    /**
     * Retrieves the maximum modification time for vertices and edges.
     *
     * @return a map containing the maximum modification time for vertices and edges
     */
    @Override
    public Map<String, String> getMaxModificationTime() {
        logger.info("Received request for max modification time");
        return janusgraphService.getMaxModificationTime();
    }

    /**
     * Retrieves alarm-based nodes from JanusGraph.
     *
     * @param wrapper optional filter for alarms (nullable)
     * @return list of alarm-based node details from JanusGraph
     */
    @Override
    public List<Map<String, Object>> getAlarmBasedNodes(AlarmMonitoringRequestFilterWrapper wrapper) {
        return janusgraphService.getAlarmBasedNodes(wrapper);
    }

    /**
     * Retrieves topology detail data (vertices and/or edges) based on operation
     * mode and filters.
     * <p>
     * This method supports flexible filtering with comma-separated values (OR
     * logic) and
     * semicolon-separated values (AND logic). The operation mode determines what
     * data to return:
     * - "vertex": returns only vertex data
     * - "edge": returns only edge data
     * - "both": returns both vertex and edge data
     * </p>
     * 
     * @param requestPayload map containing "opmode" and "filters" for the topology
     *                       detail operation
     * @param lLimit         lower limit for pagination (optional)
     * @param uLimit         upper limit for pagination (optional)
     * @return list of maps containing the topology detail data
     */
    @Override
    public Map<String, Object> getTopologyDetail(Map<String, Object> requestPayload, Integer lLimit, Integer uLimit) {
        logger.info("Received request for topology detail with payload: {}, lLimit: {}, uLimit: {}", requestPayload,
                lLimit, uLimit);

        if (requestPayload == null || requestPayload.isEmpty()) {
            throw new InvalidInputException("Request payload cannot be null or empty");
        }

        String opmode = (String) requestPayload.get(JanusGraphUtilsConstants.OPMODE);
        @SuppressWarnings("unchecked")
        Map<String, Object> filters = (Map<String, Object>) requestPayload.get(JanusGraphUtilsConstants.FILTERS);

        // Set default opmode to "vertex" if not provided
        if (opmode == null || opmode.trim().isEmpty()) {
            opmode = JanusGraphUtilsConstants.MODE_VERTEX;
            logger.info("No opmode provided, using default: vertex");
        }

        try {
            List<Map<String, Object>> result = janusgraphService.getTopologyDetail(opmode, filters, lLimit, uLimit);
            logger.info("Successfully retrieved topology detail with {} items (lLimit: {}, uLimit: {})", result.size(),
                    lLimit, uLimit);

            // Wrap results in structured format
            Map<String, Object> response = new HashMap<>();
            if (JanusGraphUtilsConstants.MODE_VERTEX.equalsIgnoreCase(opmode.trim())) {
                response.put("nodes", result);
            } else if ("edge".equalsIgnoreCase(opmode.trim())) {
                response.put("links", result);
            }

            return response;
        } catch (Exception e) {
            logger.error("Error in getTopologyDetail");
            throw new DataProcessingException("Failed to get topology detail: " + e.getMessage(), e);
        }
    }

    /**
     * Retrieves count of topology detail data (vertices and/or edges) based on
     * operation mode and filters.
     * <p>
     * This method supports the same flexible filtering logic as getTopologyDetail
     * but returns
     * only the count of matching records instead of the actual data.
     * </p>
     * 
     * @param requestPayload map containing "opmode" and "filters" for the count
     *                       operation
     * @return the count as a Long value
     */
    @Override
    public Long getTopologyDetailCount(Map<String, Object> requestPayload) {
        logger.info("Received request for topology detail count with payload: {}", requestPayload);

        if (requestPayload == null || requestPayload.isEmpty()) {
            throw new InvalidInputException("Request payload cannot be null or empty");
        }

        String opmode = (String) requestPayload.get(JanusGraphUtilsConstants.OPMODE);
        @SuppressWarnings("unchecked")
        Map<String, Object> filters = (Map<String, Object>) requestPayload.get(JanusGraphUtilsConstants.FILTERS);

        // Set default opmode to "vertex" if not provided
        if (opmode == null || opmode.trim().isEmpty()) {
            opmode = JanusGraphUtilsConstants.MODE_VERTEX;
            logger.info("No opmode provided, using default: vertex");
        }

        try {
            Long count = janusgraphService.getTopologyDetailCount(opmode, filters);
            logger.info("Successfully retrieved topology detail count: {}", count);

            return count;
        } catch (Exception e) {
            logger.error("Error in getTopologyDetailCount");
            throw new DataProcessingException("Failed to get topology detail count: " + e.getMessage(), e);
        }
    }

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
    @Override
    public ResponseEntity<byte[]> downloadCsvFile(String fileName) {
        logger.info("Received request to download CSV file: {}", fileName);

        if (fileName == null || fileName.trim().isEmpty()) {
            throw new IllegalArgumentException("File name cannot be null or empty");
        }

        try {
            byte[] csvContent = janusgraphService.downloadCsvFile(fileName);
            
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.parseMediaType("text/csv"));
            headers.setContentDispositionFormData("attachment", fileName);
            headers.setCacheControl("no-cache, no-store, must-revalidate");
            headers.setPragma("no-cache");
            headers.setExpires(0);
            
            logger.info("Successfully generated CSV file: {} with {} bytes", fileName, csvContent.length);
            
            return ResponseEntity.ok()
                    .headers(headers)
                    .body(csvContent);
                    
        } catch (IllegalArgumentException e) {
            logger.error("Invalid file name for CSV download");
            throw e;
        } catch (ResourceNotFoundException e) {
            logger.error("CSV file not found");
            throw e;
        } catch (Exception e) {
            logger.error("Error downloading CSV file");
            throw new DataProcessingException("Failed to download CSV file: " + fileName, e);
        }
    }

    /**
     * Retrieves all links (edges) and their connected nodes (vertices) based on area IP address and/or tag.
     * 
     * <p>
     * This method searches for all edges that have the specified areaIpAddress and/or tag property value,
     * and returns those edges along with their connected start and end vertices.
     * If both parameters are provided, edges must match both conditions (AND logic).
     * </p>
     * 
     * @param areaIpAddress the area IP address to search for (optional, can be null)
     * @param tag the tag to search for (optional, can be null)
     * @return a list containing a single map with "links" (edges) and "nodes" (vertices) arrays
     * @throws IllegalArgumentException if both areaIpAddress and tag are null or empty
     * @throws DataProcessingException if there is an error processing the data
     */
    @Override
    public List<Map<String, Object>> getLinksByAreaIpAddress(String areaIpAddress, String tag) {
        logger.info("Received request to get links by areaIpAddress: {}, tag: {}", areaIpAddress, tag);

        try {
            List<Map<String, Object>> response = janusgraphService.getLinksByAreaIpAddress(areaIpAddress, tag);
            logger.info("Successfully retrieved links and nodes for areaIpAddress: {}, tag: {}", areaIpAddress, tag);
            return response;
        } catch (InvalidParametersException e) {
            throw e;
        } catch (IllegalArgumentException e) {
            logger.error("Invalid parameters - areaIpAddress: {}, tag: {}", areaIpAddress, tag);
            throw e;
        } catch (Exception e) {
            throw new DataProcessingException("Failed to get links: " + e.getMessage());
        }
    }

    /**
     * Deletes vertices and edges based on current date.
     * 
     * <p>
     * This method automatically uses the current date and time to delete all vertices and edges
     * that have a lastDiscoveryTime property value less than the current date/time.
     * Deletes vertices first, commits, then deletes edges and commits again.
     * </p>
     * 
     * @return a map containing the status of the deletion operation
     * @throws DataProcessingException if there is an error during deletion
     */
    @Override
    public Map<String, Object> deleteByLastDiscoveryTime() {
        logger.info("Received request to delete vertices and edges with lastDiscoveryTime less than current date/time");

        try {
            Map<String, Object> response = janusgraphService.deleteByLastDiscoveryTime();
            logger.info("Successfully deleted vertices and edges with lastDiscoveryTime less than current date/time");
            return response;
        } catch (DataProcessingException e) {
            logger.error("Data processing error during deletion");
            throw e;
        } catch (Exception e) {
            logger.error("Unexpected error during deletion");
            throw new DataProcessingException("Failed to delete vertices and edges: " + e.getMessage(), e);
        }
    }
}
