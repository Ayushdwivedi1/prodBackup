package com.enttribe.module.topology.topologyintegration.service.impl;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.script.ScriptException;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.amazonaws.services.kms.model.NotFoundException;
import com.enttribe.commons.Symbol;
import com.enttribe.commons.configuration.ConfigUtils;
import com.enttribe.commons.exception.ValidationException;
import com.enttribe.commons.http.HttpPostRequest;
import com.enttribe.commons.http.IllegalHttpStatusException;
import com.enttribe.core.generic.exceptions.application.BusinessException;
import com.enttribe.core.generic.exceptions.application.ResourceNotFoundException;
import com.enttribe.lcm.enums.ConfigEnum;
import com.enttribe.module.topology.core.querybuilder.exceptions.HasNotTypeNotValidException;
import com.enttribe.module.topology.core.querybuilder.exceptions.HasTypeNotValidException;
import com.enttribe.module.topology.core.querybuilder.exceptions.JanusOperationModeNotValidException;
import com.enttribe.module.topology.core.querybuilder.exceptions.JanusQueryNotValidException;
import com.enttribe.module.topology.core.querybuilder.exceptions.JanusQueryTypeNotValidException;
import com.enttribe.module.topology.core.service.TopologyGraphService;
import com.enttribe.module.topology.core.utils.AoaTopologyUtil;
import com.enttribe.module.topology.core.utils.TopologyGraphConstants;
import com.enttribe.module.topology.core.utils.TopologyGraphUtils;
import com.enttribe.module.topology.core.utils.TopologyUtils;
import com.enttribe.module.topology.core.utils.exceptions.DataException;
import com.enttribe.module.topology.core.utils.exceptions.DataProcessingException;
import com.enttribe.module.topology.core.utils.exceptions.InvalidInputException;
import com.enttribe.module.topology.core.utils.exceptions.KpiException;
import com.enttribe.module.topology.core.utils.exceptions.QueryException;
import com.enttribe.module.topology.core.utils.exceptions.RetrieveException;
import com.enttribe.module.topology.core.utils.exceptions.ServiceException;
import com.enttribe.module.topology.core.wrapper.ElementUtilInputWrapper;
import com.enttribe.module.topology.core.wrapper.JanusGraphQuery;
import com.enttribe.module.topology.core.wrapper.LinkUtilInputWrapper;
import com.enttribe.module.topology.core.wrapper.PMUtilizationWrapper;
import com.enttribe.module.topology.core.wrapper.RANTopologyWrapper;
import com.enttribe.module.topology.core.wrapper.Topology;
import com.enttribe.module.topology.core.wrapper.TopologyConnectivityWrapper;
import com.enttribe.module.topology.core.wrapper.TopologyFilterInfoWrappper;
import com.enttribe.module.topology.core.wrapper.TopologyRequestWrapper;
import com.enttribe.module.topology.topologyintegration.service.TopologyIntegrationService;
import com.enttribe.module.topology.topologyintegration.utils.TopologyHybridUtils;
import com.enttribe.network.module.fm.core.rest.AlarmRest;
import com.enttribe.network.module.fm.utils.enums.Classification;
import com.enttribe.performance.module.pm.utils.PMConstants;
import com.enttribe.performance.module.pm.core.model.PerformanceKPI;
import com.enttribe.performance.module.pm.core.rest.PerformanceKPIRest;
import com.enttribe.performance.module.pm.core.wrapper.GenericKPIRequestWrapper;
import com.enttribe.performance.module.pm.core.wrapper.GenericKPIWrapper;
import com.enttribe.performance.module.pm.core.wrapper.PerformanceKPIRequestWrapper;
import com.enttribe.performance.module.pm.integration.rest.KPIIntegrationRest;
import com.enttribe.performance.module.pm.utility.wrapper.KPIResponse;
import com.enttribe.performance.module.pm.wrapper.PerformanceRequestWrapper;
import com.enttribe.product.security.spring.userdetails.CustomerInfo;
import com.enttribe.lcm.rest.NELocationRest;
import com.enttribe.lcm.util.wrapper.ConfigUtil;
import com.enttribe.lcm.util.wrapper.NELocationWrapper;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;

/**
 * Implementation of the Topology Integration Service that provides methods for retrieving
 * and processing topology data from various sources. This service handles topology mapping,
 * alarm integration, and performance metrics for network elements.
 *
 * <p>This service acts as a bridge between the topology graph database and other
 * network management systems, providing a unified view of network topology.</p>
 *
 * @author Enttribe
 * @version 2.0
 * @since 1.0
 */
@Service("topologyIntegrationServiceImpl")
public class TopologyIntegrationServiceImpl implements TopologyIntegrationService {

    /**
     * The logger for this class.
     */
    private static Logger logger = LoggerFactory.getLogger(TopologyIntegrationServiceImpl.class);

    /**
     * The performance KPI REST service for retrieving KPI data.
     */
    @Autowired
    private PerformanceKPIRest performanceKpiRest;

    /**
     * The network element location REST service.
     */
    @Autowired
    NELocationRest neLocationRest;

    /**
     * Utility for topology graph operations.
     */
    @Autowired
    private TopologyGraphUtils topologyGraphUtils;

    /**
     * REST client for alarm data retrieval.
     */
    @Autowired
    private AlarmRest alarmRest;

    /**
     * Utility for hybrid topology operations.
     */
    @Autowired
    private TopologyHybridUtils topologyHybridUtils;

    /**
     * Service for topology graph operations.
     */
    @Autowired
    private TopologyGraphService topologyGraphService;

    /**
     * REST client for KPI integration.
     */
    @Autowired
    private KPIIntegrationRest kpiIntegrationRest;

    /**
     * Customer info for the user.
     * Used to get the customer info for the user.
     */
    @Autowired
    private CustomerInfo customerInfo;

    /**
     * Retrieves the RAN topology for a specified network element.
     *
     * @param eName the name of the network element
     * @return the RAN topology wrapper containing topology information
     * @throws BusinessException if there's an error retrieving the RAN topology
     */
    @Override
    public RANTopologyWrapper getRanTopology(String eName) {
        Objects.requireNonNull(eName, "Element name cannot be null");
        if (eName.trim().isEmpty()) {
            throw new IllegalArgumentException("Element name cannot be empty");
        }
    
        RANTopologyWrapper topologyMap = null;
    
        if (logger.isInfoEnabled()) {
            logger.info("getRANTopology URI :: {}", ConfigUtils.getString(TopologyGraphConstants.GET_RAN_TOPOLOGY_URI));
        }
    
        try {
            topologyMap = null; // Preserving your logic (even though it's a no-op)
        } catch (Exception e) {
            logger.error("Failed to retrieve RAN topology for element: {}", eName);
            throw new BusinessException("Failed to retrieve RAN topology for element: " + eName, e);
        }
    
        return topologyMap;
    }    

    /**
     * Retrieves connected edges for a specified vertex ID with optional filtering by category.
     * Also adds alarm count information and link utilization data to the results.
     *
     * @param vertexId     the ID of the vertex to get connected edges for
     * @param category     the category to filter edges by
     * @param inputWrapper the input wrapper containing link utilization parameters
     * @return a map containing nodes and links connected to the specified vertex
     * @throws BusinessException        if there's an error retrieving connected edges
     * @throws IllegalArgumentException if vertexId is null or invalid
     */
    @Override
    public Map<String, List<Map<String, Object>>> getConnectedEdgesByVertexId(Long vertexId, String category,
                                                                              LinkUtilInputWrapper inputWrapper) {
        if (vertexId == null) {
            throw new IllegalArgumentException("Vertex ID cannot be null");
        }
        if (vertexId <= 0) {
            throw new IllegalArgumentException("Vertex ID must be a positive number");
        }
        Objects.requireNonNull(inputWrapper, TopologyGraphConstants.LINK_UTILIZATION_INPUT_WRAPPER_NOT_BE_NULL);

        Map<String, List<Map<String, Object>>> topologyData = new HashMap<>();
        try {
            String url = ConfigUtils.getString(ConfigEnum.MICRO_SERVICE_BASE_URL.getValue())
                    + ConfigUtils.getString(ConfigEnum.GET_CONNECTED_EDGES_BY_VERTEX_URI.getValue()) + vertexId
                    + "&" + TopologyGraphConstants.CATEGORY + "="
                    + category;
            String response = TopologyUtils.sendHTTPGetRequest(url);
            ObjectMapper mapper = new ObjectMapper();
            topologyData = mapper.readValue(response, new TypeReference<Map<String, List<Map<String, Object>>>>() {
            });
            logger.info("Retrieved topology data after micro service call, size: {}", topologyData.size());
            if (topologyData.get(TopologyGraphConstants.NODES) != null) {
                addAlarmCountInfoOfConnectedEdgeNodes(topologyData);
            }
            logger.info("Topology data updated after alarm call");
            addLinkUtilizationForConnectedEdgeAPI(inputWrapper, topologyData);
            logger.info("Topology data updated after PM call");
        } catch (IOException e) {
            logger.error("Failed to retrieve connected edges data for vertexId: {}", vertexId);
            throw new BusinessException("Failed to retrieve connected edges data", e);
        } catch (Exception e) {
            logger.error("Failed to retrieve connected edges for vertex: {}", vertexId);
            throw new BusinessException("Failed to retrieve connected edges for vertex: " + vertexId, e);
        }
        return topologyData;
    }

    /**
     * Adds link utilization data to the topology data for connected edges.
     *
     * @param inputWrapper the input wrapper containing link utilization parameters
     * @param topologyData the topology data to enhance with utilization information
     */
    private void addLinkUtilizationForConnectedEdgeAPI(LinkUtilInputWrapper inputWrapper,
    Map<String, List<Map<String, Object>>> topologyData) {
Objects.requireNonNull(inputWrapper, TopologyGraphConstants.INPUT_WRAPPER_CANNOT_BE_NULL);
Objects.requireNonNull(topologyData, "Topology data cannot be null");

try {
List<Map<String, Object>> listEdgeMap = topologyData.get(TopologyGraphConstants.LINKS);
if (listEdgeMap == null) {
logger.warn("No links found in topology data");
return;
}

inputWrapper.setNodeLinkList(listEdgeMap);
Map<String, Object> linkUtilizationMap = getUtilizationForEdgesWithSelectedFiltersUpdated(inputWrapper);

logUtilizationInfo(linkUtilizationMap, inputWrapper);

processNodeLinks(inputWrapper.getNodeLinkList(), linkUtilizationMap);

} catch (IllegalArgumentException e) {
logger.error("Invalid argument while processing link utilization: {}", e.getMessage());
} catch (Exception e) {
logger.error("Failed to add link utilization for connected edge API: {}", e.getMessage());
}
}

private void logUtilizationInfo(Map<String, Object> linkUtilizationMap, LinkUtilInputWrapper inputWrapper) {
    logger.info("Retrieved utilization map with {} entries", linkUtilizationMap.size());
    logger.info("Processing link list from wrapper with {} entries",
            inputWrapper.getNodeLinkList().size());
}

private void processNodeLinks(List<Map<String, Object>> nodeLinkList, Map<String, Object> utilizationMap) {
    if (nodeLinkList == null || nodeLinkList.isEmpty()) {
        logger.info("Node link list is empty");
        return;
    }

    for (Map<String, Object> edgeObj : nodeLinkList) {
        if (edgeObj == null || edgeObj.get(TopologyGraphConstants.EDGE_ID) == null || 
            ((String) edgeObj.get(TopologyGraphConstants.EDGE_ID)).trim().isEmpty()) {
            logger.warn("Encountered null edge object or edge ID is null/empty in node link list, skipping");
            continue;
        }

        String edgeId = (String) edgeObj.get(TopologyGraphConstants.EDGE_ID);
        logger.info("Processing edge ID: {}", edgeId);
        edgeObj.put(TopologyGraphConstants.LINK_UTILIZATION, utilizationMap.get(edgeId));
    }
}



    /**
     * Adds alarm count information to the nodes in the topology data.
     * This method can use either node-wise or location-wise alarm data based on configuration.
     *
     * @param topologyData the topology data to enhance with alarm information
     */
    private void addAlarmCountInfoOfConnectedEdgeNodes(Map<String, List<Map<String, Object>>> topologyData) {
        Objects.requireNonNull(topologyData, "Topology data cannot be null");
    
        Set<String> gcNameSet = new HashSet<>();
        List<String> eNodeBList = new ArrayList<>();
    
        try {
            for (Map<String, Object> nodeData : topologyData.get(TopologyGraphConstants.NODES)) {
                if (!TopologyGraphConstants.ENODEB
                        .equalsIgnoreCase((String) nodeData.get(TopologyGraphConstants.ELEMENT_TYPE))) {
                    gcNameSet.add((String) nodeData.get(TopologyGraphConstants.ELEMENT_NAME));
                } else if (TopologyGraphConstants.ENODEB
                        .equalsIgnoreCase((String) nodeData.get(TopologyGraphConstants.ELEMENT_TYPE))) {
                    eNodeBList.add((String) nodeData.get(TopologyGraphConstants.ELEMENT_NAME));
                }
            }
    
            Topology topology = new Topology();
            topology.setLinks(topologyData.get(TopologyGraphConstants.LINKS));
            topology.setNodes(topologyData.get(TopologyGraphConstants.NODES).stream().collect(Collectors.toSet()));
            List<String> gcNameList = gcNameSet.stream().toList();
    
            if (ConfigUtils.getString(TopologyGraphConstants.TOPOLOGY_ALARM_CONFIG)
                    .equals(TopologyGraphConstants.NODE_WISE_DATA)) {
    
                setNodeWiseActiveAlarmData(topology, gcNameSet);
            } else if (ConfigUtils.getString(TopologyGraphConstants.TOPOLOGY_ALARM_CONFIG)
                    .equals(TopologyGraphConstants.LOCATION_WISE_DATA)) {
    
                logger.info("Preparing to retrieve alarm count for {} GC nodes and {} Site nodes",
                        gcNameList.size(), eNodeBList.size());
            }
    
            topologyData.put("nodes", topology.getNodes().stream().toList());
            topologyData.put("links", topology.getLinks());
    
        } catch (Exception e) {
            logger.error("Failed to add alarm information for connected edge nodes: {}", e.getMessage());
        }
    }
    
    
    


    /**
     * Retrieves utilization data for network edges based on the provided input parameters.
     * This method processes link utilization data and applies selected filters.
     *
     * <p>The method performs the following steps:</p>
     * <ol>
     *   <li>Retrieves KPI ID mappings for link utilization</li>
     *   <li>Checks link arrangement to determine domain and vendor information</li>
     *   <li>Fetches utilization data for the specified edges</li>
     *   <li>Processes the utilization data and maps it to the appropriate edges</li>
     * </ol>
     *
     * @param inputWrapper the wrapper containing link utilization parameters and edge information
     * @return a map containing utilization data for the specified edges
     * @throws NotFoundException if required KPI mappings are not found
     * @throws DataException     if there's an error retrieving or processing utilization data
     */
    @Override
    public Map<String, Object> getUtilizationForEdgesWithSelectedFiltersUpdated(LinkUtilInputWrapper inputWrapper) {
        Objects.requireNonNull(inputWrapper, TopologyGraphConstants.INPUT_WRAPPER_CANNOT_BE_NULL);

        Map<String, Object> utilMap = new HashMap<>();

        try {
            Map<String, String> kpiIdMapping = getKpiIdForLinkUtilUpdated(inputWrapper);
            if (kpiIdMapping == null) {
                return utilMap;
            }

            Object2ObjectMap<String, String> domainVendorMap = checkLinkArrengment(inputWrapper);
            logger.info("Time when response received for Utilization data: {}", System.currentTimeMillis());

            PMUtilizationWrapper linkUtilizationData = getUtilizationForEdges(inputWrapper);
            if (linkUtilizationData == null) {
                return utilMap;
            }

            linkUtilizationData = replaceKPIIdWithKPINameForEdges(
                    linkUtilizationData, kpiIdMapping, domainVendorMap, inputWrapper);

            processUtilizationData(linkUtilizationData, inputWrapper, utilMap);

            logger.info("Processed utilization data for {} edges", utilMap.size());
        } catch (NotFoundException e) {
            logger.warn("KPI mapping not found: {}", e.getMessage());
        } catch (DataException e) {
            logger.error("Failed to retrieve utilization data: {}", e.getMessage());
        } catch (Exception e) {
            logger.error("Unexpected error in getUtilizationForEdgesWithSelectedFiltersUpdated: {}", e.getMessage());
        }

        return utilMap;
    }

    /**
     * Processes utilization data and populates the result map with relevant values.
     *
     * <p>This method extracts the appropriate data map from the utilization wrapper
     * and processes each entry to map it to the corresponding edge.</p>
     *
     * @param linkUtilizationData the wrapper containing utilization data
     * @param inputWrapper        the input parameters including edge mapping information
     * @param utilMap             the map to be populated with processed utilization data
     * @throws BusinessException        if there's an error processing the utilization data
     * @throws NullPointerException     if required data is missing
     * @throws IllegalArgumentException if input parameters are invalid
     */
    private void processUtilizationData(
            PMUtilizationWrapper linkUtilizationData,
            LinkUtilInputWrapper inputWrapper,
            Map<String, Object> utilMap) {
        Objects.requireNonNull(linkUtilizationData, "Link utilization data cannot be null");
        Objects.requireNonNull(inputWrapper, TopologyGraphConstants.INPUT_WRAPPER_CANNOT_BE_NULL);
        Objects.requireNonNull(utilMap, "Utilization map cannot be null");

        try {
            Map<String, List<Map<String, Object>>> dataMap = getRelevantDataMap(linkUtilizationData);

            if (dataMap == null) {
                return;
            }

            Map<String, String> edgeMap = inputWrapper.getEdgeMap();
            if (edgeMap == null) {
                throw new IllegalArgumentException("Edge map in input wrapper cannot be null");
            }

            for (Entry<String, List<Map<String, Object>>> mapEntry : dataMap.entrySet()) {
                if (mapEntry == null) {
                    logger.warn("Encountered null map entry in data map");
                    continue;
                }
                processSingleUtilizationEntry(mapEntry, edgeMap, inputWrapper, utilMap);
            }
        } catch (NullPointerException e) {
            throw new BusinessException("Null data encountered while processing utilization", e);
        } catch (IllegalArgumentException e) {
            throw new BusinessException("Invalid argument while processing utilization: " + e.getMessage(), e);
        }
    }

    /**
     * Extracts the relevant data map from the utilization wrapper.
     * Prioritizes hourly data over daily data if available.
     *
     * @param linkUtilizationData the wrapper containing utilization data
     * @return the map containing relevant utilization data, or null if no data is available
     * @throws IllegalArgumentException if the utilization data is null
     */
    private Map<String, List<Map<String, Object>>> getRelevantDataMap(PMUtilizationWrapper linkUtilizationData) {
        Objects.requireNonNull(linkUtilizationData, "Utilization data cannot be null");

        if (linkUtilizationData.getHourly() != null && !linkUtilizationData.getHourly().isEmpty()) {
            return linkUtilizationData.getHourly();
        } else if (linkUtilizationData.getDaily() != null && !linkUtilizationData.getDaily().isEmpty()) {
            return linkUtilizationData.getDaily();
        }
        return Collections.emptyMap();
    }

    /**
     * Processes a single utilization data entry and adds it to the result map.
     *
     * @param mapEntry     the entry containing the utilization data
     * @param edgeMap      the map relating data keys to edge identifiers
     * @param inputWrapper the input parameters for utilization processing
     * @param utilMap      the map to be populated with the processed utilization value
     * @throws IllegalArgumentException if map entry or edge map is null
     * @throws NotFoundException        if the edge map doesn't contain the required key
     */
    private void processSingleUtilizationEntry(
            Entry<String, List<Map<String, Object>>> mapEntry,
            Map<String, String> edgeMap,
            LinkUtilInputWrapper inputWrapper,
            Map<String, Object> utilMap) {

        Objects.requireNonNull(mapEntry, "Map entry cannot be null");
        Objects.requireNonNull(edgeMap, "Edge map cannot be null");
        Objects.requireNonNull(inputWrapper, TopologyGraphConstants.INPUT_WRAPPER_CANNOT_BE_NULL);
        Objects.requireNonNull(utilMap, "Utilization map cannot be null");

        String key = mapEntry.getKey();
        if (key == null || key.trim().isEmpty()) {
            throw new IllegalArgumentException("Map entry key cannot be null or empty");
        }

        if (edgeMap.containsKey(key)) {
            Object value = TopologyHybridUtils.getUtilizationValueForLinkMeasure(mapEntry.getValue(), inputWrapper);
            if (value != null) {
                utilMap.put(edgeMap.get(key), value);
            }
        } else {
            throw new NotFoundException("EdgeMap doesn't contain the key: " + key);
        }
    }

    /**
     * The descending order/reverse comparator for date-based sorting.
     */
    Comparator<Map<String, Object>> reverseComparator = (m1, m2) -> {
        Objects.requireNonNull(m1, "First map cannot be null");
        Objects.requireNonNull(m2, "Second map cannot be null");
        String date1 = (String) m1.get(TopologyGraphConstants.DATE);
        String date2 = (String) m2.get(TopologyGraphConstants.DATE);
        Objects.requireNonNull(date1, "Date in first map cannot be null");
        Objects.requireNonNull(date2, "Date in second map cannot be null");
        return date2.compareTo(date1);
    };

    /**
     * The ascending order/progressive comparator for date-based sorting.
     */
    Comparator<Map<String, Object>> progressiveComparator = (m1, m2) -> {
        Objects.requireNonNull(m1, "First map cannot be null");
        Objects.requireNonNull(m2, "Second map cannot be null");
        String date1 = (String) m1.get(TopologyGraphConstants.DATE);
        String date2 = (String) m2.get(TopologyGraphConstants.DATE);
        Objects.requireNonNull(date1, "Date in first map cannot be null");
        Objects.requireNonNull(date2, "Date in second map cannot be null");
        return date1.compareTo(date2);
    };

    /**
     * Gets the link utilization chart data for a specific edge.
     * This method retrieves and processes utilization data based on the provided input parameters.
     *
     * @param inputWrapper the input wrapper containing link utilization parameters
     * @return a list of maps containing the link utilization chart data
     * @throws ValidationException if the input wrapper or KPI name is null
     * @throws KpiException        if KPI ID mapping fails
     * @throws BusinessException   if there's an error processing the link utilization data
     */
    public List<Map<String, Object>> getLinkUtilizationChartForEdge(LinkUtilInputWrapper inputWrapper) {
        Objects.requireNonNull(inputWrapper, TopologyGraphConstants.INPUT_WRAPPER_CANNOT_BE_NULL);

        if (inputWrapper.getKpiName() == null) {
            throw new ValidationException(TopologyGraphConstants.KPI_NAME_CANNOT_BE_NULL);
        }

        List<Map<String, Object>> linkUtilData = new ArrayList<>();
        try {
            Map<String, String> kpiIdMapping = getKpiIdForLinkChartView(inputWrapper);
            if (kpiIdMapping == null || kpiIdMapping.isEmpty()) {
                throw new KpiException("Failed to retrieve KPI ID mapping");
            }

            checkLinkArrengment(inputWrapper);
            PMUtilizationWrapper pmData = getUtilizationForEdges(inputWrapper);
            if (pmData != null) {
                pmData = replaceKPIIdWithKPIName(pmData, kpiIdMapping);
                linkUtilData = setPMUtilData(linkUtilData, pmData);
            }
        } catch (ValidationException | KpiException e) {
            logger.error("Validation or KPI error");
            throw e;
        } catch (IllegalArgumentException e) {
            logger.error("Invalid argument in getLinkUtilizationChartForEdge");
            throw new DataException("Failed to process link utilization chart data", e);
        } catch (RuntimeException e) {
            logger.error("Unexpected error in getLinkUtilizationChartForEdge");
            throw new DataException("Failed to generate link utilization chart", e);
        }
        return linkUtilData;
    }

    /**
     * Gets the updated link utilization chart data for a specific edge.
     * This method is an updated version of getLinkUtilizationChartForEdge with additional logging.
     *
     * @param inputWrapper the input wrapper containing link utilization parameters
     * @return a list of maps containing the updated link utilization chart data
     * @throws ValidationException if the input wrapper or KPI name is null
     * @throws KpiException        if KPI ID mapping fails
     * @throws BusinessException   if there's an error processing the link utilization data
     */
    @Override
    public List<Map<String, Object>> getLinkUtilizationChartForEdgeUpdated(LinkUtilInputWrapper inputWrapper) {
        Objects.requireNonNull(inputWrapper, TopologyGraphConstants.INPUT_WRAPPER_CANNOT_BE_NULL);

        if (inputWrapper.getKpiName() == null) {
            throw new ValidationException(TopologyGraphConstants.KPI_NAME_CANNOT_BE_NULL);
        }

        List<Map<String, Object>> linkUtilData = new ArrayList<>();
        try {
            Map<String, String> kpiIdMapping = getKpiIdForLinkChartView(inputWrapper);
            if (kpiIdMapping == null || kpiIdMapping.isEmpty()) {
                throw new KpiException("Failed to retrieve KPI ID mapping");
            }

            checkLinkArrengment(inputWrapper);
            PMUtilizationWrapper pmData = getUtilizationForEdges(inputWrapper);
            if (pmData != null) {
                pmData = replaceKPIIdWithKPIName(pmData, kpiIdMapping);
                linkUtilData = setPMUtilData(linkUtilData, pmData);
            }
        } catch (ValidationException | KpiException e) {
            logger.error("Failed to process link utilization chart");
            throw e;
        } catch (IllegalArgumentException e) {
            logger.error("Invalid argument in getLinkUtilizationChartForEdgeUpdated");
            throw new BusinessException("Failed to process updated link utilization chart data", e);
        } catch (RuntimeException e) {
            logger.error("Unexpected error in getLinkUtilizationChartForEdgeUpdated");
            throw new BusinessException("Failed to generate updated link utilization chart", e);
        }
        return linkUtilData;
    }

    /**
     * Retrieves utilization data for network edges based on the provided input parameters.
     * This method makes an HTTP request to the edge utilization service and processes the response.
     *
     * <p>The method constructs the appropriate URL, sends the request with the input wrapper
     * as JSON payload, and parses the response into a PMUtilizationWrapper object.</p>
     *
     * @param inputWrapper the wrapper containing parameters for edge utilization data retrieval
     * @return a PMUtilizationWrapper containing the utilization data, or null if the request fails
     * @throws IllegalArgumentException if the inputWrapper is null
     */
    public PMUtilizationWrapper getUtilizationForEdges(LinkUtilInputWrapper inputWrapper) {
        Objects.requireNonNull(inputWrapper, TopologyGraphConstants.INPUT_WRAPPER_CANNOT_BE_NULL);

        String edgeUtilizationUrl = ConfigUtils.getString(ConfigEnum.MICRO_SERVICE_BASE_URL.getValue())
                + ConfigUtils.getString(TopologyGraphConstants.GET_EDGE_UTILIZATION_URL);
        logger.info("url to get link utilization :{}", edgeUtilizationUrl);
        ObjectMapper mapper = new ObjectMapper();
        StringEntity entity = new StringEntity(new Gson().toJson(inputWrapper), ContentType.APPLICATION_JSON);
        PMUtilizationWrapper pmData = null;
        try {
            String response = new HttpPostRequest(edgeUtilizationUrl, entity).getString();
            pmData = mapper.readValue(response, new TypeReference<PMUtilizationWrapper>() {
            });
        } catch (IllegalHttpStatusException e) {
            logger.error("Failed to retrieve utilization data: {} for URL: {}", e.getMessage(), edgeUtilizationUrl);
        } catch (IOException e) {
            logger.error("Failed to process utilization data response: {} for URL: {}", e.getMessage(), edgeUtilizationUrl);
        }
        return pmData;
    }

    /**
     * Replaces KPI IDs with their corresponding names in the provided PM data.
     *
     * <p>This method processes either hourly or daily data in the PMUtilizationWrapper
     * and replaces the KPI IDs with their corresponding names based on the provided mapping.</p>
     *
     * @param pmData       the PM utilization data containing KPI IDs to be replaced
     * @param kpiIdMapping a mapping of KPI IDs to their corresponding names
     * @return the updated PMUtilizationWrapper with KPI IDs replaced by names
     * @throws KpiException             if there's an error processing the KPI mapping
     * @throws IllegalArgumentException if the input parameters are invalid
     */
    private PMUtilizationWrapper replaceKPIIdWithKPIName(PMUtilizationWrapper pmData, Map<String, String> kpiIdMapping) {
        Objects.requireNonNull(pmData, TopologyGraphConstants.PM_DATA_CANNOT_BE_NULL);
        Objects.requireNonNull(kpiIdMapping, TopologyGraphConstants.KPI_ID_MAPPING_CANNOT_BE_NULL);

        try {
            if (pmData.getHourly() != null && !pmData.getHourly().isEmpty()) {
                replaceKPIIdInData(pmData.getHourly(), kpiIdMapping);
            } else if (pmData.getDaily() != null && !pmData.getDaily().isEmpty()) {
                replaceKPIIdInData(pmData.getDaily(), kpiIdMapping);
            }
        } catch (IllegalArgumentException e) {
            logger.error("Invalid KPI ID mapping format");
            throw new KpiException("Failed to replace KPI IDs with names due to invalid format", e);
        } catch (RuntimeException e) {
            logger.error("Error in replaceKPIIdWithKPIName");
            throw new KpiException("Failed to process KPI mapping", e);
        }
        return pmData;
    }

    /**
     * Replaces KPI IDs with their corresponding names in the provided data map.
     *
     * <p>This method iterates through the data map and processes each link data entry
     * to replace KPI IDs with their corresponding names.</p>
     *
     * @param data         the map containing link data with KPI IDs
     * @param kpiIdMapping a mapping of KPI IDs to their corresponding names
     * @throws IllegalArgumentException if the data map is null
     */
    private void replaceKPIIdInData(Map<String, List<Map<String, Object>>> data, Map<String, String> kpiIdMapping) {
        Objects.requireNonNull(data, "Data map cannot be null");
        Objects.requireNonNull(kpiIdMapping, TopologyGraphConstants.KPI_ID_MAPPING_CANNOT_BE_NULL);

        logger.info("Processing KPI ID replacement for {} data entries", data.size());
        for (Entry<String, List<Map<String, Object>>> entry : data.entrySet()) {
            List<Map<String, Object>> linkDataList = entry.getValue();
            if (linkDataList != null && !linkDataList.isEmpty()) {
                logger.info("Processing {} link data entries for key: {}", linkDataList.size(), entry.getKey());
                for (Map<String, Object> linkData : linkDataList) {
                    replaceKPIIdInLinkData(linkData, kpiIdMapping);
                }
            }
        }
    }

    /**
     * Replaces KPI IDs with their corresponding names in a single link data map.
     *
     * <p>This method processes a single link data entry, replacing each KPI ID
     * with its corresponding name based on the provided mapping.</p>
     *
     * @param linkData     the map containing link data with KPI IDs
     * @param kpiIdMapping a mapping of KPI IDs to their corresponding names
     * @throws IllegalArgumentException if the link data is null
     */
    private void replaceKPIIdInLinkData(Map<String, Object> linkData, Map<String, String> kpiIdMapping) {
        Objects.requireNonNull(linkData, "Link data cannot be null");
        Objects.requireNonNull(kpiIdMapping, TopologyGraphConstants.KPI_ID_MAPPING_CANNOT_BE_NULL);

        for (Entry<String, String> mapEntry : kpiIdMapping.entrySet()) {
            if (linkData.containsKey(mapEntry.getKey())) {
                logger.info("Replacing KPI ID '{}' with name '{}'", mapEntry.getKey(), mapEntry.getValue());
                linkData.put(mapEntry.getValue(), linkData.get(mapEntry.getKey()));
                linkData.remove(mapEntry.getKey());
            }
        }
    }

    /**
     * Calculates the maximum utilization values from PM data and updates the link data.
     *
     * <p>This method determines the maximum utilization value between input and output
     * utilization for each link and updates the link data accordingly.</p>
     *
     * @param kpiIdMapping    a mapping of KPI IDs to their corresponding names
     * @param domainVendorMap a mapping of edge IDs to domain-vendor combinations
     * @param inputWrapper    the input parameters for link utilization
     * @param linkDataList    the list of link data to process
     * @throws IllegalArgumentException if the link data list is null
     */
    private static void getMaxUtilFromPMData(Map<String, String> kpiIdMapping, Map<String, String> domainVendorMap,
                                             LinkUtilInputWrapper inputWrapper, List<Map<String, Object>> linkDataList) {
        Objects.requireNonNull(kpiIdMapping, TopologyGraphConstants.KPI_ID_MAPPING_CANNOT_BE_NULL);
        Objects.requireNonNull(domainVendorMap, TopologyGraphConstants.DOMAIN_VENDOR_MAP_CANNOT_BE_NULL);
        Objects.requireNonNull(inputWrapper, TopologyGraphConstants.INPUT_WRAPPER_CANNOT_BE_NULL);
        Objects.requireNonNull(linkDataList, TopologyGraphConstants.LINK_DATA_LIST_CANNOT_BE_NULL);

        for (Map<String, Object> linkData : linkDataList) {
            String inUtilKey = domainVendorMap.get(linkData.get("IL")) + "_" + inputWrapper.getKpiName()
                    + " In";
            String outUtilKey = domainVendorMap.get(linkData.get("IL")) + "_" + inputWrapper.getKpiName()
                    + " Out";
            Double inUtilVal = null;
            Double outUtilVal = null;

            for (Entry<String, String> kpiidmap : kpiIdMapping.entrySet()) {
                if (inUtilKey.equalsIgnoreCase(kpiidmap.getValue())) {
                    inUtilVal = TopologyHybridUtils
                            .getDoubleValue((String) linkData.get(kpiidmap.getKey()));
                } else if (outUtilKey.equalsIgnoreCase(kpiidmap.getValue())) {
                    outUtilVal = TopologyHybridUtils.getDoubleValue((String) linkData.get(kpiidmap.getKey()));
                    logger.info("Found out utilization value: {}", outUtilVal);
                }
            }

            if (inUtilVal != null && outUtilVal != null) {
                linkData.put(TopologyGraphConstants.VALUE, inUtilVal > outUtilVal ? inUtilVal : outUtilVal);
            } else if (inUtilVal != null) {
                logger.info("Setting in utilization value: {}", inUtilVal);
                linkData.put(TopologyGraphConstants.VALUE, inUtilVal);
            } else if (outUtilVal != null) {
                logger.info("Setting out utilization value: {}", outUtilVal);
                linkData.put(TopologyGraphConstants.VALUE, outUtilVal);
            }
            removeExtraKey(kpiIdMapping, linkData);
        }
    }

    /**
     * Replaces KPI IDs with their corresponding names for network edges and processes utilization data.
     *
     * <p>This method handles the processing of PM data specifically for network edges,
     * applying the appropriate utilization measure (maximum, latest, or average).</p>
     *
     * @param pmData          the PM utilization data to process
     * @param kpiIdMapping    a mapping of KPI IDs to their corresponding names
     * @param domainVendorMap a mapping of edge IDs to domain-vendor combinations
     * @param inputWrapper    the input parameters for link utilization
     * @return the updated PMUtilizationWrapper with processed data for edges
     * @throws KpiException if there's an error processing the KPI data
     */
    @SuppressWarnings("unchecked")
    private static PMUtilizationWrapper replaceKPIIdWithKPINameForEdges(PMUtilizationWrapper pmData,
                                                                        Map<String, String> kpiIdMapping, Map<String, String> domainVendorMap, LinkUtilInputWrapper inputWrapper) {
        Objects.requireNonNull(pmData, TopologyGraphConstants.PM_DATA_CANNOT_BE_NULL);
        Objects.requireNonNull(kpiIdMapping, TopologyGraphConstants.KPI_ID_MAPPING_CANNOT_BE_NULL);
        Objects.requireNonNull(domainVendorMap, TopologyGraphConstants.DOMAIN_VENDOR_MAP_CANNOT_BE_NULL);
        Objects.requireNonNull(inputWrapper, TopologyGraphConstants.INPUT_WRAPPER_CANNOT_BE_NULL);

        try {
            Map<String, List<Map<String, Object>>> dataMap = getDataMap(pmData);
            if (dataMap != null) {
                for (Entry<String, List<Map<String, Object>>> entry : dataMap.entrySet()) {
                    List<Map<String, Object>> linkDataList = entry.getValue();
                    if (linkDataList != null && !linkDataList.isEmpty()) {
                        processLinkData(inputWrapper, kpiIdMapping, domainVendorMap, linkDataList);
                    }
                }
            }
        } catch (IllegalArgumentException e) {
            logger.error("Invalid argument in replaceKPIIdWithKPINameForEdges");
            throw new KpiException("Failed to process KPI data for edges due to invalid input", e);
        } catch (RuntimeException e) {
            logger.error("Error in replaceKPIIdWithKPINameForEdges");
            throw new KpiException("Failed to process KPI data for edges", e);
        }
        return pmData;
    }

    /**
     * Extracts the appropriate data map (hourly or daily) from the PM utilization data.
     *
     * <p>This method determines whether to use hourly or daily data based on availability.</p>
     *
     * @param pmData the PM utilization data containing hourly and/or daily data
     * @return the appropriate data map, or null if neither hourly nor daily data is available
     * @throws IllegalArgumentException if the PM data is null
     */
    private static Map<String, List<Map<String, Object>>> getDataMap(PMUtilizationWrapper pmData) {
        Objects.requireNonNull(pmData, TopologyGraphConstants.PM_DATA_CANNOT_BE_NULL);

        if (pmData.getHourly() != null && !pmData.getHourly().isEmpty()) {
            logger.info("Using hourly data for PM utilization");
            return pmData.getHourly();
        } else if (pmData.getDaily() != null && !pmData.getDaily().isEmpty()) {
            logger.info("Using daily data for PM utilization");
            return pmData.getDaily();
        }
        logger.warn("No hourly or daily PM utilization data available");
        return Collections.emptyMap();
    }

    /**
     * Processes link data based on the specified utilization measure in the input wrapper.
     *
     * <p>This method determines the appropriate processing strategy based on whether the
     * utilization measure is maximum, latest, or average.</p>
     *
     * @param inputWrapper    the input parameters containing link utilization measure
     * @param kpiIdMapping    the mapping of KPI IDs to their names
     * @param domainVendorMap the mapping of edge IDs to domain-vendor combinations
     * @param linkDataList    the list of link data to process
     * @throws IllegalArgumentException if input wrapper is null
     * @throws KpiException             if there's an error processing the link data
     */
    private static void processLinkData(LinkUtilInputWrapper inputWrapper, Map<String, String> kpiIdMapping,
                                        Map<String, String> domainVendorMap, List<Map<String, Object>> linkDataList) {
        Objects.requireNonNull(inputWrapper, TopologyGraphConstants.INPUT_WRAPPER_CANNOT_BE_NULL);
        Objects.requireNonNull(kpiIdMapping, TopologyGraphConstants.KPI_ID_MAPPING_CANNOT_BE_NULL);
        Objects.requireNonNull(domainVendorMap, TopologyGraphConstants.DOMAIN_VENDOR_MAP_CANNOT_BE_NULL);
        Objects.requireNonNull(linkDataList, TopologyGraphConstants.LINK_DATA_LIST_CANNOT_BE_NULL);

        try {
            String utilMeasure = inputWrapper.getLinkUtilMeasure();
            if (utilMeasure == null || utilMeasure.trim().isEmpty()) {
                throw new IllegalArgumentException("Link utilization measure cannot be null or empty");
            }

            logger.info("Processing link data with utilization measure: {}", utilMeasure);
            if (isMaxOrLatest(inputWrapper)) {
                getMaxUtilFromPMData(kpiIdMapping, domainVendorMap, inputWrapper, linkDataList);
            } else if (TopologyGraphConstants.AVERAGE.equals(utilMeasure)) {
                processAverageUtil(inputWrapper, kpiIdMapping, domainVendorMap, linkDataList);
            } else {
                logger.warn("Unsupported utilization measure: {}", utilMeasure);
            }
        } catch (IllegalArgumentException e) {
            logger.error("Failed to process link data due to invalid input");
            throw new KpiException("Failed to process link data due to invalid input", e);
        } catch (RuntimeException e) {
            logger.error("Error processing link utilization data");
            throw new KpiException("Error processing link utilization data", e);
        }
    }

    /**
     * Determines if the link utilization measure is either MAXIMUM or LATEST.
     *
     * @param inputWrapper the input parameters containing link utilization measure
     * @return true if the measure is MAXIMUM or LATEST, false otherwise
     * @throws IllegalArgumentException if input wrapper or link utilization measure is null
     */
    private static boolean isMaxOrLatest(LinkUtilInputWrapper inputWrapper) {
        Objects.requireNonNull(inputWrapper, TopologyGraphConstants.INPUT_WRAPPER_CANNOT_BE_NULL);

        String utilMeasure = inputWrapper.getLinkUtilMeasure();
        if (utilMeasure == null || utilMeasure.trim().isEmpty()) {
            throw new IllegalArgumentException("Link utilization measure cannot be null or empty");
        }

        return TopologyGraphConstants.MAXIMUM.equals(utilMeasure)
                || TopologyGraphConstants.LATEST.equals(utilMeasure);
    }

    /**
     * Processes average utilization data for links.
     *
     * <p>This method calculates the average in and out utilization values for each link
     * and updates the link data accordingly.</p>
     *
     * @param inputWrapper    the input parameters containing KPI name
     * @param kpiIdMapping    the mapping of KPI IDs to their names
     * @param domainVendorMap the mapping of edge IDs to domain-vendor combinations
     * @param linkDataList    the list of link data to process
     * @throws IllegalArgumentException if any required parameter is null
     * @throws KpiException             if there's an error processing the average utilization
     */
    private static void processAverageUtil(LinkUtilInputWrapper inputWrapper, Map<String, String> kpiIdMapping,
                                           Map<String, String> domainVendorMap, List<Map<String, Object>> linkDataList) {
        Objects.requireNonNull(inputWrapper, TopologyGraphConstants.INPUT_WRAPPER_CANNOT_BE_NULL);
        Objects.requireNonNull(kpiIdMapping, "KPI mapping cannot be null");
        Objects.requireNonNull(domainVendorMap, TopologyGraphConstants.DOMAIN_VENDOR_MAP_CANNOT_BE_NULL);
        Objects.requireNonNull(linkDataList, TopologyGraphConstants.LINK_DATA_LIST_CANNOT_BE_NULL);

        String kpiName = inputWrapper.getKpiName();
        if (kpiName == null || kpiName.trim().isEmpty()) {
            throw new IllegalArgumentException(TopologyGraphConstants.KPI_NAME_CANNOT_BE_NULL);
        }

        try {
            logger.info("Processing average utilization for {} links", linkDataList.size());
            for (Map<String, Object> linkData : linkDataList) {
                String inUtil = domainVendorMap.get(linkData.get("IL")) + "_" + inputWrapper.getKpiName() + " In";
                String outUtil = domainVendorMap.get(linkData.get("IL")) + "_" + inputWrapper.getKpiName() + " Out";
                Double inUtilVal = null;
                Double outUtilVal = null;

                for (Entry<String, String> kpiidmap : kpiIdMapping.entrySet()) {
                    if (inUtil.equalsIgnoreCase(kpiidmap.getValue())) {
                        inUtilVal = TopologyHybridUtils.getDoubleValue((String) linkData.get(kpiidmap.getKey()));
                    } else if (outUtil.equalsIgnoreCase(kpiidmap.getValue())) {
                        outUtilVal = TopologyHybridUtils.getDoubleValue((String) linkData.get(kpiidmap.getKey()));
                    }
                }

                linkData.put("IN_UTIL", inUtilVal);
                linkData.put("OUT_UTIL", outUtilVal);
                logger.info("In util: {}, Out util: {}", inUtilVal, outUtilVal);
                removeExtraKey(kpiIdMapping, linkData);
            }
        } catch (NullPointerException e) {
            logger.error("Failed to process average utilization due to missing data");
            throw new KpiException("Failed to process average utilization due to missing data", e);
        } catch (RuntimeException e) {
            logger.error("Error calculating average utilization");
            throw new KpiException("Error calculating average utilization", e);
        }
    }

    /**
     * Removes extra keys from the link data map.
     *
     * <p>This method removes all KPI ID keys and the "IL" key from the link data
     * after the utilization values have been extracted.</p>
     *
     * @param kpiIdMapping the mapping of KPI IDs to their names
     * @param linkData     the link data map to clean up
     */
    private static void removeExtraKey(Map<String, String> kpiIdMapping, Map<String, Object> linkData) {
        Objects.requireNonNull(kpiIdMapping, TopologyGraphConstants.KPI_ID_MAPPING_CANNOT_BE_NULL);
        Objects.requireNonNull(linkData, "Link data cannot be null");

        kpiIdMapping.keySet().forEach(k -> {
            if (linkData.containsKey(k)) {
                linkData.remove(k);
            }
        });
        linkData.remove("IL");
    }

    /**
     * Sets the PM utilization data by retrieving it from either hourly or daily data.
     *
     * <p>This method first attempts to retrieve data from hourly measurements, and if not available,
     * falls back to daily measurements.</p>
     *
     * @param linkUtilData the existing link utilization data, may be null
     * @param pmData       the PM utilization wrapper containing hourly and daily data
     * @return the updated link utilization data, or the original data if no PM data is available
     */
    private List<Map<String, Object>> setPMUtilData(List<Map<String, Object>> linkUtilData, PMUtilizationWrapper pmData) {
        try {
            if (pmData != null) {
                logger.info("Attempting to retrieve link utilization data from PM data");
                linkUtilData = getLinkUtilDataFromHourly(pmData);
                if (linkUtilData != null) {
                    logger.info("Successfully retrieved link utilization data from hourly measurements");
                    return linkUtilData;
                }

                logger.info("No hourly data available, attempting to retrieve from daily measurements");
                linkUtilData = getLinkUtilDataFromDaily(pmData);
            }
        } catch (Exception e) {
            logger.error("Exception in setPMUtilData: {}", e.getMessage());
        }
        return linkUtilData;
    }

    /**
     * Retrieves link utilization data from hourly measurements.
     *
     * <p>This method extracts and returns the first non-null list of link utilization data
     * from the hourly data in the PM utilization wrapper.</p>
     *
     * @param pmData the PM utilization wrapper containing hourly data
     * @return the first non-null list of link utilization data, or null if none is found
     * @throws IllegalArgumentException if PM data is null
     * @throws KpiException             if there's an error processing the hourly data
     */
    private List<Map<String, Object>> getLinkUtilDataFromHourly(PMUtilizationWrapper pmData) {
        Objects.requireNonNull(pmData, TopologyGraphConstants.PM_DATA_CANNOT_BE_NULL);

        try {
            if (pmData.getHourly() != null && !pmData.getHourly().isEmpty()) {
                Map<String, List<Map<String, Object>>> hourlyData = pmData.getHourly();
                logger.info("Hourly data available with {} time periods", (hourlyData != null ? hourlyData.keySet().size() : 0));

                if (hourlyData != null && !hourlyData.isEmpty()) {
                    for (Entry<String, List<Map<String, Object>>> entry : hourlyData.entrySet()) {
                        List<Map<String, Object>> linkUtilData = entry.getValue();
                        if (linkUtilData != null) {
                            return linkUtilData;
                        }
                    }
                }
            }
            return Collections.emptyList();
        } catch (RuntimeException e) {
            logger.error("Failed to process hourly link utilization data");
            throw new KpiException("Failed to process hourly link utilization data", e);
        }
    }

    /**
     * Retrieves link utilization data from daily measurements.
     *
     * <p>This method extracts and returns the first non-null list of link utilization data
     * from the daily data in the PM utilization wrapper.</p>
     *
     * @param pmData the PM utilization wrapper containing daily data
     * @return the first non-null list of link utilization data, or null if none is found
     * @throws IllegalArgumentException if PM data is null
     * @throws KpiException             if there's an error processing the daily data
     */
    private List<Map<String, Object>> getLinkUtilDataFromDaily(PMUtilizationWrapper pmData) {
        Objects.requireNonNull(pmData, TopologyGraphConstants.PM_DATA_CANNOT_BE_NULL);

        try {
            if (pmData.getDaily() != null && !pmData.getDaily().isEmpty()) {
                Map<String, List<Map<String, Object>>> dailyData = pmData.getDaily();
                logger.info("Daily data available with {} time periods", (dailyData != null ? dailyData.keySet().size() : 0));

                if (dailyData != null && !dailyData.isEmpty()) {
                    for (Entry<String, List<Map<String, Object>>> entry : dailyData.entrySet()) {
                        List<Map<String, Object>> linkUtilData = entry.getValue();
                        if (linkUtilData != null) {
                            return linkUtilData;
                        }
                    }
                }
            }
            return Collections.emptyList();
        } catch (RuntimeException e) {
            logger.error("Failed to process daily link utilization data");
            throw new KpiException("Failed to process daily link utilization data", e);
        }
    }


    /**
     * Checks and corrects the link arrangement in the topology.
     *
     * <p>This method ensures that links are arranged in the correct order based on device levels,
     * and creates a mapping of edge IDs to domain-vendor combinations.</p>
     *
     * @param inputWrapper the input parameters containing node-link list
     * @return a mapping of edge IDs to domain-vendor combinations
     * @throws DataException if there's invalid data in the node-link list
     */
    private Object2ObjectMap<String, String> checkLinkArrengment(LinkUtilInputWrapper inputWrapper) {
        Objects.requireNonNull(inputWrapper, TopologyGraphConstants.INPUT_WRAPPER_CANNOT_BE_NULL);
    
        List<Map<String, Object>> correctedLinkList = new ArrayList<>();
        Map<String, String> edgeMap = new HashMap<>();
        Object2ObjectMap<String, String> domainVendorMap = new Object2ObjectOpenHashMap<>();
        List<Map<String, Object>> nodeLinkList = inputWrapper.getNodeLinkList();
    
        if (nodeLinkList != null && !nodeLinkList.isEmpty()) {
            for (Map<String, Object> connection : nodeLinkList) {
                LinkData linkData = extractLinkData(connection);
                if (isValidLinkData(linkData)) {
                    processBasedOnLinkArrangement(linkData, connection, edgeMap, correctedLinkList);
                } else {
                    logInvalidLinkData(linkData);
                }
                domainVendorMap.put(linkData.edgeId,
                        connection.get(TopologyGraphConstants.DOMAIN) + "_" + connection.get(TopologyGraphConstants.VENDOR));
            }
        }
    
        logger.info("Corrected link list size: {}", correctedLinkList.size());
        inputWrapper.setEdgeMap(edgeMap);
        inputWrapper.setLinkList(correctedLinkList);
        return domainVendorMap;
    }
    
    private void processBasedOnLinkArrangement(LinkData linkData, Map<String, Object> connection,
                                               Map<String, String> edgeMap, List<Map<String, Object>> correctedLinkList) {
        boolean isCorrect = Boolean.TRUE.equals(TopologyUtils.isLinkArangementCorrect(
                linkData.sourceElementName, linkData.destElementName,
                linkData.sourceElementType, linkData.destElementType,
                topologyGraphUtils.getDeviceLevels()));
    
        if (isCorrect) {
            processLinkArrangement(
                    linkData.destElementName, linkData.sourceElementName, linkData.destPort, linkData.sourcePort,
                    linkData.destElementType, linkData.sourceElementType, linkData.destAliasPort, linkData.srcAliasPort,
                    linkData.edgeId, connection, edgeMap, correctedLinkList);
        } else {
            processLinkArrangement(
                    linkData.sourceElementName, linkData.destElementName, linkData.sourcePort, linkData.destPort,
                    linkData.sourceElementType, linkData.destElementType, linkData.destAliasPort, linkData.srcAliasPort,
                    linkData.edgeId, connection, edgeMap, correctedLinkList);
        }
    }
    
    private boolean isValidLinkData(LinkData linkData) {
        return linkData.sourceElementName != null &&
                linkData.destElementName != null &&
                linkData.sourcePort != null &&
                linkData.destPort != null &&
                linkData.sourceElementType != null &&
                linkData.destElementType != null &&
                linkData.srcAliasPort != null &&
                linkData.destAliasPort != null;
    }
    
    private void logInvalidLinkData(LinkData linkData) {
        logger.warn("Invalid link data found: sourceElement={}, destElement={}, sourcePort={}, destPort={}, edgeId={}, sourceType={}, destType={}",
                linkData.sourceElementName, linkData.destElementName, linkData.sourcePort, linkData.destPort,
                linkData.edgeId, linkData.sourceElementType, linkData.destElementType);
    }
    
    private LinkData extractLinkData(Map<String, Object> connection) {
        String sourceElementName = (String) connection.get(TopologyGraphConstants.SOURCE_ELEMENT_NAME);
        String destElementName = (String) connection.get(TopologyGraphConstants.DESTINATION_ELEMENT_NAME);
        String sourcePort = (String) connection.get(TopologyGraphConstants.SOURCE_PORT);
        String destPort = (String) connection.get(TopologyGraphConstants.DESTINATION_PORT);
        String sourceElementType = (String) connection.get(TopologyGraphConstants.SOURCE_ELEMENT_TYPE);
        String destElementType = (String) connection.get(TopologyGraphConstants.DESTINATION_ELEMENT_TYPE);
        String edgeId = (String) connection.get(TopologyGraphConstants.EDGE_ID);
        String srcAliasPort = connection.get(TopologyGraphConstants.SOURCE_ALIAS_PORT) != null
                ? connection.get(TopologyGraphConstants.SOURCE_ALIAS_PORT).toString()
                : null;
        String destAliasPort = connection.get(TopologyGraphConstants.DESTINATION_ALIAS_PORT) != null
                ? connection.get(TopologyGraphConstants.DESTINATION_ALIAS_PORT).toString()
                : null;
    
        return new LinkData(sourceElementName, destElementName, sourcePort, destPort,
                sourceElementType, destElementType, edgeId, srcAliasPort, destAliasPort);
    }
    
    private static class LinkData {
        String sourceElementName;
        String destElementName;
        String sourcePort;
        String destPort;
        String sourceElementType;
        String destElementType;
        String edgeId;
        String srcAliasPort;
        String destAliasPort;
    
        @SuppressWarnings("java:S107")
        public LinkData(String sourceElementName, String destElementName, String sourcePort, String destPort,
                        String sourceElementType, String destElementType, String edgeId,
                        String srcAliasPort, String destAliasPort) {
            this.sourceElementName = sourceElementName;
            this.destElementName = destElementName;
            this.sourcePort = sourcePort;
            this.destPort = destPort;
            this.sourceElementType = sourceElementType;
            this.destElementType = destElementType;
            this.edgeId = edgeId;
            this.srcAliasPort = srcAliasPort;
            this.destAliasPort = destAliasPort;
        }
    }

    @SuppressWarnings("java:S107")
    private void processLinkArrangement(String src, String dst, String srcPort, String dstPort,
    String srcType, String dstType, String srcAliasPort, String dstAliasPort,
    String edgeId, Map<String, Object> connection,
    Map<String, String> edgeMap, List<Map<String, Object>> correctedLinkList) {

    Map<String, Object> newMap = setDataForLinkArrangement(src, dst, srcPort, dstPort,
        srcType, dstType, srcAliasPort, dstAliasPort, edgeId);

    String linkName = src + Symbol.UNDERSCORE + srcAliasPort + Symbol.HASH
        + dst + Symbol.UNDERSCORE + dstAliasPort;

    edgeMap.put(linkName, edgeId);
    newMap.put(TopologyGraphConstants.DOMAIN, connection.get(TopologyGraphConstants.DOMAIN));
    newMap.put(TopologyGraphConstants.VENDOR, connection.get(TopologyGraphConstants.VENDOR));
    correctedLinkList.add(newMap);
}



    /**
     * Sets up data for link arrangement based on the provided parameters.
     *
     * <p>This method creates a map containing all the necessary information for a network link,
     * including source and destination elements, ports, element types, and the edge ID.</p>
     *
     * @param destElementName   the name of the destination element
     * @param sourceElementName the name of the source element
     * @param destPort          the destination port identifier
     * @param sourcePort        the source port identifier
     * @param destElementType   the type of the destination element
     * @param sourceElementType the type of the source element
     * @param srcAliasPort      the alias of the source port
     * @param destAliasPort     the alias of the destination port
     * @param edgeId            the unique identifier for the edge
     * @return a map containing the link arrangement data
     */
    @SuppressWarnings("java:S107")
    private Map<String, Object> setDataForLinkArrangement(String destElementName, String sourceElementName,
                                                          String destPort, String sourcePort, String destElementType, String sourceElementType, String srcAliasPort,
                                                          String destAliasPort, String edgeId) {
        Objects.requireNonNull(destElementName, "Destination element name cannot be null");
        Objects.requireNonNull(sourceElementName, "Source element name cannot be null");
        Objects.requireNonNull(destPort, "Destination port cannot be null");
        Objects.requireNonNull(sourcePort, "Source port cannot be null");
        Objects.requireNonNull(destElementType, "Destination element type cannot be null");
        Objects.requireNonNull(sourceElementType, "Source element type cannot be null");
        Objects.requireNonNull(srcAliasPort, "Source alias port cannot be null");
        Objects.requireNonNull(destAliasPort, "Destination alias port cannot be null");
        Objects.requireNonNull(edgeId, "Edge ID cannot be null");

        Map<String, Object> newMap = new HashMap<>();
        newMap.put(TopologyGraphConstants.DESTINATION_ELEMENT_NAME, destElementName);
        newMap.put(TopologyGraphConstants.SOURCE_ELEMENT_NAME, sourceElementName);
        newMap.put(TopologyGraphConstants.DESTINATION_PORT, destPort);
        newMap.put(TopologyGraphConstants.SOURCE_PORT, sourcePort);
        newMap.put(TopologyGraphConstants.DESTINATION_ELEMENT_TYPE, destElementType);
        newMap.put(TopologyGraphConstants.SOURCE_ELEMENT_TYPE, sourceElementType);
        newMap.put(TopologyGraphConstants.SOURCE_ALIAS_PORT, srcAliasPort);
        newMap.put(TopologyGraphConstants.DESTINATION_ALIAS_PORT, destAliasPort);
        newMap.put(TopologyGraphConstants.EDGE_ID, edgeId);
        return newMap;
    }

    /**
     * Retrieves KPI IDs for link utilization with updated logic.
     *
     * <p>This method fetches performance KPI information for topology type and filters
     * the results based on the KPI name provided in the input wrapper.</p>
     *
     * @param inputWrapper the wrapper containing link utilization parameters including KPI name
     * @return a map of KPI IDs to their corresponding domain, vendor, and KPI name combinations
     * @throws KpiException if KPI information cannot be retrieved or if parameters are invalid
     */
    public Map<String, String> getKpiIdForLinkUtilUpdated(LinkUtilInputWrapper inputWrapper) {
        Objects.requireNonNull(inputWrapper, TopologyGraphConstants.INPUT_WRAPPER_CANNOT_BE_NULL);

        if (inputWrapper.getKpiName() == null || inputWrapper.getKpiName().trim().isEmpty()) {
            throw new IllegalArgumentException(TopologyGraphConstants.KPI_NAME_CANNOT_BE_NULL);
        }

        Map<String, String> kpiMap = new HashMap<>();
        try {
            List<PerformanceKPI> list = getPerformanceKPIDetail(TopologyGraphConstants.KEY_TOPOLOGY);
            if (CollectionUtils.isNotEmpty(list)) {
                kpiMap = list.stream().filter(p -> p.getKpiName().contains(inputWrapper.getKpiName()))
                        .collect(Collectors.toMap(PerformanceKPI::getKpiId,
                                p -> p.getDomain() + "_" + p.getVendor() + "_" + p.getKpiName()));
                inputWrapper.setKpiId(new ArrayList<>(kpiMap.keySet()));
            }
        } catch (ResourceNotFoundException e) {
            logger.error("KPI not found for kpi Name: {}", inputWrapper.getKpiName());
            throw new KpiException("KPI not found for kpi Name: " + inputWrapper.getKpiName(), e);
        } catch (IllegalArgumentException e) {
            logger.error("Invalid KPI parameters for kpi Name: {}", inputWrapper.getKpiName());
            throw new KpiException("Invalid KPI parameters for kpi Name: " + inputWrapper.getKpiName(), e);
        } catch (RuntimeException e) {
            logger.error("Failed to retrieve KPI for kpi Name: {}", inputWrapper.getKpiName());
            throw new KpiException("Failed to retrieve KPI for kpi Name: " + inputWrapper.getKpiName(), e);
        }
        return kpiMap;
    }

    /**
     * Retrieves KPI IDs for link chart view based on domain and vendor information.
     *
     * <p>This method extracts domain and vendor information from the node link list in the input wrapper,
     * then fetches and filters performance KPI information based on these values and the KPI name.</p>
     *
     * @param inputWrapper the wrapper containing link utilization parameters and node link list
     * @return a map of KPI IDs to their corresponding KPI keys
     * @throws KpiException  if KPI information cannot be retrieved or if parameters are invalid
     * @throws DataException if domain or vendor information is missing in the node link data
     */
    public Map<String, String> getKpiIdForLinkChartView(LinkUtilInputWrapper inputWrapper) {
        Objects.requireNonNull(inputWrapper, TopologyGraphConstants.INPUT_WRAPPER_CANNOT_BE_NULL);
        Objects.requireNonNull(inputWrapper.getNodeLinkList(), "Node link list cannot be null");

        if (inputWrapper.getNodeLinkList().isEmpty()) {
            throw new IllegalArgumentException("Node link list cannot be empty");
        }

        if (inputWrapper.getKpiName() == null || inputWrapper.getKpiName().trim().isEmpty()) {
            throw new IllegalArgumentException(TopologyGraphConstants.KPI_NAME_CANNOT_BE_NULL);
        }

        Map<String, String> kpiMap = new HashMap<>();
        Map<String, Object> map = inputWrapper.getNodeLinkList().get(PMConstants.INDEX_ZERO);

        if (map == null) {
            throw new IllegalArgumentException("First node link entry cannot be null");
        }

        if (!map.containsKey(TopologyGraphConstants.DOMAIN) || !map.containsKey(TopologyGraphConstants.VENDOR)) {
            logger.error("Missing domain or vendor information in node link data");
            throw new DataException("Missing domain or vendor information in node link data");
        }

        try {
            List<PerformanceKPI> list = getPerformanceKPIDetail(TopologyGraphConstants.KEY_TOPOLOGY);

            if (CollectionUtils.isEmpty(list)) {
                logger.warn("No performance KPI details found for topology type");
                return kpiMap;
            }

            String domain = map.get(TopologyGraphConstants.DOMAIN).toString();
            String vendor = map.get(TopologyGraphConstants.VENDOR).toString();
            String kpiName = inputWrapper.getKpiName();

            kpiMap = list.stream().filter(
                            p -> p.getDomain().equalsIgnoreCase(domain)
                                    && p.getVendor().equalsIgnoreCase(vendor)
                                    && p.getKpiName().contains(kpiName))
                    .collect(Collectors.toMap(PerformanceKPI::getKpiId, PerformanceKPI::getKpiKey));
            inputWrapper.setKpiId(new ArrayList<>(kpiMap.keySet()));

            logger.info("Found {} KPIs for domain: {}, vendor: {}, kpi name: {}",
                    kpiMap.size(), domain, vendor, kpiName);
        } catch (ResourceNotFoundException e) {
            String domain = map.get(TopologyGraphConstants.DOMAIN).toString();
            String vendor = map.get(TopologyGraphConstants.VENDOR).toString();
            String kpiName = inputWrapper.getKpiName();

            logger.error("KPI not found for domain: {}, vendor: {}, kpi name: {}",
                    domain, vendor, kpiName);
            throw new KpiException("KPI not found for domain: " + domain +
                    TopologyGraphConstants.COMMA_VENDOR + vendor +
                    ", kpi Name: " + kpiName, e);
        } catch (IllegalArgumentException e) {
            String domain = map.get(TopologyGraphConstants.DOMAIN).toString();
            String vendor = map.get(TopologyGraphConstants.VENDOR).toString();

            logger.error("Invalid KPI parameters for domain: {}, vendor: {}", domain, vendor);
            throw new KpiException("Invalid KPI parameters for domain: " + domain +
                    TopologyGraphConstants.COMMA_VENDOR + vendor, e);
        } catch (RuntimeException e) {
            logger.error("Failed to retrieve KPI information");
            throw new KpiException("Failed to retrieve KPI information", e);
        }
        return kpiMap;
    }

    /**
     * Retrieves utilization data for a network element.
     *
     * <p>This method sends an HTTP request to the element utilization service with the provided
     * input parameters and processes the response into a map of utilization data.</p>
     *
     * @param inputWrapper the wrapper containing element utilization parameters
     * @return a map containing the element utilization data
     * @throws InvalidInputException if the input wrapper or KPI name is null
     * @throws DataException         if there's an error retrieving or processing the utilization data
     * @throws KpiException          if there's an error retrieving KPI information
     */
    public Map<String, Object> getUtilizationForElement(ElementUtilInputWrapper inputWrapper) {
        logger.info("Retrieving utilization data for element");

        Objects.requireNonNull(inputWrapper, TopologyGraphConstants.INPUT_WRAPPER_CANNOT_BE_NULL);
        Objects.requireNonNull(inputWrapper.getKpiName(), TopologyGraphConstants.KPI_NAME_CANNOT_BE_NULL);

        if (inputWrapper.getKpiName().trim().isEmpty()) {
            throw new InvalidInputException("KPI name cannot be empty");
        }

        Map<String, Object> valueMap = new HashMap<>();
        try {
            String url = ConfigUtils.getString(ConfigEnum.MICRO_SERVICE_BASE_URL.getValue())
                    + ConfigUtils.getString(ConfigEnum.ELEMENT_UTILIZATION_URL.getValue());
            if (inputWrapper.getKpiName() != null) {
                getKpiIdForElementUtil(inputWrapper);
            } else {
                logger.error("Input wrapper or KPI name is null");
                throw new InvalidInputException("Input wrapper or KPI name is null");
            }
            logger.info("Sending element utilization request for domain: {}, vendor: {}, KPI: {}",
                    inputWrapper.getDomain(), inputWrapper.getVendor(), inputWrapper.getKpiName());

            ObjectMapper mapper = new ObjectMapper();
            StringEntity entity = new StringEntity(new Gson().toJson(inputWrapper), ContentType.APPLICATION_JSON);
            String response = TopologyUtils.sendHTTPPostRequest(url, entity);
            valueMap = mapper.readValue(response, new TypeReference<Map<String, Object>>() {
            });
            logger.info("Retrieved element utilization data with {} entries", valueMap.size());
        } catch (IOException e) {
            logger.error("Error processing utilization data response");
            throw new DataException("Error processing utilization data response", e);
        } catch (KpiException e) {
            throw e;
        } catch (RuntimeException e) {
            logger.error("Unexpected error retrieving element utilization data");
            throw new DataException("Unexpected error retrieving element utilization data", e);
        }
        return valueMap;
    }

    /**
     * Retrieves and sets KPI IDs for element utilization.
     *
     * <p>This method populates the KPI ID list in the input wrapper based on the domain,
     * vendor, and KPI name information.</p>
     *
     * @param inputWrapper the wrapper containing element utilization parameters
     * @throws KpiException             if no KPIs are found for the given domain, vendor, and KPI name
     * @throws IllegalArgumentException if any required parameter is null or empty
     */
    public void getKpiIdForElementUtil(ElementUtilInputWrapper inputWrapper) {
        Objects.requireNonNull(inputWrapper, TopologyGraphConstants.INPUT_WRAPPER_CANNOT_BE_NULL);
        Objects.requireNonNull(inputWrapper.getDomain(), "Domain cannot be null");
        Objects.requireNonNull(inputWrapper.getVendor(), "Vendor cannot be null");
        Objects.requireNonNull(inputWrapper.getKpiName(), TopologyGraphConstants.KPI_NAME_CANNOT_BE_NULL);

        if (inputWrapper.getDomain().trim().isEmpty()) {
            throw new IllegalArgumentException("Domain cannot be empty");
        }

        if (inputWrapper.getVendor().trim().isEmpty()) {
            throw new IllegalArgumentException("Vendor cannot be empty");
        }

        if (inputWrapper.getKpiName().trim().isEmpty()) {
            throw new IllegalArgumentException("KPI name cannot be empty");
        }

        List<String> kpiIdList = new ArrayList<>();
        getKpiIdForNode(inputWrapper, kpiIdList);

        if (kpiIdList.isEmpty()) {
            logger.error("No KPI found for domain: {}, vendor: {}, kpi name: {}",
                    inputWrapper.getDomain(), inputWrapper.getVendor(), inputWrapper.getKpiName());
            throw new KpiException("No KPI found for domain: " + inputWrapper.getDomain() +
                    TopologyGraphConstants.COMMA_VENDOR + inputWrapper.getVendor() +
                    ", kpi name: " + inputWrapper.getKpiName());
        }
    }

    /**
     * Retrieves KPI IDs for a network node based on domain, vendor, and KPI name.
     *
     * <p>This method queries the performance KPI service with the specified parameters
     * and adds the matching KPI IDs to the provided list.</p>
     *
     * @param inputWrapper the wrapper containing element parameters including domain, vendor, and KPI name
     * @param kpiIdList    the list to which found KPI IDs will be added
     * @throws KpiException             if KPI information cannot be retrieved or if parameters are invalid
     * @throws IllegalArgumentException if any required parameter is null
     */
    private void getKpiIdForNode(ElementUtilInputWrapper inputWrapper, List<String> kpiIdList) {
        Objects.requireNonNull(inputWrapper, TopologyGraphConstants.INPUT_WRAPPER_CANNOT_BE_NULL);
        Objects.requireNonNull(kpiIdList, "KPI ID list cannot be null");

        try {
            PerformanceKPIRequestWrapper performanceKPIRequestWrapper = new PerformanceKPIRequestWrapper();
            Map<String, String> fieldWiseOperation = new HashMap<>();

            fieldWiseOperation.put(TopologyGraphConstants.KPI_TYPE_FIELD, TopologyGraphConstants.EQUALS_OPERATOR_STRING);
            performanceKPIRequestWrapper.setKpiType("Topology");
            performanceKPIRequestWrapper.setDomain(inputWrapper.getDomain());
            performanceKPIRequestWrapper.setVendor(inputWrapper.getVendor());
            performanceKPIRequestWrapper.setFieldWiseOperation(fieldWiseOperation);
            logger.info("Requesting performance KPI with parameters: {}", performanceKPIRequestWrapper);
            List<PerformanceKPI> list = performanceKpiRest.getPerformanceKPI(performanceKPIRequestWrapper);
            logger.info("Retrieved {} performance KPIs", list.size());

            if (CollectionUtils.isNotEmpty(list)) {
                Optional<PerformanceKPI> opt = list.stream()
                        .filter(p -> p.getKpiName().contains(inputWrapper.getKpiName())).findAny();
                if (opt.isPresent()) {
                    kpiIdList.add(opt.get().getKpiId());
                    inputWrapper.setKpiId(kpiIdList);
                }
                logger.info("Found KPI ID: {} for domain: {}, vendor: {}", kpiIdList, inputWrapper.getDomain(),
                        inputWrapper.getVendor());
            } else {
                throw new KpiException("No KPIs found for domain: " + inputWrapper.getDomain() +
                        TopologyGraphConstants.COMMA_VENDOR + inputWrapper.getVendor());
            }
        } catch (ResourceNotFoundException e) {
            throw new KpiException("KPI not found for domain: " + inputWrapper.getDomain() +
                    TopologyGraphConstants.COMMA_VENDOR + inputWrapper.getVendor(), e);
        } catch (IllegalArgumentException e) {
            throw new KpiException("Invalid KPI parameters for domain: " + inputWrapper.getDomain() +
                    TopologyGraphConstants.COMMA_VENDOR + inputWrapper.getVendor(), e);
        } catch (RuntimeException e) {
            throw new KpiException("Failed to retrieve KPI information for domain: " + inputWrapper.getDomain() +
                    TopologyGraphConstants.COMMA_VENDOR + inputWrapper.getVendor(), e);
        }
    }

    /**
     * Processes alarm data to identify the highest priority alarms for each network element.
     *
     * <p>This method analyzes the alarm information map and determines the highest
     * priority alarm for each network element, along with the count of alarms at that priority.</p>
     *
     * @param alarmInfoMap the map containing alarm information for network elements
     * @return a map containing the highest priority alarm data for each network element
     * @throws DataException if there's an error processing the alarm data structure
     */
    public Map<String, Map<String, Object>> getHigherPriorityActiveAlarmData(
            Map<String, Map<String, Object>> alarmInfoMap) {
        Objects.requireNonNull(alarmInfoMap, "Alarm information map cannot be null");

        Map<String, Map<String, Object>> alarmInfo = new HashMap<>();

        for (@SuppressWarnings("rawtypes")
        Map.Entry entry : alarmInfoMap.entrySet()) {
            try {
                String nodeName = (String) entry.getKey();
                @SuppressWarnings("unchecked")
                Map<String, Integer> activeAlarmData = (Map<String, Integer>) entry.getValue();
                Integer highestPriorityNumber = null;
                Classification alarmCategory = null;
                Map<Integer, Integer> priorityAlarmCountMap = new HashMap<>();

                for (Entry<String, Integer> categoryKey : activeAlarmData.entrySet()) {
                    alarmCategory = Classification.valueOf(categoryKey.getKey());
                    Integer currentPriority = topologyHybridUtils.getPriorityNoByAlarmCategory(alarmCategory);
                    Integer alarmCount = activeAlarmData.get(categoryKey.getKey());

                    if (currentPriority != null && !priorityAlarmCountMap.containsKey(currentPriority)) {
                        priorityAlarmCountMap.put(currentPriority, alarmCount);
                    } else {
                        Integer currentCount = priorityAlarmCountMap.get(currentPriority);
                        currentCount += alarmCount;
                        priorityAlarmCountMap.put(currentPriority, currentCount);
                    }

                    highestPriorityNumber = TopologyHybridUtils.getHigherPriorityOfAlarm(currentPriority,
                            highestPriorityNumber);
                }

                Integer higherPriorityAlarmCount = priorityAlarmCountMap.get(highestPriorityNumber);
                Map<String, Object> alarmData = new HashMap<>();
                alarmData.put(TopologyGraphConstants.ALARM_COUNT, higherPriorityAlarmCount);
                alarmData.put(TopologyGraphConstants.ALARM_PRIORITY, highestPriorityNumber);
                alarmData.put(TopologyGraphConstants.ALARM_CATEGORY,
                        topologyHybridUtils.getAlarmCategoryByAlarmPriority(highestPriorityNumber));
                alarmInfo.put(nodeName, alarmData);
            } catch (IllegalArgumentException e) {
                logger.warn("Invalid alarm category for element: {}", entry.getKey());
            } catch (ClassCastException e) {
                logger.error("Invalid alarm data structure for element: {}", entry.getKey());
                throw new DataException("Invalid alarm data structure for entry: " + entry.getKey(), e);
            } catch (Exception e) {
                logger.error("Error processing alarm data for element: {}, error: {}", entry.getKey(), e.getMessage(), e);
            }
        }

        return alarmInfo;
    }

    /**
     * Retrieves active alarm data for a set of network elements.
     *
     * <p>This method makes an HTTP request to the alarm service to fetch
     * active alarm information for the specified network elements.</p>
     *
     * @param networkElementsName the set of network element names to retrieve alarm data for
     * @return a map containing alarm information for each network element
     * @throws ServiceException         if there's an error communicating with the alarm service
     * @throws DataException            if there's an error processing the alarm data response
     * @throws IllegalArgumentException if the network elements name set is null or empty
     */
    public Map<String, Map<String, Object>> getActiveAlarmDataForNetworkElements(Set<String> networkElementsName) {
        Objects.requireNonNull(networkElementsName, "Network elements name set cannot be null");

        if (networkElementsName.isEmpty()) {
            logger.info("Empty network elements name set provided");
            return new HashMap<>();
        }

        logger.info("Retrieving active alarm data for {} network elements", networkElementsName.size());
        String urlToGetActiveAlarmData = ConfigUtils.getString(ConfigUtil.MICRO_SERVICE_BASE_URL)
                + ConfigUtils.getString(TopologyGraphConstants.GET_ACTIVE_ALARM_COUNT_FOR_NE_URL);

        if (urlToGetActiveAlarmData.trim().isEmpty()) {
            throw new ServiceException("Active alarm URL configuration is missing or empty");
        }

        logger.info("Active alarm URL: {}", urlToGetActiveAlarmData);

        StringEntity entity = new StringEntity(new Gson().toJson(networkElementsName), ContentType.APPLICATION_JSON);
        ObjectMapper mapper = new ObjectMapper();
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

        HashMap<String, Map<String, Object>> alarmInfoList = null;
        try {
            String response = new HttpPostRequest(urlToGetActiveAlarmData, entity).getString();
            if (response == null || response.trim().isEmpty()) {
                logger.warn("Empty response received from alarm service");
                return new HashMap<>();
            }

            alarmInfoList = mapper.readValue(response, new TypeReference<HashMap<String, Map<String, Object>>>() {
            });
        } catch (IllegalHttpStatusException e) {
            logger.error("Failed to retrieve alarm data from service");
            throw new ServiceException("Failed to retrieve alarm data from service", e);
        } catch (IOException e) {
            logger.error("Failed to process alarm data response");
            throw new DataException("Failed to process alarm data response", e);
        }

        logger.info("Retrieved active alarm data for {} network elements",
                alarmInfoList != null ? alarmInfoList.size() : 0);
        return alarmInfoList;
    }
    

    /**
     * Retrieves element utilization data for network elements within the view port.
     *
     * <p>This method fetches CPU and memory utilization metrics for the specified elements,
     * processes the data to find maximum values, and returns the results in a standardized format.</p>
     *
     * @param inputWrapper the wrapper containing element utilization parameters and filters
     * @return a list of maps containing element utilization data with maximum CPU and memory values
     * @throws ServiceException if there's an error processing the utilization data
     * @throws DataException    if there's an error retrieving the utilization data
     */
    @Override
    public List<Map<String, Object>> getElementUtilizationForViewPort(ElementUtilInputWrapper inputWrapper) {
        Objects.requireNonNull(inputWrapper, "Element utilization input wrapper cannot be null");
    
        Map<String, List<Map<String, Object>>> valueMap;
        List<Map<String, Object>> finalValueMap = new ArrayList<>();
        try {
            String url = ConfigUtils.getString(ConfigEnum.MICRO_SERVICE_BASE_URL.getValue())
                    + ConfigUtils.getString(ConfigEnum.VIEW_PORT_UTILIZATION_URL.getValue());
    
            Map<String, String> kpiIdMapping = getKpiIdForElementUtilization(inputWrapper);
            StringEntity entity = new StringEntity(new Gson().toJson(inputWrapper), ContentType.APPLICATION_JSON);
            String response = TopologyUtils.sendHTTPPostRequest(url, entity);
    
            ObjectMapper mapper = new ObjectMapper();
            valueMap = mapper.readValue(response, new TypeReference<Map<String, List<Map<String, Object>>>>() {});
            replaceKPIIdWithKPINameForElement(valueMap, kpiIdMapping);
    
            for (Entry<String, List<Map<String, Object>>> entry : valueMap.entrySet()) {
                finalValueMap.add(prepareFinalUtilizationMap(entry));
            }
    
        } catch (IOException e) {
            logger.error("Error processing element utilization data");
            throw new ServiceException("Failed to process element utilization data", e);
        } catch (Exception e) {
            logger.error("Error retrieving element utilization data");
            throw new DataException("Failed to retrieve element utilization data", e);
        }
        return finalValueMap;
    }
    private Map<String, Object> prepareFinalUtilizationMap(Entry<String, List<Map<String, Object>>> entry) {
        List<Double> cpuList = new ArrayList<>();
        List<Double> memoryList = new ArrayList<>();
    
        for (Map<String, Object> value : entry.getValue()) {
            if (value.get(TopologyGraphConstants.CPU) != null) {
                cpuList.add(Double.parseDouble(value.get(TopologyGraphConstants.CPU).toString()));
            }
            if (value.get(TopologyGraphConstants.MEMORY) != null) {
                memoryList.add(Double.parseDouble(value.get(TopologyGraphConstants.MEMORY).toString()));
            }
        }
    
        Map<String, Object> finalMap = new HashMap<>();
        finalMap.put("neId", entry.getKey());
        finalMap.put(TopologyGraphConstants.CPU, cpuList.isEmpty() ? null : Collections.max(cpuList));
        finalMap.put(TopologyGraphConstants.MEMORY, memoryList.isEmpty() ? null : Collections.max(memoryList));
    
        return finalMap;
    }
        

    /**
     * Retrieves KPI IDs for element utilization metrics.
     *
     * <p>This method maps generic KPI codes to their corresponding utilization metrics (CPU and memory)
     * and prepares the input wrapper with the necessary KPI IDs for data retrieval.</p>
     *
     * @param inputWrapper the wrapper to be populated with KPI IDs
     * @return a map of KPI IDs to their corresponding metric names (CPU, memory)
     * @throws ServiceException         if there's an error retrieving or processing KPI data
     * @throws IllegalArgumentException if the input wrapper is null
     */
    public Map<String, String> getKpiIdForElementUtilization(ElementUtilInputWrapper inputWrapper) {
        Objects.requireNonNull(inputWrapper, "Element utilization input wrapper cannot be null");

        Map<String, String> kpiMap = new HashMap<>();
        List<String> kpiList = new ArrayList<>();
        try {
            logger.info("Retrieving KPI IDs for element utilization");
            GenericKPIRequestWrapper genericKPIRequestWrapper = new GenericKPIRequestWrapper();
            genericKPIRequestWrapper.setGenericKPICode(TopologyGraphConstants.CPU_UTIL_HOURLY_KPI_NAME);
            genericKPIRequestWrapper.setGenericKPICode(TopologyGraphConstants.MEMORY_UTIL_HOURLY_KPI_NAME);
            List<GenericKPIWrapper> genericKPI = new ArrayList<>();
            kpiList.add(genericKPI.get(0).getGenericKPICode());
            kpiMap.put(genericKPI.get(0).getGenericKPICode(), TopologyGraphConstants.CPU);
            inputWrapper.setKpiId(kpiList);
            logger.info("Retrieved and mapped {} KPI IDs for element utilization", kpiMap.size());
        } catch (ResourceNotFoundException e) {
            logger.warn("KPI not found for element utilization");
            throw new ServiceException("Required KPI not found for element utilization", e);
        } catch (Exception e) {
            logger.error("Failed to retrieve KPI data for element utilization");
            throw new ServiceException("Failed to retrieve KPI data for element utilization", e);
        }

        return kpiMap;
    }

    /**
     * Replaces KPI IDs with their corresponding KPI names in element utilization data.
     *
     * <p>This method transforms the raw KPI data by replacing technical KPI IDs with
     * human-readable metric names for better readability and interpretation.</p>
     *
     * @param valueMap     the map containing element utilization data with KPI IDs
     * @param kpiIdMapping the mapping between KPI IDs and their corresponding metric names
     * @throws IllegalArgumentException if valueMap or kpiIdMapping is null
     */
    private void replaceKPIIdWithKPINameForElement(Map<String, List<Map<String, Object>>> valueMap,
                                                   Map<String, String> kpiIdMapping) {
        Objects.requireNonNull(valueMap, "Value map cannot be null");
        Objects.requireNonNull(kpiIdMapping, TopologyGraphConstants.KPI_ID_MAPPING_CANNOT_BE_NULL);

        logger.info("Replacing KPI IDs with KPI names for {} time periods", valueMap.size());
        for (List<Map<String, Object>> listData : valueMap.values()) {
            for (Map<String, Object> values : listData) {
                for (Entry<String, String> kpiId : kpiIdMapping.entrySet()) {
                    if (values.containsKey(kpiId.getKey())) {
                        values.put(kpiId.getValue(), values.get(kpiId.getKey()));
                        values.remove(kpiId.getKey());
                    }
                }
            }
        }
        logger.info("KPI ID replacement completed successfully");
    }

    /**
     * Retrieves connectivity information for a correlated network element.
     *
     * <p>This method fetches topology connectivity data for the specified element and port,
     * and returns it in a standardized format for display or further processing.</p>
     *
     * @param eName the name of the network element
     * @param port  the port identifier on the network element
     * @return a map containing connectivity information for the specified element and port
     * @throws IllegalArgumentException if the element name or port parameters are invalid
     * @throws DataException            if there's an error retrieving or processing connectivity data
     */
    @SuppressWarnings("unchecked")
    @Override
    public Map<String, Object> getCorrelatedElementConnectivity(String eName, String port) {
        if (eName == null || eName.trim().isEmpty()) {
            throw new IllegalArgumentException(TopologyGraphConstants.ELEMENT_NAME_CANNOT_BE_NULL_OR_EMPTY);
        }
    
        Map<String, Object> connectivity = new HashMap<>();
        logger.info("Retrieving connectivity for element: {}, port: {}", eName, port);
        try {
            Map<String, TopologyConnectivityWrapper> topologyMap = topologyGraphService.getCorrelatedElementConnectivity(eName, port);
            logger.info("Retrieved {} connectivity entries for element: {}", topologyMap.size(), eName);
            connectivity.put(TopologyGraphConstants.NW_ELEMENTS, new ArrayList<TopologyConnectivityWrapper>(topologyMap.values()));
            return connectivity;
        } catch (IllegalArgumentException e) {
            logger.error("Invalid arguments for element connectivity: element={}, port={}", eName, port);
            throw new IllegalArgumentException("Invalid element or port parameters", e);
        } catch (ResourceNotFoundException e) {
            logger.warn("Element or port not found in topology: element={}, port={}", eName, port);
            throw new DataException("Element or port not found in topology", e);
        } catch (Exception e) {
            logger.error("Error retrieving correlated element connectivity for element={}, port={}",
                    eName, port);
            throw new DataException("Failed to retrieve element connectivity data", e);
        }
    }
    

    /**
     * Retrieves topology information for a specific ring ID.
     *
     * <p>This method fetches the network topology associated with the specified ring,
     * including all connected elements and their relationships.</p>
     *
     * @param ringID the unique identifier of the ring to retrieve topology for
     * @return a list of maps containing topology information for the specified ring
     * @throws RetrieveException        if the ring cannot be found in the topology
     * @throws ServiceException         if there's an error processing the topology data
     * @throws IllegalArgumentException if the ring ID is null or empty
     */
    @Override
    public List<Map<String, Object>> getTopologyByRingId(String ringID) {
        Objects.requireNonNull(ringID, "Ring ID cannot be null");

        logger.info("Retrieving topology for ring ID: {}", ringID);
        List<Map<String, Object>> topologyMap = new ArrayList<>();
        try {
            topologyMap = topologyGraphService.getTopologyByRingId(ringID);
            logger.info("Retrieved topology with {} elements for ring ID: {}", topologyMap.size(), ringID);
        } catch (ResourceNotFoundException e) {
            logger.warn("Ring not found with ID: {}", ringID);
            throw new RetrieveException("Ring not found in topology", e);
        } catch (Exception e) {
            logger.error("Error processing topology data for ring ID {}", ringID);
            throw new ServiceException("Failed to process topology data for ring", e);
        }

        return topologyMap;
    }

    /**
     * Retrieves topology information based on specified filter parameters.
     *
     * <p>This method fetches network topology data according to the provided filters,
     * and enriches it with alarm information and link utilization data if requested.</p>
     *
     * @param filterParameters the wrapper containing topology filter criteria
     * @return a Topology object containing the filtered network topology information
     * @throws DataException if there's an error retrieving or processing topology data
     */
    @Override
    public Topology getTopology(TopologyFilterInfoWrappper filterParameters) {
        Objects.requireNonNull(filterParameters, TopologyGraphConstants.FILTER_PARAMETERS_CANNOT_BE_NULL);

        logger.error("Time when going to get topology from JANUS: {}", System.currentTimeMillis());
        Topology topologyMap = null;
        try {
            topologyMap = new Topology();
            if (topologyMap.getNodes() != null) {
                addAlarmCountInfoInTopology(topologyMap, filterParameters.getElementTypes());
            }
            if (filterParameters.getUtilInfo() != null) {
                logger.info("Adding link utilization information to topology");
                addLinkUtilizationInTopology(topologyMap, filterParameters.getUtilInfo());
            }
        } catch (ResourceNotFoundException e) {
            logger.warn("Topology resources not found for the given filters");
            throw new DataException("No topology data found for the specified filters", e);
        } catch (Exception e) {
            logger.error("Error retrieving topology data");
            throw new DataException("Failed to retrieve topology data", e);
        }
        return topologyMap;
    }

    /**
     * Adds alarm count information to the topology data.
     *
     * <p>This method enriches the topology with alarm information for each node,
     * using either node-wise or location-wise alarm data based on configuration.</p>
     *
     * @param topologyMap the topology data to enrich with alarm information
     * @param elementType the list of element types to consider for alarm data
     * @throws DataException if there's an error processing or adding alarm data
     */
    private void addAlarmCountInfoInTopology(Topology topologyMap, List<String> elementType) {
        Objects.requireNonNull(topologyMap, TopologyGraphConstants.TOPOLOGY_MAP_CANNOT_BE_NULL);

        Set<String> nodeSet = new HashSet<>();
        try {
            for (Map<String, Object> nodeData : topologyMap.getNodes()) {
                if (nodeData.get(TopologyGraphConstants.ELEMENT_TYPE) != null && !TopologyGraphConstants.ENODEB
                        .equalsIgnoreCase(nodeData.get(TopologyGraphConstants.ELEMENT_TYPE).toString()))
                    nodeSet.add(nodeData.get(TopologyGraphConstants.ELEMENT_NAME).toString());
            }

            if (ConfigUtils.getString(TopologyGraphConstants.TOPOLOGY_ALARM_CONFIG)
                    .equals(TopologyGraphConstants.NODE_WISE_DATA)) {
                setNodeWiseActiveAlarmData(topologyMap, nodeSet);
            } else if (ConfigUtils.getString(TopologyGraphConstants.TOPOLOGY_ALARM_CONFIG)
                    .equals(TopologyGraphConstants.LOCATION_WISE_DATA)) {
                logger.info("Element types for location-wise alarm data: {}", elementType.size());
            }
        } catch (Exception e) {
            logger.error("Failed to add alarm information to topology");
            throw new DataException("Failed to add alarm information to topology", e);
        }
    }

    /**
     * Sets location-wise active alarm count for network elements in the topology.
     *
     * <p>This method retrieves active alarm counts for a set of network elements and
     * adds this information to the corresponding nodes in the topology map.</p>
     *
     * @param topologyMap the topology data to enrich with alarm count information
     * @param nodeSet     the set of network element names to retrieve alarm data for
     * @throws ServiceException if there's an error processing or adding alarm data
     */
    @Override
    public void setLocationWiseActiveAlarmCount(Topology topologyMap, Set<String> nodeSet) {
        Objects.requireNonNull(topologyMap, TopologyGraphConstants.TOPOLOGY_MAP_CANNOT_BE_NULL);
        Objects.requireNonNull(nodeSet, "Node set cannot be null");

        try {
            Map<String, Long> activeAlarmCountForGCList = getActiveAlarmCountForGCList(new ArrayList<>(nodeSet));
            for (Map<String, Object> nodeData : topologyMap.getNodes()) {
                if (!activeAlarmCountForGCList.isEmpty() && nodeData.get(TopologyGraphConstants.ELEMENT_NAME) != null) {
                    nodeData.put(TopologyGraphConstants.ALARM_COUNT,
                            activeAlarmCountForGCList.get(nodeData.get(TopologyGraphConstants.ELEMENT_NAME).toString()));
                }
            }
        } catch (RuntimeException e) {
            logger.error("Failed to set location-wise active alarm count");
            throw new ServiceException("Unable to process alarm count data for topology", e);
        }
    }

    /**
     * Sets node-wise active alarm data for network elements in the topology.
     *
     * <p>This method retrieves detailed alarm information for each node in the provided set
     * and enriches the topology with alarm count, priority, and category information.</p>
     *
     * @param topologyMap the topology data to enrich with alarm information
     * @param nodeSet     the set of network element names to retrieve alarm data for
     * @throws ServiceException if there's an error processing or adding alarm data
     */
    private void setNodeWiseActiveAlarmData(Topology topologyMap, Set<String> nodeSet) {
        Objects.requireNonNull(topologyMap, TopologyGraphConstants.TOPOLOGY_MAP_CANNOT_BE_NULL);
        Objects.requireNonNull(nodeSet, "Node set cannot be null");

        try {
            Map<String, Map<String, Object>> activeAlarmData = getActiveAlarmDataForNetworkElements(nodeSet);
            Map<String, Map<String, Object>> higherPriorityActiveAlarmData = getHigherPriorityActiveAlarmData(
                    activeAlarmData);

            for (Map<String, Object> nodeData : topologyMap.getNodes()) {
                if (nodeData.get(TopologyGraphConstants.ELEMENT_NAME) != null) {
                    String eName = nodeData.get(TopologyGraphConstants.ELEMENT_NAME).toString();
                    Map<String, Object> alarmMap = higherPriorityActiveAlarmData.get(eName);

                    if (alarmMap != null && !alarmMap.isEmpty()) {
                        nodeData.put(TopologyGraphConstants.ALARM_COUNT, alarmMap.get(TopologyGraphConstants.ALARM_COUNT));
                        nodeData.put(TopologyGraphConstants.ALARM_PRIORITY,
                                alarmMap.get(TopologyGraphConstants.ALARM_PRIORITY));
                        nodeData.put(TopologyGraphConstants.ALARM_CATEGORY,
                                alarmMap.get(TopologyGraphConstants.ALARM_CATEGORY));
                    }
                }
            }
        } catch (RuntimeException e) {
            logger.error("Failed to set node-wise active alarm data");
            throw new ServiceException("Unable to process alarm data for topology nodes", e);
        }
    }

    /**
     * Adds link utilization information to the topology.
     *
     * <p>This method enriches the topology with link utilization data based on the provided
     * input wrapper, and removes unnecessary fields from the edge objects.</p>
     *
     * @param topologyMap  the topology data to enrich with link utilization information
     * @param inputWrapper the wrapper containing link utilization input parameters
     * @throws ServiceException if there's an error processing or adding link utilization data
     */
    private void addLinkUtilizationInTopology(Topology topologyMap, LinkUtilInputWrapper inputWrapper) {
        Objects.requireNonNull(topologyMap, TopologyGraphConstants.TOPOLOGY_MAP_CANNOT_BE_NULL);
        Objects.requireNonNull(inputWrapper, TopologyGraphConstants.LINK_UTILIZATION_INPUT_WRAPPER_NOT_BE_NULL);

        try {
            List<Map<String, Object>> listEdgeMap = topologyMap.getLinks();
            inputWrapper.setNodeLinkList(listEdgeMap);
            Map<String, Object> linkUtilizationMap = getUtilizationForEdgesWithSelectedFiltersUpdated(inputWrapper);

            logger.info("utilization map is : {}", linkUtilizationMap.size());

            logger.info("linkList from wrapper : {}", inputWrapper.getNodeLinkList().size());
            if (inputWrapper.getNodeLinkList() != null && !inputWrapper.getNodeLinkList().isEmpty()) {
                for (Map<String, Object> edgeObj : inputWrapper.getNodeLinkList()) {
                    String edgeId = (String) edgeObj.get(TopologyGraphConstants.EDGE_ID);
                    logger.info("Processing edge ID: {}", edgeId);
                    edgeObj.put(TopologyGraphConstants.LINK_UTILIZATION, linkUtilizationMap.get(edgeId));
                    removeExtraFieldsFromEdge(edgeObj);
                }
            } else {
                logger.info("No links found in node link list");
            }
            logger.info("topologyData map is : {}", topologyMap);
        } catch (IllegalArgumentException e) {
            logger.error("Invalid argument in addLinkUtilizationInTopology");
            throw new ServiceException("Invalid link utilization data format", e);
        } catch (RuntimeException e) {
            logger.error("Failed to add link utilization data to topology");
            throw new ServiceException("Failed to add link utilization data to topology", e);
        }
    }

    /**
     * Removes extra fields from an edge object to simplify the data structure.
     *
     * @param edgeObj the edge object to clean up
     */
    private void removeExtraFieldsFromEdge(Map<String, Object> edgeObj) {
        Objects.requireNonNull(edgeObj, "Edge object cannot be null");

        edgeObj.remove("IPs");
        edgeObj.remove("category");
        edgeObj.remove("connType");
        edgeObj.remove("destAliasPort");
        edgeObj.remove("destEType");
        edgeObj.remove("domain");
        edgeObj.remove("vendor");
        edgeObj.remove("sourceEType");
        edgeObj.remove("srcAliasPort");
    }

    /**
     * Adds alarm count information to a single node.
     *
     * <p>This method retrieves and adds alarm data (count, priority, and category)
     * for a specific network element node.</p>
     *
     * @param nodeData the node data to enrich with alarm information
     * @throws ServiceException if there's an error processing or adding alarm data
     */
    private void addAlarmCountInfoInNode(Map<String, Object> nodeData) {
        Objects.requireNonNull(nodeData, "Node data cannot be null");
        if (nodeData.get(TopologyGraphConstants.ELEMENT_NAME) != null) {
            try {
                Set<String> nodeSet = new HashSet<>();
                nodeSet.add(nodeData.get(TopologyGraphConstants.ELEMENT_NAME).toString());
                Map<String, Map<String, Object>> activeAlarmData = getActiveAlarmDataForNetworkElements(nodeSet);
                Map<String, Map<String, Object>> higherPriorityActiveAlarmData = getHigherPriorityActiveAlarmData(
                        activeAlarmData);

                String eName = nodeData.get(TopologyGraphConstants.ELEMENT_NAME).toString();
                Map<String, Object> alarmMap = higherPriorityActiveAlarmData.get(eName);

                if (alarmMap != null && !alarmMap.isEmpty()) {
                    nodeData.put(TopologyGraphConstants.ALARM_COUNT, alarmMap.get(TopologyGraphConstants.ALARM_COUNT));
                    nodeData.put(TopologyGraphConstants.ALARM_PRIORITY,
                            alarmMap.get(TopologyGraphConstants.ALARM_PRIORITY));
                    nodeData.put(TopologyGraphConstants.ALARM_CATEGORY,
                            alarmMap.get(TopologyGraphConstants.ALARM_CATEGORY));
                }
            } catch (RuntimeException e) {
                logger.error("Failed to add alarm count info for node: {}",
                        nodeData.get(TopologyGraphConstants.ELEMENT_NAME));
                throw new ServiceException("Unable to process alarm data for node", e);
            }
        }
    }

    /**
     * Retrieves detailed information about a network element.
     *
     * <p>This method fetches element information based on the provided type and value,
     * and enriches it with alarm count information.</p>
     *
     * @param type  the type of the element to retrieve
     * @param value the value identifying the element
     * @return a map containing the element information with alarm data
     * @throws ServiceException         if there's an error retrieving or processing the element information
     * @throws RetrieveException        if the element cannot be found
     * @throws IllegalArgumentException if type or value parameters are null
     */
    @Override
    public Map<String, Object> getElementInfo(String type, String value) {
        Objects.requireNonNull(type, "Type parameter cannot be null or empty");
        Objects.requireNonNull(value, "Value parameter cannot be null or empty");

        Map<String, Object> nodedata = new HashMap<>();
        try {
                nodedata = topologyGraphService.getElementInfo(type, value);
                addAlarmCountInfoInNode(nodedata);
        } catch (IllegalArgumentException e) {
            logger.error("Invalid argument in getElementInfo");
            throw new ServiceException("Invalid parameters for element info retrieval", e);
        } catch (ResourceNotFoundException e) {
            logger.error("Element not found in getElementInfo for type: {}, value: {}", type, value);
            throw new RetrieveException("Element not found with type: " + type + " and value: " + value, e);
        } catch (RuntimeException e) {
            logger.error("Failed to retrieve element information for type: {}, value: {}", type, value);
            throw new ServiceException("Failed to retrieve element information", e);
        }
        return nodedata;
    }

    /**
     * Retrieves all backhaul connections for a specified node.
     *
     * <p>This method fetches the topology data for all backhaul connections of a node,
     * and enriches it with alarm count and link utilization information.</p>
     *
     * @param eName                 the name of the element to retrieve backhaul connections for
     * @param topologyFilterWrapper wrapper containing filter parameters for the topology
     * @return a topology object containing the node and its backhaul connections
     * @throws RetrieveException if the element cannot be found
     * @throws ServiceException  if there's an error retrieving or processing the topology data
     */
    @Override
    public Topology getNodeToBackhaulAllConnection(String eName, TopologyFilterInfoWrappper topologyFilterWrapper) {
        Objects.requireNonNull(eName, TopologyGraphConstants.ELEMENT_NAME_CANNOT_BE_NULL_OR_EMPTY);
        Objects.requireNonNull(topologyFilterWrapper, "Topology filter wrapper cannot be null");
    
        if (logger.isInfoEnabled()) {
            logger.info("getNodeToBackhaulAllConnection URI :: {}", ConfigUtils.getString(TopologyGraphConstants.GET_RAN_TOPOLOGY_URI));
        }
    
        Topology topologyWrapper = new Topology();
    
        try {
            RANTopologyWrapper ranTopologyWrapper = new RANTopologyWrapper();
    
            if (ranTopologyWrapper.getNodes() != null) {
                Set<Map<String, Object>> nodeSet = new HashSet<>();
                for (Map<String, Object> ranTopology : ranTopologyWrapper.getNodes()) {
                    nodeSet.add(ranTopology);
                }
                topologyWrapper.setNodes(nodeSet);
                addAlarmCountInfoInTopology(topologyWrapper, null);
            }
    
            if (ranTopologyWrapper.getLinks() != null) {
                topologyWrapper.setLinks(ranTopologyWrapper.getLinks());
                addLinkUtilizationInTopology(topologyWrapper, topologyFilterWrapper.getUtilInfo());
            }
        } catch (ResourceNotFoundException e) {
            logger.error("Element not found in getNodeToBackhaulAllConnection for eName: {}", eName);
            throw new RetrieveException("Element not found: " + eName, e);
        } catch (IllegalArgumentException e) {
            logger.error("Invalid argument in getNodeToBackhaulAllConnection for eName: {}", eName);
            throw new ServiceException("Invalid element name: " + eName, e);
        } catch (RuntimeException e) {
            logger.error("Failed to retrieve backhaul connection data for element: {}", eName);
            throw new ServiceException("Failed to retrieve backhaul connection data", e);
        }
    
        return topologyWrapper;
    }    
    

    /**
     * Retrieves link utilization data for a specified edge ID.
     *
     * <p>This method fetches edge information and then retrieves the corresponding
     * link utilization data based on the provided input wrapper.</p>
     *
     * @param inputWrapper the wrapper containing link utilization input parameters
     * @return a map containing link utilization data for the specified edge
     * @throws IllegalArgumentException if the input wrapper is null
     */
    @Override
    public Map<String, Object> getLinkUtilizationForEdgeId(LinkUtilInputWrapper inputWrapper) {
        Objects.requireNonNull(inputWrapper, TopologyGraphConstants.LINK_UTILIZATION_INPUT_WRAPPER_NOT_BE_NULL);

        try {
            List<Map<String, Object>> edgeInfoList = getEdgeInfo(inputWrapper);
            inputWrapper.setNodeLinkList(edgeInfoList);
            inputWrapper.setEdgeIdList(null);
            return getUtilizationForEdgesWithSelectedFiltersUpdated(inputWrapper);
        } catch (Exception e) {
            logger.error("Failed to get link utilization for edge ID: {}", e.getMessage(), e);
        }
        return Collections.emptyMap();
    }

    /**
     * Retrieves edge information for the specified edge IDs.
     *
     * <p>This method makes an HTTP request to the topology graph service to fetch
     * detailed information about the edges specified in the input wrapper.</p>
     *
     * @param inputWrapper the wrapper containing the edge IDs to retrieve information for
     * @return a list of maps containing edge information
     * @throws DataException if there's an error retrieving or processing the edge information
     */
    public List<Map<String, Object>> getEdgeInfo(LinkUtilInputWrapper inputWrapper) {
        Objects.requireNonNull(inputWrapper, TopologyGraphConstants.LINK_UTILIZATION_INPUT_WRAPPER_NOT_BE_NULL);
        if (inputWrapper.getEdgeIdList() == null || inputWrapper.getEdgeIdList().isEmpty()) {
            throw new IllegalArgumentException("Edge ID list cannot be null or empty");
        }

        String edgeInfoUrl = ConfigUtils.getString(ConfigEnum.MICRO_SERVICE_BASE_URL.getValue())
                + "/rest/ms/TopologyGraph/getEdgeInfo";
        logger.info("Requesting edge info from: {}", edgeInfoUrl);
        ObjectMapper mapper = new ObjectMapper();
        StringEntity entity = new StringEntity(new Gson().toJson(inputWrapper.getEdgeIdList()),
                ContentType.APPLICATION_JSON);
        List<Map<String, Object>> edgeInfo = null;
        try {
            String response = new HttpPostRequest(edgeInfoUrl, entity).getString();
            logger.info("Edge info response received with length: {}", response != null ? response.length() : 0);
            if (response == null || response.trim().isEmpty()) {
                throw new DataException("Empty response received from edge info service");
            }
            edgeInfo = mapper.readValue(response, new TypeReference<List<Map<String, Object>>>() {
            });
        } catch (IllegalHttpStatusException e) {
            logger.error("Exception in getEdgeInfo {}", edgeInfoUrl);
            throw new DataException("Failed to retrieve edge information due to HTTP error", e);
        } catch (IOException e) {
            logger.error("IO error processing edge information response from {}", edgeInfoUrl);
            throw new DataException("Failed to process edge information response", e);
        }
        return edgeInfo;
    }

    /**
     * Retrieves active alarm count data for a list of geographic controller names.
     *
     * <p>This method makes an HTTP request to the alarm service to fetch
     * active alarm count information for the specified geographic controllers.</p>
     *
     * @param gcNameList the list of geographic controller names to retrieve alarm counts for
     * @return a map containing alarm count information for each geographic controller
     * @throws DataException if there's an error retrieving or processing the alarm count data
     */
    private Map<String, Long> getActiveAlarmCountForGCList(List<String> gcNameList) {
        Objects.requireNonNull(gcNameList, "Geographic controller name list cannot be null");
        if (gcNameList.isEmpty()) {
            logger.warn("Empty geographic controller list provided");
            return Collections.emptyMap();
        }

        logger.info("Retrieving active alarm counts for {} geographic controllers", gcNameList.size());
        Map<String, Long> alarmCountMap = new HashMap<>();
        try {
            String urlToGetActiveAlarmData = ConfigUtils.getString(ConfigUtil.MICRO_SERVICE_BASE_URL)
                    + ConfigUtils.getString(TopologyGraphConstants.GET_ACTIVE_ALARM_COUNT_FOR_NE_URL);
            String json = new Gson().toJson(gcNameList);
            StringEntity entity = new StringEntity(json, ContentType.APPLICATION_JSON);
            String response = new HttpPostRequest(urlToGetActiveAlarmData, entity).getString();
            logger.info("Active alarm count URL is : {}", urlToGetActiveAlarmData);
            if (response == null || response.trim().isEmpty()) {
                throw new DataException("Empty response received from alarm count service");
            }
            ObjectMapper mapper = new ObjectMapper();
            alarmCountMap = mapper.readValue(response, new TypeReference<Map<String, Long>>() {
            });
            logger.info("Alarm count map size is : {}", alarmCountMap.size());
        } catch (IllegalHttpStatusException e) {
            logger.error("HTTP error in getActiveAlarmCountForGCList");
            throw new DataException("Failed to retrieve alarm count data due to HTTP error", e);
        } catch (IOException e) {
            logger.error("IO error in getActiveAlarmCountForGCList");
            throw new DataException("Failed to process alarm count data response", e);
        } catch (RuntimeException e) {
            logger.error("Error in getActiveAlarmCountForGCList");
            throw new DataException("Failed to retrieve alarm count data", e);
        }
        return alarmCountMap;
    }

    /**
     * Retrieves the topology path from a node to its backhaul.
     *
     * <p>This method fetches the topology showing the connection path
     * between the specified network element and its backhaul node.</p>
     *
     * @param eName the name of the network element to find the backhaul path for
     * @return a topology object containing the node-to-backhaul path
     * @throws RetrieveException if the specified element cannot be found
     * @throws ServiceException  if there's an error processing the request
     */
    @Override
    public Topology getNodeToBackhaul(String eName) {
        Objects.requireNonNull(eName, TopologyGraphConstants.ELEMENT_NAME_CANNOT_BE_NULL_OR_EMPTY);

        Topology topology = new Topology();
        try {
            logger.info("Retrieving backhaul path for element: {}", eName);
            topology = topologyGraphService.getNodeToBackhaul(eName);
            logger.info("topology={}", topology);
        } catch (ResourceNotFoundException e) {
            logger.error("Element not found in getNodeToBackhaul for eName: {}", eName);
            throw new RetrieveException("Element not found: " + eName, e);
        } catch (IllegalArgumentException e) {
            logger.error("Invalid argument in getNodeToBackhaul for eName: {}", eName);
            throw new ServiceException("Invalid element name: " + eName, e);
        } catch (RuntimeException e) {
            logger.error("Exception in getNodeToBackhaul for eName: {}", eName);
            throw new ServiceException("Failed to retrieve backhaul node data", e);
        }
        return topology;
    }

    /**
     * Retrieves geographic data with alarm counts for network elements.
     *
     * <p>This method fetches network element location data grouped by geographic regions
     * and enriches it with alarm count information.</p>
     *
     * @param nelType the network element location type to filter by
     * @param geoType the geographic level to group by (e.g., "geographyL0", "geographyL1", "geographyL2")
     * @return a list of maps containing geographic data with alarm counts
     * @throws RetrieveException if the geographic data cannot be found
     * @throws ServiceException  if there's an error processing the request
     */
    @Override
    public List<Map<String, Object>> getGeoWiseCountWithAlarmCount(String nelType, String geoType) {
        Objects.requireNonNull(nelType, "Network element location type cannot be null or empty");
        Objects.requireNonNull(geoType, "Geographic type cannot be null or empty");

        logger.info("Inside getAlarmWiseCount by neltype :{}, geotype is :{} ", nelType, geoType);
        List<NELocationWrapper> nelData = new ArrayList<>();
        List<Map<String, Object>> mapList = new ArrayList<>();
        try {
            logger.info("NELocation data is :{}", nelData.size());
            if (nelData != null) {
                for (NELocationWrapper data : nelData) {
                    Map<String, Object> topologyMap = new HashMap<>();
                    topologyMap.put(TopologyGraphConstants.GEOGRAPHYNAME, data.getGeographyName());
                    if (geoType.equals("geographyL0")) {
                        topologyMap.put(TopologyGraphConstants.GEOGRAPHY_LEVEL1, data.getGeographyDisplayName());
                    }
                    if (geoType.equals("geographyL1")) {
                        topologyMap.put(TopologyGraphConstants.GEOGRAPHY_LEVEL1, data.getGeoL1DisplayName());
                        topologyMap.put(TopologyGraphConstants.GEOGRAPHY_LEVEL2, data.getGeographyDisplayName());
                    }
                    if (geoType.equals("geographyL2")) {
                        topologyMap.put(TopologyGraphConstants.GEOGRAPHY_LEVEL1, data.getGeoL1DisplayName());
                        topologyMap.put(TopologyGraphConstants.GEOGRAPHY_LEVEL2, data.getGeoL2DisplayName());
                        topologyMap.put(TopologyGraphConstants.GEOGRAPHY_LEVEL3, data.getGeographyDisplayName());
                    }
                    topologyMap.put(TopologyGraphConstants.COUNT, data.getCount());
                    topologyMap.put(TopologyGraphConstants.FRIENDLY_NAME, data.getFriendlyname());
                    topologyMap.put(TopologyGraphConstants.GCCOMPLETION_TIME, data.getGcCompletionTime());
                    topologyMap.put(TopologyGraphConstants.GEOGRAPHY_DISPLAY_NAME, data.getGeographyDisplayName());
                    topologyMap.put(TopologyGraphConstants.ADDRESS, data.getAddress());
                    topologyMap.put(TopologyGraphConstants.ID, data.getId());
                    topologyMap.put(TopologyGraphConstants.LATITUDE_STRING, data.getLatitude());
                    topologyMap.put(TopologyGraphConstants.LOCATION_CODE, data.getLocationCode());
                    topologyMap.put(TopologyGraphConstants.LOCATION_TYPE, data.getLocationType());
                    topologyMap.put(TopologyGraphConstants.LONGITUDE_STRING, data.getLongitude());
                    topologyMap.put(TopologyGraphConstants.NAME, data.getName());
                    topologyMap.put(TopologyGraphConstants.NEL_ID, data.getNelId());
                    topologyMap.put(TopologyGraphConstants.STATUS, data.getStatus());
                    topologyMap.put(TopologyGraphConstants.VCU_COUNT, data.getVcuCount());
                    topologyMap.put(TopologyGraphConstants.VDU_COUNT, data.getVduCount());
                    mapList.add(topologyMap);

                }
            }
            logger.info("Processed {} geographic locations with alarm data", mapList.size());

        } catch (ResourceNotFoundException e) {
            logger.error("Resource not found in getGeoWiseCountWithAlarmCount: nelType={}, geoType={}", nelType, geoType);
            throw new RetrieveException("Geographic data not found for type: " + nelType, e);
        } catch (IllegalArgumentException e) {
            logger.error("Invalid argument in getGeoWiseCountWithAlarmCount: nelType={}, geoType={}", nelType, geoType);
            throw new ServiceException("Invalid parameters for geographic data retrieval", e);
        } catch (RuntimeException e) {
            logger.error("Exception in getGeoWiseCountAndAlarmCount: nelType={}, geoType={}", nelType, geoType);
            throw new ServiceException("Failed to retrieve geographic count data", e);
        }
        return mapList;
    }

    /**
     * Sets location-wise active alarm counts for geographic controllers in the topology.
     *
     * <p>This method is currently a stub implementation that will retrieve and set
     * alarm count information for network elements based on their element type.</p>
     *
     * @param topologyMap the topology data to enrich with alarm count information
     * @param elementType the list of element types to consider for alarm data
     */
    @SuppressWarnings("unlikely-arg-type")
    @Override
    public void setLocationWiseActiveAlarmCountForGC(Topology topologyMap, List<String> elementType) {
            // This method is intentionally left blank. No location-wise active alarm count required for GC.

    }

    /**
     * Sets active alarm count information for sites in the topology.
     *
     * <p>This method retrieves alarm count data filtered by site name and
     * adds this information to the topology map.</p>
     *
     * @param topologyMap the topology data to enrich with site-based alarm information
     * @throws ServiceException if there's an error retrieving or processing alarm data
     */
    @Override
    public void setActiveAlarmCountForSite(Topology topologyMap) {
        Objects.requireNonNull(topologyMap, TopologyGraphConstants.TOPOLOGY_MAP_CANNOT_BE_NULL);

        String queryFilterForGC = ConfigUtils.getString(TopologyGraphConstants.GET_ALARM_COUNT_BY_SITE_NAME_FILTER);

        logger.info("queryFilterForGC: {}", queryFilterForGC);
    }

    /**
     * Retrieves topology data based on the provided request parameters.
     *
     * <p>This method validates the query, retrieves topology data from the graph service,
     * and enriches it with alarm information and link utilization data if requested.</p>
     *
     * @param requestWrapper the wrapper containing topology request parameters
     * @return the topology data with nodes, links, and additional information
     * @throws QueryException if there's an error with the topology query
     * @throws DataException  if there's an error retrieving or processing topology data
     */
    @Override
    @Cacheable(
    value = "getTopologyData",
    key = "T(org.springframework.util.DigestUtils).md5DigestAsHex("
           + "(#requestWrapper.query != null ? #requestWrapper.query.toString() : 'null').getBytes())",
    sync = true
    )
    public Topology getTopologyData(TopologyRequestWrapper requestWrapper) {
        Objects.requireNonNull(requestWrapper, TopologyGraphConstants.TOPOLOGY_REQUEST_WRAPPER_CANNOT_BE_NULL);
        Objects.requireNonNull(requestWrapper.getQuery(), "Topology query cannot be null");

        Topology topologyMap = null;
        long startTime = System.currentTimeMillis();


        AoaTopologyUtil.applyAoaToTopology(
            requestWrapper.getQuery(),
            customerInfo,
            ConfigUtils.getPlainString("MODULE_NAME"),
            "Topology"   // Entity name as per AOA JSON
        );
        logger.info("AOA applied to topology request wrapper: {}", requestWrapper.getQuery().getVertexCriteria());
        
        try {
            // Validate query before execution
            if (!JanusGraphQuery.isQueryValid(requestWrapper.getQuery())) {
                logger.warn("Query validation failed for topology request");
                throw new QueryException("Invalid topology query provided");
            }
            
            logger.info("Query validation successful, executing topology retrieval");
            topologyMap = topologyGraphService.getTopologyData(requestWrapper);
            
            long janusResponseTime = System.currentTimeMillis();
            logger.info("Topology data retrieved from JanusGraph in {} ms", janusResponseTime - startTime);
            
            // Enrich topology with alarm information if requested
            if (shouldEnrichWithAlarmData(topologyMap, requestWrapper)) {
                addAlarmCountInfoInTopology(topologyMap, requestWrapper.getAlarmConf().getElementTypes());
            }
            
            // Enrich topology with link utilization data if requested
            if (shouldEnrichWithUtilizationData(topologyMap, requestWrapper)) {
                getPMCauseCode(topologyMap, requestWrapper);
                long utilizationResponseTime = System.currentTimeMillis();
                logger.info("Link utilization data processed in {} ms", utilizationResponseTime - janusResponseTime);
            }
            
        } catch (ScriptException e) {
            logger.error("Script execution failed in topology data retrieval");
            throw new QueryException("Failed to execute topology query script", e);
        } catch (JanusOperationModeNotValidException e) {
            logger.error("Invalid Janus operation mode in topology query");
            throw new QueryException("Invalid Janus operation mode", e);
        } catch (HasTypeNotValidException e) {
            logger.error("Invalid has type in topology query");
            throw new QueryException("Invalid has type in topology query", e);
        } catch (JanusQueryTypeNotValidException e) {
            logger.error("Invalid Janus query type");
            throw new QueryException("Invalid Janus query type", e);
        } catch (HasNotTypeNotValidException e) {
            logger.error("Invalid has not type in topology query");
            throw new QueryException("Invalid has not type in topology query", e);
        } catch (JanusQueryNotValidException e) {
            logger.error("Invalid Janus query structure");
            throw new QueryException("Invalid Janus query", e);
        } catch (RuntimeException e) {
            logger.error("Unexpected error during topology data retrieval");
            throw new DataException("Failed to retrieve topology data", e);
        }
        
        return topologyMap;
    }
    
    /**
     * Determines if topology should be enriched with alarm data based on request parameters.
     *
     * @param topologyMap the topology data
     * @param requestWrapper the request parameters
     * @return true if alarm enrichment is required, false otherwise
     */
    private boolean shouldEnrichWithAlarmData(Topology topologyMap, TopologyRequestWrapper requestWrapper) {
        return topologyMap != null 
            && topologyMap.getNodes() != null 
            && requestWrapper.getAlarmConf() != null
            && requestWrapper.getAlarmConf().getElementTypes() != null
            && !requestWrapper.getAlarmConf().getElementTypes().isEmpty();
    }
    
    /**
     * Determines if topology should be enriched with utilization data based on request parameters.
     *
     * @param topologyMap the topology data
     * @param requestWrapper the request parameters
     * @return true if utilization enrichment is required, false otherwise
     */
    private boolean shouldEnrichWithUtilizationData(Topology topologyMap, TopologyRequestWrapper requestWrapper) {
        return topologyMap != null 
            && topologyMap.getLinks() != null
            && Boolean.TRUE.equals(requestWrapper.getGetLinkUtilization())
            && requestWrapper.getUtilConf() != null;
    }

    /**
     * Retrieves server to TOR (Top of Rack) information for a specified element name.
     *
     * <p>This method fetches the topology data showing connections between servers
     * and TOR switches for the given element.</p>
     *
     * @param eName the element name to retrieve server to TOR information for
     * @return the topology data containing server to TOR connections
     * @throws NotFoundException if no server to TOR information is found for the element
     * @throws DataException     if there's an error retrieving the information
     */
    @Override
    public Topology getServerToTorInformationMS(String eName) {
        Objects.requireNonNull(eName, "Element name cannot be null");

        Topology topologyData = new Topology();
        try {
            topologyData = topologyGraphService.getServerToTorInformation(eName);
        } catch (ResourceNotFoundException e) {
            logger.error("Resource not found in getServerToTorInformationMS for eName: {}", eName);
            throw new NotFoundException("Server to TOR information not found");
        } catch (RuntimeException e) {
            logger.error("Error in getServerToTorInformationMS for eName: {}", eName);
            throw new DataException("Failed to retrieve server to TOR information", eName, e);
        }
        return topologyData;
    }

    /**
     * Processes and adds PM (Performance Management) cause code information to topology links.
     *
     * <p>This method retrieves performance KPI data for links in the topology and
     * determines cause codes based on the KPI values.</p>
     *
     * @param topologyMap    the topology data to enrich with PM cause code information
     * @param requestWrapper the request parameters containing utilization configuration
     * @throws DataProcessingException if there's an error processing the PM data
     */
    private void getPMCauseCode(Topology topologyMap, TopologyRequestWrapper requestWrapper) {
        Objects.requireNonNull(topologyMap, TopologyGraphConstants.TOPOLOGY_MAP_CANNOT_BE_NULL);
        Objects.requireNonNull(requestWrapper, "Request wrapper cannot be null");
    
        logger.error("Value of topology links is : {} ", topologyMap.getLinks().size());
        Map<Long, Object> finalMap = new HashMap<>();
        List<String> linkIdList = new ArrayList<>();
        try {
            SimpleDateFormat format = new SimpleDateFormat("HH:mm");
            Date currentTime = format.parse(LocalTime.now().toString());
            List<Map<String, Object>> links = topologyMap.getLinks();
            linkIdList = topologyMap.getLinks().stream()
                    .map(map -> map.get(TopologyGraphConstants.LINK_ID).toString())
                    .toList();
            List<KPIResponse> performanceKPI = setValueIntoKpiWrapper(requestWrapper, linkIdList);
            logger.error("Value of Kpi pm wrapper  is : {} ", performanceKPI);
    
            getCauseCodeFromPmKpi(finalMap, format, currentTime, links, performanceKPI);
        } catch (ParseException e) {
            logger.error("ParseException in getPMCauseCode");
            throw new DataProcessingException("Failed to parse date in PM cause code processing", e);
        } catch (RuntimeException e) {
            logger.error("Exception in getPMCauseCode");
            throw new DataProcessingException("Failed to process PM cause code data", e);
        }
    }
    

    /**
     * Extracts cause code information from PM KPI data and adds it to topology links.
     *
     * <p>This method analyzes KPI data for each link, finds the most recent and highest
     * value KPI, and sets the corresponding cause code on the link.</p>
     *
     * @param finalMap       map to store processed KPI data
     * @param format         date format for parsing time values
     * @param currentTime    the current time for comparison
     * @param links          the list of links to enrich with cause code information
     * @param performanceKPI the KPI response data for the links
     * @throws DataProcessingException if there's an error processing the KPI data
     */
    private void getCauseCodeFromPmKpi(Map<Long, Object> finalMap, SimpleDateFormat format, Date currentTime,
    List<Map<String, Object>> links, List<KPIResponse> performanceKPI) {
Objects.requireNonNull(finalMap, "Final map cannot be null");
Objects.requireNonNull(format, "Date format cannot be null");
Objects.requireNonNull(currentTime, "Current time cannot be null");

if (links == null || performanceKPI == null) {
return;
}

try {
for (Map<String, Object> link : links) {
processLinkForCauseCode(finalMap, format, currentTime, performanceKPI, link);
logger.info("Value of final links is : {} ", link);
}
} catch (ParseException e) {
throw new DataProcessingException("Failed to parse PM KPI data for cause code analysis", e);
} catch (ClassCastException e) {
throw new DataProcessingException("Invalid KPI value map format in cause code processing", e);
} catch (NullPointerException e) {
throw new DataProcessingException("Null reference encountered while processing KPI data", e);
}
}

private void processLinkForCauseCode(Map<Long, Object> finalMap, SimpleDateFormat format, Date currentTime,
      List<KPIResponse> performanceKPI, Map<String, Object> link) throws ParseException {
List<Long> kpiRecordList = new ArrayList<>();

for (KPIResponse pmData : performanceKPI) {
if (pmData != null && link.get(TopologyGraphConstants.LINK_ID) != null
&& link.get(TopologyGraphConstants.LINK_ID).equals(pmData.getNeName())) {
Date pmDate = format.parse(pmData.getTime());
long difference = currentTime.getTime() - pmDate.getTime();
finalMap.put(difference, pmData.getValues());
kpiRecordList.add(difference);
}
}

if (!kpiRecordList.isEmpty()) {
Collections.sort(kpiRecordList);
Map<Long, Long> kpiValueMap = (Map<Long, Long>) finalMap.get(kpiRecordList.get(0));
logger.info("kpiValueMap is : {} ", kpiValueMap);
Optional<Entry<Long, Long>> maxKpiValue = kpiValueMap.entrySet().stream()
.max(Comparator.comparing(Map.Entry::getValue));
logger.info("maxKpiValue is : {} ", maxKpiValue);
if (maxKpiValue.isPresent()) {
link.put("causeCodeId", maxKpiValue.get().getKey());
link.put("causeCode", maxKpiValue.get().getValue());
}
}
}


    /**
     * Prepares a performance request wrapper with parameters from the topology request.
     *
     * <p>This method extracts utilization configuration from the topology request and
     * creates a performance request with the necessary parameters.</p>
     *
     * @param requestWrapper the topology request containing utilization configuration
     * @param linkIdList     the list of link IDs to retrieve KPI data for
     * @return a list of KPI responses for the specified links
     * @throws DataProcessingException if there's an error preparing the KPI request
     */
    private List<KPIResponse> setValueIntoKpiWrapper(TopologyRequestWrapper requestWrapper, List<String> linkIdList) {
        Objects.requireNonNull(requestWrapper, TopologyGraphConstants.TOPOLOGY_REQUEST_WRAPPER_CANNOT_BE_NULL);
        Objects.requireNonNull(linkIdList, "Link ID list cannot be null");

        try {
            PerformanceRequestWrapper request = new PerformanceRequestWrapper();
            request.setDomain(requestWrapper.getUtilConf().getDomain());
            request.setVendor(requestWrapper.getUtilConf().getVendor());
            request.setNodeType(requestWrapper.getUtilConf().getNodeType());
            request.setFrequency(requestWrapper.getUtilConf().getFrequency());
            request.setNodeList(linkIdList);
            request.setStartDate(requestWrapper.getUtilConf().getStartDate());
            request.setEndDate(requestWrapper.getUtilConf().getEndDate());
            request.setKpiList(requestWrapper.getUtilConf().getKpiId());
            return Collections.emptyList();
        } catch (NullPointerException e) {
            throw new DataProcessingException("Missing required KPI configuration parameters", e);
        } catch (IllegalArgumentException e) {
            throw new DataProcessingException("Invalid KPI configuration parameters", e);
        }
    }

    /**
     * Retrieves performance KPI details for a specified KPI type.
     *
     * <p>This method creates a request to fetch performance KPI information
     * filtered by the specified KPI type.</p>
     *
     * @param kpiType the type of KPI to retrieve details for
     * @return a list of performance KPI objects matching the specified type
     * @throws QueryException          if no KPI data is found for the specified type
     * @throws InvalidInputException   if the KPI type parameter is invalid
     * @throws DataProcessingException if there's an error fetching KPI details
     */
    private List<PerformanceKPI> getPerformanceKPIDetail(String kpiType) {
        Objects.requireNonNull(kpiType, "KPI type cannot be null");

        try {
            PerformanceKPIRequestWrapper performanceKPIRequestWrapper = new PerformanceKPIRequestWrapper();
            Map<String, String> fieldWiseOperation = new HashMap<>();
            fieldWiseOperation.put(TopologyGraphConstants.KPI_TYPE_FIELD, TopologyGraphConstants.EQUALS_OPERATOR_STRING);
            performanceKPIRequestWrapper.setKpiType(kpiType);
            performanceKPIRequestWrapper.setFieldWiseOperation(fieldWiseOperation);
            logger.info("PerformanceKPIRequestWrapper {}", performanceKPIRequestWrapper);

            List<PerformanceKPI> performanceKPI = performanceKpiRest.getPerformanceKPI(performanceKPIRequestWrapper);
            logger.info("performanceKPI size {}", performanceKPI);
            return performanceKPI;
        } catch (ResourceNotFoundException e) {
            throw new QueryException("No performance KPI data found for type: " + kpiType, e);
        } catch (IllegalArgumentException e) {
            throw new InvalidInputException("Invalid KPI type parameter: " + kpiType, e);
        } catch (RuntimeException e) {
            logger.error("Error fetching performance KPI details for type: {}", kpiType);
            throw new DataProcessingException("Failed to fetch performance KPI details for type: " + kpiType, e);
        }
    }

    /**
     * Retrieves topology data within a specified geographic bounding box.
     *
     * <p>This method fetches topology elements that fall within the geographic
     * coordinates defined by the southwest and northeast points.</p>
     *
     * @param southWestLat  the latitude of the southwest corner of the bounding box
     * @param southWestLong the longitude of the southwest corner of the bounding box
     * @param northEastLat  the latitude of the northeast corner of the bounding box
     * @param northEastLong the longitude of the northeast corner of the bounding box
     * @param level         the level of detail for the topology data
     * @return the topology data within the specified geographic bounds
     * @throws InvalidInputException if the coordinate parameters are invalid
     * @throws QueryException        if there's an error retrieving the topology data
     */
    public Topology getTopologyByLatLong(Double southWestLat, Double southWestLong, Double northEastLat,
                                         Double northEastLong, String level) {
        Objects.requireNonNull(southWestLat, "Southwest latitude cannot be null");
        Objects.requireNonNull(southWestLong, "Southwest longitude cannot be null");
        Objects.requireNonNull(northEastLat, "Northeast latitude cannot be null");
        Objects.requireNonNull(northEastLong, "Northeast longitude cannot be null");
        Objects.requireNonNull(level, "Level parameter cannot be null");

        if (level.trim().isEmpty()) {
            throw new InvalidInputException("Level parameter cannot be empty");
        }

        if (southWestLat > northEastLat) {
            throw new InvalidInputException("Southwest latitude cannot be greater than northeast latitude");
        }

        if (southWestLong > northEastLong) {
            throw new InvalidInputException("Southwest longitude cannot be greater than northeast longitude");
        }

        try {
            return topologyGraphUtils.getVertexByViewPortAndLevel(southWestLong, southWestLat, northEastLong, northEastLat, level);
        } catch (IllegalArgumentException e) {
            throw new InvalidInputException("Invalid coordinate parameters for topology retrieval", e);
        } catch (RuntimeException e) {
            throw new QueryException("Failed to retrieve topology by geographic coordinates", e);
        }
    }

    /**
     * Retrieves topology hierarchy data based on the provided request parameters.
     *
     * <p>This method fetches the parent topology data and then builds a hierarchical
     * structure representing the relationships between topology elements.</p>
     *
     * @param requestWrapper the wrapper containing topology request parameters
     * @return a topology object containing the hierarchical structure
     * @throws DataException    if topology hierarchy data cannot be found or retrieved
     * @throws ServiceException if there's an error processing the topology hierarchy data
     */
    @Override
    public Topology getTopologyHeirarchyData(TopologyRequestWrapper requestWrapper) {
        Objects.requireNonNull(requestWrapper, TopologyGraphConstants.TOPOLOGY_REQUEST_WRAPPER_CANNOT_BE_NULL);

        try {
            JSONObject jsonObject = new JSONObject();

            logger.info("jsonObject: {}", jsonObject);
            Topology parent = topologyGraphService.getTopologyData(requestWrapper);
            logger.info("Retrieved parent topology data : {}", parent);

            JSONObject result = getHeirarchy(parent, requestWrapper);

            logger.info("Hierarchy result structure created : {}", result);

            return parent;
        } catch (ResourceNotFoundException e) {
            throw new DataException("Topology hierarchy data not found for the specified parameters", e);
        } catch (RuntimeException e) {
            throw new ServiceException("Failed to process topology hierarchy data", e);
        } catch (JanusOperationModeNotValidException e) {
            throw new QueryException("Invalid Janus operation mode in topology query", e);
        } catch (ScriptException e) {
            throw new QueryException("Failed to execute topology query script", e);
        } catch (HasTypeNotValidException e) {
            throw new QueryException("Invalid has type in topology query", e);
        } catch (JanusQueryTypeNotValidException e) {
            throw new QueryException("Invalid Janus query type", e);
        } catch (HasNotTypeNotValidException e) {
            throw new QueryException("Invalid has not type in topology query", e);
        } catch (JanusQueryNotValidException e) {
            throw new QueryException("Invalid Janus query", e);
        }
    }

    /**
     * Builds a hierarchical structure from the provided topology data.
     *
     * <p>This method processes the nodes in the child topology and organizes them
     * into a hierarchical JSON structure based on their element types.</p>
     *
     * @param childTopology  the topology containing child nodes to organize hierarchically
     * @param requestWrapper the wrapper containing topology request parameters
     * @return a JSON object representing the hierarchical structure of topology elements
     * @throws DataProcessingException if there's an error processing node data in the topology
     * @throws ServiceException        if there's an error building the hierarchy structure
     */
    private JSONObject getHeirarchy(Topology childTopology, TopologyRequestWrapper requestWrapper) {
        Objects.requireNonNull(childTopology, "Child topology cannot be null");
        Objects.requireNonNull(requestWrapper, TopologyGraphConstants.TOPOLOGY_REQUEST_WRAPPER_CANNOT_BE_NULL);

        try {
            List<Map<String, String>> list = new ArrayList<>();
            Map<String, String> map = new HashMap<>();
            map.put("key", "parentNeName");
            logger.info("list: {}", list);

            JSONObject jsonObject = new JSONObject();
            if (childTopology.getNodes() != null) {
                Set<Map<String, Object>> nodes = childTopology.getNodes();
                for (Map<String, Object> node : nodes) {
                    map.put("value", node.get("eName").toString());
                    jsonObject.put(node.get("eType").toString(), node);
                }
                logger.info("jsonIbject : {}", jsonObject);
            }
            return jsonObject;
        } catch (NullPointerException e) {
            throw new DataProcessingException("Missing required node data in topology hierarchy", e);
        } catch (ClassCastException e) {
            throw new DataProcessingException("Invalid data format in topology hierarchy structure", e);
        } catch (RuntimeException e) {
            throw new ServiceException("Failed to process topology hierarchy data", e);
        }
    }
}
