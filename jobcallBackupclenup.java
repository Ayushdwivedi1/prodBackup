package com.enttribe.module.topology.core.service.impl;

import com.enttribe.module.topology.core.utils.exceptions.GraphicalException;
import com.enttribe.module.topology.core.utils.exceptions.InvalidInputException;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.both;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.bothE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.has;

import java.util.*;
import java.util.Map.Entry;
import java.util.function.Consumer;

import javax.script.ScriptException;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphEdge;
import org.janusgraph.core.JanusGraphVertex;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.enttribe.commons.Symbol;
import com.enttribe.commons.lang.StringUtils;
import com.enttribe.core.generic.exceptions.application.BusinessException;
import com.enttribe.core.generic.exceptions.application.ResourceNotFoundException;
import com.enttribe.performance.module.pm.utils.PMConstants;
import com.enttribe.platform.configuration.baseconfig.rest.BaseConfigurationRest;
import com.enttribe.module.topology.core.dao.TopologyGraphDao;
import com.enttribe.module.topology.core.dao.TopologyHbaseDao;
import com.enttribe.module.topology.core.graphinstancemgmt.enums.JanusGraphInstance;
import com.enttribe.module.topology.core.graphinstancemgmt.service.JanusGraphInstanceManagerService;
import com.enttribe.module.topology.core.graphinstancemgmt.service.impl.JanusGraphInstanceManagerServiceImpl;
import com.enttribe.module.topology.core.janusgraphutils.constants.JanusGraphUtilsConstants;
import com.enttribe.module.topology.core.janusgraphutils.utils.OperationUtils;
import com.enttribe.module.topology.core.janusgraphutils.wrapper.VertexToEdgeRelationConfig;
import com.enttribe.module.topology.core.querybuilder.JanusGraphQueryBuilder;
import com.enttribe.module.topology.core.querybuilder.exceptions.HasNotTypeNotValidException;
import com.enttribe.module.topology.core.querybuilder.exceptions.HasTypeNotValidException;
import com.enttribe.module.topology.core.querybuilder.exceptions.JanusOperationModeNotValidException;
import com.enttribe.module.topology.core.querybuilder.exceptions.JanusQueryNotValidException;
import com.enttribe.module.topology.core.querybuilder.exceptions.JanusQueryTypeNotValidException;
import com.enttribe.module.topology.core.querybuilder.utils.JanusGraphQueryBuilderUtils;
import com.enttribe.module.topology.core.service.TopologyGraphService;
import com.enttribe.module.topology.core.utils.TopologyGraphConstants;
import com.enttribe.module.topology.core.utils.TopologyGraphUtils;
import com.enttribe.module.topology.core.utils.TopologyGremlinScriptUtils;
import com.enttribe.module.topology.core.utils.exceptions.ConnectivityException;
import com.enttribe.module.topology.core.utils.exceptions.DataException;
import com.enttribe.module.topology.core.utils.exceptions.DataNotFoundException;
import com.enttribe.module.topology.core.utils.exceptions.DataProcessingException;
import com.enttribe.module.topology.core.utils.exceptions.VertexCreationException;
import com.enttribe.module.topology.core.utils.exceptions.VertexNotFoundException;
import com.enttribe.module.topology.core.wrapper.ConnectionInfoWrapper;
import com.enttribe.module.topology.core.wrapper.ElementConnectivityWrapper;
import com.enttribe.module.topology.core.wrapper.ElementInfoRestWrapper;
import com.enttribe.module.topology.core.wrapper.ElementInfoWrapper;
import com.enttribe.module.topology.core.wrapper.Topology;
import com.enttribe.module.topology.core.wrapper.TopologyConnectivityWrapper;
import com.enttribe.module.topology.core.wrapper.TopologyRequestWrapper;

import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;

/**
 * The Class TopologyGraphServiceImpl.
 */
@Service
/**
 * Implementation of the Topology Graph Service interface that provides methods
 * for
 * retrieving and manipulating topology graph data.
 *
 * <p>
 * This service handles operations related to network topology including
 * retrieving
 * element connectivity, processing topology data, and managing graph
 * relationships.
 * </p>
 *
 * @author Enttribe
 * @version 1.0
 * @since 1.0
 */
public class TopologyGraphServiceImpl implements TopologyGraphService {

    /**
     * The logger for this class.
     */
    private static final Logger logger = LoggerFactory.getLogger(TopologyGraphServiceImpl.class);

    /**
     * The topology graph data access object.
     */
    @Autowired
    private TopologyGraphDao iTopologyGraphDao;

    /**
     * Constant for vertex logging.
     */
    private static final String VERTEXT = "FINAL Vertex: {}";

    /**
     * Constant for vendor property.
     */
    private static final String VENDOR = "vendor";

    /**
     * Constant for domain property.
     */
    private static final String DOMAIN = "domain";

    /**
     * Constant for connected property.
     */
    private static final String CONNECTED = "connected";

    /**
     * Constant for vertex property.
     */
    private static final String VERTEX = "vertex";

    /**
     * Constant for element name property.
     */
    private static final String ENAME = "neName";

    private static final String EID = "neId";

    private static final String NETYPE = "neType";

    private static final String ENDNEID = "endNeId";

    private static final String SOURCE = "source";

    private static final String DEST = "dest";
    /**
     * The topology HBase data access object.
     */
    @Autowired
    TopologyHbaseDao iTopologyHbaseDao;

    /**
     * The Janus graph instance manager service.
     */
    @Autowired
    JanusGraphInstanceManagerService iJanusGraphInstanceManagerService;

    /**
     * The system configuration REST client.
     */
    @Autowired
    private BaseConfigurationRest systemConfigurationRest;

    /**
     * The topology graph utilities.
     */
    @Autowired
    TopologyGraphUtils topologyGraphUtils;

    /**
     * Instantiates a new topology graph service implementation.
     */
    public TopologyGraphServiceImpl() {
        super();
    }

    /**
     * Gets the element connectivity by element name.
     *
     * @param request the request
     * @return the element connectivity by element name
     */

    /**
     * Gets the IP link by source and destination.
     *
     * @param source      the source
     * @param destination the destination
     * @param request     the request
     * @return the IP link by source and destination
     */

    /**
     * Gets the janus query output.
     *
     * @param query     the query
     * @param queryType the query type
     * @param request   the request
     * @return the janus query output
     */

    /**
     * Retrieves topology data for a specific element type and link relation type.
     *
     * @param elementType      the element type to filter vertices
     * @param linkRelationType the link relation type to filter edges
     * @return a map containing connection information wrappers
     * @throws ResourceNotFoundException if no vertices are found for the specified
     *                                   element type
     * @throws BusinessException         if a business rule is violated during
     *                                   processing
     */
    public Map<String, List<ConnectionInfoWrapper>> getTopologyByETypeAndLinkType(String elementType,
                                                                                  String linkRelationType) {
        Map<String, List<ConnectionInfoWrapper>> coreConnections = new HashMap<>();
        List<ConnectionInfoWrapper> connectionList = new ArrayList<>();
        try {
            if (elementType != null && linkRelationType != null) {
                List<Vertex> coreVertices = topologyGraphUtils
                        .getAllVertexByPropertyValue(TopologyGraphConstants.ELEMENT_TYPE, elementType);

                if (coreVertices != null && !coreVertices.isEmpty()) {
                    List<Edge> coreEdges = iTopologyGraphDao
                            .getAllEdgesByPrepertyValue(TopologyGraphConstants.CONNECTION_TYPE, linkRelationType);

                    Map<Object, Map<String, Object>> coreEdgesMap = topologyGraphUtils
                            .getEdgeVertexIDMapByEdgeList(coreEdges);
                    Map<Object, Map<String, Object>> vertexIdMap = topologyGraphUtils
                            .getVertexPropertyMapByVertexList(coreVertices);

                    getconnectionList(connectionList, coreVertices, coreEdgesMap, vertexIdMap);

                    coreConnections.put(TopologyGraphConstants.NW_ELEMENTS, connectionList);
                } else {
                    throw new ResourceNotFoundException("No vertices found for element type: " + elementType);
                }
            }
        } catch (ResourceNotFoundException e) {
            logger.error("Resource not found in getTopologyByETypeAndLinkType");
            throw e;
        } catch (BusinessException e) {
            logger.error("Business exception in getTopologyByETypeAndLinkType");
            throw e;
        } catch (Exception e) {
            logger.error("Exception in getTopologyByETypeAndLinkType: {}", e.getMessage(), e);
        }
        return coreConnections;
    }

    /**
     * Processes a list of vertices and builds connection information for each
     * vertex.
     *
     * @param connectionList the list to populate with connection information
     * @param coreVertices   the list of vertices to process
     * @param coreEdgesMap   the map of edges associated with the vertices
     * @param vertexIdMap    the map of vertex properties indexed by vertex ID
     * @throws NullPointerException if any parameter is null
     */
    private void getconnectionList(List<ConnectionInfoWrapper> connectionList, List<Vertex> coreVertices,
                                   Map<Object, Map<String, Object>> coreEdgesMap, Map<Object, Map<String, Object>> vertexIdMap) {
        Objects.requireNonNull(connectionList, "Connection list cannot be null");
        Objects.requireNonNull(coreVertices, "Core vertices cannot be null");
        Objects.requireNonNull(coreEdgesMap, TopologyGraphConstants.CORE_EDGES_NOT_BE_NULL);
        Objects.requireNonNull(vertexIdMap, "Vertex ID map cannot be null");

        for (Vertex vertex : coreVertices) {
            if (vertex.id() != null) {
                ConnectionInfoWrapper connection = setConnectionInfoForVertex(vertex, coreEdgesMap, vertexIdMap);
                if (connection.getVertexId() != null && connection.geteName() != null) {
                    connectionList.add(connection);
                }
            }
        }
    }

    /**
     * Retrieves topology data for multiple element types and link relation types.
     *
     * @param elementType      the list of element types to filter vertices
     * @param linkRelationType the list of link relation types to filter edges
     * @return a map containing connection information wrappers
     * @throws InvalidInputException     if element types or link relation types are
     *                                   null or empty
     * @throws ResourceNotFoundException if no vertices are found for the specified
     *                                   element types
     * @throws DataException             if there is an error processing the data
     */
    @Override
    public Map<String, List<ConnectionInfoWrapper>> getTopologyByETypesAndLinkTypes(List<Object> elementType,
                                                                                    List<Object> linkRelationType) {
        Map<String, List<ConnectionInfoWrapper>> coreConnections = new HashMap<>();
        List<ConnectionInfoWrapper> connectionList = new ArrayList<>();
        try {
            if (elementType == null || elementType.isEmpty()) {
                throw new InvalidInputException("Element types cannot be null or empty");
            }

            if (linkRelationType == null || linkRelationType.isEmpty()) {
                throw new InvalidInputException("Link relation types cannot be null or empty");
            }

            List<Vertex> coreVertices = topologyGraphUtils
                    .getAllVertexByPropertyValue(TopologyGraphConstants.ELEMENT_TYPE, elementType);

            if (coreVertices == null || coreVertices.isEmpty()) {
                throw new ResourceNotFoundException("No vertices found for the specified element types");
            }

            List<Edge> coreEdges = iTopologyGraphDao
                    .getAllEdgesByPrepertyValue(TopologyGraphConstants.CONNECTION_TYPE, linkRelationType);

            Map<Object, Map<String, Object>> coreEdgesMap = topologyGraphUtils
                    .getEdgeVertexIDMapByEdgeList(coreEdges);
            Map<Object, Map<String, Object>> vertexIdMap = topologyGraphUtils
                    .getVertexPropertyMapByVertexList(coreVertices);

            getconnectionList(connectionList, coreVertices, coreEdgesMap, vertexIdMap);
            coreConnections.put(TopologyGraphConstants.NW_ELEMENTS, connectionList);

        } catch (ResourceNotFoundException e) {
            logger.error("Resource not found in getTopologyByETypesAndLinkTypes");
            throw e;
        } catch (InvalidInputException e) {
            logger.error("Invalid input in getTopologyByETypesAndLinkTypes");
            throw e;
        } catch (DataException e) {
            logger.error("Data exception in getTopologyByETypesAndLinkTypes");
            throw e;
        } catch (Exception e) {
            logger.error("Exception in getTopologyByETypesAndLinkTypes");
            throw new DataException("Failed to retrieve topology data for multiple types", e);
        }
        return coreConnections;
    }

    /**
     * Creates a connection information wrapper for a vertex with its connectivity
     * data.
     *
     * @param vertex       the vertex to process
     * @param coreEdgesMap the map of edges associated with vertices
     * @param vertexMap    the map of vertex properties indexed by vertex ID
     * @return a connection information wrapper for the vertex
     * @throws NullPointerException if any parameter is null
     * @throws DataException        if required vertex properties are missing
     */
    private ConnectionInfoWrapper setConnectionInfoForVertex(Vertex vertex,
                                                             Map<Object, Map<String, Object>> coreEdgesMap, Map<Object, Map<String, Object>> vertexMap) {
        Objects.requireNonNull(vertex, TopologyGraphConstants.VERTEX_NOT_BE_NULL);
        Objects.requireNonNull(coreEdgesMap, TopologyGraphConstants.CORE_EDGES_NOT_BE_NULL);
        Objects.requireNonNull(vertexMap, TopologyGraphConstants.VERTEX_MAP_NOT_BE_NULL);

        ConnectionInfoWrapper connection = new ConnectionInfoWrapper();
        try {
            Map<String, Object> vertexPropertyMap = vertexMap.get(vertex.id());

            if (vertexPropertyMap == null) {
                throw new DataException("No property map found for vertex ID: " + vertex.id());
            }

            if (vertexPropertyMap.get(TopologyGraphConstants.LATITUDE) == null) {
                throw new DataException("Latitude is missing for vertex ID: " + vertex.id());
            }

            if (vertexPropertyMap.get(TopologyGraphConstants.LONGITUDE) == null) {
                throw new DataException("Longitude is missing for vertex ID: " + vertex.id());
            }

            if (vertexPropertyMap.get(TopologyGraphConstants.VERTEX_ID) == null) {
                throw new DataException("Vertex ID is missing in property map for vertex ID: " + vertex.id());
            }

            setSourceProperties(connection, vertexPropertyMap);

            List<Map<String, Object>> connectivityList = getConnectivityByVertex(vertex, coreEdgesMap,
                    vertexPropertyMap);

            connection.setConnectivity(connectivityList);
        } catch (DataException e) {
            logger.warn("Checked business exception in setConnectionInfoForVertex: {}", e.getMessage());
        } catch (Exception e) {
            logger.error("Exception in setConnectionInfoForVertex: {}", e.getMessage(), e);
        }
        return connection;
    }

    /**
     * Creates a topology connectivity wrapper for a vertex with its connectivity
     * data.
     *
     * @param vertex       the vertex to process
     * @param coreEdgesMap the map of edges associated with vertices
     * @param vertexMap    the map of vertex properties indexed by vertex ID
     * @return a topology connectivity wrapper for the vertex
     * @throws DataException if required vertex properties are missing or if a
     *                       parameter is null
     */
    private TopologyConnectivityWrapper setTopologyConnectionForVertex(Vertex vertex,
                                                                       Map<Object, Map<String, Object>> coreEdgesMap, Map<Object, Map<String, Object>> vertexMap) {
        TopologyConnectivityWrapper connection = new TopologyConnectivityWrapper();
        try {
            Objects.requireNonNull(vertex, TopologyGraphConstants.VERTEX_NOT_BE_NULL);
            Objects.requireNonNull(coreEdgesMap, TopologyGraphConstants.CORE_EDGES_NOT_BE_NULL);
            Objects.requireNonNull(vertexMap, TopologyGraphConstants.VERTEX_MAP_NOT_BE_NULL);

            Map<String, Object> vertexPropertyMap = vertexMap.get(vertex.id());
            if (vertexPropertyMap == null) {
                throw new DataException("No property map found for vertex ID: " + vertex.id());
            }

            if (vertexPropertyMap.get(TopologyGraphConstants.LATITUDE) == null) {
                throw new DataException("Latitude is missing for vertex ID: " + vertex.id());
            }

            if (vertexPropertyMap.get(TopologyGraphConstants.LONGITUDE) == null) {
                throw new DataException("Longitude is missing for vertex ID: " + vertex.id());
            }

            if (vertexPropertyMap.get(TopologyGraphConstants.VERTEX_ID) == null) {
                throw new DataException("Vertex ID is missing in property map for vertex ID: " + vertex.id());
            }

            setTopologySourceProperties(connection, vertexPropertyMap);

            List<Map<String, Object>> connectivityList = getConnectivityByVertex(vertex, coreEdgesMap,
                    vertexPropertyMap);

            connection.setConnectivity(connectivityList);

        } catch (DataException e) {
            logger.warn("Data exception in setTopologyConnectionForVertex: {}", e.getMessage());
        } catch (NullPointerException e) {
            logger.error("Null pointer exception in setTopologyConnectionForVertex");
            throw new DataException("Required parameter is null: " + e.getMessage(), e);
        } catch (Exception e) {
            logger.error("Exception in setTopologyConnectionForVertex: {}", e.getMessage(), e);
        }
        return connection;
    }

    /**
     * Sets the source properties in a connection information wrapper from a vertex
     * property map.
     *
     * @param connection        the connection wrapper to populate
     * @param vertexPropertyMap the map containing vertex properties
     * @throws NullPointerException if any parameter is null
     * @throws DataException        if there is an error processing the data
     */
    private void setSourceProperties(ConnectionInfoWrapper connection, Map<String, Object> vertexPropertyMap) {
        Objects.requireNonNull(connection, TopologyGraphConstants.CONN_WRAP_NOT_BE_NULL);
        Objects.requireNonNull(vertexPropertyMap, TopologyGraphConstants.VERTEX_PROPERTY_NOT_BE_NULL);

        try {
            connection.seteName(vertexPropertyMap.get(TopologyGraphConstants.ELEMENT_NAME) != null
                    ? vertexPropertyMap.get(TopologyGraphConstants.ELEMENT_NAME).toString()
                    : null);
            connection.setLatitude(vertexPropertyMap.get(TopologyGraphConstants.LATITUDE) != null ? topologyGraphUtils
                    .decodeNumeric(vertexPropertyMap.get(TopologyGraphConstants.LATITUDE).toString()) : null);
            connection.setLongitude(vertexPropertyMap.get(TopologyGraphConstants.LONGITUDE) != null ? topologyGraphUtils
                    .decodeNumeric(vertexPropertyMap.get(TopologyGraphConstants.LONGITUDE).toString()) : null);

            connection.setVertexId(vertexPropertyMap.get(TopologyGraphConstants.VERTEX_ID) != null
                    ? Long.valueOf(vertexPropertyMap.get(TopologyGraphConstants.VERTEX_ID).toString())
                    : null);
            connection.setCity(vertexPropertyMap.get(TopologyGraphConstants.GEOGRAPHY_LEVEL3) != null
                    ? vertexPropertyMap.get(TopologyGraphConstants.GEOGRAPHY_LEVEL3).toString()
                    : null);
            connection.seteType(vertexPropertyMap.get(TopologyGraphConstants.ELEMENT_TYPE) != null
                    ? vertexPropertyMap.get(TopologyGraphConstants.ELEMENT_TYPE).toString()
                    : null);

            connection.setFibertakeEnable(vertexPropertyMap.get(TopologyGraphConstants.FIBERTAKE_ENABLE) != null
                    ? vertexPropertyMap.get(TopologyGraphConstants.FIBERTAKE_ENABLE).toString()
                    : null);
            connection.setSiteId(vertexPropertyMap.get(TopologyGraphConstants.SITEID) != null
                    ? vertexPropertyMap.get(TopologyGraphConstants.SITEID).toString()
                    : null);
            connection.setIpv6(vertexPropertyMap.get(TopologyGraphConstants.IPV6) != null
                    ? vertexPropertyMap.get(TopologyGraphConstants.IPV6).toString()
                    : null);

        } catch (NumberFormatException e) {
            String vertexId = vertexPropertyMap.get(TopologyGraphConstants.VERTEX_ID) != null
                    ? vertexPropertyMap.get(TopologyGraphConstants.VERTEX_ID).toString()
                    : TopologyGraphConstants.UNKNOWN;
            logger.error("Invalid numeric format for vertex ID");
            throw new DataException("Invalid numeric format for vertex ID: " + vertexId, e);
        } catch (Exception e) {
            String elementName = vertexPropertyMap.get(TopologyGraphConstants.ELEMENT_NAME) != null
                    ? vertexPropertyMap.get(TopologyGraphConstants.ELEMENT_NAME).toString()
                    : TopologyGraphConstants.UNKNOWN;
            logger.error("Failed to set source properties for element");
            throw new DataException("Failed to process topology data for element: " + elementName, e);
        }
    }

    /**
     * Sets the topology source properties in a topology connectivity wrapper from a
     * vertex property map.
     *
     * @param connection        the topology connection wrapper to populate
     * @param vertexPropertyMap the map containing vertex properties
     * @throws NullPointerException if any parameter is null
     * @throws DataException        if there is an error processing the data
     */
    private void setTopologySourceProperties(TopologyConnectivityWrapper connection,
                                             Map<String, Object> vertexPropertyMap) {
        Objects.requireNonNull(connection, "Topology connection wrapper cannot be null");
        Objects.requireNonNull(vertexPropertyMap, TopologyGraphConstants.VERTEX_PROPERTY_NOT_BE_NULL);

        try {
            connection.seteName(vertexPropertyMap.get(TopologyGraphConstants.ELEMENT_NAME) != null
                    ? vertexPropertyMap.get(TopologyGraphConstants.ELEMENT_NAME).toString()
                    : null);
            connection.setLatitude(vertexPropertyMap.get(TopologyGraphConstants.LATITUDE) != null ? topologyGraphUtils
                    .decodeNumeric(vertexPropertyMap.get(TopologyGraphConstants.LATITUDE).toString()) : null);
            connection.setLongitude(vertexPropertyMap.get(TopologyGraphConstants.LONGITUDE) != null ? topologyGraphUtils
                    .decodeNumeric(vertexPropertyMap.get(TopologyGraphConstants.LONGITUDE).toString()) : null);

            connection.setVertexId(vertexPropertyMap.get(TopologyGraphConstants.VERTEX_ID) != null
                    ? Long.valueOf(vertexPropertyMap.get(TopologyGraphConstants.VERTEX_ID).toString())
                    : null);
            connection.setGeographyL3(vertexPropertyMap.get(TopologyGraphConstants.GEOGRAPHY_LEVEL3) != null
                    ? vertexPropertyMap.get(TopologyGraphConstants.GEOGRAPHY_LEVEL3).toString()
                    : null);
            connection.seteType(vertexPropertyMap.get(TopologyGraphConstants.ELEMENT_TYPE) != null
                    ? vertexPropertyMap.get(TopologyGraphConstants.ELEMENT_TYPE).toString()
                    : null);
            if (vertexPropertyMap.get(TopologyGraphConstants.ENABLED_ORIGIN_NW) != null) {
                String enableNW = vertexPropertyMap.get(TopologyGraphConstants.ENABLED_ORIGIN_NW).toString();
                List<String> enableNWList = new ArrayList<>(Arrays.asList(enableNW.split(",")));
                connection.setEnabledOriginNW(enableNWList);
            }
            connection.setSiteId(vertexPropertyMap.get(TopologyGraphConstants.SITEID) != null
                    ? vertexPropertyMap.get(TopologyGraphConstants.SITEID).toString()
                    : null);
            connection.setIpv6(vertexPropertyMap.get(TopologyGraphConstants.IPV6) != null
                    ? vertexPropertyMap.get(TopologyGraphConstants.IPV6).toString()
                    : null);

        } catch (NumberFormatException e) {
            String vertexId = vertexPropertyMap.get(TopologyGraphConstants.VERTEX_ID) != null
                    ? vertexPropertyMap.get(TopologyGraphConstants.VERTEX_ID).toString()
                    : TopologyGraphConstants.UNKNOWN;
            logger.error("Invalid numeric format for topology vertex ID");
            throw new DataException("Invalid numeric format for topology vertex ID: " + vertexId, e);
        } catch (Exception e) {
            String elementName = vertexPropertyMap.get(TopologyGraphConstants.ELEMENT_NAME) != null
                    ? vertexPropertyMap.get(TopologyGraphConstants.ELEMENT_NAME).toString()
                    : TopologyGraphConstants.UNKNOWN;
            logger.error("Failed to set topology source properties for element");
            throw new DataException("Failed to process topology data for element: " + elementName, e);
        }
    }

    /**
     * Retrieves connectivity information for a vertex based on its connected edges.
     *
     * @param vertex            the vertex to process
     * @param coreEdgesMap      the map of edges associated with vertices
     * @param vertexPropertyMap the map containing vertex properties
     * @return a list of maps containing connectivity information
     * @throws NullPointerException if any parameter is null
     * @throws DataException        if there is an error processing the data
     */
    private List<Map<String, Object>> getConnectivityByVertex(Vertex vertex,
                                                              Map<Object, Map<String, Object>> coreEdgesMap, Map<String, Object> vertexPropertyMap) {
        Objects.requireNonNull(vertex, TopologyGraphConstants.VERTEX_NOT_BE_NULL);
        Objects.requireNonNull(coreEdgesMap, TopologyGraphConstants.CORE_EDGES_NOT_BE_NULL);
        Objects.requireNonNull(vertexPropertyMap, TopologyGraphConstants.VERTEX_PROPERTY_NOT_BE_NULL);

        List<Map<String, Object>> connectivityList = new ArrayList<>();

        try {
            for (Entry<Object, Map<String, Object>> entry : coreEdgesMap.entrySet()) {
                String key = entry.getKey().toString();
                Map<String, Object> edgeProperty = entry.getValue();

                if (key.contains(Symbol.HASH_STRING + vertex.id().toString() + Symbol.HASH_STRING)) {
                    Map<String, Object> connectionData = setDestinationProperty(vertexPropertyMap, edgeProperty);

                    if (connectionData.get(TopologyGraphConstants.ELEMENT_NAME) != null) {
                        connectivityList.add(connectionData);
                    }
                }
            }
        } catch (DataException e) {
            throw e;
        } catch (Exception e) {
            String vertexId = (vertex != null && vertex.id() != null) ? vertex.id().toString() : "null";
            logger.error("Failed to get connectivity for vertex ID");
            throw new DataException("Failed to process connectivity data for vertex ID: " + vertexId, e);
        }
        return connectivityList;
    }

    /**
     * Sets destination properties in a connection data map based on vertex and edge
     * properties.
     *
     * @param vertexPropertyMap the map containing vertex properties
     * @param edgeProperty      the map containing edge properties
     * @return a map containing destination connection data
     * @throws NullPointerException if any parameter is null
     * @throws DataException        if there is an error processing the data
     */
    private Map<String, Object> setDestinationProperty(Map<String, Object> vertexPropertyMap,
                                                       Map<String, Object> edgeProperty) {
        Objects.requireNonNull(vertexPropertyMap, TopologyGraphConstants.VERTEX_PROPERTY_NOT_BE_NULL);
        Objects.requireNonNull(edgeProperty, TopologyGraphConstants.EDGE_PROPERTY_NOT_BE_NULL);

        Map<String, Object> connectionData = new HashMap<>();

        try {
            connectionData.put(TopologyGraphConstants.EDGE_ID, edgeProperty.get(TopologyGraphConstants.EDGE_ID));
            String elementName = vertexPropertyMap.get(TopologyGraphConstants.ELEMENT_NAME) != null
                    ? vertexPropertyMap.get(TopologyGraphConstants.ELEMENT_NAME).toString()
                    : null;

            if (elementName != null) {

                String secNodeName = edgeProperty.get(TopologyGraphConstants.SOURCE_ELEMENT_NAME) != null
                        ? edgeProperty.get(TopologyGraphConstants.SOURCE_ELEMENT_NAME).toString()
                        : null;
                String destNodeName = edgeProperty.get(TopologyGraphConstants.DESTINATION_ELEMENT_NAME) != null
                        ? edgeProperty.get(TopologyGraphConstants.DESTINATION_ELEMENT_NAME).toString()
                        : null;

                if (secNodeName != null && secNodeName.equalsIgnoreCase(elementName)) {
                    connectionData.put(TopologyGraphConstants.ELEMENT_NAME,
                            edgeProperty.get(TopologyGraphConstants.DESTINATION_ELEMENT_NAME));
                    connectionData.put(TopologyGraphConstants.DESTINATION_PORT,
                            edgeProperty.get(TopologyGraphConstants.DESTINATION_PORT));
                    connectionData.put(TopologyGraphConstants.SOURCE_PORT,
                            edgeProperty.get(TopologyGraphConstants.SOURCE_PORT));
                } else if (destNodeName != null && destNodeName.equalsIgnoreCase(elementName)) {
                    connectionData.put(TopologyGraphConstants.ELEMENT_NAME,
                            edgeProperty.get(TopologyGraphConstants.SOURCE_ELEMENT_NAME));
                    connectionData.put(TopologyGraphConstants.DESTINATION_PORT,
                            edgeProperty.get(TopologyGraphConstants.SOURCE_PORT));
                    connectionData.put(TopologyGraphConstants.SOURCE_PORT,
                            edgeProperty.get(TopologyGraphConstants.DESTINATION_PORT));
                }

            }
        } catch (Exception e) {
            String edgeId = edgeProperty.get(TopologyGraphConstants.EDGE_ID) != null
                    ? edgeProperty.get(TopologyGraphConstants.EDGE_ID).toString()
                    : TopologyGraphConstants.UNKNOWN;
            logger.error("Failed to set destination properties for edge ID");
            throw new DataException("Failed to process destination data for edge ID: " + edgeId, e);
        }
        return connectionData;
    }

    /**
     * Retrieves topology data for a specific city.
     *
     * @param city the city name to filter vertices
     * @return a map containing topology connectivity wrappers for the city
     * @throws IllegalArgumentException if city is null or empty
     * @throws DataNotFoundException    if no topology data is found for the city
     * @throws DataProcessingException  if there is an error processing the data
     */
    @Override
    public Map<String, List<TopologyConnectivityWrapper>> getCityTopology(String city) {
        if (city == null || city.trim().isEmpty()) {
            throw new IllegalArgumentException("City name cannot be null or empty");
        }

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        Map<String, List<TopologyConnectivityWrapper>> cityConnections = new HashMap<>();
        try {
            List<Vertex> cityVertices = topologyGraphUtils
                    .getAllVertexByPropertyValue(TopologyGraphConstants.GEOGRAPHY_LEVEL3, city);
            logger.info("Size of cityVertices list is : {}", cityVertices != null ? cityVertices.size() : 0);

            if (cityVertices != null && !cityVertices.isEmpty()) {
                List<TopologyConnectivityWrapper> connectionList = processCityTopology(city, cityVertices);
                cityConnections.put(TopologyGraphConstants.NW_ELEMENTS, connectionList);
            }
            stopWatch.stop();
            logger.info("Time Taken in getCityTopology : {}", stopWatch);
        } catch (ResourceNotFoundException e) {
            logger.error("City topology not found for city");
            throw new DataNotFoundException("No topology found for city: " + city, e);
        } catch (NoSuchElementException e) {
            logger.error("Element not found in getCityTopology for city");
            throw new DataNotFoundException("Required element not found for city topology: " + city, e);
        } catch (Exception e) {
            logger.error("Error processing city topology for city");
            throw new DataProcessingException("Failed to process topology for city: " + city, e);
        }

        return cityConnections;
    }

    private List<TopologyConnectivityWrapper> processCityTopology(String city, List<Vertex> cityVertices) {
        Set<Vertex> cityFilteredVertices = new HashSet<>();

        List<Edge> coreEdges = topologyGraphUtils
                .getAllEdgeByVertexPropertyValue(TopologyGraphConstants.GEOGRAPHY_LEVEL3, city);
        logger.info("Size of coreEdges list is : {}", coreEdges != null ? coreEdges.size() : 0);

        Map<Object, Map<String, Object>> cityEdgesMap = topologyGraphUtils
                .getVListAndEVertexIDMapByEdgeList(coreEdges, cityFilteredVertices, cityVertices);

        Map<Object, Map<String, Object>> vertexIdMap = topologyGraphUtils
                .getVertexPropertyMapByVertexList(cityVertices);

        List<TopologyConnectivityWrapper> connectionList = new ArrayList<>();
        for (Vertex vertex : cityFilteredVertices) {
            TopologyConnectivityWrapper connection = setTopologyConnectionForVertex(vertex, cityEdgesMap, vertexIdMap);
            connectionList.add(connection);
        }

        return connectionList;
    }

    /**
     * Retrieves connected edges for a specific vertex ID and category.
     *
     * @param vertexId the ID of the vertex to find connected edges for
     * @param category the category of edges to filter
     * @return a map containing nodes and links information
     * @throws IllegalArgumentException if vertex ID is null
     * @throws VertexNotFoundException  if the vertex is not found
     * @throws DataProcessingException  if there is an error processing the data
     */
    @Override
    public Map<String, Object> getConnectedEdgesByVertexId(Long vertexId, String category) {
        if (vertexId == null) {
            throw new IllegalArgumentException("Vertex ID cannot be null");
        }

        Map<String, Object> topologyWrapper = new HashMap<>();
        List<Map<String, Object>> edgeList = new ArrayList<>();
        Set<Map<String, Object>> vertexSet = new HashSet<>();
        Set<Edge> connectedEdgesByVertexId;

        try {
            connectedEdgesByVertexId = topologyGraphUtils.getConnectedEdgesByVIdAndCategory(vertexId, category);
            logger.info("getConnectedEdgesByVertexId connectedEdgesByVertexId size {}",
                    connectedEdgesByVertexId.size());

            if (!connectedEdgesByVertexId.isEmpty()) {
                for (Edge connectedEdge : connectedEdgesByVertexId) {
                    Map<String, Object> propertyMapByEdge = iTopologyHbaseDao.getEdgeData(connectedEdge);
                    addElementTypeAndAliasPortInEdgeProperty(propertyMapByEdge);

                    if (isValidEdge(propertyMapByEdge)) {
                        edgeList.add(propertyMapByEdge);
                        processConnectedVertices(connectedEdge, vertexSet);
                    }
                }
            }

            topologyWrapper.put(TopologyGraphConstants.NODES, vertexSet);
            topologyWrapper.put(TopologyGraphConstants.LINKS, edgeList);

        } catch (NoSuchElementException e) {
            logger.error("Vertex with ID not found");
            throw new VertexNotFoundException("Vertex with ID " + vertexId + " not found", e);
        } catch (IllegalArgumentException e) {
            logger.error("Invalid argument for getConnectedEdgesByVertexId");
            throw new BusinessException("Invalid argument: " + e.getMessage(), e);
        } catch (DataException e) {
            logger.error("Data error for vertex ID: {}, category: {}", vertexId, category);
            throw e;
        } catch (Exception e) {
            logger.error("Error processing connected edges for vertex ID: {}, category: {}", vertexId, category);
            throw new DataProcessingException("Failed to process connected edges for vertex ID: " + vertexId, e);
        }

        return topologyWrapper;
    }

    /**
     * Processes the vertices connected to the given edge and adds them to the
     * provided vertex set.
     * This method retrieves data for each vertex connected to the edge and adds it
     * to the vertex set.
     *
     * @param connectedEdge the edge whose connected vertices need to be processed,
     *                      must not be null
     * @param vertexSet     the set to which the processed vertex data will be
     *                      added, must not be null
     * @throws NullPointerException if connectedEdge or vertexSet is null
     * @throws DataException        if vertex data cannot be retrieved or processed
     */
    private void processConnectedVertices(Edge connectedEdge, Set<Map<String, Object>> vertexSet) {
        Objects.requireNonNull(connectedEdge, TopologyGraphConstants.CONNECTED_EDGE_CANNOT_BE_NULL);
        Objects.requireNonNull(vertexSet, TopologyGraphConstants.VERTEX_SET_NOT_BE_NULL);

        Iterator<Vertex> bothVertices = connectedEdge.bothVertices();
        if (bothVertices != null) {
            while (bothVertices.hasNext()) {
                Vertex vertex = bothVertices.next();
                try {
                    Map<String, Object> propertyMapByVertex = iTopologyHbaseDao.getVertexData(vertex);

                    if (propertyMapByVertex.get(TopologyGraphConstants.ELEMENT_NAME) != null) {
                        addElementAliasName(propertyMapByVertex);
                    }

                    updateGeographyLevels(propertyMapByVertex);

                    vertexSet.add(propertyMapByVertex);
                } catch (NoSuchElementException e) {
                    logger.warn("Vertex data not found for edge: {}", connectedEdge.id());
                    throw new DataException("Vertex data not found for connected edge", e);
                }
            }
        }
    }

    /**
     * Adds the element alias name to the vertex property map if the element name
     * exists.
     * Retrieves the element info wrapper by element name and adds the source alias
     * name to the property map.
     *
     * @param propertyMapByVertex the vertex property map to which the alias name
     *                            will be added, must not be null
     * @throws NullPointerException if propertyMapByVertex is null
     */
    private void addElementAliasName(Map<String, Object> propertyMapByVertex) {
        Objects.requireNonNull(propertyMapByVertex, TopologyGraphConstants.PROPERTY_MAP_NOT_BE_NULL);

        if (propertyMapByVertex.get(TopologyGraphConstants.ELEMENT_NAME) == null) {
            return;
        }

        try {
            String elementName = propertyMapByVertex.get(TopologyGraphConstants.ELEMENT_NAME).toString();
            ElementInfoWrapper elementInfoWrapper = getElementInfoWrapperByEName(elementName);
            if (elementInfoWrapper.getSrcEliaseName() != null) {
                propertyMapByVertex.put(TopologyGraphConstants.SOURCE_ALIASE_ENAME,
                        elementInfoWrapper.getSrcEliaseName());
            }
        } catch (NoSuchElementException e) {
            logger.warn("Element alias name not found for element: {}",
                    propertyMapByVertex.get(TopologyGraphConstants.ELEMENT_NAME), e);
        }
    }

    /**
     * Updates the geography level values in the vertex property map.
     * Replaces the geography level values with their corresponding display name
     * values if available.
     *
     * @param propertyMapByVertex the vertex property map whose geography levels
     *                            need to be updated, must not be null
     * @throws NullPointerException if propertyMapByVertex is null
     * @throws DataException        if geography level data is missing in vertex
     *                              properties
     */
    private void updateGeographyLevels(Map<String, Object> propertyMapByVertex) {
        Objects.requireNonNull(propertyMapByVertex, TopologyGraphConstants.PROPERTY_MAP_NOT_BE_NULL);

        try {
            propertyMapByVertex.replace(TopologyGraphConstants.GEOGRAPHY_LEVEL1,
                    getGeographyLevelValue(propertyMapByVertex, TopologyGraphConstants.GEOGRAPHYL1_DNAME,
                            TopologyGraphConstants.GEOGRAPHY_LEVEL1));
            propertyMapByVertex.replace(TopologyGraphConstants.GEOGRAPHY_LEVEL2,
                    getGeographyLevelValue(propertyMapByVertex, TopologyGraphConstants.GEOGRAPHYL2_DNAME,
                            TopologyGraphConstants.GEOGRAPHY_LEVEL2));
            propertyMapByVertex.replace(TopologyGraphConstants.GEOGRAPHY_LEVEL3,
                    getGeographyLevelValue(propertyMapByVertex, TopologyGraphConstants.GEOGRAPHYL3_DNAME,
                            TopologyGraphConstants.GEOGRAPHY_LEVEL3));
            propertyMapByVertex.replace(TopologyGraphConstants.GEOGRAPHY_LEVEL4,
                    getGeographyLevelValue(propertyMapByVertex, TopologyGraphConstants.GEOGRAPHYL4_DNAME,
                            TopologyGraphConstants.GEOGRAPHY_LEVEL4));
        } catch (NullPointerException e) {
            logger.warn("Geography level data missing in vertex properties");
            throw new DataException("Geography level data missing in vertex properties", e);
        }
    }

    /**
     * Gets the geography level value from the vertex property map.
     * Returns the display name value if available, otherwise returns the default
     * level value.
     *
     * @param propertyMapByVertex the vertex property map containing the geography
     *                            level values, must not be null
     * @param levelName           the name of the display name property to check
     *                            first, must not be null
     * @param defaultLevel        the name of the default level property to use if
     *                            display name is not available, must not be null
     * @return the geography level value as a string
     * @throws NullPointerException if any parameter is null
     */
    private String getGeographyLevelValue(Map<String, Object> propertyMapByVertex, String levelName,
                                          String defaultLevel) {
        Objects.requireNonNull(propertyMapByVertex, TopologyGraphConstants.PROPERTY_MAP_NOT_BE_NULL);
        Objects.requireNonNull(levelName, "Level name cannot be null");
        Objects.requireNonNull(defaultLevel, "Default level cannot be null");

        return propertyMapByVertex.get(levelName) != null
                ? propertyMapByVertex.get(levelName).toString()
                : propertyMapByVertex.get(defaultLevel).toString();
    }

    /**
     * Validates if an edge should be included in the topology.
     * <p>
     * Excludes edges where source node (any router except ENODEB) connects to
     * destination node of type ENODEB with mwType MICROWAVE.
     *
     * @param propertyMapByEdge the property map containing edge data, must not be
     *                          null
     * @return true if the edge is valid and should be included, false otherwise
     * @throws IllegalArgumentException if propertyMapByEdge is null
     * @throws DataNotFoundException    if required elements cannot be found
     * @throws DataException            if required properties are missing for edge
     *                                  validation
     */
    private boolean isValidEdge(Map<String, Object> propertyMapByEdge) {
        if (propertyMapByEdge == null) {
            throw new IllegalArgumentException(TopologyGraphConstants.EDGE_PROPERTY_NOT_BE_NULL);
        }

        try {
            String destMWType = null;
            String srcNodeType = (String) propertyMapByEdge.get(TopologyGraphConstants.SOURCE_ELEMENT_TYPE);
            String destNodeType = (String) propertyMapByEdge.get(TopologyGraphConstants.DESTINATION_ELEMENT_TYPE);
            ElementInfoWrapper destVertext = getElementInfoWrapperByEName(
                    TopologyGraphConstants.DESTINATION_ELEMENT_NAME);
            if (destVertext != null) {
                destMWType = destVertext.getMwType();
            }

            logger.error("srcNode :{},srcNodeType :{},===> destNode :{},destNodeType :{},destMWType :{}",
                    propertyMapByEdge.get(TopologyGraphConstants.SOURCE_ELEMENT_NAME), srcNodeType,
                    propertyMapByEdge.get(TopologyGraphConstants.DESTINATION_ELEMENT_NAME), destNodeType, destMWType);

            return !(!TopologyGraphConstants.ENODEB.equalsIgnoreCase(srcNodeType)
                    && TopologyGraphConstants.ENODEB.equalsIgnoreCase(destNodeType)
                    && TopologyGraphConstants.MICROWAVE.equalsIgnoreCase(destMWType));
        } catch (ResourceNotFoundException e) {
            throw new DataNotFoundException("Element not found when validating edge", e);
        } catch (NullPointerException e) {
            String edgeId = propertyMapByEdge.get(TopologyGraphConstants.EDGE_ID) != null
                    ? propertyMapByEdge.get(TopologyGraphConstants.EDGE_ID).toString()
                    : TopologyGraphConstants.UNKNOWN;
            throw new DataException("Missing required properties for edge validation: " + edgeId, e);
        } catch (Exception e) {
            logger.error("Exception inside isValidEdge {} ", e.getMessage());
        }
        return true;
    }

    /**
     * Adds element type and alias port information to the edge property map.
     * Retrieves element information for source and destination elements and adds it
     * to the edge properties.
     *
     * @param propertyMapByEdge the edge property map to be enriched with element
     *                          type and alias port information, must not be null
     * @throws IllegalArgumentException if propertyMapByEdge is null
     * @throws DataException            if required element names are missing
     * @throws DataNotFoundException    if elements cannot be found
     * @throws DataProcessingException  if processing fails
     */
    private void addElementTypeAndAliasPortInEdgeProperty(Map<String, Object> propertyMapByEdge) {
        if (propertyMapByEdge == null) {
            throw new IllegalArgumentException(TopologyGraphConstants.EDGE_PROPERTY_NOT_BE_NULL);
        }

        try {
            String sourceElementName = extractRequiredString(propertyMapByEdge,
                    TopologyGraphConstants.SOURCE_ELEMENT_NAME, "Source element name is missing in edge properties");
            String destElementName = extractRequiredString(propertyMapByEdge,
                    TopologyGraphConstants.DESTINATION_ELEMENT_NAME,
                    "Destination element name is missing in edge properties");

            String srcPort = getOptionalString(propertyMapByEdge, TopologyGraphConstants.SOURCE_PORT);
            String destPort = getOptionalString(propertyMapByEdge, TopologyGraphConstants.DESTINATION_PORT);

            propertyMapByEdge.put(TopologyGraphConstants.SOURCE_ALIAS_PORT, srcPort);
            propertyMapByEdge.put(TopologyGraphConstants.DESTINATION_ALIAS_PORT, destPort);

            addElementMetadata(propertyMapByEdge, sourceElementName, destElementName);

        } catch (ResourceNotFoundException e) {
            logger.error("Element not found when adding element type and alias port");
            throw new DataNotFoundException("Element not found when adding element type and alias port", e);
        } catch (NullPointerException e) {
            String edgeId = getOptionalString(propertyMapByEdge, TopologyGraphConstants.EDGE_ID,
                    TopologyGraphConstants.UNKNOWN);
            logger.error(TopologyGraphConstants.MISSING_REQUIRED_PROPERTIES_FOR_EDGE);
            throw new DataException(TopologyGraphConstants.MISSING_REQUIRED_PROPERTIES_FOR_EDGE + edgeId, e);
        } catch (Exception e) {
            String sourceElement = getOptionalString(propertyMapByEdge, TopologyGraphConstants.SOURCE_ELEMENT_NAME,
                    TopologyGraphConstants.UNKNOWN);
            String destElement = getOptionalString(propertyMapByEdge, TopologyGraphConstants.DESTINATION_ELEMENT_NAME,
                    TopologyGraphConstants.UNKNOWN);
            logger.error("Failed to add element type and alias port for edge");
            throw new DataProcessingException("Failed to add element type and alias port for edge between "
                    + sourceElement + " and " + destElement, e);
        }
    }

    private String extractRequiredString(Map<String, Object> map, String key, String errorMessage) {
        Object value = map.get(key);
        if (value == null) {
            throw new DataException(errorMessage);
        }
        return value.toString();
    }

    private String getOptionalString(Map<String, Object> map, String key) {
        return getOptionalString(map, key, null);
    }

    private String getOptionalString(Map<String, Object> map, String key, String defaultValue) {
        Object value = map.get(key);
        return value != null ? value.toString() : defaultValue;
    }

    private void addElementMetadata(Map<String, Object> map, String sourceElementName, String destElementName) {
        ElementInfoWrapper srcVertex = getElementInfoWrapperByEName(sourceElementName);
        ElementInfoWrapper destVertex = getElementInfoWrapperByEName(destElementName);

        if (srcVertex != null) {
            map.put(TopologyGraphConstants.SOURCE_ELEMENT_TYPE, srcVertex.getElementAliaseType());
            map.put(TopologyGraphConstants.DOMAIN, srcVertex.getEquipDomain());
            map.put(TopologyGraphConstants.VENDOR, srcVertex.getEquipVendor());
        }
        if (destVertex != null) {
            map.put(TopologyGraphConstants.DESTINATION_ELEMENT_TYPE, destVertex.getElementAliaseType());
        }
    }

    /**
     * Processes a traversed path and builds a path wrapper for vertex path mapping.
     * Extracts connection information from the path and adds it to the stack of
     * path wrappers.
     *
     * @param traversedPath      the path to be processed, must not be null
     * @param stackOfpathWrap    the stack to which the processed path wrappers will
     *                           be added, must not be null
     * @param processedElementId set of element IDs that have already been
     *                           processed, must not be null
     * @throws NullPointerException    if any parameter is null
     * @throws DataException           if required data is missing in path traversal
     * @throws DataProcessingException if path wrapper processing fails
     */
    public void getPathWrapperByVertexPathforMap(Path traversedPath, Stack<List<ConnectionInfoWrapper>> stackOfpathWrap,
                                                 Set<Long> processedElementId) {
        Objects.requireNonNull(traversedPath, "Path cannot be null");
        Objects.requireNonNull(stackOfpathWrap, "Stack cannot be null");
        Objects.requireNonNull(processedElementId, "Processed element set cannot be null");

        List<ConnectionInfoWrapper> nwInfoList = new ArrayList<>();
        try {
            getConnection(traversedPath, stackOfpathWrap, nwInfoList);
            logger.info("nwInfoList is :::{}", nwInfoList);
            if (!nwInfoList.isEmpty()) {
                getConnectedLink(stackOfpathWrap, processedElementId, nwInfoList);
            }
        } catch (NullPointerException e) {
            logger.error("Missing required data in path traversal");
            throw new DataException("Missing required data in path traversal", e);
        } catch (Exception e) {
            logger.error("Failed to process path wrapper");
            throw new DataProcessingException("Failed to process path wrapper", e);
        }
    }

    /**
     * Extracts connection information from a traversed path.
     * Processes each element in the path and creates connection info wrappers.
     *
     * @param traversedPath   the path from which to extract connection information,
     *                        must not be null
     * @param stackOfpathWrap the stack to which the list of connection info
     *                        wrappers will be added, must not be null
     * @param nwInfoList      the list to which the created connection info wrappers
     *                        will be added, must not be null
     * @throws NullPointerException    if any parameter is null
     * @throws DataException           if the path is empty or has an invalid
     *                                 structure
     * @throws DataProcessingException if connection data processing fails
     */
    @SuppressWarnings("java:S107")
    private void getConnection(Path traversedPath, Stack<List<ConnectionInfoWrapper>> stackOfpathWrap,
                               List<ConnectionInfoWrapper> nwInfoList) {
        Objects.requireNonNull(traversedPath, "Path cannot be null");
        Objects.requireNonNull(stackOfpathWrap, "Stack cannot be null");
        Objects.requireNonNull(nwInfoList, "Network info list cannot be null");
        try {
            Iterator<Object> iterator = traversedPath.iterator();
            if (!iterator.hasNext()) {
                throw new DataException("Empty path provided for connection processing");
            }
            while (iterator.hasNext()) {
                Object next = iterator.next();
                ConnectionInfoWrapper nwInfo = new ConnectionInfoWrapper();
                if (TopologyGraphUtils.isVertex(next)) {
                    setSiteDetails(next, nwInfo);
                }
                next = iterator.hasNext() ? iterator.next() : next;
                if (TopologyGraphUtils.isEdge(next)) {
                    setPortDetails(next, nwInfo);
                }
                nwInfoList.add(nwInfo);
            }
            stackOfpathWrap.push(nwInfoList);
        } catch (NoSuchElementException e) {
            logger.error("Invalid path structure encountered during traversal");
            throw new DataException("Invalid path structure encountered during traversal", e);
        } catch (Exception e) {
            logger.error("Failed to process connection data from path");
            throw new DataProcessingException("Failed to process connection data from path", e);
        }
    }


    /**
     * Sets port details in the network info wrapper based on the edge object.
     * Extracts port information from the edge and sets it in the network info
     * wrapper.
     *
     * @param next   the edge object from which to extract port details, must not be
     *               null and must be an Edge
     * @param nwInfo the network info wrapper in which to set the port details, must
     *               not be null
     * @throws NullPointerException     if next or nwInfo is null
     * @throws IllegalArgumentException if next is not an Edge
     * @throws DataException            if required edge properties are missing
     * @throws DataNotFoundException    if referenced elements cannot be found
     * @throws DataProcessingException  if port details processing fails
     */
    private void setPortDetails(Object next, ConnectionInfoWrapper nwInfo) {
        Objects.requireNonNull(next, "Edge object cannot be null");
        Objects.requireNonNull(nwInfo, TopologyGraphConstants.NETWORK_INFO_NOT_BE_NULL);

        if (!TopologyGraphUtils.isEdge(next)) {
            throw new IllegalArgumentException("Object is not an edge: " + next.getClass().getName());
        }

        try {
            Map<String, Object> edgeMap = topologyGraphUtils.getPropertyMapByEdge((Edge) next);
            if (edgeMap == null || edgeMap.isEmpty())
                return;

            setBasicEdgeInfo(nwInfo, edgeMap);

            String sourcePort = getEdgeValue(edgeMap, TopologyGraphConstants.SOURCE_PORT);
            String destPort = getEdgeValue(edgeMap, TopologyGraphConstants.DESTINATION_PORT);
            String srcElementName = edgeMap.get(TopologyGraphConstants.SOURCE_ELEMENT_NAME).toString();

            setElementMetaInfo(nwInfo, srcElementName);

            if (srcElementName.equalsIgnoreCase(nwInfo.geteName())) {
                setPortData(nwInfo, sourcePort, destPort, sourcePort, destPort);
            } else {
                setPortData(nwInfo, destPort, sourcePort, destPort, sourcePort);
            }

        } catch (ClassCastException e) {
            logger.error("Invalid edge object provided for port details");
            throw new DataException("Invalid edge object provided for port details", e);
        } catch (NullPointerException e) {
            logger.error("Missing required edge properties for port details");
            throw new DataException("Missing required edge properties for port details", e);
        } catch (ResourceNotFoundException e) {
            logger.error("Element not found when setting port details");
            throw new DataNotFoundException("Element not found when setting port details", e);
        } catch (Exception e) {
            logger.error("Failed to set port details");
            throw new DataProcessingException("Failed to set port details", e);
        }
    }


    private void setBasicEdgeInfo(ConnectionInfoWrapper nwInfo, Map<String, Object> edgeMap) {
        nwInfo.setEdge(getEdgeValue(edgeMap, TopologyGraphConstants.EDGE_ID));
        nwInfo.setVlanConnInfo(getEdgeValue(edgeMap, TopologyGraphConstants.VLAN_CONNECTION_INFORMATION));
    }

    private void setElementMetaInfo(ConnectionInfoWrapper nwInfo, String elementName) {
        ElementInfoWrapper elementInfoWrapper = getElementInfoWrapperByEName(elementName);
        if (elementInfoWrapper != null) {
            nwInfo.setDomain(elementInfoWrapper.getEquipDomain());
            nwInfo.setVendor(elementInfoWrapper.getEquipVendor());
        }
    }

    private String getEdgeValue(Map<String, Object> edgeMap, String key) {
        Object value = edgeMap.get(key);
        return value != null ? value.toString() : null;
    }

    /**
     * Sets port data in the network info wrapper.
     * Assigns source and destination port values and their aliases to the network
     * info wrapper.
     *
     * @param nwInfo      the network info wrapper in which to set the port data,
     *                    must not be null
     * @param srcPort     the source port value
     * @param destPort    the destination port value
     * @param sourceAlias the source port alias
     * @param destAlias   the destination port alias
     * @throws NullPointerException if nwInfo is null
     */
    private static void setPortData(ConnectionInfoWrapper nwInfo, String srcPort, String destPort, String sourceAlias,
                                    String destAlias) {
        Objects.requireNonNull(nwInfo, TopologyGraphConstants.NETWORK_INFO_NOT_BE_NULL);
        nwInfo.setSrcPort(srcPort);
        nwInfo.setDestPort(destPort);
        nwInfo.setSrcAliasPort(sourceAlias);
        nwInfo.setDestAliasPort(destAlias);
    }

    /**
     * Sets site details in the network info wrapper based on the vertex object.
     * Extracts site information from the vertex and sets it in the network info
     * wrapper.
     *
     * @param next   the vertex object from which to extract site details, must not
     *               be null and must be a Vertex
     * @param nwInfo the network info wrapper in which to set the site details, must
     *               not be null
     * @throws NullPointerException    if next or nwInfo is null
     * @throws DataException           if vertex property map is null or empty
     * @throws DataProcessingException if site details processing fails
     */
    private void setSiteDetails(Object next, ConnectionInfoWrapper nwInfo) {
        Objects.requireNonNull(next, "Vertex object cannot be null");
        Objects.requireNonNull(nwInfo, TopologyGraphConstants.NETWORK_INFO_NOT_BE_NULL);

        Map<String, Object> vertexMap = topologyGraphUtils.getPropertyMapByVertex((Vertex) next);
        if (vertexMap == null || vertexMap.isEmpty()) {
            throw new DataException("Vertex property map is null or empty");
        }

        try {
            setBasicDetails(vertexMap, nwInfo);
            setNetworkDetails(vertexMap, nwInfo);
            setElementInfo(vertexMap, nwInfo);
        } catch (NullPointerException e) {
            String vertexId = ((Vertex) next).id().toString();
            logger.error("Missing required vertex properties for vertex ID: {}", vertexId);
            throw new DataProcessingException("Missing required vertex properties for vertex ID: " + vertexId, e);
        } catch (ClassCastException e) {
            logger.error("Invalid vertex object provided for site details");
            throw new DataProcessingException("Invalid vertex object provided for site details", e);
        } catch (Exception e) {
            String vertexId = ((Vertex) next).id().toString();
            logger.error("Exception in setSiteDetails for vertex ID: {}", vertexId);
            throw new DataProcessingException("Failed to process site details for vertex ID: " + vertexId, e);
        }
    }

    /**
     * Sets basic details in the network info wrapper from the vertex map.
     * Extracts vertex ID and element type and sets them in the network info
     * wrapper.
     *
     * @param vertexMap the vertex property map containing the basic details, must
     *                  not be null
     * @param nwInfo    the network info wrapper in which to set the basic details,
     *                  must not be null
     * @throws NullPointerException    if vertexMap or nwInfo is null
     * @throws NumberFormatException   if vertex ID has an invalid numeric format
     * @throws DataProcessingException if basic details processing fails
     */
    private void setBasicDetails(Map<String, Object> vertexMap, ConnectionInfoWrapper nwInfo) {
        Objects.requireNonNull(vertexMap, TopologyGraphConstants.VERTEX_MAP_NOT_BE_NULL);
        Objects.requireNonNull(nwInfo, TopologyGraphConstants.NETWORK_INFO_WRAPPER_NOT_BE_NULL);

        try {
            nwInfo.setVertexId(getLongValue(vertexMap, TopologyGraphConstants.VERTEX_ID));

            nwInfo.seteType(getStringValue(vertexMap, TopologyGraphConstants.ELEMENT_TYPE));
        } catch (NumberFormatException e) {
            String vertexId = getStringValue(vertexMap, TopologyGraphConstants.VERTEX_ID);
            logger.error("Invalid numeric format for vertex ID: {}", vertexId);
            throw new DataException("Invalid numeric format for vertex ID: " + vertexId, e);
        } catch (Exception e) {
            String elementName = getStringValue(vertexMap, TopologyGraphConstants.ELEMENT_NAME);
            logger.error("Failed to set basic details for element: {}", elementName);
            throw new DataProcessingException("Failed to set basic details for element: " + elementName, e);
        }
    }

    /**
     * Sets network details in the network info wrapper from the vertex map.
     * Extracts network-related information such as coordinates, IP addresses, and
     * site IDs.
     *
     * @param vertexMap the vertex property map containing the network details, must
     *                  not be null
     * @param nwInfo    the network info wrapper in which to set the network
     *                  details, must not be null
     * @throws NullPointerException    if vertexMap or nwInfo is null
     * @throws NumberFormatException   if numeric values have invalid formats
     * @throws DataProcessingException if network details processing fails
     */
    private void setNetworkDetails(Map<String, Object> vertexMap, ConnectionInfoWrapper nwInfo) {
        Objects.requireNonNull(vertexMap, TopologyGraphConstants.VERTEX_MAP_NOT_BE_NULL);
        Objects.requireNonNull(nwInfo, TopologyGraphConstants.NETWORK_INFO_WRAPPER_NOT_BE_NULL);

        try {
            nwInfo.setLatitude(getDoubleValue(vertexMap, TopologyGraphConstants.LATITUDE));
            nwInfo.setLongitude(getDoubleValue(vertexMap, TopologyGraphConstants.LONGITUDE));

            nwInfo.seteName(getStringValue(vertexMap, TopologyGraphConstants.ELEMENT_NAME));
            nwInfo.setIpv6(getStringValue(vertexMap, TopologyGraphConstants.IPV6));
            nwInfo.setIpv4(getStringValue(vertexMap, TopologyGraphConstants.IPV4));
            nwInfo.setSiteId(getStringValue(vertexMap, TopologyGraphConstants.SITEID));
            nwInfo.setFiberTakeSite(getStringValue(vertexMap, TopologyGraphConstants.FIBERTAKE_SITEID));
            nwInfo.setIsENBPresent(getStringValue(vertexMap, TopologyGraphConstants.IS_ENB_PRESENT));
            nwInfo.setMwType(getStringValue(vertexMap, TopologyGraphConstants.MICROWAVE_TYPE));
            nwInfo.setEnabledOriginNW(getStringValue(vertexMap, TopologyGraphConstants.ENABLED_ORIGIN_NW));
        } catch (NumberFormatException e) {
            String elementName = getStringValue(vertexMap, TopologyGraphConstants.ELEMENT_NAME);
            logger.error("Invalid numeric format in network details for element: {}", elementName);
            throw new DataException("Invalid numeric format in network details for element: " + elementName, e);
        } catch (Exception e) {
            String elementName = getStringValue(vertexMap, TopologyGraphConstants.ELEMENT_NAME);
            logger.error("Failed to set network details for element: {}", elementName);
            throw new DataProcessingException("Failed to set network details for element: " + elementName, e);
        }
    }

    /**
     * Sets element information in the network info wrapper from the vertex map.
     * Retrieves element info wrapper by element name and sets alias name, domain,
     * and vendor.
     *
     * @param vertexMap the vertex property map containing the element name, must
     *                  not be null
     * @param nwInfo    the network info wrapper in which to set the element
     *                  information, must not be null
     * @throws NullPointerException    if vertexMap or nwInfo is null
     * @throws DataNotFoundException   if the element cannot be found
     * @throws DataProcessingException if element info processing fails
     */
    private void setElementInfo(Map<String, Object> vertexMap, ConnectionInfoWrapper nwInfo) {
        Objects.requireNonNull(vertexMap, TopologyGraphConstants.VERTEX_MAP_NOT_BE_NULL);
        Objects.requireNonNull(nwInfo, TopologyGraphConstants.NETWORK_INFO_WRAPPER_NOT_BE_NULL);

        try {
            String elementName = getStringValue(vertexMap, TopologyGraphConstants.ELEMENT_NAME);
            if (elementName != null) {
                ElementInfoWrapper elementInfoWrapper = getElementInfoWrapperByEName(elementName);
                if (elementInfoWrapper != null) {
                    nwInfo.setSrcAliaseEname(elementInfoWrapper.getSrcEliaseName());
                    nwInfo.setDomain(elementInfoWrapper.getEquipDomain());
                    nwInfo.setVendor(elementInfoWrapper.getEquipVendor());
                }
            }
        } catch (ResourceNotFoundException e) {
            String elementName = getStringValue(vertexMap, TopologyGraphConstants.ELEMENT_NAME);
            logger.warn("Element not found when setting element info: {}", elementName);
            throw new DataNotFoundException("Element not found when setting element info: " + elementName, e);
        } catch (Exception e) {
            String elementName = getStringValue(vertexMap, TopologyGraphConstants.ELEMENT_NAME);
            logger.error("Failed to set element info for element: {}", elementName);
            throw new DataProcessingException("Failed to set element info for element: " + elementName, e);
        }
    }

    /**
     * Retrieves a Long value from the vertex property map for the specified key.
     *
     * @param vertexMap the vertex property map containing the data, must not be
     *                  null
     * @param key       the key to retrieve the value for, must not be null
     * @return the Long value associated with the key, or null if the key doesn't
     *         exist
     * @throws NullPointerException  if vertexMap or key is null
     * @throws NumberFormatException if the value cannot be converted to a Long
     */
    private Long getLongValue(Map<String, Object> vertexMap, String key) {
        Objects.requireNonNull(vertexMap, TopologyGraphConstants.VERTEX_MAP_NOT_BE_NULL);
        Objects.requireNonNull(key, TopologyGraphConstants.KEY_NOT_BE_NULL);

        Object value = vertexMap.get(key);
        return value != null ? Long.valueOf(value.toString()) : null;
    }

    /**
     * Retrieves a String value from the vertex property map for the specified key.
     *
     * @param vertexMap the vertex property map containing the data, must not be
     *                  null
     * @param key       the key to retrieve the value for, must not be null
     * @return the String value associated with the key, or null if the key doesn't
     *         exist
     * @throws NullPointerException if vertexMap or key is null
     */
    private String getStringValue(Map<String, Object> vertexMap, String key) {
        Objects.requireNonNull(vertexMap, TopologyGraphConstants.VERTEX_MAP_NOT_BE_NULL);
        Objects.requireNonNull(key, TopologyGraphConstants.KEY_NOT_BE_NULL);

        Object value = vertexMap.get(key);
        return value != null ? value.toString() : null;
    }

    /**
     * Retrieves a Double value from the vertex property map for the specified key.
     *
     * @param vertexMap the vertex property map containing the data, must not be
     *                  null
     * @param key       the key to retrieve the value for, must not be null
     * @return the Double value associated with the key, or null if the key doesn't
     *         exist
     * @throws NullPointerException  if vertexMap or key is null
     * @throws NumberFormatException if the value cannot be converted to a Double
     */
    private Double getDoubleValue(Map<String, Object> vertexMap, String key) {
        Objects.requireNonNull(vertexMap, TopologyGraphConstants.VERTEX_MAP_NOT_BE_NULL);
        Objects.requireNonNull(key, TopologyGraphConstants.KEY_NOT_BE_NULL);

        Object value = vertexMap.get(key);
        return value != null ? Double.valueOf(value.toString()) : null;
    }

    /**
     * Processes connected links for the given network information list.
     * Traverses the network topology to find connected elements based on the last
     * element
     * in the provided network info list.
     *
     * @param stackOfpathWrap    stack of path wrappers to store traversal results,
     *                           must not be null
     * @param processedElementId set of element IDs that have already been processed
     *                           to avoid cycles, must not be null
     * @param nwInfoList         list of network information wrappers to process,
     *                           must not be null or empty
     * @throws NullPointerException     if any parameter is null
     * @throws IllegalArgumentException if nwInfoList is empty
     * @throws DataException            if required element properties are missing
     *                                  or invalid
     * @throws DataProcessingException  if processing of connected link data fails
     */
    private void getConnectedLink(Stack<List<ConnectionInfoWrapper>> stackOfpathWrap,
                                  Set<Long> processedElementId,
                                  List<ConnectionInfoWrapper> nwInfoList) {

        Objects.requireNonNull(stackOfpathWrap, "Stack of path wrappers cannot be null");
        Objects.requireNonNull(processedElementId, "Processed element ID set cannot be null");
        Objects.requireNonNull(nwInfoList, "Network info list cannot be null");

        if (nwInfoList.isEmpty()) {
            logger.error("Empty network info list provided");
            throw new IllegalArgumentException("Network info list cannot be empty");
        }

        try {
            ConnectionInfoWrapper lastInfo = getLastConnectionInfo(nwInfoList);
            String elementType = lastInfo.geteType();
            Long elementId = lastInfo.getVertexId();

            validateElementProperties(elementType, elementId);

            logger.info("Processing connected link for element type: {}, ID: {}", elementType, elementId);

            if (!"type".equalsIgnoreCase(elementType)) {
                processLinkForNonTypeElement(stackOfpathWrap, processedElementId, elementType, elementId);
            }

        } catch (NullPointerException e) {
            logger.error("Missing required element properties for connected link");
            throw new DataException("Missing required element properties for connected link", e);
        } catch (DataException e) {
            throw e;
        } catch (Exception e) {
            logger.error("Failed to process connected link data");
            throw new DataProcessingException("Failed to process connected link data", e);
        }
    }

    /**
     * Inserts a list of objects into the specified database table.
     *
     * @param subList                              the list of objects to insert,
     *                                             must not be null
     * @param epnmIpnodeInterfaceLevelKpiDataTable the name of the table to insert
     *                                             into, must not be null or empty
     * @throws NullPointerException     if subList or
     *                                  epnmIpnodeInterfaceLevelKpiDataTable is null
     * @throws IllegalArgumentException if epnmIpnodeInterfaceLevelKpiDataTable is
     *                                  empty
     */
    @Override
    public void insertListInTable(List<Object> subList, String epnmIpnodeInterfaceLevelKpiDataTable) {
        Objects.requireNonNull(subList, "Sub list cannot be null");
        Objects.requireNonNull(epnmIpnodeInterfaceLevelKpiDataTable, "Table name cannot be null or empty");

        if (epnmIpnodeInterfaceLevelKpiDataTable.trim().isEmpty()) {
            throw new IllegalArgumentException("Table name cannot be empty");
        }

        iTopologyGraphDao.insertListInTable(subList, epnmIpnodeInterfaceLevelKpiDataTable);
    }

    private ConnectionInfoWrapper getLastConnectionInfo(List<ConnectionInfoWrapper> nwInfoList) {
        return nwInfoList.get(nwInfoList.size() - 1);
    }

    private void validateElementProperties(String elementType, Long elementId) {
        if (elementType == null) {
            throw new DataException("Element type is null for the last network info item");
        }
        if (elementId == null) {
            throw new DataException("Element ID is null for the last network info item");
        }
    }

    private void processLinkForNonTypeElement(Stack<List<ConnectionInfoWrapper>> stackOfpathWrap,
                                              Set<Long> processedElementId,
                                              String elementType,
                                              Long elementId) {

        List<String> allowedTypesInPath = topologyGraphUtils.getAllowedTypesInPath(elementType);
        List<String> terminationTypeList = topologyGraphUtils.getTerminationType(elementType);

        if (allowedTypesInPath == null) {
            throw new DataException("Allowed types list is null for element type: " + elementType);
        }

        if (terminationTypeList == null) {
            throw new DataException("Termination type list is null for element type: " + elementType);
        }

        if (!processedElementId.contains(elementId)) {
            processedElementId.add(elementId);
            Set<Path> pathTraversedSet = topologyGraphUtils.getTotalPathTraversed(elementId, allowedTypesInPath,
                    terminationTypeList);

            if (pathTraversedSet != null && pathTraversedSet.isEmpty()) {
                for (Path traversedPaths : pathTraversedSet) {
                    getPathWrapperByVertexPathforMap(traversedPaths, stackOfpathWrap, processedElementId);
                }
            }
        } else {
            logger.info("Element ID {} already processed, skipping", elementId);
        }
    }

    /**
     * Processes connectivity information for a set of edges and updates the
     * topology connectivity map.
     * This method iterates through the provided edges and updates connectivity
     * information for source
     * and destination vertices based on the edge properties.
     *
     * <p>
     * Example usage:
     * </p>
     *
     * <pre>
     * Map<String, TopologyConnectivityWrapper> connectivityMap = new HashMap<>();
     * Set<Edge> edges = getEdges();
     * Set<Long> vertexIds = new HashSet<>();
     * getConnectivityInInfoWrapper(connectivityMap, edges, vertexIds, "TOPOLOGY");
     * </pre>
     *
     * @param topologyConnectivityMap map to store connectivity information, must
     *                                not be null
     * @param edgesList               set of edges to process, must not be null
     * @param vertexIds               set of vertex IDs to track processed vertices,
     *                                must not be null
     * @param connectivityType        type of connectivity to filter edges by, can
     *                                be null for no filtering
     * @throws NullPointerException if any required parameter is null
     */
    private void getConnectivityInInfoWrapper(Map<String, TopologyConnectivityWrapper> topologyConnectivityMap,
                                              Set<Edge> edgesList, Set<Long> vertexIds, String connectivityType) {
        Objects.requireNonNull(topologyConnectivityMap, "Topology connectivity map cannot be null");
        Objects.requireNonNull(edgesList, "Edges list cannot be null");
        Objects.requireNonNull(vertexIds, "Vertex IDs set cannot be null");

        if (edgesList.isEmpty()) {
            logger.info("Empty edges list provided, no connectivity to process");
            return;
        }

        if (!topologyConnectivityMap.isEmpty() && !edgesList.isEmpty()) {
            logger.info("Processing {} edges for connectivity information", edgesList.size());
            List<String> ipTopoRequiredConnType = new ArrayList<>();

            for (Edge edge : edgesList) {
                processEdge(topologyConnectivityMap, vertexIds, connectivityType, ipTopoRequiredConnType, edge);
            }
        }
    }

    /**
     * Processes a single edge to update connectivity information in the topology
     * map.
     * This method checks if the edge should be processed based on connectivity type
     * and updates
     * the corresponding source and destination wrappers in the topology map.
     *
     * @param topologyConnectivityMap map containing topology connectivity
     *                                information
     * @param vertexIds               set of vertex IDs to track processed vertices
     * @param connectivityType        type of connectivity to filter by
     * @param ipTopoRequiredConnType  list of required connection types for IP
     *                                topology
     * @param edge                    the edge to process
     * @throws DataException           if required edge properties are missing
     * @throws DataProcessingException if edge processing fails
     */
    private void processEdge(Map<String, TopologyConnectivityWrapper> topologyConnectivityMap,
                             Set<Long> vertexIds, String connectivityType,
                             List<String> ipTopoRequiredConnType, Edge edge) {
        try {
            if (shouldProcessEdge(connectivityType, ipTopoRequiredConnType, edge)) {
                TopologyConnectivityWrapper srcEdgeInfoWrapperOfMap = topologyConnectivityMap
                        .get(edge.value(TopologyGraphConstants.SOURCE_ELEMENT_NAME));
                TopologyConnectivityWrapper destEdgeInfoWrapperOfMap = topologyConnectivityMap
                        .get(edge.value(TopologyGraphConstants.DESTINATION_ELEMENT_NAME));

                if (srcEdgeInfoWrapperOfMap != null || destEdgeInfoWrapperOfMap != null) {
                    setConnectivityInfoWrapper(topologyConnectivityMap, vertexIds,
                            srcEdgeInfoWrapperOfMap, destEdgeInfoWrapperOfMap, edge);
                }
            }
        } catch (NullPointerException ex) {
            handleEdgeException(edge, ex, true);
        } catch (Exception ex) {
            handleEdgeException(edge, ex, false);
        }
    }

    /**
     * Determines whether an edge should be processed based on connectivity type and
     * connection requirements.
     *
     * @param connectivityType       the type of connectivity to filter by
     * @param ipTopoRequiredConnType list of required connection types for IP
     *                               topology
     * @param edge                   the edge to evaluate
     * @return true if the edge should be processed, false otherwise
     */
    private boolean shouldProcessEdge(String connectivityType,
                                      List<String> ipTopoRequiredConnType, Edge edge) {
        if (connectivityType == null) {
            return true;
        }
        return TopologyGraphConstants.TOPOLOGY.equalsIgnoreCase(connectivityType)
                && ipTopoRequiredConnType != null
                && edge.value(TopologyGraphConstants.CONNECTION_TYPE) != null
                && ipTopoRequiredConnType.contains(
                edge.value(TopologyGraphConstants.CONNECTION_TYPE).toString());
    }

    /**
     * Handles exceptions that occur during edge processing by logging and throwing
     * appropriate exceptions.
     *
     * @param edge          the edge being processed when the exception occurred
     * @param ex            the exception that was caught
     * @param isNullPointer true if the exception is a NullPointerException, false
     *                      otherwise
     * @throws DataException           if required edge properties are missing
     * @throws DataProcessingException if edge processing fails for other reasons
     */
    private void handleEdgeException(Edge edge, Exception ex, boolean isNullPointer) {
        String edgeId = (edge != null) ? edge.id().toString() : TopologyGraphConstants.UNKNOWN;
        if (isNullPointer) {
            logger.error("Missing required edge properties for edge ID: {}", edgeId, ex);
            throw new DataException("Missing required edge properties for edge ID: " + edgeId, ex);
        } else {
            logger.error("Error while processing edge ID: {} for connectivity information", edgeId, ex);
            throw new DataProcessingException("Failed to process connectivity data for edge ID: " + edgeId, ex);
        }
    }

    /**
     * Sets connectivity information in the topology connectivity map based on edge
     * data.
     * Updates the connectivity information for either source or destination element
     * based on the edge.
     *
     * @param topologyConnectivityMap  map containing topology connectivity
     *                                 information, must not be null
     * @param vertexIds                set of vertex IDs to track processed vertices
     * @param srcEdgeInfoWrapperOfMap  source element connectivity wrapper
     * @param destEdgeInfoWrapperOfMap destination element connectivity wrapper
     * @param edge                     the edge containing connectivity information,
     *                                 must not be null
     * @throws NullPointerException  if topologyConnectivityMap or edge is null
     * @throws ConnectivityException if setting connectivity information fails
     */
    private void setConnectivityInfoWrapper(Map<String, TopologyConnectivityWrapper> topologyConnectivityMap,
                                            Set<Long> vertexIds,
                                            TopologyConnectivityWrapper srcEdgeInfoWrapperOfMap,
                                            TopologyConnectivityWrapper destEdgeInfoWrapperOfMap,
                                            Edge edge) {
        Objects.requireNonNull(topologyConnectivityMap, "Topology connectivity map cannot be null");
        Objects.requireNonNull(edge, TopologyGraphConstants.EDGE_NOT_BE_NULL);

        try {
            TopologyConnectivityWrapper infoWrapperOfMap = getInfoWrapper(srcEdgeInfoWrapperOfMap,
                    destEdgeInfoWrapperOfMap);
            if (infoWrapperOfMap != null) {
                String connector = srcEdgeInfoWrapperOfMap != null
                        ? TopologyGraphConstants.SOURCE
                        : TopologyGraphConstants.DESTINATION;

                List<Map<String, Object>> connectivity = infoWrapperOfMap.getConnectivity();
                if (connectivity == null) {
                    connectivity = new ArrayList<>();
                }

                Map<String, Object> mapOfEdgeData = getMapOfEdgeData(edge, connector);
                connectivity.add(mapOfEdgeData);
                infoWrapperOfMap.setConnectivity(connectivity);

                String key = edge.value(connector + TopologyGraphConstants.ELEMENT_NAME);
                topologyConnectivityMap.put(key, infoWrapperOfMap);

                logIfWrapperIsNull(srcEdgeInfoWrapperOfMap, destEdgeInfoWrapperOfMap, vertexIds);
            }

        } catch (NullPointerException ex) {
            String edgeId = edge.id().toString();
            logger.error("Failed to set connectivity info for edge ID: {}", edgeId);
            throw new ConnectivityException("Failed to set connectivity info for edge ID: " + edgeId, ex);
        } catch (IllegalArgumentException ex) {
            String edgeId = edge.id().toString();
            throw new ConnectivityException("Invalid argument while processing edge ID: " + edgeId, ex);
        }
    }

    private TopologyConnectivityWrapper getInfoWrapper(TopologyConnectivityWrapper src,
                                                       TopologyConnectivityWrapper dest) {
        return src != null ? src : dest;
    }

    private void logIfWrapperIsNull(TopologyConnectivityWrapper src,
                                    TopologyConnectivityWrapper dest,
                                    Set<Long> vertexIds) {
        if (src == null && vertexIds != null) {
            logger.info("srcEdgeInfoWrapperOfMap is null, vertexIds: {}", vertexIds);
        }
        if (dest == null && vertexIds != null) {
            logger.info("destEdgeInfoWrapperOfMap is null, vertexIds: {}", vertexIds);
        }
    }

    /**
     * Extracts edge data and creates a map containing connectivity information
     * based on the specified connector type.
     * This method processes edge properties and retrieves associated vertex data to
     * build a complete connectivity map.
     *
     * <p>
     * Example usage:
     * </p>
     *
     * <pre>
     * Edge edge = graph.edges().next();
     * Map<String, Object> edgeData = getMapOfEdgeData(edge, TopologyGraphConstants.SOURCE);
     * // Returns map with source/destination ports, element name, edge ID etc.
     * </pre>
     *
     * @param edge      the edge to extract data from, must not be null
     * @param connector the connector type ("SOURCE" or "DESTINATION"), must not be
     *                  null
     * @return a map containing the extracted edge and vertex data
     * @throws IllegalArgumentException if connector is not "SOURCE" or
     *                                  "DESTINATION"
     * @throws DataNotFoundException    if associated vertex cannot be found
     * @throws DataException            if vertex property map is null or empty
     * @throws DataProcessingException  if there is a general error processing the
     *                                  data
     */
    private Map<String, Object> getMapOfEdgeData(Edge edge, String connector) {
        Objects.requireNonNull(edge, TopologyGraphConstants.EDGE_NOT_BE_NULL);
        Objects.requireNonNull(connector, "Connector cannot be null");

        logger.info("Starting to process edge ID: {} with connector: {}", edge.id(), connector);

        if (!TopologyGraphConstants.SOURCE.equalsIgnoreCase(connector) &&
                !TopologyGraphConstants.DESTINATION.equalsIgnoreCase(connector)) {
            logger.error("Invalid connector value: {}", connector);
            throw new IllegalArgumentException("Connector must be either SOURCE or DESTINATION");
        }

        try {
            Map<String, Object> edgeMap = new HashMap<>();
            boolean isSource = TopologyGraphConstants.SOURCE.equalsIgnoreCase(connector);

            String sourcePortKey = isSource ? TopologyGraphConstants.SOURCE_PORT
                    : TopologyGraphConstants.DESTINATION_PORT;
            String sourcePort = getEdgePropertyValue(edge, sourcePortKey);
            logger.info("Resolved sourcePort (key: {}) = {}", sourcePortKey, sourcePort);
            edgeMap.put(TopologyGraphConstants.SOURCE_PORT, sourcePort);
            edgeMap.put(TopologyGraphConstants.SOURCE_ALIAS_PORT, sourcePort);

            String destinationPortKey = isSource ? TopologyGraphConstants.DESTINATION_PORT
                    : TopologyGraphConstants.SOURCE_PORT;
            String destinationPort = getEdgePropertyValue(edge, destinationPortKey);
            logger.info("Resolved destinationPort (key: {}) = {}", destinationPortKey, destinationPort);
            edgeMap.put(TopologyGraphConstants.DESTINATION_PORT, destinationPort);
            edgeMap.put(TopologyGraphConstants.DESTINATION_ALIAS_PORT, destinationPort);

            String elementNameKey = isSource ? TopologyGraphConstants.DESTINATION_ELEMENT_NAME
                    : TopologyGraphConstants.SOURCE_ELEMENT_NAME;
            String elementName = edge.value(elementNameKey);
            logger.info("Resolved elementName (key: {}) = {}", elementNameKey, elementName);

            if (elementName == null || elementName.trim().isEmpty()) {
                logger.error("Element name is null or empty for edge ID: {}", edge.id());
                throw new IllegalArgumentException(TopologyGraphConstants.ELEMENT_NAME_CANNOT_BE_NULL_OR_EMPTY);
            }

            edgeMap.put(TopologyGraphConstants.ELEMENT_NAME, elementName);
            edgeMap.put(TopologyGraphConstants.EDGE_ID, edge.id().toString());

            String ringType = getEdgePropertyValue(edge, TopologyGraphConstants.RING_TYPE);
            logger.info("Resolved ringType = {}", ringType);
            edgeMap.put(TopologyGraphConstants.RING_TYPE, ringType);

            String ringId = getEdgePropertyValue(edge, TopologyGraphConstants.RING_ID);
            logger.info("Resolved ringId = {}", ringId);
            edgeMap.put(TopologyGraphConstants.RING_ID, ringId);

            logger.info("Fetching vertex for elementName: {}", elementName);
            Vertex vertex = topologyGraphUtils.getVertexByPropertyValue(TopologyGraphConstants.ELEMENT_NAME,
                    elementName);
            if (vertex == null) {
                logger.error("Vertex not found for elementName: {}", elementName);
                throw new DataNotFoundException("Vertex not found for element name: " + elementName);
            }

            Map<String, Object> propertyMapOfVertex = topologyGraphUtils.getPropertyMapByVertex(vertex);
            if (propertyMapOfVertex == null || propertyMapOfVertex.isEmpty()) {
                logger.error("Property map is null or empty for vertex with elementName: {}", elementName);
                throw new DataException("Property map is null or empty for vertex with element name: " + elementName);
            }

            Object elementType = propertyMapOfVertex.get(TopologyGraphConstants.ELEMENT_TYPE);
            logger.info("Resolved elementType = {}", elementType);
            edgeMap.put(TopologyGraphConstants.ELEMENT_TYPE, elementType);

            logger.info("Completed processing edge ID: {}", edge.id());
            return edgeMap;
        } catch (ResourceNotFoundException ex) {
            logger.error("ResourceNotFoundException for edge ID: {}", edge.id());
            throw new DataNotFoundException("Element not found for edge processing: " + edge.id().toString(), ex);
        } catch (Exception ex) {
            logger.error("Unexpected exception for edge ID: {}", edge.id());
            throw new DataProcessingException("Failed to process edge data for edge ID: " + edge.id().toString(), ex);
        }
    }

    /**
     * Helper method to safely retrieve property values from an edge.
     * Checks if the property exists before attempting to get its value.
     *
     * @param edge        the edge to get the property from, must not be null
     * @param propertyKey the key of the property to retrieve, must not be null
     * @return the property value as a String, or null if the property doesn't exist
     */
    private String getEdgePropertyValue(Edge edge, String propertyKey) {
        boolean present = edge.property(propertyKey).isPresent();
        String value = present ? edge.value(propertyKey) : null;
        logger.info("Fetched property '{}' from edge: present={}, value={}", propertyKey, present, value);
        return value;
    }

    /**
     * Creates a topology connectivity wrapper from a JanusGraph vertex.
     * Extracts vertex properties and populates the wrapper with element
     * information.
     *
     * @param element the JanusGraph vertex to extract data from, must not be null
     * @return a populated topology connectivity wrapper
     * @throws NullPointerException if element is null
     */
    private TopologyConnectivityWrapper getTopologyConnectivityWrapper(JanusGraphVertex element) {
        Objects.requireNonNull(element, "Element cannot be null");

        TopologyConnectivityWrapper topologyConnectivity = new TopologyConnectivityWrapper();
        try {
            topologyConnectivity.setGeographyL3(element.property(TopologyGraphConstants.GEOGRAPHY_LEVEL3).isPresent()
                    ? element.value(TopologyGraphConstants.GEOGRAPHY_LEVEL3).toString()
                    : null);
            topologyConnectivity.seteName(element.property(TopologyGraphConstants.ELEMENT_NAME).isPresent()
                    ? element.value(TopologyGraphConstants.ELEMENT_NAME).toString()
                    : null);
            topologyConnectivity.setLatitude((element.property(TopologyGraphConstants.LATITUDE).isPresent()
                    && element.value(TopologyGraphConstants.LATITUDE) != null)
                    ? topologyGraphUtils.decodeNumeric(element.value(TopologyGraphConstants.LATITUDE))
                    : null);
            topologyConnectivity.setLongitude((element.property(TopologyGraphConstants.LONGITUDE).isPresent()
                    && element.value(TopologyGraphConstants.LONGITUDE) != null)
                    ? topologyGraphUtils.decodeNumeric(element.value(TopologyGraphConstants.LONGITUDE))
                    : null);
            topologyConnectivity.setVertexId(element.id() != null ? Long.valueOf(element.id().toString()) : null);
            topologyConnectivity.seteType(element.property(TopologyGraphConstants.ELEMENT_TYPE).isPresent()
                    ? element.value(TopologyGraphConstants.ELEMENT_TYPE).toString()
                    : null);
            topologyConnectivity.setEnabledOriginNW(element.properties(TopologyGraphConstants.ENABLED_ORIGIN_NW) != null
                    ? topologyGraphUtils.getAllValueFromMultiProps(element, TopologyGraphConstants.ENABLED_ORIGIN_NW)
                    : null);
            topologyConnectivity.setSiteId(element.property(TopologyGraphConstants.SITEID).isPresent()
                    ? element.value(TopologyGraphConstants.SITEID).toString()
                    : null);
            topologyConnectivity.setIpv4(element.property(TopologyGraphConstants.IPV4).isPresent()
                    ? element.value(TopologyGraphConstants.IPV4).toString()
                    : null);
            topologyConnectivity.setIpv6(element.property(TopologyGraphConstants.IPV6).isPresent()
                    ? element.value(TopologyGraphConstants.IPV6).toString()
                    : null);
            topologyConnectivity.setIsENBPresent(element.property(TopologyGraphConstants.IS_ENB_PRESENT).isPresent()
                    ? element.value(TopologyGraphConstants.IS_ENB_PRESENT).toString()
                    : null);
        } catch (Exception ex) {
            logger.error("Error processing connectivity data for element ID: {}",
                    element.id(), ex);
        }
        return topologyConnectivity;
    }

    /**
     * Retrieves the element name from a vertex.
     *
     * @param vertex the vertex to extract the element name from, must not be null
     * @return the element name, or an empty string if not found
     * @throws NullPointerException    if vertex is null
     * @throws DataNotFoundException   if the vertex is invalid or element name
     *                                 cannot be found
     * @throws DataProcessingException if processing fails
     */
    public String getEnameByVertex(Vertex vertex) {
        Objects.requireNonNull(vertex, TopologyGraphConstants.VERTEX_NOT_BE_NULL);

        String ename = "";
        try {
            Map<String, Object> propertyMapByVertex = topologyGraphUtils.getPropertyMapByVertex(vertex);
            if (propertyMapByVertex != null && propertyMapByVertex.get(TopologyGraphConstants.ELEMENT_NAME) != null) {
                return propertyMapByVertex.get(TopologyGraphConstants.ELEMENT_NAME).toString();
            }
        } catch (NullPointerException e) {
            throw new DataNotFoundException("Vertex is null or invalid", e);
        } catch (DataNotFoundException e) {
            throw e;
        } catch (Exception e) {
            throw new DataProcessingException("Failed to retrieve element name for vertex: " + vertex.id().toString(),
                    e);
        }
        return ename;
    }


    /**
     * Retrieves vertices of PAN level within the specified range.
     *
     * <p>
     * This method fetches vertices that fall within the given ID range and
     * constructs a map containing their properties including city, latitude,
     * longitude, and element type.
     * </p>
     *
     * @param startRange the lower bound of the vertex ID range, must not be null or
     *                   negative
     * @param endRange   the upper bound of the vertex ID range, must not be null or
     *                   negative
     * @return a map with element names as keys and their property maps as values
     * @throws NullPointerException     if startRange or endRange is null
     * @throws IllegalArgumentException if startRange or endRange is negative, or if
     *                                  startRange > endRange
     * @throws DataNotFoundException    if no vertices are found in the specified
     *                                  range
     * @throws DataProcessingException  if an error occurs during data retrieval or
     *                                  processing
     */
    @Override
    public Map<String, Map<String, Object>> getVertexOfPanLevelByRange(Long startRange, Long endRange) {
        Objects.requireNonNull(startRange, "Start range cannot be null");
        Objects.requireNonNull(endRange, "End range cannot be null");

        validateRange(startRange, endRange);

        Map<String, Map<String, Object>> elementMap = new HashMap<>();
        int cityNotAvailable = 0;

        try {
            Set<Vertex> setOfVertices = topologyGraphUtils.getVertexOfPanLevelByRange(startRange, endRange);
            if (setOfVertices != null && !setOfVertices.isEmpty()) {
                for (Vertex vertex : setOfVertices) {
                    Objects.requireNonNull(vertex, TopologyGraphConstants.VERTEX_NOT_BE_NULL);

                    Map<String, Object> propertyMapByVertex = topologyGraphUtils.getPropertyMapByVertex(vertex);
                    String elementName = propertyMapByVertex.get(TopologyGraphConstants.ELEMENT_NAME).toString();

                    Map<String, Object> dataMap = extractDataMap(propertyMapByVertex);

                    elementMap.put(elementName, dataMap);
                }
            }

            logger.info("Retrieved {} vertices for range {} to {}, cities not available: {}",
                    elementMap.size(), startRange, endRange, cityNotAvailable);

        } catch (DataNotFoundException e) {
            throw e;
        } catch (Exception e) {
            throw new DataProcessingException(
                    "Error retrieving vertex data for range: " + startRange + " to " + endRange, e);
        }
        return elementMap;
    }

    private void validateRange(Long startRange, Long endRange) {
        if (startRange < 0 || endRange < 0) {
            throw new IllegalArgumentException("Range values cannot be negative");
        }
        if (startRange > endRange) {
            throw new IllegalArgumentException("Start range cannot be greater than end range");
        }
    }

    private Map<String, Object> extractDataMap(Map<String, Object> propertyMapByVertex) {
        Map<String, Object> dataMap = new HashMap<>();

        String city = safeToString(propertyMapByVertex.get(TopologyGraphConstants.GEOGRAPHY_LEVEL3));
        Double lat = decodeCoordinate(propertyMapByVertex.get(TopologyGraphConstants.LATITUDE));
        Double lng = decodeCoordinate(propertyMapByVertex.get(TopologyGraphConstants.LONGITUDE));
        String eType = safeToString(propertyMapByVertex.get(TopologyGraphConstants.ELEMENT_TYPE));

        dataMap.put(TopologyGraphConstants.CITY, city);
        dataMap.put(TopologyGraphConstants.LATITUDE, lat);
        dataMap.put(TopologyGraphConstants.LONGITUDE, lng);
        dataMap.put(TopologyGraphConstants.ELEMENT_TYPE, eType);

        return dataMap;
    }

    private String safeToString(Object obj) {
        return obj != null ? obj.toString() : null;
    }

    private Double decodeCoordinate(Object obj) {
        if (obj != null) {
            return topologyGraphUtils.decodeNumeric(obj.toString());
        }
        return null;
    }

    /**
     * Retrieves edges of PAN level within the specified range.
     *
     * <p>
     * This method fetches edges with the domain name IP label that fall within
     * the given ID range and constructs a list containing their properties.
     * </p>
     *
     * @param startRange the lower bound of the edge ID range, must not be null or
     *                   negative
     * @param endRange   the upper bound of the edge ID range, must not be null or
     *                   negative
     * @return a list of maps containing edge properties
     * @throws NullPointerException     if startRange or endRange is null
     * @throws IllegalArgumentException if startRange or endRange is negative, or if
     *                                  startRange > endRange
     * @throws DataNotFoundException    if no edges are found in the specified range
     * @throws DataProcessingException  if an error occurs during data retrieval or
     *                                  processing
     */
    @Override
    public List<Map<String, Object>> getEdgesOfPANLevelByRange(Long startRange, Long endRange) {
        Objects.requireNonNull(startRange, "Start range cannot be null");
        Objects.requireNonNull(endRange, "End range cannot be null");

        if (startRange < 0 || endRange < 0) {
            throw new IllegalArgumentException("Range values cannot be negative");
        }

        if (startRange > endRange) {
            throw new IllegalArgumentException("Start range cannot be greater than end range");
        }

        List<Map<String, Object>> listOfPropertyMap = new ArrayList<>();
        try {
            List<Edge> edgesList = topologyGraphUtils
                    .getAllEdgesByEdgeLabelByRange(TopologyGraphConstants.DOMAIN_NAME_IP, startRange, endRange);
            if (edgesList != null && !edgesList.isEmpty()) {
                for (Edge edge : edgesList) {
                    Objects.requireNonNull(edge, TopologyGraphConstants.EDGE_NOT_BE_NULL);
                    Map<String, Object> propertyMapByEdge = topologyGraphUtils.getPropertyMapByEdge(edge);
                    listOfPropertyMap.add(propertyMapByEdge);
                }
            }
            logger.info("Retrieved {} edges for PAN level in range {} to {}",
                    listOfPropertyMap.size(), startRange, endRange);
        } catch (DataNotFoundException e) {
            throw e;
        } catch (Exception e) {
            throw new DataProcessingException(
                    "Failed to retrieve edges for PAN level in range: " + startRange + " to " + endRange, e);
        }
        return listOfPropertyMap;
    }

    /**
     * Retrieves connectivity information for a specific network element.
     *
     * <p>
     * This method fetches all edges connected to the specified element and
     * constructs connectivity wrapper objects containing details about the
     * connections.
     * </p>
     *
     * @param elementName the name of the element to retrieve connectivity for, must
     *                    not be null or empty
     * @param category    the category of connections to filter by, can be null
     * @return a list of element connectivity wrapper objects
     * @throws NullPointerException     if elementName is null
     * @throws IllegalArgumentException if elementName is empty
     * @throws ConnectivityException    if the element is not found or if
     *                                  connectivity information cannot be retrieved
     * @throws DataProcessingException  if an error occurs during data processing
     */
    @Override
    public List<ElementConnectivityWrapper> getElementConnectivityByElementName(String elementName, String category) {
        validateElementName(elementName);

        if (category == null || category.trim().isEmpty()) {
            logger.warn("Category is null or empty, using default behavior");
        }

        List<ElementConnectivityWrapper> elementWrapper = new ArrayList<>();
        try {
            Vertex vertex = getVertexByElementName(elementName);
            Set<Edge> edges = getConnectedEdges(vertex, category);

            logger.info("Retrieved {} edges for element: {}, category: {}",
                    edges.size(), elementName, category);

            if (!edges.isEmpty()) {
                List<Map<String, Object>> edgeData = iTopologyHbaseDao.getEdgeData(edges);
                addConnectionWrappers(edgeData, elementWrapper, elementName, category);
            }
        } catch (ResourceNotFoundException e) {
            throw new ConnectivityException("Element not found: " + elementName, e);
        } catch (ConnectivityException e) {
            throw new ConnectivityException("Failed to get connectivity for element: " + elementName, e);
        } catch (Exception e) {
            throw new DataProcessingException("Error processing connectivity data for element: " + elementName, e);
        }

        logger.info("element list size is: {}", elementWrapper.size());
        return elementWrapper;
    }

    private void validateElementName(String elementName) {
        Objects.requireNonNull(elementName, TopologyGraphConstants.ELEMENT_NAME_NOT_BE_NULL);

        if (elementName.trim().isEmpty()) {
            throw new IllegalArgumentException(TopologyGraphConstants.ELEMENT_NAME_CANNOT_BE_EMPTY);
        }
    }

    private void addConnectionWrappers(List<Map<String, Object>> edgeData,
                                       List<ElementConnectivityWrapper> elementWrapper,
                                       String elementName, String category) {
        if (edgeData == null || edgeData.isEmpty()) {
            return;
        }

        for (Map<String, Object> edgeProperty : edgeData) {
            if (edgeProperty != null) {
                ElementConnectivityWrapper connectionWrapper = createConnectionWrapper(edgeProperty, elementName,
                        category);
                elementWrapper.add(connectionWrapper);
            }
        }
    }

    /**
     * Retrieves a vertex by its element name.
     *
     * @param elementName the name of the element to find, must not be null or empty
     * @return the vertex corresponding to the element name
     * @throws NullPointerException      if elementName is null
     * @throws IllegalArgumentException  if elementName is empty
     * @throws ResourceNotFoundException if no vertex is found with the given
     *                                   element name
     */
    private Vertex getVertexByElementName(String elementName) {
        Objects.requireNonNull(elementName, TopologyGraphConstants.ELEMENT_NAME_NOT_BE_NULL);

        if (elementName.trim().isEmpty()) {
            throw new IllegalArgumentException(TopologyGraphConstants.ELEMENT_NAME_CANNOT_BE_EMPTY);
        }

        try {
            Vertex vertex = topologyGraphUtils.getVertexByPropertyValue(TopologyGraphConstants.ELEMENT_NAME,
                    elementName);
            if (vertex == null) {
                throw new ResourceNotFoundException(TopologyGraphConstants.ELEMENT_NOT_FOUND_WITH_NAME + elementName);
            }
            return vertex;
        } catch (DataNotFoundException e) {
            throw new ResourceNotFoundException(TopologyGraphConstants.ELEMENT_NOT_FOUND_WITH_NAME + elementName, e);
        }
    }

    /**
     * Retrieves edges connected to the specified vertex, optionally filtered by
     * category.
     *
     * @param vertex   the vertex to find connected edges for, must not be null
     * @param category the category to filter edges by, can be null
     * @return a set of edges connected to the vertex
     * @throws NullPointerException  if vertex is null
     * @throws ConnectivityException if edges cannot be retrieved
     */
    private Set<Edge> getConnectedEdges(Vertex vertex, String category) {
        Objects.requireNonNull(vertex, TopologyGraphConstants.VERTEX_NOT_BE_NULL);

        try {
            Long vertexId = Long.parseLong(vertex.id().toString());
            return topologyGraphUtils.getConnectedEdgesByVertexId(vertexId, category);
        } catch (ConnectivityException e) {
            throw new ConnectivityException("Failed to retrieve connected edges for vertex: " + vertex.id(), e);
        } catch (NumberFormatException e) {
            throw new ConnectivityException("Invalid vertex ID format: " + vertex.id(), e);
        }
    }

    /**
     * Creates a connectivity wrapper from edge properties for a specific element.
     *
     * <p>
     * This method constructs an ElementConnectivityWrapper object containing
     * connection details based on the provided edge properties and element name.
     * </p>
     *
     * @param edgeProperty the map of edge properties, must not be null
     * @param elementName  the name of the element, must not be null or empty
     * @param category     the category of the connection, can be null
     * @return an ElementConnectivityWrapper containing connection details
     * @throws NullPointerException     if edgeProperty or elementName is null
     * @throws IllegalArgumentException if elementName is empty
     */
    private ElementConnectivityWrapper createConnectionWrapper(Map<String, Object> edgeProperty, String elementName,
                                                               String category) {
        Objects.requireNonNull(edgeProperty, TopologyGraphConstants.EDGE_PROPERTY_NOT_BE_NULL);
        Objects.requireNonNull(elementName, TopologyGraphConstants.ELEMENT_NAME_NOT_BE_NULL);

        if (elementName.trim().isEmpty()) {
            throw new IllegalArgumentException(TopologyGraphConstants.ELEMENT_NAME_CANNOT_BE_EMPTY);
        }

        ElementConnectivityWrapper connectionWrapper = new ElementConnectivityWrapper();

        String sourceIp = null;
        String destIp = null;

        if (TopologyGraphConstants.CATEGORY_LOGICAL.equalsIgnoreCase(category)) {
            String ipsList = (String) edgeProperty.get(TopologyGraphConstants.IPS);
            if (ipsList != null) {
                String[] ipsArray = ipsList.split(PMConstants.UNDERSCORE);
                if (ipsArray.length > 1) {
                    sourceIp = ipsArray[0];
                    destIp = ipsArray[1];
                }
            }
        }

        String paramSourceIp;
        String paramDestIp;

        if (elementName.equalsIgnoreCase((String) edgeProperty.get(TopologyGraphConstants.SOURCE_ELEMENT_NAME))) {
            paramSourceIp = sourceIp;
            paramDestIp = destIp;
        } else {
            paramSourceIp = destIp;
            paramDestIp = sourceIp;
        }

        setConnectionWrapperProperties(connectionWrapper, edgeProperty, paramSourceIp, paramDestIp);

        return connectionWrapper;
    }

    /**
     * Sets the properties of a connection wrapper based on edge properties.
     *
     * <p>
     * This method populates the connection wrapper with source and destination
     * information from the edge properties.
     * </p>
     *
     * @param connectionWrapper the wrapper to populate with properties, must not be
     *                          null
     * @param edgeProperty      the map of edge properties to extract data from,
     *                          must not be null
     * @param sourceIp          the source IP address, can be null
     * @param destIp            the destination IP address, can be null
     * @throws NullPointerException if connectionWrapper or edgeProperty is null
     */
    private void setConnectionWrapperProperties(ElementConnectivityWrapper connectionWrapper,
                                                Map<String, Object> edgeProperty, String sourceIp, String destIp) {
        Objects.requireNonNull(connectionWrapper, TopologyGraphConstants.CONN_WRAP_NOT_BE_NULL);
        Objects.requireNonNull(edgeProperty, TopologyGraphConstants.EDGE_PROPERTY_NOT_BE_NULL);

        String sourceElementName = (String) edgeProperty.get(TopologyGraphConstants.SOURCE_ELEMENT_NAME);
        String destElementName = (String) edgeProperty.get(TopologyGraphConstants.DESTINATION_ELEMENT_NAME);

        if (sourceElementName == null || sourceElementName.trim().isEmpty()) {
            logger.warn("Source element name is null or empty in edge properties");
        }

        if (destElementName == null || destElementName.trim().isEmpty()) {
            logger.warn("Destination element name is null or empty in edge properties");
        }

        connectionWrapper.setSourceElementName(sourceElementName);
        connectionWrapper.setSourcePort((String) edgeProperty.get(TopologyGraphConstants.SOURCE_PORT));
        connectionWrapper.setSourceLogicalPort((String) edgeProperty.get(TopologyGraphConstants.LOGICAL_SOURCE_PORT));
        connectionWrapper.setSourceBundle((String) edgeProperty.get(TopologyGraphConstants.SOURCE_BUNDLE));
        connectionWrapper.setSourceVLan((String) edgeProperty.get(TopologyGraphConstants.SOURCE_VLAN));
        connectionWrapper.setDestElementName(destElementName);
        connectionWrapper.setDestPort((String) edgeProperty.get(TopologyGraphConstants.DESTINATION_PORT));
        connectionWrapper
                .setDestLogicalPort((String) edgeProperty.get(TopologyGraphConstants.LOGICAL_DESTINATION_PORT));
        connectionWrapper.setDestBundel((String) edgeProperty.get(TopologyGraphConstants.DESTINATION_BUNDLE));
        connectionWrapper.setDestVLan((String) edgeProperty.get(TopologyGraphConstants.DESTINATION_VLAN));
        connectionWrapper.setSourceIp(sourceIp);
        connectionWrapper.setDestIp(destIp);
        connectionWrapper.setLinkStatus((String) edgeProperty.get(TopologyGraphConstants.LINK_STATUS));
        connectionWrapper.setDisplayName((String) edgeProperty.get(TopologyGraphConstants.DISPLAY_NAME));
        connectionWrapper.setSrcAType((String) edgeProperty.get(TopologyGraphConstants.SOURCE_ALIASE_ETYPE));
        connectionWrapper.setDestAType((String) edgeProperty.get(TopologyGraphConstants.DESTINATION_ALIASE_ETYPE));

        try {
            setElementPropertiesFromVertex(connectionWrapper, sourceElementName, destElementName);
        } catch (ResourceNotFoundException e) {
            logger.info("Could not find one of the elements: source={}, destination={}", sourceElementName,
                    destElementName);
            throw new DataProcessingException("Failed to set element properties from vertex", e);
        }
    }

    /**
     * Sets element properties from vertex data for both source and destination
     * elements.
     *
     * <p>
     * This method retrieves element information for source and destination elements
     * and populates the connection wrapper with their properties.
     * </p>
     *
     * @param connectionWrapper the wrapper to populate with vertex properties, must
     *                          not be null
     * @param sourceElementName the name of the source element, can be null
     * @param destElementName   the name of the destination element, can be null
     * @throws NullPointerException      if connectionWrapper is null
     * @throws ResourceNotFoundException if an element cannot be found
     */
    private void setElementPropertiesFromVertex(ElementConnectivityWrapper connectionWrapper, String sourceElementName,
                                                String destElementName) {
        Objects.requireNonNull(connectionWrapper, TopologyGraphConstants.CONN_WRAP_NOT_BE_NULL);

        if (sourceElementName != null) {
            try {
                ElementInfoWrapper srcVertax = getElementInfoWrapperByEName(sourceElementName);
                setSrcVertexProperty(connectionWrapper, srcVertax);
            } catch (ResourceNotFoundException e) {
                logger.info("Source element not found: {}", sourceElementName);
                throw new ResourceNotFoundException("Source element not found: " + sourceElementName, e);
            }
        }

        if (destElementName != null) {
            try {
                ElementInfoWrapper destVertax = getElementInfoWrapperByEName(destElementName);
                setDestVertexProperty(connectionWrapper, destVertax);
            } catch (ResourceNotFoundException e) {
                logger.info("Destination element not found: {}", destElementName);
                throw new ResourceNotFoundException("Destination element not found: " + destElementName, e);
            }
        }
    }

    /**
     * Sets source vertex properties in the connection wrapper.
     *
     * <p>
     * This method populates the connection wrapper with properties from the source
     * vertex.
     * </p>
     *
     * @param connectionWrapper the wrapper to populate with source properties, must
     *                          not be null
     * @param srcVertax         the source element information wrapper, can be null
     * @throws NullPointerException if connectionWrapper is null
     */
    private void setSrcVertexProperty(ElementConnectivityWrapper connectionWrapper, ElementInfoWrapper srcVertax) {
        Objects.requireNonNull(connectionWrapper, TopologyGraphConstants.CONN_WRAP_NOT_BE_NULL);

        if (srcVertax != null) {
            connectionWrapper.setSourceSiteId(srcVertax.getSiteId());
            connectionWrapper.setSourceEnabledOriginNW(
                    srcVertax.getEnabledOriginNW() != null ? srcVertax.getEnabledOriginNW().toString() : null);
            connectionWrapper.setSourceEType(
                    srcVertax.getElementAliaseType() != null ? srcVertax.getElementAliaseType() : null);
        }
    }

    /**
     * Sets destination vertex properties in the connection wrapper.
     *
     * <p>
     * This method populates the connection wrapper with properties from the
     * destination vertex.
     * </p>
     *
     * @param connectionWrapper the wrapper to populate with destination properties,
     *                          must not be null
     * @param destVertax        the destination element information wrapper, can be
     *                          null
     * @throws NullPointerException if connectionWrapper is null
     */
    private void setDestVertexProperty(ElementConnectivityWrapper connectionWrapper, ElementInfoWrapper destVertax) {
        Objects.requireNonNull(connectionWrapper, TopologyGraphConstants.CONN_WRAP_NOT_BE_NULL);

        if (destVertax != null) {
            connectionWrapper.setDestSiteId(destVertax.getSiteId());
            connectionWrapper.setDestEnabledOriginNW(
                    destVertax.getEnabledOriginNW() != null ? destVertax.getEnabledOriginNW().toString() : null);
            connectionWrapper.setDestEType(
                    destVertax.getElementAliaseType() != null ? destVertax.getElementAliaseType() : null);
            connectionWrapper
                    .setNodePriority(destVertax.getNodePriority() != null ? destVertax.getNodePriority() : null);
        }
    }

    /**
     * Retrieves correlated element connectivity information.
     *
     * <p>
     * This method finds all elements connected to the specified element (optionally
     * filtered by port)
     * and constructs a map of connectivity wrappers containing their relationship
     * details.
     * </p>
     *
     * @param eName the name of the element to find connectivity for, must not be
     *              null or empty
     * @param port  the specific port to filter by, can be null
     * @return a map of element names to their connectivity wrappers
     * @throws IllegalArgumentException if eName is null or empty
     * @throws ConnectivityException    if connectivity information cannot be
     *                                  retrieved
     */
    @Override
    public Map<String, TopologyConnectivityWrapper> getCorrelatedElementConnectivity(String eName, String port) {
        if (StringUtils.isEmpty(eName)) {
            throw new IllegalArgumentException(TopologyGraphConstants.ELEMENT_NAME_CANNOT_BE_NULL_OR_EMPTY);
        }

        Map<String, TopologyConnectivityWrapper> wrapperMap = new HashMap<>();

        try {
            List<Edge> allEdgeByVertexPropertyValue = topologyGraphUtils
                    .getAllEdgeByVertexPropertyValue(TopologyGraphConstants.ELEMENT_NAME, eName);
            logger.info("allEdgeByVertexPropertyValue={}",
                    allEdgeByVertexPropertyValue != null ? allEdgeByVertexPropertyValue.size() : null);

            Vertex targetVertex = JanusGraphInstanceManagerServiceImpl
                    .getGraphByTraversalByInstance(JanusGraphInstance.TOPOLOGY_GRAPH).V()
                    .has(TopologyGraphConstants.ELEMENT_NAME, eName).next();

            Set<Vertex> dependentVertices = new HashSet<>();
            Set<Vertex> nonDependentVertices = new HashSet<>();
            Set<Vertex> traversedVertex = new HashSet<>();
            Set<Edge> edges = new HashSet<>();

            if (CollectionUtils.isNotEmpty(allEdgeByVertexPropertyValue)) {
                for (Edge edge : allEdgeByVertexPropertyValue) {
                    if (isRelevantEdge(edge, eName, port)) {
                        getedges(eName, port, targetVertex, dependentVertices, nonDependentVertices, traversedVertex,
                                edges, edge);
                    }
                }
                getVertiesInConnWrapper(nonDependentVertices, wrapperMap, false);
                getVertiesInConnWrapper(dependentVertices, wrapperMap, true);
                getConnectivityInInfoWrapper(wrapperMap, edges, null, null);
            }

            logger.info("Created connectivity wrapper map with {} elements for element: {}", wrapperMap.size(), eName);
            return wrapperMap;

        } catch (NoSuchElementException e) {
            logger.error("Element not found with name: {}", eName);
            throw new ConnectivityException(TopologyGraphConstants.ELEMENT_NOT_FOUND_WITH_NAME + eName, e);
        } catch (IllegalStateException e) {
            logger.error("Invalid graph state when retrieving connectivity for element: {}", eName);
            throw new ConnectivityException("Invalid graph state when retrieving connectivity for element: " + eName,
                    e);
        } catch (Exception e) {
            logger.error("Failed to retrieve correlated element connectivity for element: {} {}",
                    eName, port != null ? "and port: " + port : "");
            throw new ConnectivityException("Failed to retrieve correlated element connectivity for element: " + eName +
                    (port != null ? " and port: " + port : ""), e);
        }
    }

    // Helper method extracted to reduce cognitive complexity
    private boolean isRelevantEdge(Edge edge, String eName, String port) {
        if (StringUtils.isEmpty(port)) {
            return true;
        }

        boolean isSourceMatch = edge.value(TopologyGraphConstants.SOURCE_ELEMENT_NAME).toString()
                .equalsIgnoreCase(eName)
                && edge.value(TopologyGraphConstants.SOURCE_PORT).toString().equalsIgnoreCase(port);

        boolean isDestMatch = edge.value(TopologyGraphConstants.DESTINATION_ELEMENT_NAME).toString()
                .equalsIgnoreCase(eName)
                && edge.value(TopologyGraphConstants.DESTINATION_PORT).toString().equalsIgnoreCase(port);

        return isSourceMatch || isDestMatch;
    }

    /**
     * Processes edges connected to a vertex and categorizes connected vertices.
     *
     * <p>
     * This method examines an edge connected to the target element and categorizes
     * the vertices on the other end as dependent or non-dependent based on their
     * relationship to higher-level elements.
     * </p>
     *
     * @param eName                the name of the target element, must not be null
     * @param port                 the specific port to filter by, can be null
     * @param targetVertex         the vertex of the target element, must not be
     *                             null
     * @param dependentVertices    set to collect dependent vertices, must not be
     *                             null
     * @param nonDependentVertices set to collect non-dependent vertices, must not
     *                             be null
     * @param traversedVertex      set to track traversed vertices, must not be null
     * @param edges                set to collect processed edges, must not be null
     * @param edge                 the edge to process, must not be null
     * @throws NullPointerException  if any required parameter is null
     * @throws ConnectivityException if edge processing fails
     */

    @SuppressWarnings("java:S107")
    private void getedges(String eName, String port, Vertex targetVertex, Set<Vertex> dependentVertices,
                          Set<Vertex> nonDependentVertices, Set<Vertex> traversedVertex, Set<Edge> edges, Edge edge) {
        Objects.requireNonNull(edge, TopologyGraphConstants.EDGE_NOT_BE_NULL);
        Objects.requireNonNull(eName, TopologyGraphConstants.ELEMENT_NAME_NOT_BE_NULL);
        Objects.requireNonNull(targetVertex, TopologyGraphConstants.TARGET_VERTEX_NOT_BE_NULL);
        Objects.requireNonNull(dependentVertices, "Dependent vertices set cannot be null");
        Objects.requireNonNull(nonDependentVertices, "Non-dependent vertices set cannot be null");
        Objects.requireNonNull(traversedVertex, TopologyGraphConstants.TRAVERSED_VERTEX_SET_NOT_BE_NULL);
        Objects.requireNonNull(edges, TopologyGraphConstants.EDGES_SET_NOT_BE_NULL);

        try {
            Iterator<Vertex> bothVertices = edge.bothVertices();
            while (bothVertices.hasNext()) {
                Vertex vertex = bothVertices.next();
                String vertexElementName = vertex.value(TopologyGraphConstants.ELEMENT_NAME).toString();

                if (!vertexElementName.equalsIgnoreCase(eName)) {
                    handleNonMatchingVertex(eName, port, targetVertex, dependentVertices, traversedVertex, edges,
                            vertex);
                } else {
                    dependentVertices.add(vertex);
                }

                if (!dependentVertices.contains(vertex)) {
                    nonDependentVertices.add(vertex);
                }
            }
            edges.add(edge);
        } catch (NullPointerException e) {
            String edgeId = edge.id() != null ? edge.id().toString() : TopologyGraphConstants.UNKNOWN;
            logger.error(TopologyGraphConstants.MISSING_REQUIRED_PROPERTIES_FOR_EDGE, edgeId);
            throw new ConnectivityException(TopologyGraphConstants.MISSING_REQUIRED_PROPERTIES_FOR_EDGE + edgeId, e);
        } catch (Exception e) {
            String edgeId = edge.id() != null ? edge.id().toString() : TopologyGraphConstants.UNKNOWN;
            logger.error("Failed to process edge: {} for element: {}", edgeId, eName);
            throw new ConnectivityException("Failed to process edge: " + edgeId + " for element: " + eName, e);
        }
    }

    private void handleNonMatchingVertex(String eName, String port, Vertex targetVertex,
                                         Set<Vertex> dependentVertices, Set<Vertex> traversedVertex,
                                         Set<Edge> edges, Vertex vertex) {
        String eType = vertex.value(TopologyGraphConstants.ELEMENT_TYPE);
        List<String> eTypeInEdge = topologyGraphUtils.getRequiredTopologyLevelType(eType,
                TopologyGraphConstants.UPWORD);
        eTypeInEdge.add(eType);

        List<String> eTypeForUpToEdge = topologyGraphUtils.getRequiredTopologyLevelType(eType,
                TopologyGraphConstants.UPWORD);
        if (eTypeForUpToEdge.isEmpty()) {
            eTypeForUpToEdge.add(eType);
        }

        if (eTypeForUpToEdge.size() > 1
                && !"type".equalsIgnoreCase(targetVertex.value(TopologyGraphConstants.ELEMENT_TYPE).toString())) {
            eTypeForUpToEdge.remove(targetVertex.value(TopologyGraphConstants.ELEMENT_TYPE).toString());
        }

        Set<Path> set2 = JanusGraphInstanceManagerServiceImpl
                .getGraphByTraversalByInstance(JanusGraphInstance.TOPOLOGY_GRAPH).V()
                .has(TopologyGraphConstants.ELEMENT_NAME, vertex.value(TopologyGraphConstants.ELEMENT_NAME).toString())
                .repeat(bothE()
                        .hasNot(TopologyGraphConstants.SOURCE_BUNDLE)
                        .hasNot(TopologyGraphConstants.DESTINATION_BUNDLE)
                        .otherV()
                        .has(TopologyGraphConstants.ELEMENT_TYPE, P.within(eTypeInEdge))
                        .not(has(TopologyGraphConstants.ELEMENT_NAME, eName))
                        .simplePath())
                .until(has(TopologyGraphConstants.ELEMENT_TYPE, P.within(eTypeForUpToEdge)))
                .path()
                .limit(TopologyGraphConstants.TEN)
                .toSet();

        if (CollectionUtils.isEmpty(set2)) {
            getConnectedEdge(vertex, eName, edges, dependentVertices, targetVertex, port, traversedVertex);
        }
    }

    /**
     * Adds vertices to the connectivity wrapper map.
     *
     * <p>
     * This method processes a set of vertices and adds them to the wrapper map,
     * marking them as dependent or non-dependent based on the provided flag.
     * </p>
     *
     * @param vertices    the set of vertices to process, must not be null
     * @param wrapperMap  the map to add connectivity wrappers to, must not be null
     * @param isDependent flag indicating if vertices should be marked as dependent,
     *                    must not be null
     * @throws NullPointerException  if any parameter is null
     * @throws ConnectivityException if vertex processing fails
     */
    private void getVertiesInConnWrapper(Set<Vertex> vertices, Map<String, TopologyConnectivityWrapper> wrapperMap,
                                         Boolean isDependent) {
        Objects.requireNonNull(vertices, TopologyGraphConstants.VERTICES_SET_NOT_BE_NULL);
        Objects.requireNonNull(wrapperMap, "Wrapper map cannot be null");
        Objects.requireNonNull(isDependent, "isDependent flag cannot be null");

        try {
            for (Vertex vertex : vertices) {
                if (isRequiredVertex(vertex)) {
                    TopologyConnectivityWrapper connInfoWrapper = getTopologyConnectivityWrapper(
                            (JanusGraphVertex) vertex);
                    if (Boolean.TRUE.equals(isDependent)) {
                        connInfoWrapper.setDependent(true);
                    }
                    wrapperMap.put(connInfoWrapper.geteName(), connInfoWrapper);
                }
            }
        } catch (ClassCastException e) {
            logger.error("Invalid vertex type encountered when processing connectivity");
            throw new ConnectivityException("Invalid vertex type encountered when processing connectivity", e);
        } catch (Exception e) {
            logger.error("Failed to process vertices for connectivity wrapper");
            throw new ConnectivityException("Failed to process vertices for connectivity wrapper", e);
        }
    }

    /**
     * Checks if a vertex is required for topology processing.
     *
     * <p>
     * This method determines if a vertex should be included in topology processing
     * based on its element type and network type properties. Vertices with element
     * type "type"
     * are only included if they have a network type of "MICROWAVE_TOPOLOGY".
     * </p>
     *
     * @param vertex the vertex to check, must not be null
     * @return true if the vertex should be included in topology processing, false
     *         otherwise
     * @throws NullPointerException  if vertex is null
     * @throws ConnectivityException if required vertex properties cannot be
     *                               accessed
     */
    private boolean isRequiredVertex(Vertex vertex) {
        Objects.requireNonNull(vertex, TopologyGraphConstants.VERTEX_NOT_BE_NULL);

        Boolean result = true;
        try {
            if ("type".equalsIgnoreCase(vertex.value(TopologyGraphConstants.ELEMENT_TYPE))) {
                result = checkNetworkType(vertex);
            }
        } catch (NullPointerException e) {
            String vertexId = vertex.id() != null ? vertex.id().toString() : TopologyGraphConstants.UNKNOWN;
            logger.error("Missing required properties for vertex: {}", vertexId);
            throw new ConnectivityException("Missing required properties for vertex: " + vertexId, e);
        }

        return result;
    }

    private boolean checkNetworkType(Vertex vertex) {
        try {
            List<String> allValueFromMultiProps = topologyGraphUtils.getAllValueFromMultiProps(vertex,
                    TopologyGraphConstants.NETWORK_TYPE);
            if (CollectionUtils.isNotEmpty(allValueFromMultiProps)
                    && !allValueFromMultiProps.contains(TopologyGraphConstants.MICROWAVE_TOPOLOGY))
                return false;
        } catch (Exception e) {
            logger.info("TopologyGraphConstants.NETWORK_TYPE property not found for vertex ID: {}",
                    vertex.id() != null ? vertex.id().toString() : TopologyGraphConstants.UNKNOWN);
        }
        return true;
    }

    /**
     * Retrieves connected edges for a vertex in the topology graph.
     *
     * <p>
     * This method traverses the graph to find connected edges between the specified
     * vertex
     * and other vertices. It handles traversal through multiple levels of the
     * topology hierarchy
     * and collects edges and vertices that form the connectivity path.
     * </p>
     *
     * @param vertex          the starting vertex for edge traversal, must not be
     *                        null
     * @param eName           the element name of the starting vertex, must not be
     *                        null
     * @param edges           the set to collect connected edges, must not be null
     * @param vertices        the set to collect connected vertices, must not be
     *                        null
     * @param targetVertex    the target vertex to connect to, may be null
     * @param port            the port specification for the connection, may be null
     * @param traversedVertex the set of already traversed vertices to prevent
     *                        cycles, must not be null
     * @throws NullPointerException  if vertex, eName, edges, vertices, or
     *                               traversedVertex is null
     * @throws ConnectivityException if there is an error accessing vertex data
     * @throws GraphicalException    if there is an error in graph traversal
     */
    private void getConnectedEdge(Vertex vertex, String eName, Set<Edge> edges, Set<Vertex> vertices,
                                  Vertex targetVertex, String port, Set<Vertex> traversedVertex) {
        Objects.requireNonNull(vertex, TopologyGraphConstants.VERTEX_NOT_BE_NULL);
        Objects.requireNonNull(eName, TopologyGraphConstants.ELEMENT_NAME_NOT_BE_NULL);
        Objects.requireNonNull(edges, TopologyGraphConstants.EDGES_SET_NOT_BE_NULL);
        Objects.requireNonNull(vertices, TopologyGraphConstants.VERTICES_SET_NOT_BE_NULL);
        Objects.requireNonNull(traversedVertex, TopologyGraphConstants.TRAVERSED_VERTEX_SET_NOT_BE_NULL);

        if (!traversedVertex.contains(vertex)) {
            traversedVertex.add(vertex);

            try {
                List<String> eTypeForDownEdge = getDownwardEdgeTypes(vertex);

                Set<Path> paths = getPaths(vertex, eTypeForDownEdge, eName);

                if (CollectionUtils.isNotEmpty(paths)) {
                    processPaths(paths, targetVertex, port, edges, vertices, traversedVertex);
                }
            } catch (IllegalArgumentException e) {
                logger.error("Invalid vertex data when traversing graph for element: {}", eName);
                throw new ConnectivityException("Invalid vertex data when traversing graph", e);
            } catch (NullPointerException e) {
                String vertexId = vertex.id() != null ? vertex.id().toString() : TopologyGraphConstants.UNKNOWN;
                logger.error("Missing required properties for vertex: {} when processing element: {}", vertexId, eName);
                throw new GraphicalException("Missing required properties for vertex: " + vertexId, e);
            }
        }
    }

    /**
     * Retrieves the element types required for downward edge traversal.
     *
     * <p>
     * This method gets the element types that should be considered when traversing
     * downward in the topology hierarchy from the specified vertex.
     * </p>
     *
     * @param vertex the vertex to get downward edge types for, must not be null
     * @return a list of element types for downward traversal
     * @throws NullPointerException if vertex is null or missing required properties
     * @throws GraphicalException   if element type information cannot be retrieved
     */
    private List<String> getDownwardEdgeTypes(Vertex vertex) {
        Objects.requireNonNull(vertex, "Vertex cannot be null when getting downward edge types");

        try {
            List<String> eTypeForDownEdge = topologyGraphUtils.getRequiredTopologyLevelType(
                    vertex.value(TopologyGraphConstants.ELEMENT_TYPE), TopologyGraphConstants.DOWNWORD);
            eTypeForDownEdge.add(vertex.value(TopologyGraphConstants.ELEMENT_TYPE));
            return eTypeForDownEdge;
        } catch (NullPointerException e) {
            String vertexId = vertex.id() != null ? vertex.id().toString() : TopologyGraphConstants.UNKNOWN;
            logger.error("Missing element type for vertex: {}", vertexId);
            throw new GraphicalException("Missing element type for vertex: " + vertexId, e);
        }
    }

    /**
     * Retrieves paths from a vertex to other vertices of specified element types.
     *
     * <p>
     * This method performs a graph traversal to find paths from the specified
     * vertex
     * to other vertices that match the given element types, excluding paths that
     * lead
     * back to the original element name.
     * </p>
     *
     * @param vertex           the starting vertex for path traversal, must not be
     *                         null
     * @param eTypeForDownEdge the list of element types to consider in the
     *                         traversal, must not be null or empty
     * @param eName            the element name to exclude from the traversal, must
     *                         not be null
     * @return a set of paths from the starting vertex to matching vertices
     * @throws NullPointerException     if any parameter is null
     * @throws IllegalArgumentException if eTypeForDownEdge is empty
     * @throws GraphicalException       if graph traversal fails
     */
    private Set<Path> getPaths(Vertex vertex, List<String> eTypeForDownEdge, String eName) {
        Objects.requireNonNull(vertex, "Vertex cannot be null when getting paths");
        Objects.requireNonNull(eTypeForDownEdge, "Edge types list cannot be null");
        Objects.requireNonNull(eName, TopologyGraphConstants.ELEMENT_NAME_NOT_BE_NULL);

        if (CollectionUtils.isEmpty(eTypeForDownEdge)) {
            throw new IllegalArgumentException("Edge types list cannot be empty");
        }

        try {
            return JanusGraphInstanceManagerServiceImpl
                    .getGraphByTraversalByInstance(JanusGraphInstance.TOPOLOGY_GRAPH).V()
                    .has(TopologyGraphConstants.ELEMENT_NAME,
                            vertex.value(TopologyGraphConstants.ELEMENT_NAME).toString())
                    .repeat(bothE().otherV().has(TopologyGraphConstants.ELEMENT_TYPE, P.within(eTypeForDownEdge))
                            .not(has(TopologyGraphConstants.ELEMENT_NAME, eName)).simplePath())
                    .until(has(TopologyGraphConstants.ELEMENT_TYPE, P.within(eTypeForDownEdge))).path().toSet();
        } catch (IllegalStateException e) {
            throw new GraphicalException("Graph traversal failed for vertex: " + vertex.id(), e);
        }
    }

    /**
     * Processes paths found during graph traversal.
     *
     * <p>
     * This method examines each path found during graph traversal, checks if it
     * meets
     * the criteria for inclusion, and processes the edges and vertices in valid
     * paths.
     * </p>
     *
     * @param paths           the set of paths to process, must not be null
     * @param targetVertex    the target vertex to connect to, may be null
     * @param port            the port specification for the connection, may be null
     * @param edges           the set to collect connected edges, must not be null
     * @param vertices        the set to collect connected vertices, must not be
     *                        null
     * @param traversedVertex the set of already traversed vertices to prevent
     *                        cycles, must not be null
     * @throws NullPointerException if paths, edges, vertices, or traversedVertex is
     *                              null
     * @throws GraphicalException   if path processing fails
     */
    private void processPaths(Set<Path> paths, Vertex targetVertex, String port, Set<Edge> edges,
                              Set<Vertex> vertices, Set<Vertex> traversedVertex) {
        Objects.requireNonNull(paths, "Paths cannot be null");
        Objects.requireNonNull(edges, TopologyGraphConstants.EDGES_SET_NOT_BE_NULL);
        Objects.requireNonNull(vertices, TopologyGraphConstants.VERTICES_SET_NOT_BE_NULL);
        Objects.requireNonNull(traversedVertex, TopologyGraphConstants.TRAVERSED_VERTEX_SET_NOT_BE_NULL);

        try {
            for (Path path : paths) {
                Path clone = path.clone();
                if (checkPath(clone, targetVertex, port)) {
                    processPathEdges(path, edges, vertices, targetVertex, port, traversedVertex);
                }
            }
        } catch (UnsupportedOperationException e) {
            logger.error("Failed to process paths in graph traversal");
            throw new GraphicalException("Failed to process paths in graph traversal", e);
        }
    }

    /**
     * Processes the edges and vertices in a valid path.
     *
     * <p>
     * This method extracts edges and vertices from a path and adds them to the
     * respective collection sets. It also handles further traversal from the last
     * vertex in the path if needed.
     * </p>
     *
     * @param path            the path to process, must not be null
     * @param edges           the set to collect edges from the path, must not be
     *                        null
     * @param vertices        the set to collect vertices from the path, must not be
     *                        null
     * @param targetVertex    the target vertex to connect to, may be null
     * @param port            the port specification for the connection, may be null
     * @param traversedVertex the set of already traversed vertices, must not be
     *                        null
     * @throws NullPointerException if path, edges, vertices, or traversedVertex is
     *                              null
     * @throws DataException        if path elements are of invalid type
     */
    private void processPathEdges(Path path, Set<Edge> edges, Set<Vertex> vertices, Vertex targetVertex,
                                  String port, Set<Vertex> traversedVertex) {
        Objects.requireNonNull(path, "Path cannot be null when processing edges");
        Objects.requireNonNull(edges, TopologyGraphConstants.EDGES_SET_NOT_BE_NULL);
        Objects.requireNonNull(vertices, TopologyGraphConstants.VERTICES_SET_NOT_BE_NULL);
        Objects.requireNonNull(traversedVertex, TopologyGraphConstants.TRAVERSED_VERTEX_SET_NOT_BE_NULL);

        try {
            Iterator<Object> iterator = path.iterator();
            Edge lastEdgeOfPath = null;

            while (iterator.hasNext()) {
                Vertex next = (Vertex) iterator.next();
                vertices.add(next);

                if (iterator.hasNext()) {
                    Edge nextEdge = (Edge) iterator.next();
                    edges.add(nextEdge);
                    lastEdgeOfPath = nextEdge;
                } else {
                    handleLastEdge(lastEdgeOfPath, next, targetVertex, port, edges, vertices, traversedVertex);
                }
            }
        } catch (ClassCastException e) {
            logger.error("Invalid path element type encountered during traversal");
            throw new DataException("Invalid path element type encountered during traversal", e);
        }
    }

    /**
     * Handles the last edge in a path for further traversal.
     *
     * <p>
     * This method processes the last edge in a path and initiates further traversal
     * from connected vertices if needed.
     * </p>
     *
     * @param lastEdgeOfPath  the last edge in the path, may be null
     * @param next            the last vertex in the path, must not be null
     * @param targetVertex    the target vertex to connect to, may be null
     * @param port            the port specification for the connection, may be null
     * @param edges           the set to collect edges, must not be null
     * @param vertices        the set to collect vertices, must not be null
     * @param traversedVertex the set of already traversed vertices, must not be
     *                        null
     * @throws NullPointerException if next, edges, vertices, or traversedVertex is
     *                              null
     * @throws GraphicalException   if edge properties cannot be accessed
     */
    private void handleLastEdge(Edge lastEdgeOfPath, Vertex next, Vertex targetVertex, String port,
                                Set<Edge> edges, Set<Vertex> vertices, Set<Vertex> traversedVertex) {
        Objects.requireNonNull(next, "Next vertex cannot be null");
        Objects.requireNonNull(edges, TopologyGraphConstants.EDGES_SET_NOT_BE_NULL);
        Objects.requireNonNull(vertices, TopologyGraphConstants.VERTICES_SET_NOT_BE_NULL);
        Objects.requireNonNull(traversedVertex, TopologyGraphConstants.TRAVERSED_VERTEX_SET_NOT_BE_NULL);

        if (lastEdgeOfPath != null) {
            try {
                Iterator<Vertex> bothVertices = lastEdgeOfPath.bothVertices();
                while (bothVertices.hasNext()) {
                    Vertex next2 = bothVertices.next();
                    if (next != next2) {
                        getConnectedEdge(next, next2.value(TopologyGraphConstants.ELEMENT_NAME),
                                edges, vertices, targetVertex, port, traversedVertex);
                    }
                }
            } catch (NullPointerException e) {
                String edgeId = lastEdgeOfPath.id() != null ? lastEdgeOfPath.id().toString()
                        : TopologyGraphConstants.UNKNOWN;
                logger.error(TopologyGraphConstants.MISSING_REQUIRED_PROPERTIES_FOR_EDGE, edgeId);
                throw new GraphicalException(TopologyGraphConstants.MISSING_REQUIRED_PROPERTIES_FOR_EDGE + edgeId, e);
            }
        }
    }

    /**
     * Checks if a path meets the criteria for inclusion in the topology.
     *
     * <p>
     * This method evaluates a path to determine if it should be included in the
     * topology based on the target vertex and port specifications.
     * </p>
     *
     * @param clone        a clone of the path to check, must not be null
     * @param targetVertex the target vertex to connect to, must not be null
     * @param port         the port specification for the connection, may be null
     * @return true if the path meets the inclusion criteria, false otherwise
     * @throws NullPointerException if clone or targetVertex is null
     * @throws DataException        if path elements are of invalid type
     * @throws GraphicalException   if path structure is invalid
     */
    private boolean checkPath(Path clone, Vertex targetVertex, String port) {
        Objects.requireNonNull(clone, "Path clone cannot be null");
        Objects.requireNonNull(targetVertex, TopologyGraphConstants.TARGET_VERTEX_NOT_BE_NULL);

        try {
            Iterator<Object> pathIterator = clone.iterator();
            while (pathIterator.hasNext()) {
                Vertex vertex = (Vertex) pathIterator.next();
                if (pathIterator.hasNext()) {
                    pathIterator.next();
                } else {
                    return evaluatePath(vertex, targetVertex, port);
                }
            }
            return false;
        } catch (ClassCastException e) {
            logger.error("Invalid path element type during path validation");
            throw new DataException("Invalid path element type during path validation", e);
        } catch (NoSuchElementException e) {
            logger.error("Path structure is invalid during validation");
            throw new GraphicalException("Path structure is invalid during validation", e);
        }
    }

    /**
     * Evaluates if a path endpoint meets the criteria for connection to the target
     * vertex.
     *
     * <p>
     * This method checks if the last vertex in a path can be connected to the
     * target vertex
     * based on their element types, device levels, and connectivity rules.
     * </p>
     *
     * @param vertex       the last vertex in the path, must not be null
     * @param targetVertex the target vertex to connect to, must not be null
     * @param port         the port specification for the connection, may be null
     * @return true if the path endpoint can connect to the target, false otherwise
     * @throws NullPointerException      if vertex or targetVertex is null
     * @throws InvalidInputException     if element types are null
     * @throws ResourceNotFoundException if required element types are not found
     */
    private boolean evaluatePath(Vertex vertex, Vertex targetVertex, String port) {
        Objects.requireNonNull(vertex, TopologyGraphConstants.VERTEX_NOT_BE_NULL);
        Objects.requireNonNull(targetVertex, TopologyGraphConstants.TARGET_VERTEX_NOT_BE_NULL);

        try {
            Map<String, Integer> deviceLevels = topologyGraphUtils.getDeviceLevels();
            if (deviceLevels == null || deviceLevels.isEmpty()) {
                throw new IllegalStateException("Device levels map is null or empty");
            }
            logger.info("Device levels for path evaluation: {}", deviceLevels);

            String vertexElementType = vertex.value(TopologyGraphConstants.ELEMENT_TYPE).toString();
            String targetElementType = targetVertex.value(TopologyGraphConstants.ELEMENT_TYPE).toString();

            if (vertexElementType == null || targetElementType == null) {
                throw new InvalidInputException("Element type cannot be null for vertex or target vertex");
            }

            int targetDeviceLevel = deviceLevels.get(targetElementType);
            int currentDeviceLevel = deviceLevels.get(vertexElementType);

            if (targetDeviceLevel >= currentDeviceLevel) {
                return true;
            }

            List<String> eTypeInEdge = topologyGraphUtils.getRequiredTopologyLevelType(
                    vertex.value(TopologyGraphConstants.ELEMENT_TYPE), TopologyGraphConstants.UPWORD);
            eTypeInEdge.add(vertex.value(TopologyGraphConstants.ELEMENT_TYPE));

            List<String> terminationEType = getTerminationEType(targetVertex);
            Set<Vertex> verticesSet = new HashSet<>();
            getTargetLevelVertices(eTypeInEdge, vertex, verticesSet, terminationEType);

            if (CollectionUtils.isNotEmpty(verticesSet)) {
                return checkConnectivityWithPort(verticesSet, targetVertex, port)
                        || checkUpperLevelConnectivity(verticesSet, targetVertex);
            }

            return true;
        } catch (NoSuchElementException e) {
            logger.error("Required element type not found in device levels");
            throw new ResourceNotFoundException("Required element type not found in device levels", e);
        } catch (NullPointerException e) {
            logger.error("Null element type encountered during path evaluation");
            throw new InvalidInputException("Null element type encountered during path evaluation", e);
        }
    }

    /**
     * Gets the element types for termination of path traversal.
     *
     * <p>
     * This method determines which element types should be considered as valid
     * termination points when traversing upward from the target vertex.
     * </p>
     *
     * @param targetVertex the target vertex to get termination types for, must not
     *                     be null
     * @return a list of element types for path termination
     * @throws NullPointerException    if targetVertex is null or missing required
     *                                 properties
     * @throws InvalidInputException   if target vertex element type is null
     * @throws VertexCreationException if required properties cannot be accessed
     */
    private List<String> getTerminationEType(Vertex targetVertex) {
        Objects.requireNonNull(targetVertex, TopologyGraphConstants.TARGET_VERTEX_NOT_BE_NULL);

        try {
            String targetElementType = targetVertex.value(TopologyGraphConstants.ELEMENT_TYPE).toString();
            if (targetElementType == null) {
                throw new InvalidInputException("Target vertex element type cannot be null");
            }

            List<String> terminationEType = topologyGraphUtils.getRequiredTopologyLevelType(
                    targetElementType, TopologyGraphConstants.UPWORD);
            if (terminationEType == null) {
                terminationEType = new ArrayList<>();
            }
            terminationEType.add(targetElementType);
            return terminationEType;
        } catch (NullPointerException e) {
            logger.error("Target vertex missing required element type property");
            throw new VertexCreationException("Target vertex missing required element type property", e);
        }
    }

    /**
     * Checks if vertices in a set can connect to the target vertex through a
     * specific port.
     *
     * <p>
     * This method determines if any vertex in the provided set can connect to the
     * target
     * vertex, optionally through a specified port.
     * </p>
     *
     * @param verticesSet  the set of vertices to check for connectivity, must not
     *                     be null
     * @param targetVertex the target vertex to connect to, must not be null
     * @param port         the port specification for the connection, may be null
     * @return true if connectivity exists, false otherwise
     * @throws NullPointerException  if verticesSet or targetVertex is null
     * @throws InvalidInputException if invalid arguments are provided
     */
    private boolean checkConnectivityWithPort(Set<Vertex> verticesSet, Vertex targetVertex, String port) {
        Objects.requireNonNull(verticesSet, TopologyGraphConstants.VERTICES_SET_NOT_BE_NULL);
        Objects.requireNonNull(targetVertex, TopologyGraphConstants.TARGET_VERTEX_NOT_BE_NULL);

        if (port != null && verticesSet.contains(targetVertex)) {
            try {
                Set<Path> paths = findPathsWithPort(verticesSet, targetVertex, port);
                return isValidPath(paths, targetVertex, port);
            } catch (IllegalArgumentException e) {
                logger.error("Invalid arguments when checking connectivity with port");
                throw new InvalidInputException("Invalid arguments when checking connectivity with port", e);
            }
        }
        return verticesSet.contains(targetVertex);
    }

    /**
     * Finds paths between vertices in a set and a target vertex that involve a
     * specific port.
     *
     * <p>
     * This method performs a graph traversal to find paths between vertices in the
     * provided
     * set and the target vertex, which will be evaluated for port-specific
     * connectivity.
     * </p>
     *
     * @param verticesSet  the set of vertices to find paths from, must not be null
     *                     or empty
     * @param targetVertex the target vertex to find paths to, must not be null
     * @param port         the port specification for the connection, must not be
     *                     null
     * @return a set of paths between the vertices and the target
     * @throws NullPointerException      if any parameter is null
     * @throws IllegalArgumentException  if verticesSet is empty
     * @throws ResourceNotFoundException if vertices cannot be found
     * @throws GraphicalException        if graph traversal fails
     */
    private Set<Path> findPathsWithPort(Set<Vertex> verticesSet, Vertex targetVertex, String port) {
        Objects.requireNonNull(verticesSet, TopologyGraphConstants.VERTICES_SET_NOT_BE_NULL);
        Objects.requireNonNull(targetVertex, TopologyGraphConstants.TARGET_VERTEX_NOT_BE_NULL);
        Objects.requireNonNull(port, "Port cannot be null when finding paths with port");

        if (verticesSet.isEmpty()) {
            throw new IllegalArgumentException("Vertices set cannot be empty");
        }

        try {
            Vertex firstVertex = verticesSet.iterator().next();
            String elementName = firstVertex.value(TopologyGraphConstants.ELEMENT_NAME).toString();
            String elementType = firstVertex.value(TopologyGraphConstants.ELEMENT_TYPE).toString();

            if (elementName == null || elementType == null) {
                throw new InvalidInputException("Element name or type cannot be null");
            }

            return JanusGraphInstanceManagerServiceImpl.getGraphByTraversalByInstance(JanusGraphInstance.TOPOLOGY_GRAPH)
                    .V()
                    .has(TopologyGraphConstants.ELEMENT_NAME,
                            verticesSet.iterator().next().value(TopologyGraphConstants.ELEMENT_NAME).toString())
                    .repeat(bothE().otherV()
                            .has(TopologyGraphConstants.ELEMENT_TYPE,
                                    P.within(verticesSet.iterator().next().value(TopologyGraphConstants.ELEMENT_TYPE)
                                            .toString()))
                            .simplePath())
                    .until(has(TopologyGraphConstants.ELEMENT_NAME,
                            P.within(targetVertex.value(TopologyGraphConstants.ELEMENT_NAME).toString())))
                    .path().toSet();
        } catch (NoSuchElementException e) {
            logger.error("Vertex not found during path traversal");
            throw new ResourceNotFoundException("Vertex not found during path traversal", e);
        } catch (IllegalStateException e) {
            logger.error("Graph traversal error when finding paths with port");
            throw new GraphicalException("Graph traversal error when finding paths with port", e);
        }
    }

    /**
     * Checks if any path in a set is valid for connecting to the target vertex
     * through a specific port.
     *
     * <p>
     * This method evaluates each path in the provided set to determine if it
     * represents
     * a valid connection to the target vertex through the specified port.
     * </p>
     *
     * @param paths        the set of paths to evaluate, must not be null
     * @param targetVertex the target vertex to connect to, must not be null
     * @param port         the port specification for the connection, must not be
     *                     null
     * @return true if any path is valid, false otherwise
     * @throws NullPointerException  if any parameter is null
     * @throws InvalidInputException if path elements are of invalid type
     */
    private boolean isValidPath(Set<Path> paths, Vertex targetVertex, String port) {
        Objects.requireNonNull(paths, "Paths set cannot be null");
        Objects.requireNonNull(targetVertex, TopologyGraphConstants.TARGET_VERTEX_NOT_BE_NULL);
        Objects.requireNonNull(port, "Port cannot be null when validating path");

        try {
            for (Path path : paths) {
                Iterator<Object> iterator = path.iterator();
                while (iterator.hasNext()) {
                    iterator.next();
                    if (iterator.hasNext()) {
                        Edge edge = (Edge) iterator.next();
                        if (isEdgeValidForPort(edge, targetVertex, port)) {
                            return true;
                        }
                    }
                }
            }
            return false;
        } catch (ClassCastException e) {
            logger.error("Invalid path element type during validation");
            throw new InvalidInputException("Invalid path element type during validation", e);
        }
    }

    /**
     * Checks if an edge represents a valid connection to the target vertex through
     * a specific port.
     *
     * <p>
     * This method determines if the edge connects to the target vertex through a
     * port
     * that is different from the specified port.
     * </p>
     *
     * @param edge         the edge to evaluate, must not be null
     * @param targetVertex the target vertex to connect to, must not be null
     * @param port         the port specification for the connection, must not be
     *                     null
     * @return true if the edge is valid for the port connection, false otherwise
     * @throws NullPointerException  if any parameter is null
     * @throws InvalidInputException if target vertex element name is null
     * @throws DataException         if edge is missing required properties
     */
    private boolean isEdgeValidForPort(Edge edge, Vertex targetVertex, String port) {
        Objects.requireNonNull(edge, TopologyGraphConstants.EDGE_NOT_BE_NULL);
        Objects.requireNonNull(targetVertex, TopologyGraphConstants.TARGET_VERTEX_NOT_BE_NULL);
        Objects.requireNonNull(port, "Port cannot be null when validating edge");

        try {
            String targetElementName = targetVertex.value(TopologyGraphConstants.ELEMENT_NAME).toString();
            if (targetElementName == null) {
                throw new InvalidInputException("Target vertex element name cannot be null");
            }

            return (edge.values(TopologyGraphConstants.SOURCE_ELEMENT_NAME)
                    .equals(targetVertex.value(TopologyGraphConstants.ELEMENT_NAME))
                    && !port.equalsIgnoreCase(edge.values(TopologyGraphConstants.SOURCE_PORT).toString()))
                    || (edge.values(TopologyGraphConstants.DESTINATION_ELEMENT_NAME)
                    .equals(targetVertex.value(TopologyGraphConstants.ELEMENT_NAME))
                    && !port.equalsIgnoreCase(edge.values(TopologyGraphConstants.DESTINATION_PORT).toString()));
        } catch (NullPointerException e) {
            logger.error("Edge missing required properties for port validation");
            throw new DataException("Edge missing required properties for port validation", e);
        }
    }

    /**
     * Checks if vertices in a set have connectivity at upper levels of the topology
     * hierarchy.
     *
     * <p>
     * This method determines if any vertex in the provided set has connectivity
     * to vertices at higher levels of the topology hierarchy.
     * </p>
     *
     * @param verticesSet  the set of vertices to check for upper level
     *                     connectivity, must not be null
     * @param targetVertex the target vertex for connectivity, must not be null
     * @return true if upper level connectivity exists, false otherwise
     * @throws NullPointerException      if verticesSet or targetVertex is null
     * @throws ResourceNotFoundException if required element types are not found
     * @throws ConnectivityException     if null elements are encountered
     */
    private boolean checkUpperLevelConnectivity(Set<Vertex> verticesSet, Vertex targetVertex) {
        Objects.requireNonNull(verticesSet, TopologyGraphConstants.VERTICES_SET_NOT_BE_NULL);
        Objects.requireNonNull(targetVertex, TopologyGraphConstants.TARGET_VERTEX_NOT_BE_NULL);

        try {
            Set<Vertex> upperVertex = new HashSet<>();
            for (Vertex ver : verticesSet) {
                List<String> eTypeInEdge = topologyGraphUtils.getRequiredTopologyLevelType(
                        ver.value(TopologyGraphConstants.ELEMENT_TYPE), TopologyGraphConstants.UPWORD);
                eTypeInEdge.add(ver.value(TopologyGraphConstants.ELEMENT_TYPE));

                List<String> eTypeForUpToEdge = topologyGraphUtils.getRequiredTopologyLevelType(
                        ver.value(TopologyGraphConstants.ELEMENT_TYPE), TopologyGraphConstants.UPWORD);
                upperVertex.addAll(getUpperLevelVertices(ver, eTypeInEdge, eTypeForUpToEdge));
            }
            return CollectionUtils.isNotEmpty(upperVertex);
        } catch (NoSuchElementException e) {
            logger.error("Required element type not found when checking upper level connectivity");
            throw new ResourceNotFoundException(
                    "Required element type not found when checking upper level connectivity", e);
        } catch (NullPointerException e) {
            logger.error("Null element encountered during upper level connectivity check");
            throw new ConnectivityException("Null element encountered during upper level connectivity check", e);
        }
    }

    /**
     * Retrieves vertices at upper levels of the topology hierarchy connected to the
     * specified vertex.
     *
     * <p>
     * This method traverses the graph to find vertices at higher levels in the
     * topology
     * hierarchy that are connected to the specified vertex. The traversal is guided
     * by
     * the provided element type lists.
     * </p>
     *
     * @param ver              the starting vertex for traversal, must not be null
     * @param eTypeInEdge      the list of element types to include in the traversal
     *                         path, must not be null
     * @param eTypeForUpToEdge the list of element types that mark the end of
     *                         traversal, must not be null
     * @return a set of vertices at upper levels connected to the specified vertex
     * @throws NullPointerException    if any parameter is null
     * @throws InvalidInputException   if vertex element name is null
     * @throws VertexCreationException if graph traversal fails or vertex is missing
     *                                 required properties
     */
    private Set<Vertex> getUpperLevelVertices(Vertex ver, List<String> eTypeInEdge, List<String> eTypeForUpToEdge) {
        Objects.requireNonNull(ver, TopologyGraphConstants.VERTEX_NOT_BE_NULL);
        Objects.requireNonNull(eTypeInEdge, "Element types in edge list cannot be null");
        Objects.requireNonNull(eTypeForUpToEdge, "Element types for up to edge list cannot be null");

        if (eTypeInEdge.isEmpty() || eTypeForUpToEdge.isEmpty()) {
            return Collections.emptySet();
        }

        try {
            String elementName = ver.value(TopologyGraphConstants.ELEMENT_NAME).toString();
            if (elementName == null) {
                throw new InvalidInputException("Vertex element name cannot be null");
            }

            return JanusGraphInstanceManagerServiceImpl.getGraphByTraversalByInstance(JanusGraphInstance.TOPOLOGY_GRAPH)
                    .V()
                    .has(TopologyGraphConstants.ELEMENT_NAME, ver.value(TopologyGraphConstants.ELEMENT_NAME).toString())
                    .repeat(bothE().otherV()
                            .has(TopologyGraphConstants.ELEMENT_TYPE, P.within(eTypeInEdge))
                            .not(has(TopologyGraphConstants.ELEMENT_NAME,
                                    ver.value(TopologyGraphConstants.ELEMENT_NAME).toString()))
                            .simplePath())
                    .until(has(TopologyGraphConstants.ELEMENT_TYPE, P.within(eTypeForUpToEdge)))
                    .toSet();
        } catch (IllegalStateException e) {
            logger.error("Graph traversal error when finding upper level vertices");
            throw new VertexCreationException("Graph traversal error when finding upper level vertices", e);
        } catch (NullPointerException e) {
            logger.error("Vertex missing required properties for upper level traversal");
            throw new VertexCreationException("Vertex missing required properties for upper level traversal", e);
        }
    }

    /**
     * Finds vertices at the target level of the topology hierarchy.
     *
     * <p>
     * This method traverses the graph to find vertices at the target level of the
     * topology
     * hierarchy that are connected to the search vertex. The traversal is guided by
     * the provided element type lists and considers the hierarchy levels of
     * different device types.
     * </p>
     *
     * @param eTypeInEdge       the list of element types to include in the
     *                          traversal path, must not be null
     * @param searchVertex      the starting vertex for traversal, must not be null
     * @param verticesSet       the set to collect found vertices, must not be null
     * @param targetVertexEType the list of target element types to search for, must
     *                          not be null
     * @throws NullPointerException    if any parameter is null
     * @throws InvalidInputException   if last element type in target vertex types
     *                                 is null
     * @throws IllegalStateException   if device levels map is null
     * @throws VertexNotFoundException if vertex element is not found during
     *                                 traversal
     * @throws DataProcessingException if null element is encountered during
     *                                 traversal
     * @throws GraphicalException      if graph traversal error occurs
     */
    private void getTargetLevelVertices(List<String> eTypeInEdge, Vertex searchVertex, Set<Vertex> verticesSet,
                                        List<String> targetVertexEType) {
        Objects.requireNonNull(eTypeInEdge, "Element types in edge list cannot be null");
        Objects.requireNonNull(searchVertex, "Search vertex cannot be null");
        Objects.requireNonNull(verticesSet, TopologyGraphConstants.VERTICES_SET_NOT_BE_NULL);
        Objects.requireNonNull(targetVertexEType, "Target vertex element types cannot be null");

        try {
            Map<String, Integer> deviceLevels = topologyGraphUtils.getDeviceLevels();
            if (deviceLevels == null) {
                throw new IllegalStateException("Device levels map is null");
            }

            String targetLastType = getLastElement(targetVertexEType);
            if (targetLastType == null) {
                throw new InvalidInputException("Last element type in target vertex types cannot be null");
            }

            for (String eType : eTypeInEdge) {
                List<String> terminationEType = getTerminationTypes(deviceLevels, targetVertexEType, targetLastType,
                        eType);
                processConnectedVertices(searchVertex, eType, terminationEType, targetVertexEType, verticesSet);
            }
        } catch (NoSuchElementException e) {
            logger.error("Vertex element not found during target level traversal");
            throw new VertexNotFoundException("Vertex element not found during target level traversal", e);
        } catch (NullPointerException e) {
            logger.error("Null element encountered during target level traversal");
            throw new DataProcessingException("Null element encountered during target level traversal", e);
        } catch (IllegalStateException e) {
            logger.error("Graph traversal error when finding target level vertices");
            throw new GraphicalException("Graph traversal error when finding target level vertices", e);
        }
    }

    private String getLastElement(List<String> list) {
        return list.isEmpty() ? null : list.get(list.size() - 1);
    }

    private List<String> getTerminationTypes(Map<String, Integer> deviceLevels, List<String> targetVertexEType,
                                             String targetLastType, String eType) {
        if (deviceLevels.get(targetLastType) >= deviceLevels.get(eType)) {
            return targetVertexEType;
        } else {
            return topologyGraphUtils.getRequiredTopologyLevelType(eType, TopologyGraphConstants.UPWORD);
        }
    }

    private void processConnectedVertices(Vertex searchVertex, String eType, List<String> terminationEType,
                                          List<String> targetVertexEType, Set<Vertex> verticesSet) {
        Set<Vertex> connectedVerticesSet = JanusGraphInstanceManagerServiceImpl
                .getGraphByTraversalByInstance(JanusGraphInstance.TOPOLOGY_GRAPH).V()
                .has(TopologyGraphConstants.ELEMENT_NAME,
                        searchVertex.value(TopologyGraphConstants.ELEMENT_NAME).toString())
                .repeat(bothE().otherV().has(TopologyGraphConstants.ELEMENT_TYPE, eType).simplePath())
                .until(has(TopologyGraphConstants.ELEMENT_TYPE, P.within(terminationEType)))
                .toSet();

        for (Vertex vertex : connectedVerticesSet) {
            if (targetVertexEType.contains(vertex.value(TopologyGraphConstants.ELEMENT_TYPE))) {
                verticesSet.add(vertex);
            } else {
                List<String> eTypeInEdgeList = topologyGraphUtils.getRequiredTopologyLevelType(
                        vertex.value(TopologyGraphConstants.ELEMENT_TYPE), TopologyGraphConstants.UPWORD);
                eTypeInEdgeList.add(vertex.value(TopologyGraphConstants.ELEMENT_TYPE));
                getTargetLevelVertices(eTypeInEdgeList, vertex, verticesSet, targetVertexEType);
            }
        }
    }

    /**
     * Retrieves IP links between specified source and destination elements.
     *
     * <p>
     * This method searches for edges connecting the specified source and
     * destination
     * elements in the topology graph. It handles bidirectional links by checking
     * both
     * source-to-destination and destination-to-source connections.
     * </p>
     *
     * @param source      the source element name, must not be null or empty
     * @param destination the destination element name, must not be null or empty
     * @return a list of maps containing the properties of found IP links
     * @throws NullPointerException     if source or destination is null
     * @throws IllegalArgumentException if source or destination is empty
     */
    @Override
    public List<Map<String, Object>> getIPLinkBySourceAndDestination(String source, String destination) {
        validateSourceAndDestination(source, destination);

        List<Map<String, Object>> resultList = new ArrayList<>();
        try {
            List<Edge> edges = getEdgesBetweenSourceAndDestination(source, destination);
            logger.info("Found {} IP links between source: {} and destination: {}", edges.size(), source, destination);

            if (!edges.isEmpty()) {
                List<Map<String, Object>> edgeData = iTopologyHbaseDao.getEdgeData(edges);
                if (edgeData != null && !edgeData.isEmpty()) {
                    for (Map<String, Object> propertyMap : edgeData) {
                        if (propertyMap != null) {
                            enrichPropertyMap(propertyMap);
                            swapDirectionIfNeeded(propertyMap);
                        }
                        resultList.add(propertyMap);
                    }
                }
            }
            logger.info("getIPLinkBySourceAndDestination final result list size:{} ", resultList.size());
        } catch (Exception e) {
            logger.error("Failed to retrieve IP links between source: {} and destination: {}: {}",
                    source, destination, e.getMessage(), e);
        }
        return resultList;
    }

    private void validateSourceAndDestination(String source, String destination) {
        Objects.requireNonNull(source, "Source element name cannot be null");
        Objects.requireNonNull(destination, "Destination element name cannot be null");

        if (source.trim().isEmpty()) {
            throw new IllegalArgumentException("Source element name cannot be empty");
        }
        if (destination.trim().isEmpty()) {
            throw new IllegalArgumentException("Destination element name cannot be empty");
        }
    }

    private List<Edge> getEdgesBetweenSourceAndDestination(String source, String destination) {
        List<Edge> edges = JanusGraphInstanceManagerServiceImpl
                .getGraphByTraversalByInstance(JanusGraphInstance.TOPOLOGY_GRAPH).E()
                .has(TopologyGraphConstants.SOURCE_ELEMENT_NAME, source)
                .has(TopologyGraphConstants.DESTINATION_ELEMENT_NAME, destination)
                .has(TopologyGraphConstants.DELETED, false)
                .toList();

        if (edges == null || edges.isEmpty()) {
            logger.info("Link not found for source: {}, destination: {} - swapping source and destination", source,
                    destination);
            edges = JanusGraphInstanceManagerServiceImpl
                    .getGraphByTraversalByInstance(JanusGraphInstance.TOPOLOGY_GRAPH).E()
                    .has(TopologyGraphConstants.SOURCE_ELEMENT_NAME, destination)
                    .has(TopologyGraphConstants.DESTINATION_ELEMENT_NAME, source)
                    .has(TopologyGraphConstants.DELETED, false)
                    .toList();

            if (edges == null) {
                edges = new ArrayList<>();
            }
        }
        return edges;
    }

    private void enrichPropertyMap(Map<String, Object> propertyMap) {
        ElementInfoWrapper e = getElementInfoWrapperByEName(
                propertyMap.get(TopologyGraphConstants.SOURCE_ELEMENT_NAME).toString());
        propertyMap.put(TopologyGraphConstants.DOMAIN, e.getEquipDomain());
        propertyMap.put(TopologyGraphConstants.VENDOR, e.getEquipVendor());
    }

    private void swapDirectionIfNeeded(Map<String, Object> propertyMap) {
        final String DIRECTION = "direction";

        Object directionObj = propertyMap.get(DIRECTION);
        if (directionObj != null) {
            String direction = directionObj.toString();
            if ("FORWARD_TRUE".equals(direction) || "BACKWARD_TRUE".equals(direction)) {
                Object srcName = propertyMap.get("srcEName");
                Object destName = propertyMap.get("destEName");
                Object srcPort = propertyMap.get("srcPort");
                Object destPort = propertyMap.get("destPort");

                propertyMap.put("srcEName", destName);
                propertyMap.put("destEName", srcName);
                propertyMap.put("srcPort", destPort);
                propertyMap.put("destPort", srcPort);
            }
        }
    }

    /**
     * Executes a Janus Graph query and returns the results in a formatted output.
     *
     * <p>
     * This method supports two types of queries:
     * </p>
     * <ul>
     * <li>GET queries - Used to retrieve data from the graph</li>
     * <li>UPDATE queries - Used to modify data in the graph</li>
     * </ul>
     *
     * <p>
     * Example usage:
     * </p>
     *
     * <pre>
     * String getQuery = "g.V().hasLabel('device').count()";
     * String result = getJanusQueryOutput(getQuery, "GET");
     * </pre>
     *
     * @param query     the Gremlin query to execute, must not be null or empty
     * @param queryType the type of query to execute (GET or UPDATE), must not be
     *                  null or empty
     * @return a JSON string containing the query results or an error message
     * @throws NullPointerException if query or queryType is null
     * @throws GraphicalException   if there is an error executing the query
     */
    @Override
    public String getJanusQueryOutput(String query, String queryType) {
        Objects.requireNonNull(query, "Query cannot be null");
        Objects.requireNonNull(queryType, "Query type cannot be null");

        if (query.trim().isEmpty()) {
            logger.warn("Empty query provided");
            return TopologyGraphConstants.JANUS_QUERY_WRONG_INPUT;
        }

        if (queryType.trim().isEmpty()) {
            logger.warn("Empty query type provided");
            return TopologyGraphConstants.JANUS_QUERY_WRONG_INPUT;
        }

        String json = TopologyGraphConstants.JANUS_QUERY_FAILURE_JSON;
        try {
            if (queryType.equalsIgnoreCase(TopologyGraphConstants.JANUS_QUERY_TYPE_GET)) {
                logger.info("Executing GET query: {}", query);
                json = TopologyGremlinScriptUtils.runGremlinScriptsOutput(query);
                if (json != null) {
                    return json;
                } else {
                    logger.warn("GET query execution returned null result");
                    return TopologyGraphConstants.JANUS_QUERY_FAILURE_JSON;
                }
            } else if (queryType.equalsIgnoreCase(TopologyGraphConstants.JANUS_QUERY_TYPE_UPDATE)) {
                logger.info("Executing UPDATE query: {}", query);
                json = TopologyGremlinScriptUtils.runGremlinUpdateQuery(query);
                if (json != null) {
                    return json;
                } else {
                    logger.warn("UPDATE query execution returned null result");
                    return TopologyGraphConstants.JANUS_QUERY_FAILURE_JSON;
                }
            } else {
                logger.warn("Invalid query type: {}", queryType);
                return TopologyGraphConstants.JANUS_QUERY_WRONG_INPUT;
            }
        } catch (GraphicalException e) {
            logger.error("Script execution error in getJanusQueryOutput: {}", e.getMessage(), e);
            logger.error("Script execution error in getJanusQueryOutput: {}", e.getMessage());
            return TopologyGraphConstants.JANUS_QUERY_FAILURE_JSON;
        } catch (Exception e) {
            logger.error("Exception in getJanusQueryOutput : {}", e.getMessage());
            return TopologyGraphConstants.JANUS_QUERY_FAILURE_JSON;
        }
    }

    /**
     * Retrieves topology information based on the specified ring ID.
     *
     * <p>
     * This method queries the topology graph database to find all elements
     * that are part of the specified ring.
     * </p>
     *
     * @param ringID the unique identifier of the ring to retrieve, must not be null
     *               or empty
     * @return a list of maps containing topology information for the specified ring
     * @throws NullPointerException     if ringID is null
     * @throws IllegalArgumentException if ringID is empty
     */
    @Override
    public List<Map<String, Object>> getTopologyByRingId(String ringID) {
        Objects.requireNonNull(ringID, "Ring ID cannot be null");

        if (ringID.trim().isEmpty()) {
            throw new IllegalArgumentException("Ring ID cannot be empty");
        }

        return iTopologyGraphDao.getTopologyByRingId(ringID);
    }

    /**
     * Creates an ElementInfoWrapper object from a vertex in the graph.
     *
     * <p>
     * This method extracts all relevant properties from the vertex and
     * populates an ElementInfoWrapper object with those values.
     * </p>
     *
     * @param propertyMapByVertex the vertex to extract properties from, must not be
     *                            null
     * @return an ElementInfoWrapper containing the vertex properties
     * @throws NullPointerException    if propertyMapByVertex is null
     * @throws NumberFormatException   if numeric properties cannot be parsed
     * @throws VertexCreationException if there is an error processing vertex
     *                                 properties
     * @throws DataProcessingException if there is an error parsing numeric values
     */
    public ElementInfoWrapper getElementInfoWrapperByVertex(Vertex vertex) {
        ElementInfoWrapper elementInfo = new ElementInfoWrapper();
        try {
            logger.info("vertex getElementInfoWrapperByVertex: {}", vertex);

            elementInfo.setElementName(getPropertyValueAsString(vertex, ENAME));
            elementInfo.setNeId(getPropertyValueAsString(vertex, EID));
            elementInfo.setEquipVendor(getPropertyValueAsString(vertex, VENDOR));
            elementInfo.setEquipDomain(getPropertyValueAsString(vertex, DOMAIN));
            elementInfo.setElementType(getPropertyValueAsString(vertex, TopologyGraphConstants.ALIASE_ELEMENT_TYPE));
            elementInfo.setElementAliaseType(getPropertyValueAsString(vertex, TopologyGraphConstants.ELEMENT_TYPE));
            elementInfo.setMwType(getPropertyValueAsString(vertex, TopologyGraphConstants.MICROWAVE_TYPE));
            elementInfo.setGeographyL1(getPropertyValueAsString(vertex, TopologyGraphConstants.GEOGRAPHY_LEVEL1));
            elementInfo.setGeographyL2(getPropertyValueAsString(vertex, TopologyGraphConstants.GEOGRAPHY_LEVEL2));
            elementInfo.setGeographyL3(getPropertyValueAsString(vertex, TopologyGraphConstants.GEOGRAPHY_LEVEL3));
            elementInfo.setIpv4(getPropertyValueAsString(vertex, TopologyGraphConstants.IPV4));
            elementInfo.setIpv6(getPropertyValueAsString(vertex, TopologyGraphConstants.IPV6));
            elementInfo.setSiteId(getPropertyValueAsString(vertex, TopologyGraphConstants.SITEID));
            elementInfo.setFibertakeSiteId(getPropertyValueAsString(vertex, TopologyGraphConstants.FIBERTAKE_SITEID));
            elementInfo.setIsENBPresent(getPropertyValueAsString(vertex, TopologyGraphConstants.IS_ENB_PRESENT));
            elementInfo.setGcName(getPropertyValueAsString(vertex, TopologyGraphConstants.GC_NAME));
            elementInfo.setGeographyL1DName(getPropertyValueAsString(vertex, TopologyGraphConstants.GEOGRAPHYL1_DNAME));
            elementInfo.setGeographyL2DName(getPropertyValueAsString(vertex, TopologyGraphConstants.GEOGRAPHYL2_DNAME));
            elementInfo.setGeographyL3DName(getPropertyValueAsString(vertex, TopologyGraphConstants.GEOGRAPHYL3_DNAME));
            elementInfo.setRingId(getPropertyValueAsString(vertex, TopologyGraphConstants.RING_ID));
            elementInfo.setNodePriority(getPropertyValueAsString(vertex, TopologyGraphConstants.NODE_PRIORITY));

            elementInfo.setLat(getPropertyValueAsDouble(vertex, TopologyGraphConstants.LATITUDE));
            elementInfo.setLng(getPropertyValueAsDouble(vertex, TopologyGraphConstants.LONGITUDE));

        } catch (Exception e) {
            logger.error("Exception occurred while running: {}", e.getMessage(), e);
        }

        // Handle Enabled Origin NW separately
        if (vertex.property(TopologyGraphConstants.ENABLED_ORIGIN_NW).isPresent()) {
            List<String> origin = new ArrayList<>();
            origin.add(vertex.property(TopologyGraphConstants.ENABLED_ORIGIN_NW).value().toString());
            elementInfo.setEnabledOriginNW(origin);
        }

        // Override ElementId with vertex id as string
        elementInfo.setElementId(vertex.id().toString());

        return elementInfo;
    }

    private String getPropertyValueAsString(Vertex vertex, String propertyKey) {
        return vertex.property(propertyKey).isPresent()
                ? vertex.property(propertyKey).value().toString()
                : null;
    }

    private Double getPropertyValueAsDouble(Vertex vertex, String propertyKey) {
        try {
            if (vertex.property(propertyKey).isPresent()) {
                return Double.parseDouble(vertex.property(propertyKey).value().toString());
            }
        } catch (NumberFormatException e) {
            logger.warn("Failed to parse property {} to Double: {}", propertyKey, e.getMessage());
        }
        return null;
    }

    /**
     * Extracts specified properties from a vertex and returns them as a map.
     *
     * <p>
     * This method filters vertex properties based on the provided list of fields
     * and adds them to a map. For latitude and longitude properties, numeric
     * decoding
     * is applied.
     * </p>
     *
     * @param vertex the vertex to extract properties from, must not be null
     * @param fields the list of property keys to extract, must not be null
     * @return a map containing the requested vertex properties
     * @throws NullPointerException if vertex or fields is null
     * @throws DataException        if there is an error retrieving properties
     */
    public Map<String, Object> getPropertyMapByVertex(Vertex vertex, List<String> fields) {
        Objects.requireNonNull(vertex, TopologyGraphConstants.VERTEX_NOT_BE_NULL);
        Objects.requireNonNull(fields, "Fields cannot be null");

        Map<String, Object> propertyMap = new HashMap<>();
        try {
            Iterator<VertexProperty<Object>> properties = vertex.properties();
            if (properties != null) {
                setVertexPropertyMap(fields, propertyMap, properties);
                propertyMap.put(TopologyGraphConstants.VERTEX_ID, vertex.id());
            }
        } catch (Exception e) {
            logger.error("Exception in getPropertyMapByVertexAndProperties");
            throw new DataException("Failed to retrieve property map from vertex", e);
        }
        return propertyMap;
    }

    /**
     * Populates a property map with selected vertex properties.
     *
     * <p>
     * This helper method filters vertex properties based on the provided list of
     * fields
     * and adds them to the property map. Special handling is applied for latitude
     * and
     * longitude properties to ensure proper numeric decoding.
     * </p>
     *
     * @param fields      the list of property keys to include, must not be null
     * @param propertyMap the map to populate with properties, must not be null
     * @param properties  the iterator of vertex properties, must not be null
     * @throws NullPointerException if any parameter is null
     */
    private void setVertexPropertyMap(List<String> fields, Map<String, Object> propertyMap,
                                      Iterator<VertexProperty<Object>> properties) {
        Objects.requireNonNull(fields, TopologyGraphConstants.FIELDS_LIST_NOT_BE_NULL);
        Objects.requireNonNull(propertyMap, TopologyGraphConstants.PROPERTY_MAP_NOT_BE_NULL);
        Objects.requireNonNull(properties, "Properties iterator cannot be null");

        while (properties.hasNext()) {
            Property<Object> property = properties.next();
            if (fields.contains(property.key())) {
                if (property.key().equalsIgnoreCase(TopologyGraphConstants.LATITUDE)
                        || property.key().equalsIgnoreCase(TopologyGraphConstants.LONGITUDE)) {
                    propertyMap.put(property.key(), topologyGraphUtils.decodeNumeric(property.value().toString()));
                } else {
                    propertyMap.put(property.key(), property.value());
                }
            }
        }
    }

    /**
     * Gets the graph data from the provided connections and builds a structured
     * representation.
     *
     * <p>
     * This method processes a list of graph connections and organizes them into a
     * map
     * where keys are element names and values are lists of connected elements. The
     * method
     * handles source and destination vertices, their connecting edges, and enriches
     * the
     * element information with additional metadata.
     * </p>
     *
     * @param graphData      the list of connection maps containing vertices and
     *                       edges, must not be null
     * @param totalVertexMap the map of vertex IDs to their ElementInfoWrapper
     *                       objects, must not be null
     * @return a map of element names to lists of connected ElementInfoWrapper
     *         objects
     * @throws NullPointerException    if any parameter is null
     * @throws DataException           if there is an error processing the graph
     *                                 data
     * @throws VertexNotFoundException if a required vertex is not found
     * @throws ConnectivityException   if a required edge is not found
     * @throws GraphicalException      if an unexpected error occurs during
     *                                 processing
     */
    private Object2ObjectMap<String, List<ElementInfoWrapper>> getGraphData(
            List<Map<String, Object>> graphData,
            Map<String, ElementInfoWrapper> totalVertexMap) {

        Objects.requireNonNull(graphData, "Graph data cannot be null");
        Objects.requireNonNull(totalVertexMap, TopologyGraphConstants.TOTAL_VERTEX_MAP_NOT_BE_NULL);

        Object2ObjectMap<String, List<ElementInfoWrapper>> finalGraphData = new Object2ObjectOpenHashMap<>();

        try {
            logger.info("Processing graph data with {} connections", graphData.size());
            for (Map<String, Object> connection : graphData) {
                if (connection == null) {
                    throw new DataException("Null connection found in graph data");
                }

                Vertex source = (Vertex) connection.get(TopologyGraphServiceImpl.VERTEX);
                if (source == null) {
                    throw new VertexNotFoundException("Source vertex not found in connection data");
                }

                Edge connectedInterface = (Edge) connection.get(TopologyGraphServiceImpl.CONNECTED);
                if (connectedInterface == null) {
                    throw new ConnectivityException("Connected edge not found for vertex: " + source.id());
                }

                ElementInfoWrapper sourceElementInfo = getSourceElementInfo(source, totalVertexMap);
                ElementInfoWrapper interfaceElement = getElementInfoWrapperByEdge(connectedInterface, sourceElementInfo,
                        totalVertexMap);

                updateFinalGraphData(finalGraphData, sourceElementInfo, interfaceElement);
            }

            enrichElementInfo(finalGraphData, totalVertexMap);
            logger.info("Completed processing graph data with {} elements", finalGraphData.size());
        } catch (ClassCastException e) {
            logger.error("Invalid type in graph data");
            throw new DataProcessingException("Failed to process graph data due to invalid type", e);
        } catch (VertexNotFoundException | ConnectivityException | DataException e) {
            logger.error("Error processing graph data");
            throw e;
        } catch (Exception e) {
            logger.error("Unexpected error processing graph data");
            throw new GraphicalException("Failed to process graph data", e);
        }

        return finalGraphData;
    }

    /**
     * Retrieves or creates an ElementInfoWrapper for a source vertex.
     *
     * <p>
     * This method looks up the source vertex in the provided map. If not found,
     * it creates a new ElementInfoWrapper and adds it to the map.
     * </p>
     *
     * @param source         the source vertex to process, must not be null
     * @param totalVertexMap the map of vertex IDs to ElementInfoWrapper objects,
     *                       must not be null
     * @return the ElementInfoWrapper for the source vertex
     * @throws NullPointerException    if any parameter is null
     * @throws VertexCreationException if unable to create a valid
     *                                 ElementInfoWrapper
     */
    private ElementInfoWrapper getSourceElementInfo(Vertex source, Map<String, ElementInfoWrapper> totalVertexMap) {
        Objects.requireNonNull(source, "Source vertex cannot be null");
        Objects.requireNonNull(totalVertexMap, TopologyGraphConstants.TOTAL_VERTEX_MAP_NOT_BE_NULL);

        String sourceId = source.id().toString();
        ElementInfoWrapper sourceElementInfo = totalVertexMap.get(sourceId);

        if (sourceElementInfo == null) {
            sourceElementInfo = getElementInfoWrapperByVertex(source);
            if (sourceElementInfo.getElementId() != null) {
                totalVertexMap.put(sourceElementInfo.getElementId(), sourceElementInfo);
            } else {
                throw new VertexCreationException("Failed to get element ID for source vertex: " + sourceId);
            }
        }
        return sourceElementInfo;
    }

    /**
     * Updates the final graph data structure with source and interface elements.
     *
     * <p>
     * This method adds or updates the connection between a source element and an
     * interface
     * element in the final graph data structure. If the connection already exists,
     * it merges
     * the ports and edges information.
     * </p>
     *
     * @param finalGraphData    the map to update with element connections, must not
     *                          be null
     * @param sourceElementInfo the source element information, must not be null
     * @param interfaceElement  the interface element information to add or merge
     * @throws NullPointerException if finalGraphData or sourceElementInfo is null
     */
    private void updateFinalGraphData(Object2ObjectMap<String, List<ElementInfoWrapper>> finalGraphData,
                                      ElementInfoWrapper sourceElementInfo,
                                      ElementInfoWrapper interfaceElement) {
        Objects.requireNonNull(finalGraphData, "Final graph data map cannot be null");
        Objects.requireNonNull(sourceElementInfo, TopologyGraphConstants.SOURCE_ELEMENT_INFO_NOT_BE_NULL);

        if (interfaceElement == null || sourceElementInfo.getNeId() == null) {
            return;
        }

        String neId = sourceElementInfo.getNeId();
        String ipPart = neId.split("_")[0].trim();

        List<ElementInfoWrapper> oldConnections = finalGraphData.get(ipPart);

        if (oldConnections == null) {
            oldConnections = new ArrayList<>();
            finalGraphData.put(ipPart, oldConnections);
        }

        boolean isPresent = oldConnections.stream()
                .anyMatch(con -> con != null && con.getElementName() != null &&
                        con.getElementName().equalsIgnoreCase(interfaceElement.getElementName()));

        if (isPresent) {
            mergePortsAndEdges(oldConnections, interfaceElement);
        } else {
            oldConnections.add(interfaceElement);
        }
    }

    /**
     * Merges ports and edges from an interface element into existing connections.
     *
     * <p>
     * This method finds a matching element in the connections list and merges
     * the ports and edges from the interface element into it.
     * </p>
     *
     * @param oldConnections   the list of existing connections to update, must not
     *                         be null
     * @param interfaceElement the interface element containing ports and edges to
     *                         merge, must not be null
     * @throws NullPointerException if any parameter is null
     */
    private void mergePortsAndEdges(List<ElementInfoWrapper> oldConnections, ElementInfoWrapper interfaceElement) {
        Objects.requireNonNull(oldConnections, "Old connections list cannot be null");
        Objects.requireNonNull(interfaceElement, "Interface element cannot be null");

        for (ElementInfoWrapper con : oldConnections) {
            if (con != null && con.getElementName() != null &&
                    con.getElementName().equalsIgnoreCase(interfaceElement.getElementName())) {
                if (con.getPorts() != null && !con.getPorts().isEmpty()) {
                    con.getPorts().addAll(interfaceElement.getPorts());
                }
                if (con.getEdges() != null && !con.getEdges().isEmpty()) {
                    con.getEdges().addAll(interfaceElement.getEdges());
                }
                break;
            }
        }
    }

    /**
     * Enriches element information with additional metadata from the vertex map.
     *
     * <p>
     * This method adds domain, vendor, type, and alias type information to each
     * element in the final graph data structure by looking up the corresponding
     * element in the total vertex map.
     * </p>
     *
     * @param finalGraphData the graph data structure to enrich, must not be null
     * @param totalVertexMap the map containing complete element information, must
     *                       not be null
     * @throws NullPointerException    if any parameter is null
     * @throws DataProcessingException if an error occurs during enrichment
     */
    private void enrichElementInfo(Object2ObjectMap<String, List<ElementInfoWrapper>> finalGraphData,
                                   Map<String, ElementInfoWrapper> totalVertexMap) {
        Objects.requireNonNull(finalGraphData, "Final graph data cannot be null");
        Objects.requireNonNull(totalVertexMap, TopologyGraphConstants.TOTAL_VERTEX_MAP_NOT_BE_NULL);

        try {
            for (List<ElementInfoWrapper> value : finalGraphData.values()) {
                for (ElementInfoWrapper element : value) {
                    if (element != null && element.getElementName() != null
                            && totalVertexMap.containsKey(element.getElementName())) {
                        ElementInfoWrapper extraData = totalVertexMap.get(element.getElementName());
                        element.setEquipDomain(extraData.getEquipDomain());
                        element.setEquipVendor(extraData.getEquipVendor());
                        element.setElementType(extraData.getElementType());
                        element.setElementAliaseType(extraData.getElementAliaseType());
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error enriching element info");
            throw new DataProcessingException("Failed to enrich element information", e);
        }
    }

    /**
     * Creates an ElementInfoWrapper from an edge and associated element
     * information.
     *
     * <p>
     * This method extracts information from the edge to create an
     * ElementInfoWrapper
     * representing either the source or destination element based on the provided
     * source element info. It also builds port and edge information.
     * </p>
     *
     * @param edge              the edge containing connection information, must not
     *                          be null
     * @param sourceElementInfo the source element information, must not be null
     * @param totalVertexMap    the map of all vertices in the graph, must not be
     *                          null
     * @return an ElementInfoWrapper representing the connected element, or null if
     *         invalid
     * @throws NullPointerException      if any parameter is null
     * @throws ResourceNotFoundException if required edge properties are not found
     * @throws DataException             if there is an error processing the edge
     *                                   data
     */
    private ElementInfoWrapper getElementInfoWrapperByEdge(Edge edge, ElementInfoWrapper sourceElementInfo,
                                                           Map<String, ElementInfoWrapper> totalVertexMap) {
        Objects.requireNonNull(edge, TopologyGraphConstants.EDGE_NOT_BE_NULL);
        Objects.requireNonNull(sourceElementInfo, TopologyGraphConstants.SOURCE_ELEMENT_INFO_NOT_BE_NULL);
        Objects.requireNonNull(totalVertexMap, TopologyGraphConstants.TOTAL_VERTEX_MAP_NOT_BE_NULL);

        if (isInvalidEdge(edge, totalVertexMap)) {
            return null;
        }

        ElementInfoWrapper elementInfo = new ElementInfoWrapper();

        populateElementDataBasedOnDirection(edge, elementInfo, sourceElementInfo);

        List<Map<String, String>> ports = buildPorts(edge);
        adjustPortsIfNeeded(edge, sourceElementInfo, ports);

        elementInfo.setPorts(ports);
        elementInfo.setEdges(buildEdges(edge));
        elementInfo.setElementId(edge.id().toString());

        setPropertyIfPresent(edge, VENDOR, elementInfo::setEquipVendor);
        setPropertyIfPresent(edge, DOMAIN, elementInfo::setEquipDomain);
        setEndNeIdAsElementName(edge, elementInfo);
        setPropertyIfPresent(edge, NETYPE, elementInfo::setElementType);

        return elementInfo;
    }

    private void populateElementDataBasedOnDirection(Edge edge, ElementInfoWrapper elementInfo,
                                                     ElementInfoWrapper sourceElementInfo) {
        if (isSourceElement(edge, sourceElementInfo)) {
            populateSourceElementData(edge, elementInfo, sourceElementInfo);
        } else {
            populateDestinationElementData(edge, elementInfo, sourceElementInfo);
        }
    }

    private void adjustPortsIfNeeded(Edge edge, ElementInfoWrapper sourceElementInfo,
                                     List<Map<String, String>> ports) {
        if (edge.property("startNeId").isPresent()) {
            String startNeId = edge.property("startNeId").value().toString();
            String sourceNeId = sourceElementInfo.getNeId();
            if (sourceNeId != null && !sourceNeId.equals(startNeId)) {
                for (Map<String, String> port : ports) {
                    String temp = port.get(SOURCE);
                    port.put(SOURCE, port.get(DEST));
                    port.put(DEST, temp);
                }
            }
        }
    }

    private void setPropertyIfPresent(Edge edge, String propertyKey, Consumer<String> setter) {
        if (edge.property(propertyKey).isPresent()) {
            String value = edge.property(propertyKey).value().toString();
            if (value != null && !value.isEmpty()) {
                setter.accept(value);
            }
        }
    }

    private void setEndNeIdAsElementName(Edge edge, ElementInfoWrapper elementInfo) {
        if (edge.property(ENDNEID).isPresent()) {
            String endNeIdStr = edge.property(ENDNEID).value().toString();
            if (endNeIdStr != null && !endNeIdStr.isEmpty()) {
                String ipPart = endNeIdStr.split("_")[0].trim();
                elementInfo.setElementName(ipPart);
            }
        }
    }


    /**
     * Checks if an edge is invalid due to missing element type information.
     *
     * <p>
     * An edge is considered invalid if either the source or destination element
     * has a null element type in the vertex map.
     * </p>
     *
     * @param propertyMap the edge to check, must not be null
     * @param vertexMap   the map of vertices containing element type information,
     *                    must not be null
     * @return true if the edge is invalid, false otherwise
     * @throws NullPointerException      if any parameter is null
     * @throws ResourceNotFoundException if required element properties are not
     *                                   found
     */
    private boolean isInvalidEdge(Edge propertyMap, Map<String, ElementInfoWrapper> vertexMap) {
        Objects.requireNonNull(propertyMap, TopologyGraphConstants.EDGE_NOT_BE_NULL);
        Objects.requireNonNull(vertexMap, TopologyGraphConstants.VERTEX_MAP_NOT_BE_NULL);

        return hasNullType(propertyMap, vertexMap, TopologyGraphConstants.SOURCE_ELEMENT_NAME) ||
                hasNullType(propertyMap, vertexMap, TopologyGraphConstants.DESTINATION_ELEMENT_NAME);
    }

    /**
     * Checks if an element has a null type in the vertex map.
     *
     * <p>
     * This method retrieves the element name from the edge property and checks
     * if the corresponding element in the vertex map has a null element type.
     * </p>
     *
     * @param propertyMap the edge containing the element property, must not be null
     * @param vertexMap   the map of vertices containing element type information,
     *                    must not be null
     * @param elementKey  the key for the element name property, must not be null
     * @return true if the element has a null type, false otherwise
     * @throws NullPointerException      if any parameter is null
     * @throws ResourceNotFoundException if the element property is not found
     */
    private boolean hasNullType(Edge propertyMap, Map<String, ElementInfoWrapper> vertexMap, String elementKey) {
        Objects.requireNonNull(propertyMap, TopologyGraphConstants.EDGE_NOT_BE_NULL);
        Objects.requireNonNull(vertexMap, TopologyGraphConstants.VERTEX_MAP_NOT_BE_NULL);
        Objects.requireNonNull(elementKey, "Element key cannot be null");

        try {
            Object elementName = propertyMap.property(elementKey);
            return elementName != null &&
                    vertexMap.get(elementName.toString()) != null &&
                    vertexMap.get(elementName.toString()).getElementType() == null;
        } catch (NoSuchElementException e) {
            logger.error("Element property not found");
            throw new ResourceNotFoundException("Element property not found: " + elementKey, e);
        }
    }

    /**
     * Determines if the edge's source element matches the provided source element
     * info.
     *
     * <p>
     * This method compares the source element name from the edge property with
     * the element name in the provided source element info.
     * </p>
     *
     * @param propertyMap       the edge containing the source element property,
     *                          must not be null
     * @param sourceElementInfo the source element information to compare against,
     *                          must not be null
     * @return true if the edge's source element matches the provided source element
     *         info, false otherwise
     * @throws NullPointerException      if any parameter is null
     * @throws ResourceNotFoundException if the source element name property is not
     *                                   found
     */
    private boolean isSourceElement(Edge propertyMap, ElementInfoWrapper sourceElementInfo) {
        Objects.requireNonNull(propertyMap, TopologyGraphConstants.EDGE_NOT_BE_NULL);
        Objects.requireNonNull(sourceElementInfo, TopologyGraphConstants.SOURCE_ELEMENT_INFO_NOT_BE_NULL);

        try {
            if (!propertyMap.property(TopologyGraphConstants.SOURCE_ELEMENT_NAME).isPresent()) {
                return false;
            }

            String sourceName = propertyMap.property(TopologyGraphConstants.SOURCE_ELEMENT_NAME).value().toString();
            if (sourceName == null || sourceName.trim().isEmpty()) {
                return false;
            }

            return sourceName.equalsIgnoreCase(sourceElementInfo.getElementName());
        } catch (NoSuchElementException e) {
            logger.error("Source element name property not found in edge");
            throw new ResourceNotFoundException("Source element name property not found", e);
        }
    }

    /**
     * Populates element information for a source element.
     *
     * <p>
     * This method sets the element name and alias names in the element info
     * based on the destination properties in the edge.
     * </p>
     *
     * @param propertyMap       the edge containing the element properties, must not
     *                          be null
     * @param elementInfo       the element info to populate, must not be null
     * @param sourceElementInfo the source element information, must not be null
     * @throws NullPointerException if any parameter is null
     * @throws DataException        if there is an error retrieving properties
     */
    private void populateSourceElementData(Edge propertyMap, ElementInfoWrapper elementInfo,
                                           ElementInfoWrapper sourceElementInfo) {
        Objects.requireNonNull(propertyMap, TopologyGraphConstants.EDGE_NOT_BE_NULL);
        Objects.requireNonNull(elementInfo, "Element info cannot be null");
        Objects.requireNonNull(sourceElementInfo, TopologyGraphConstants.SOURCE_ELEMENT_INFO_NOT_BE_NULL);

        try {
            elementInfo.setElementName(getStringProperty(propertyMap, TopologyGraphConstants.DESTINATION_ELEMENT_NAME));
            elementInfo.setSrcEliaseName(getStringProperty(propertyMap, TopologyGraphConstants.SOURCE_ALIASE_ENAME));
            elementInfo
                    .setDestEliaseName(getStringProperty(propertyMap, TopologyGraphConstants.DESTINATION_ALIASE_ENAME));
        } catch (NoSuchElementException e) {
            logger.error("Failed to populate source element data for edge: {}", propertyMap.id());
            throw new DataException("Failed to populate source element data", e);
        }
    }

    /**
     * Populates element information for a destination element.
     *
     * <p>
     * This method sets the element name and alias names in the element info
     * based on the source properties in the edge.
     * </p>
     *
     * @param propertyMap       the edge containing the element properties, must not
     *                          be null
     * @param elementInfo       the element info to populate, must not be null
     * @param sourceElementInfo the source element information, must not be null
     * @throws NullPointerException if any parameter is null
     * @throws DataException        if there is an error retrieving properties
     */
    private void populateDestinationElementData(Edge propertyMap, ElementInfoWrapper elementInfo,
                                                ElementInfoWrapper sourceElementInfo) {
        Objects.requireNonNull(propertyMap, TopologyGraphConstants.EDGE_NOT_BE_NULL);
        Objects.requireNonNull(elementInfo, "Element info cannot be null");
        Objects.requireNonNull(sourceElementInfo, TopologyGraphConstants.SOURCE_ELEMENT_INFO_NOT_BE_NULL);

        try {
            elementInfo.setElementName(getStringProperty(propertyMap, TopologyGraphConstants.SOURCE_ELEMENT_NAME));
            elementInfo.setDestEliaseName(getStringProperty(propertyMap, TopologyGraphConstants.SOURCE_ALIASE_ENAME));
            elementInfo
                    .setSrcEliaseName(getStringProperty(propertyMap, TopologyGraphConstants.DESTINATION_ALIASE_ENAME));
        } catch (NoSuchElementException e) {
            logger.error("Failed to populate destination element data for edge: {}", propertyMap.id());
            throw new DataException("Failed to populate destination element data", e);
        }
    }

    /**
     * Builds a list of port information maps from an edge.
     *
     * <p>
     * This method extracts source and destination port information from
     * the edge properties and creates a map containing this information.
     * </p>
     *
     * @param propertyMap the edge containing port properties, must not be null
     * @return a list of maps containing port information
     * @throws NullPointerException if propertyMap is null
     * @throws DataException        if there is an error retrieving port properties
     */
    private List<Map<String, String>> buildPorts(Edge propertyMap) {
        Objects.requireNonNull(propertyMap, TopologyGraphConstants.EDGE_NOT_BE_NULL);

        try {
            List<Map<String, String>> ports = new ArrayList<>();
            Map<String, String> portInfo = new HashMap<>();

            addIfPresent(propertyMap, portInfo, SOURCE, TopologyGraphConstants.SOURCE_PORT);
            addIfPresent(propertyMap, portInfo, DEST, TopologyGraphConstants.DESTINATION_PORT);

            if (!portInfo.isEmpty()) {
                ports.add(portInfo);
            }
            return ports;
        } catch (NoSuchElementException e) {
            logger.error("Failed to build ports from edge properties for edge: {}", propertyMap.id());
            throw new DataException("Failed to build ports from edge properties", e);
        }
    }

    /**
     * Builds a list of edge information maps from an edge.
     *
     * <p>
     * This method extracts various edge properties such as technology, VRF,
     * connection type, etc., and creates a map containing this information.
     * </p>
     *
     * @param propertyMap the edge containing edge properties, must not be null
     * @return a list of maps containing edge information
     * @throws NullPointerException if propertyMap is null
     * @throws DataException        if there is an error retrieving edge properties
     */
    private List<Map<String, String>> buildEdges(Edge propertyMap) {
        Objects.requireNonNull(propertyMap, TopologyGraphConstants.EDGE_NOT_BE_NULL);

        try {
            List<Map<String, String>> edges = new ArrayList<>();
            Map<String, String> edgeInfo = new HashMap<>();

            addIfPresent(propertyMap, edgeInfo, TopologyGraphConstants.TECHNOLOGY, TopologyGraphConstants.TECHNOLOGY);
            addIfPresent(propertyMap, edgeInfo, TopologyGraphConstants.VRF, TopologyGraphConstants.VRF);
            addIfPresent(propertyMap, edgeInfo, TopologyGraphConstants.EDGE_ID, TopologyGraphConstants.EDGE_ID);
            addIfPresent(propertyMap, edgeInfo, TopologyGraphConstants.CONNECTION_TYPE,
                    TopologyGraphConstants.CONNECTION_TYPE);
            addIfPresent(propertyMap, edgeInfo, TopologyGraphConstants.LINK_TYPE, TopologyGraphConstants.LINK_TYPE);
            addIfPresent(propertyMap, edgeInfo, TopologyGraphConstants.HOP_TYPE, TopologyGraphConstants.HOP_TYPE);
            addIfPresent(propertyMap, edgeInfo, TopologyGraphConstants.CATEGORY, TopologyGraphConstants.CATEGORY);

            if (!edgeInfo.isEmpty()) {
                edges.add(edgeInfo);
            }
            return edges;
        } catch (NoSuchElementException e) {
            logger.error("Failed to build edges from edge properties for edge: {}", propertyMap.id());
            throw new DataException("Failed to build edges from edge properties", e);
        }
    }

    /**
     * Adds a property from the edge to the target map if the property is present.
     *
     * <p>
     * This method checks if the specified property exists in the edge and,
     * if present, adds it to the target map with the specified key.
     * </p>
     *
     * @param source    the edge containing the property, must not be null
     * @param target    the map to add the property to, must not be null
     * @param targetKey the key to use in the target map, must not be null
     * @param sourceKey the key of the property in the edge, must not be null
     * @throws NullPointerException if any parameter is null
     */
    private void addIfPresent(Edge source, Map<String, String> target, String targetKey, String sourceKey) {
        Objects.requireNonNull(source, "Edge source cannot be null");
        Objects.requireNonNull(target, "Target map cannot be null");
        Objects.requireNonNull(targetKey, "Target key cannot be null");
        Objects.requireNonNull(sourceKey, "Source key cannot be null");

        if (source.property(sourceKey).isPresent()) {
            target.put(targetKey, source.property(sourceKey).value().toString());
        }
    }

    /**
     * Gets a string property from an edge.
     *
     * <p>
     * This method retrieves the specified property from the edge as a string.
     * If the property is not present, it returns null.
     * </p>
     *
     * @param propertyMap the edge containing the property, must not be null
     * @param key         the key of the property to retrieve, must not be null
     * @return the property value as a string, or null if not present
     * @throws NullPointerException      if any parameter is null
     * @throws ResourceNotFoundException if there is an error retrieving the
     *                                   property
     */
    private String getStringProperty(Edge propertyMap, String key) {
        Objects.requireNonNull(propertyMap, TopologyGraphConstants.EDGE_NOT_BE_NULL);
        Objects.requireNonNull(key, "Property key cannot be null");

        try {
            return propertyMap.property(key).isPresent() ? propertyMap.property(key).value().toString() : null;
        } catch (NoSuchElementException e) {
            logger.error("Property not found in edge {}: {}", propertyMap.id(), key);
            throw new ResourceNotFoundException("Property not found: " + key, e);
        }
    }

    /**
     * Retrieves a property map from an edge based on specified fields.
     *
     * <p>
     * This method extracts properties from the edge that match the provided
     * field names and returns them in a map. Special handling is applied for
     * latitude and longitude properties.
     * </p>
     *
     * @param edge   the edge to extract properties from, must not be null
     * @param fields the list of property names to extract, must not be null
     * @return a map of property names to property values
     * @throws NullPointerException      if any parameter is null
     * @throws ResourceNotFoundException if a required edge property is not found
     * @throws DataException             if there is an error processing edge
     *                                   properties
     */
    public Map<String, Object> getPropertyMapByEdge(Edge edge, List<String> fields) {
        Objects.requireNonNull(edge, TopologyGraphConstants.EDGE_NOT_BE_NULL);
        Objects.requireNonNull(fields, TopologyGraphConstants.FIELDS_LIST_NOT_BE_NULL);

        Map<String, Object> propertyMap = new HashMap<>();
        try {
            Iterator<Property<Object>> properties = edge.properties();
            if (properties != null) {
                setEdgePropertyMap(fields, propertyMap, properties);
                propertyMap.put(TopologyGraphConstants.EDGE_ID, edge.id().toString());
            }
        } catch (NoSuchElementException e) {
            throw new ResourceNotFoundException("Edge property not found", e);
        } catch (IllegalArgumentException e) {
            throw new DataException("Invalid edge property format", e);
        }
        return propertyMap;
    }

    /**
     * Populates a property map with selected edge properties.
     *
     * <p>
     * This method filters edge properties based on the provided list of fields
     * and adds them to the property map. Special handling is applied for latitude
     * and longitude properties to ensure proper numeric decoding.
     * </p>
     *
     * @param fields      the list of property keys to include, must not be null
     * @param propertyMap the map to populate with properties, must not be null
     * @param properties  the iterator of edge properties, must not be null
     * @throws NullPointerException if any parameter is null
     * @throws DataException        if there is an error decoding numeric properties
     */
    private void setEdgePropertyMap(List<String> fields, Map<String, Object> propertyMap,
                                    Iterator<Property<Object>> properties) {
        Objects.requireNonNull(fields, TopologyGraphConstants.FIELDS_LIST_NOT_BE_NULL);
        Objects.requireNonNull(propertyMap, TopologyGraphConstants.PROPERTY_MAP_NOT_BE_NULL);
        Objects.requireNonNull(properties, "Properties iterator cannot be null");

        try {
            while (properties.hasNext()) {
                Property<Object> property = properties.next();
                if (fields.contains(property.key())) {
                    if (property.key().equalsIgnoreCase(TopologyGraphConstants.LATITUDE)
                            || property.key().equalsIgnoreCase(TopologyGraphConstants.LONGITUDE)) {
                        propertyMap.put(property.key(), topologyGraphUtils.decodeNumeric(property.value().toString()));
                    } else {
                        propertyMap.put(property.key(), property.value());
                    }
                }
            }
        } catch (NumberFormatException e) {
            throw new DataException("Failed to decode numeric property", e);
        }
    }

    /**
     * Retrieves element information based on a property type and value.
     *
     * <p>
     * This method finds a vertex with the specified property value and
     * returns a map of its properties.
     * </p>
     *
     * @param type  the property type to search for, must not be null or empty
     * @param value the property value to match, must not be null or empty
     * @return a map of element properties
     * @throws NullPointerException      if any parameter is null
     * @throws IllegalArgumentException  if any parameter is empty
     * @throws ResourceNotFoundException if the element is not found
     */
    @Override
    public Map<String, Object> getElementInfo(String type, String value) {
        Objects.requireNonNull(type, "Type cannot be null");
        Objects.requireNonNull(value, "Value cannot be null");

        if (type.trim().isEmpty()) {
            throw new IllegalArgumentException("Type cannot be empty");
        }

        if (value.trim().isEmpty()) {
            throw new IllegalArgumentException("Value cannot be empty");
        }

        Map<String, Object> nodedata = new HashMap<>();

        try {
            List<Vertex> nodeList = topologyGraphUtils.getVertexListByPropertyValue(type, value);
            if (nodeList != null && !nodeList.isEmpty()) {
                nodedata = topologyGraphUtils.getPropertyMapByVertex(nodeList.get(0),
                        TopologyGraphConstants.getVertexFields());
            }
        } catch (NoSuchElementException e) {
            throw new ResourceNotFoundException("Element not found with " + type + ": " + value, e);
        }

        return nodedata;
    }

    /**
     * Retrieves port information for a list of links.
     *
     * <p>
     * This method parses link names to extract source and destination elements
     * and ports, then retrieves the corresponding port information from the graph.
     * </p>
     *
     * @param linkNameList the list of link names to process, must not be null or
     *                     empty
     * @return a map of element names to lists of port names
     * @throws NullPointerException      if linkNameList is null
     * @throws IllegalArgumentException  if linkNameList is empty
     * @throws DataException             if there is an error processing link names
     * @throws ResourceNotFoundException if port information is not found
     */
    @Override
    public Map<String, List<String>> getPorts(List<String> linkNameList) {
        Objects.requireNonNull(linkNameList, "Link name list cannot be null");

        if (linkNameList.isEmpty()) {
            throw new IllegalArgumentException("Link name list cannot be empty");
        }

        Map<String, List<String>> mapToReturn = new HashMap<>();
        List<String> fields = Arrays.asList(
                TopologyGraphConstants.SOURCE_ELEMENT_NAME,
                TopologyGraphConstants.DESTINATION_ELEMENT_NAME,
                TopologyGraphConstants.SOURCE_BUNDLE,
                TopologyGraphConstants.DESTINATION_BUNDLE,
                TopologyGraphConstants.SOURCE_PORT,
                TopologyGraphConstants.DESTINATION_PORT);

        try {
            for (String link : linkNameList) {
                String[] linkParts = link.split(Symbol.HASH_STRING);
                String[] sourceParts = linkParts[PMConstants.INDEX_ZERO].split(Symbol.UNDERSCORE_STRING);
                String[] destParts = linkParts[PMConstants.INDEX_ONE].split(Symbol.UNDERSCORE_STRING);

                String source = sourceParts[PMConstants.INDEX_ZERO];
                String sourcePort = sourceParts[PMConstants.INDEX_ONE];
                String dest = destParts[PMConstants.INDEX_ZERO];
                String destPort = destParts[PMConstants.INDEX_ONE];

                List<Edge> edges = topologyGraphUtils.getAllEdgeBySourceAndDest(source, dest);
                List<Map<String, Object>> propMapList = topologyGraphUtils.getPropertyMapByEdge(edges, fields);

                if (org.apache.commons.collections4.CollectionUtils.isNotEmpty(propMapList)) {
                    List<String> sourcePortList = extractPorts(propMapList, TopologyGraphConstants.SOURCE_BUNDLE,
                            sourcePort, TopologyGraphConstants.SOURCE_PORT);
                    List<String> destPortList = extractPorts(propMapList, TopologyGraphConstants.DESTINATION_BUNDLE,
                            destPort, TopologyGraphConstants.DESTINATION_PORT);

                    mapToReturn.put(source, sourcePortList);
                    mapToReturn.put(dest, destPortList);
                }

                logger.info("Map of return is : {}", mapToReturn);
            }
        } catch (ArrayIndexOutOfBoundsException e) {
            throw new DataException("Invalid link format in link name list", e);
        } catch (NoSuchElementException e) {
            throw new ResourceNotFoundException("Port information not found for the specified links", e);
        }

        return mapToReturn;
    }

    /**
     * Extracts port information from a list of property maps based on specified
     * criteria.
     *
     * <p>
     * This method filters the provided property maps to find entries where the
     * bundle key
     * matches the specified bundle value, and extracts the corresponding port
     * values.
     * </p>
     *
     * <p>
     * Example usage:
     * </p>
     *
     * <pre>
     * List<Map<String, Object>> propMapList = getPropertyMaps();
     * List<String> sourcePorts = extractPorts(propMapList,
     *         "sourceBundle",
     *         "Bundle1",
     *         "sourcePort");
     * </pre>
     *
     * @param propMapList the list of property maps to extract ports from, must not
     *                    be null
     * @param bundleKey   the key used to identify the bundle property, must not be
     *                    null
     * @param bundleValue the value to match against the bundle property, must not
     *                    be null
     * @param portKey     the key used to identify the port property, must not be
     *                    null
     * @return a list of port values that match the criteria
     * @throws NullPointerException if any parameter is null
     * @throws DataException        if there is an error processing the port data
     */
    private List<String> extractPorts(List<Map<String, Object>> propMapList, String bundleKey, String bundleValue,
                                      String portKey) {
        Objects.requireNonNull(propMapList, "Property map list cannot be null");
        Objects.requireNonNull(bundleKey, "Bundle key cannot be null");
        Objects.requireNonNull(bundleValue, "Bundle value cannot be null");
        Objects.requireNonNull(portKey, "Port key cannot be null");

        List<String> portList = new ArrayList<>();
        try {
            for (Map<String, Object> dataMap : propMapList) {
                if (bundleValue.equals(dataMap.get(bundleKey))) {
                    portList.add(dataMap.get(portKey) != null ? String.valueOf(dataMap.get(portKey)) : "");
                }
            }
            return portList;
        } catch (ClassCastException e) {
            throw new DataException("Invalid port data format", e);
        }
    }

    /**
     * Retrieves filtered data from the Janus graph.
     *
     * <p>
     * This method obtains all Janus graph data and applies filtering to create a
     * more
     * concise representation suitable for client consumption. The resulting map
     * contains
     * element names as keys and lists of connected elements as values.
     * </p>
     *
     * @return a map of element names to lists of filtered element information
     * @throws DataProcessingException if there is an error retrieving or processing
     *                                 the graph data
     */
    @Override
    public Map<String, List<ElementInfoRestWrapper>> getJanusGraphFilteredData() {
        Map<String, List<ElementInfoRestWrapper>> graphData = new HashMap<>();
        try {
            Map<String, List<ElementInfoWrapper>> allJanusGraphData = getAllJanusGraphData();
            if (allJanusGraphData != null && !allJanusGraphData.isEmpty()) {
                allJanusGraphData.forEach((key, list) -> graphData.put(key, getConnectedNodeList(list)));
            }
        } catch (Exception e) {
            throw new DataProcessingException("Failed to retrieve filtered Janus graph data", e);
        }
        return graphData;
    }

    /**
     * Converts a list of ElementInfoWrapper objects to ElementInfoRestWrapper
     * objects.
     *
     * <p>
     * This helper method processes each element in the provided list and creates a
     * filtered representation suitable for REST API responses.
     * </p>
     *
     * @param list the list of ElementInfoWrapper objects to convert, must not be
     *             null
     * @return a list of filtered ElementInfoRestWrapper objects
     * @throws NullPointerException    if the list parameter is null
     * @throws DataProcessingException if there is an error processing the list
     *                                 elements
     */
    private List<ElementInfoRestWrapper> getConnectedNodeList(List<ElementInfoWrapper> list) {
        Objects.requireNonNull(list, "List cannot be null");

        List<ElementInfoRestWrapper> listToRerturn = new ArrayList<>();
        try {
            if (org.apache.commons.collections4.CollectionUtils.isNotEmpty(list)) {
                list.stream().forEach(wrapper -> {
                    if (wrapper != null) {
                        listToRerturn.add(getFilteredWrapper(wrapper));
                    }
                });
            }
        } catch (Exception e) {
            throw new DataProcessingException("Failed to process connected node list", e);
        }
        return listToRerturn;
    }

    /**
     * Creates a filtered REST wrapper from an ElementInfoWrapper.
     *
     * <p>
     * This method extracts essential information from the provided wrapper and
     * creates a more concise representation suitable for REST API responses. The
     * resulting wrapper contains abbreviated property names for reduced payload
     * size.
     * </p>
     *
     * @param wrapper the ElementInfoWrapper to convert, must not be null
     * @return a filtered ElementInfoRestWrapper containing essential information
     * @throws NullPointerException if the wrapper parameter is null
     * @throws DataException        if there is an error creating the filtered
     *                              wrapper
     */
    private ElementInfoRestWrapper getFilteredWrapper(ElementInfoWrapper wrapper) {
        Objects.requireNonNull(wrapper, "Wrapper cannot be null");

        try {
            ElementInfoRestWrapper wrapperToReturn = new ElementInfoRestWrapper();
            wrapperToReturn.seteN(wrapper.getElementName());
            wrapperToReturn.setP(wrapper.getPorts());
            wrapperToReturn.seteD(wrapper.getEquipDomain());
            wrapperToReturn.seteV(wrapper.getEquipVendor());
            if (wrapper.getElementType() != null) {
                wrapperToReturn.seteT(wrapper.getElementType());
            }
            wrapperToReturn.setsE(wrapper.getSrcEliaseName());
            wrapperToReturn.setdE(wrapper.getDestEliaseName());
            wrapperToReturn.setmT(wrapper.getMwType());
            return wrapperToReturn;
        } catch (NullPointerException e) {
            throw new DataException("Failed to create filtered wrapper due to null data", e);
        }
    }

    /**
     * Retrieves information about edges based on their IDs.
     *
     * <p>
     * This method fetches edge information from the graph database using the
     * provided
     * list of edge IDs. The returned information includes source and destination
     * element
     * names, ports, and edge IDs.
     * </p>
     *
     * @param edgeIdList the list of edge IDs to retrieve information for, must not
     *                   be null or empty
     * @return a list of maps containing edge information
     * @throws NullPointerException      if edgeIdList is null
     * @throws IllegalArgumentException  if edgeIdList is empty
     * @throws BusinessException         if there is an error with the edge ID
     *                                   format
     * @throws ResourceNotFoundException if any of the requested edges are not found
     * @throws DataProcessingException   if there is an error processing the edge
     *                                   data
     */
    @Override
    public List<Map<String, Object>> getEdgeInfo(List<String> edgeIdList) {
        Objects.requireNonNull(edgeIdList, "Edge ID list cannot be null");

        if (edgeIdList.isEmpty()) {
            throw new IllegalArgumentException("Edge ID list cannot be empty");
        }

        try {
            List<String> selectedEdgeFields = Arrays.asList(TopologyGraphConstants.SOURCE_ELEMENT_NAME,
                    TopologyGraphConstants.DESTINATION_ELEMENT_NAME, TopologyGraphConstants.SOURCE_PORT,
                    TopologyGraphConstants.DESTINATION_PORT, TopologyGraphConstants.EDGE_ID);

            logger.info("Selected edge fields: {}", selectedEdgeFields);

            Set<Edge> edges = topologyGraphUtils.getEdgeInfoListByEdgeId(edgeIdList);

            logger.info("Retrieved edges: {}", edges);

            return Collections.emptyList();
        } catch (IllegalArgumentException e) {
            logger.error("Invalid edge ID format in getEdgeInfo");
            throw new BusinessException("Invalid edge ID format", e);
        } catch (ResourceNotFoundException e) {
            logger.error("Edge not found in getEdgeInfo");
            throw e;
        } catch (DataProcessingException e) {
            logger.error("Data processing error in getEdgeInfo");
            throw e;
        }

    }

    /**
     * Retrieves the topology path from a node to its backhaul connection.
     *
     * <p>
     * This method traverses the network topology starting from the specified
     * element
     * and follows connections based on configured rules until reaching a
     * termination point.
     * The resulting topology includes both nodes and links along the path.
     * </p>
     *
     * <p>
     * Example usage:
     * </p>
     *
     * <pre>
     * Topology backhaul = getNodeToBackhaul("NodeA");
     * Set<Map<String, Object>> nodes = backhaul.getNodes();
     * List<Map<String, Object>> links = backhaul.getLinks();
     * </pre>
     *
     * @param eName the element name to start the backhaul path from, must not be
     *              null or empty
     * @return a Topology object containing nodes and links in the backhaul path
     * @throws NullPointerException      if eName is null
     * @throws IllegalArgumentException  if eName is empty
     * @throws BusinessException         if there is an error with configuration
     *                                   parameters or graph processing
     * @throws ResourceNotFoundException if the specified element or required
     *                                   resources are not found
     * @throws DataProcessingException   if there is an error processing the graph
     *                                   data
     * @throws GraphicalException        if there is an error traversing the graph
     */
    @Override
    public Topology getNodeToBackhaul(String eName) {
        Objects.requireNonNull(eName, "E name cannot be null");

        if (eName.trim().isEmpty()) {
            throw new IllegalArgumentException("E name cannot be empty");
        }

        Topology topology = new Topology();
        List<String> graphMapEdgeFields = topologyGraphUtils.setSystemConfigurationMapForEdge();
        try {
            Set<Path> vertexPathTraversed = null;
            List<Map<String, Object>> edges = new ArrayList<>();
            Set<Map<String, Object>> setOfVertex = new HashSet<>();
            String terminationType = null;

            String connectionLimit = "TOPOLGY_BACKHAUL_CONNECTION_LIMIT";
            String allowedTypesConfig = "TOPOLGY_BACKHAUL_CONNECTION_TYPE_LIST";
            logger.info("eName={} , allowedTypesConfig={}, terminationType={},connectionLimit={} ", eName,
                    allowedTypesConfig, terminationType, connectionLimit);
            String[] allowedTypesInPath = null;
            if (!allowedTypesConfig.isEmpty()) {
                allowedTypesInPath = allowedTypesConfig.split(PMConstants.COMMA);
            }
            if (!connectionLimit.isEmpty()) {
                Long limit = Long.parseLong(connectionLimit);
                vertexPathTraversed = topologyGraphUtils.getVertexPathTraversed(eName, allowedTypesInPath,
                        terminationType, limit);
            }
            logger.info("vertexPathTraversed={}", vertexPathTraversed);
            Set<Edge> edgeSet = new HashSet<>();
            List<Map<String, Object>> extractEdgeFromPath = new ArrayList<>();

            logger.info("Edge Set: {}, Extracted Edge Paths: {}", edgeSet, extractEdgeFromPath);

            for (Path path : vertexPathTraversed) {
                List<Map<String, Object>> extractVertexFromPath = topologyGraphUtils.extractVertexFromPath(path);
                for (Map<String, Object> vertex : extractVertexFromPath) {
                    vertex.remove("location");
                    vertex.put("lat", topologyGraphUtils.decodeNumeric(vertex.get("lat").toString()));
                    vertex.put("lng", topologyGraphUtils.decodeNumeric(vertex.get("lng").toString()));
                    logger.info(VERTEXT, vertex);
                    setOfVertex.add(vertex);
                    Set<Edge> edgeByVertexPropertyValue = topologyGraphUtils.getPhysicalEdgeByVertexPropertyValue(
                            Long.parseLong(vertex.get("vertexId").toString()));
                    logger.error("size of edge list is : {}", edgeByVertexPropertyValue.size());
                    edges.addAll(topologyGraphUtils.getPropertyMapByEdge(new ArrayList<>(edgeByVertexPropertyValue),
                            graphMapEdgeFields));
                    logger.error("edgess data is : {}", edges);
                }
            }
            logger.info("Vertex count={} , setOfVertex={}", setOfVertex.size(), setOfVertex);
            logger.info("Edge count={} , edges={}", edges.size(), edges);
            topology.setNodes(setOfVertex);
            topology.setLinks(edges);
        } catch (NumberFormatException e) {
            logger.error("Invalid number format in getNodeToBackhaul");
            throw new BusinessException("Invalid connection limit format", e);
        } catch (ResourceNotFoundException e) {
            logger.error("Resource not found in getNodeToBackhaul for eName={}", eName);
            throw e;
        } catch (DataProcessingException e) {
            logger.error("Data processing error in getNodeToBackhaul for eName={}", eName);
            throw e;
        } catch (GraphicalException e) {
            logger.error("Graph traversal error in getNodeToBackhaul for eName={}", eName);
            throw new BusinessException("Error processing graph data for node: " + eName, e);
        }
        return topology;
    }

    /**
     * Retrieves topology data based on the specified request criteria.
     *
     * <p>
     * This method processes the request wrapper to determine the operation mode and
     * query type,
     * then delegates to the appropriate handler method to retrieve the requested
     * topology data.
     * The operation modes include vertex, edge, both edges by vertex, or both
     * vertices and edges.
     * </p>
     *
     * <p>
     * Example usage:
     * </p>
     *
     * <pre>
     * TopologyRequestWrapper request = new TopologyRequestWrapper();
     * request.getQuery().setQueryType("CRITERIA");
     * request.getQuery().setOpMode("VERTEX");
     * Topology topology = getTopologyData(request);
     * </pre>
     *
     * @param requestWrapper the request containing query parameters and operation
     *                       mode, must not be null
     * @return a Topology object containing the requested nodes and links
     * @throws NullPointerException                if requestWrapper is null
     * @throws JanusOperationModeNotValidException if the operation mode is invalid
     * @throws ScriptException                     if there is an error in script
     *                                             execution
     * @throws HasTypeNotValidException            if the has-type condition is
     *                                             invalid
     * @throws JanusQueryTypeNotValidException     if the query type is invalid
     * @throws HasNotTypeNotValidException         if the has-not-type condition is
     *                                             invalid
     * @throws JanusQueryNotValidException         if the query is invalid
     */
    @Override
    public Topology getTopologyData(TopologyRequestWrapper requestWrapper)
            throws JanusOperationModeNotValidException, ScriptException, HasTypeNotValidException,
            JanusQueryTypeNotValidException, HasNotTypeNotValidException, JanusQueryNotValidException {
    
        Objects.requireNonNull(requestWrapper, TopologyGraphConstants.REQUEST_WRAPPER_NOT_BE_NULL);
        Objects.requireNonNull(requestWrapper.getQuery(), "Query cannot be null");
    
        Topology topologyData = new Topology();
        String operationMode = requestWrapper.getQuery().getOpMode();
        String queryType = requestWrapper.getQuery().getQueryType();
    
        logger.info("Processing topology request - QueryType: {}, OperationMode: {}", queryType, operationMode);
    
        if (TopologyGraphConstants.JANUS_QUERY_TYPE_STRING.equalsIgnoreCase(queryType)) {
            logger.info("Processing string query type: {}", queryType);
            // String query type processing logic would go here
        } else if (TopologyGraphConstants.JANUS_QUERY_TYPE_CRITERIA.equalsIgnoreCase(queryType)) {
            logger.info("Processing criteria query type with operation mode: {}", operationMode);
            topologyData = processCriteriaQueryByOpMode(operationMode, requestWrapper);
        } else {
            logger.warn("Unsupported query type: {}", queryType);
        }
    
        logger.info("Topology data retrieval completed successfully");
        return topologyData;
    }

    private Topology processCriteriaQueryByOpMode(String operationMode, TopologyRequestWrapper requestWrapper)
        throws ScriptException, JanusOperationModeNotValidException, HasTypeNotValidException,
        JanusQueryTypeNotValidException, HasNotTypeNotValidException, JanusQueryNotValidException {

    Topology topologyData = new Topology();

    switch (operationMode) {
        case TopologyGraphConstants.VERTEX:
            logger.info("Processing vertex operation mode");
            topologyData = getTopologyDataOfOpModeVertex(requestWrapper);
            logger.info("Vertex operation completed, nodes count: {}",
                    topologyData.getNodes() != null ? topologyData.getNodes().size() : 0);
            break;

        case TopologyGraphConstants.EDGE:
            logger.info("Processing edge operation mode");
            topologyData = getTopologyDataOfOpModeEdge(requestWrapper);
            logger.info("Edge operation completed, links count: {}",
                    topologyData.getLinks() != null ? topologyData.getLinks().size() : 0);
            break;

        case TopologyGraphConstants.BOTH_E_BY_V:
            logger.info("Processing both edges by vertex operation mode");
            topologyData = getTopologyDataOfOpModeBothEByV(requestWrapper);
            logger.info("Both edges by vertex operation completed, nodes count: {}, links count: {}",
                    topologyData.getNodes() != null ? topologyData.getNodes().size() : 0,
                    topologyData.getLinks() != null ? topologyData.getLinks().size() : 0);
            break;

        case TopologyGraphConstants.BOTH:
            logger.info("Processing both vertices and edges operation mode");
            TopologyRequestWrapper vertexRequest = createRequestCopy(requestWrapper, TopologyGraphConstants.VERTEX);
            Topology vertexData = getTopologyDataOfOpModeVertex(vertexRequest);
            topologyData.setNodes(vertexData.getNodes());

            TopologyRequestWrapper edgeRequest = createRequestCopy(requestWrapper, TopologyGraphConstants.EDGE);
            Topology edgeData = getTopologyDataOfOpModeEdge(edgeRequest);
            topologyData.setLinks(edgeData.getLinks());

            logger.info("Both operation completed, nodes count: {}, links count: {}",
                    topologyData.getNodes() != null ? topologyData.getNodes().size() : 0,
                    topologyData.getLinks() != null ? topologyData.getLinks().size() : 0);
            break;

        default:
            logger.error("Invalid operation mode: {}", operationMode);
            throw new JanusOperationModeNotValidException("Invalid operation mode: " + operationMode);
    }

    return topologyData;
}


    /**
     * Creates a copy of the request wrapper with a specific operation mode.
     * This prevents modification of the original request wrapper.
     *
     * @param originalRequest the original request wrapper
     * @param operationMode the operation mode to set in the copy
     * @return a new request wrapper with the specified operation mode
     */
    private TopologyRequestWrapper createRequestCopy(TopologyRequestWrapper originalRequest, String operationMode) {
        TopologyRequestWrapper copy = new TopologyRequestWrapper();
        copy.setQuery(originalRequest.getQuery());
        copy.getQuery().setOpMode(operationMode);
        return copy;
    }


    /**
     * Retrieves topology data for vertex operation mode.
     *
     * <p>
     * This method processes the query in the request wrapper to retrieve vertices
     * from the graph database and converts them into a topology representation.
     * </p>
     *
     * @param requestWrapper the request containing query parameters, must not be
     *                       null
     * @return a Topology object containing the retrieved vertices
     * @throws NullPointerException                if requestWrapper is null
     * @throws ScriptException                     if there is an error in script
     *                                             execution
     * @throws JanusOperationModeNotValidException if the operation mode is invalid
     * @throws HasTypeNotValidException            if the has-type condition is
     *                                             invalid
     * @throws JanusQueryTypeNotValidException     if the query type is invalid
     * @throws HasNotTypeNotValidException         if the has-not-type condition is
     *                                             invalid
     * @throws JanusQueryNotValidException         if the query is invalid
     * @throws InvalidInputException               if the query parameters are
     *                                             invalid
     * @throws GraphicalException                  if there is an error traversing
     *                                             the graph
     */
    private Topology getTopologyDataOfOpModeVertex(TopologyRequestWrapper requestWrapper)
    throws ScriptException, JanusOperationModeNotValidException, HasTypeNotValidException,
    JanusQueryTypeNotValidException, HasNotTypeNotValidException, JanusQueryNotValidException {

Objects.requireNonNull(requestWrapper, TopologyGraphConstants.REQUEST_WRAPPER_NOT_BE_NULL);

logger.info("Processing vertex operation mode query: {}", 
        requestWrapper.getQuery() != null ? requestWrapper.getQuery().toString() : "null");

Iterator<JanusGraphVertex> queryData = null;
try {
    JanusGraphQueryBuilder builder = new JanusGraphQueryBuilder(requestWrapper.getQuery());
    queryData = (Iterator<JanusGraphVertex>) builder.run();

    List<Vertex> vertices = JanusGraphQueryBuilderUtils.getVerticesByIterator(queryData);

    if (logger.isDebugEnabled()) {
        logger.info("Retrieved {} vertices for vertex operation mode", vertices.size());
    }

    return convertVertexIntoWrapper(vertices);

} catch (IllegalArgumentException e) {
    logger.error("Invalid query parameters for vertex operation");
    throw new InvalidInputException("Invalid query parameters for vertex operation", e);
} catch (IllegalStateException e) {
    logger.error("Graph traversal error when processing vertex query");
    throw new GraphicalException("Graph traversal error when processing vertex query", e);
} finally {
    // Clean up iterator to prevent memory leaks
    if (queryData instanceof AutoCloseable autoCloseable) {
        try {
            autoCloseable.close();
        } catch (Exception e) {
            logger.warn(TopologyGraphConstants.ERROR_CLOSING_QUERY_DATA_ITERATOR, e.getMessage());
        }
    }
}
}

    /**
     * Converts a list of vertices into a topology wrapper.
     *
     * <p>
     * This method extracts properties from each vertex and creates a map
     * representation
     * that can be included in the topology response.
     * </p>
     *
     * @param vertices the list of vertices to convert, must not be null
     * @return a Topology object containing the vertex data
     * @throws NullPointerException    if vertices is null or contains null elements
     * @throws VertexNotFoundException if a vertex property cannot be found
     * @throws DataProcessingException if there is an error processing vertex data
     */
    private Topology convertVertexIntoWrapper(List<Vertex> vertices) {
        Objects.requireNonNull(vertices, "Vertices cannot be null");
    
        logger.info("Converting vertices to topology wrapper, count: {}", vertices.size());
        Topology topology = new Topology();
        Set<Map<String, Object>> set = new HashSet<>();
    
        try {
            for (Vertex vertex : vertices) {
                if (vertex == null) {
                    logger.warn("Skipping null vertex during conversion");
                    continue;
                }
    
                Map<String, Object> vertexProperties = extractVertexProperties(vertex);
                set.add(vertexProperties);
            }
    
            logger.info("Vertex conversion completed, processed {} vertices", set.size());
            topology.setNodes(set);
            return topology;
    
        } catch (Exception e) {
            logger.error("Error during vertex conversion");
            throw new DataProcessingException("Error processing vertex data during conversion", e);
        }
    }

    private Map<String, Object> extractVertexProperties(Vertex vertex) {
        Map<String, Object> vertexProperties = new HashMap<>();
        Set<String> propertyKeys = vertex.keys();
    
        for (String key : propertyKeys) {
            try {
                Object value = vertex.value(key);
                vertexProperties.put(key, value);
            } catch (NoSuchElementException e) {
                logger.warn("Property key '{}' not found on vertex, skipping", key);
            }
        }
    
        return vertexProperties;
    }
    

    /**
     * Converts a set of edges into a topology wrapper.
     *
     * <p>
     * This method extracts properties from each edge and creates a map
     * representation
     * that can be included in the topology response.
     * </p>
     *
     * @param edges the set of edges to convert, must not be null
     * @return a Topology object containing the edge data
     * @throws NullPointerException    if edges is null or contains null elements
     * @throws DataNotFoundException   if an edge property cannot be found
     * @throws DataProcessingException if there is an error processing edge data
     */
    private Topology convertEdgeIntoWrapper(Set<Edge> edges) {
        Objects.requireNonNull(edges, "Edges cannot be null");
    
        logger.info("Converting edges to topology wrapper, count: {}", edges.size());
        Topology topology = new Topology();
        List<Map<String, Object>> edgeList = new ArrayList<>();
    
        try {
            for (Edge edge : edges) {
                if (edge == null) {
                    logger.warn("Skipping null edge during conversion");
                    continue;
                }
    
                Map<String, Object> edgeProperties = extractEdgeProperties(edge);
                edgeList.add(edgeProperties);
            }
    
            logger.info("Edge conversion completed, processed {} edges", edgeList.size());
            topology.setLinks(edgeList);
            return topology;
    
        } catch (Exception e) {
            logger.error("Error during edge conversion");
            throw new DataProcessingException("Failed to convert edges to topology wrapper", e);
        }
    }

    private Map<String, Object> extractEdgeProperties(Edge edge) {
        Map<String, Object> edgeProperties = new HashMap<>();
        Set<String> propertyKeys = edge.keys();
    
        for (String key : propertyKeys) {
            try {
                Object value = edge.value(key);
                edgeProperties.put(key, value);
            } catch (NoSuchElementException e) {
                logger.warn("Property key '{}' not found on edge, skipping", key);
                // continue with other keys
            }
        }
    
        return edgeProperties;
    }
    

    /**
     * Retrieves topology data for edge operation mode.
     *
     * <p>
     * This method processes the query in the request wrapper to retrieve edges
     * from the graph database and converts them into a topology representation.
     * </p>
     *
     * @param requestWrapper the request containing query parameters, must not be
     *                       null
     * @return a Topology object containing the retrieved edges
     * @throws NullPointerException                if requestWrapper is null
     * @throws ScriptException                     if there is an error in script
     *                                             execution
     * @throws JanusOperationModeNotValidException if the operation mode is invalid
     * @throws HasTypeNotValidException            if the has-type condition is
     *                                             invalid
     * @throws JanusQueryTypeNotValidException     if the query type is invalid
     * @throws HasNotTypeNotValidException         if the has-not-type condition is
     *                                             invalid
     * @throws JanusQueryNotValidException         if the query is invalid
     * @throws InvalidInputException               if the query parameters are
     *                                             invalid
     * @throws GraphicalException                  if there is an error traversing
     *                                             the graph
     */
    private Topology getTopologyDataOfOpModeEdge(TopologyRequestWrapper requestWrapper)
    throws ScriptException, JanusOperationModeNotValidException, HasTypeNotValidException,
    JanusQueryTypeNotValidException, HasNotTypeNotValidException, JanusQueryNotValidException {

Objects.requireNonNull(requestWrapper, TopologyGraphConstants.REQUEST_WRAPPER_NOT_BE_NULL);

if (logger.isInfoEnabled()) {
    logger.info("Processing edge operation mode query: {}", 
        requestWrapper.getQuery() != null ? requestWrapper.getQuery().toString() : "null");
}

JanusGraphQueryBuilder builder = null;
Iterator<JanusGraphEdge> queryData = null;
Set<Edge> edges = null;

try {
    builder = new JanusGraphQueryBuilder(requestWrapper.getQuery());
    queryData = (Iterator<JanusGraphEdge>) builder.run();
    
    if (queryData == null) {
        logger.warn("Query returned null iterator for edge operation");
        return new Topology();
    }
    
    edges = JanusGraphQueryBuilderUtils.getEdgesSetByIterator(queryData);
    
    if (logger.isInfoEnabled()) {
        logger.info("Retrieved {} edges from graph query", edges != null ? edges.size() : 0);
    }
    
    return convertEdgeIntoWrapper(edges != null ? edges : new HashSet<>());
    
} catch (IllegalArgumentException e) {
    logger.error("Invalid query parameters for edge operation");
    throw new InvalidInputException("Invalid query parameters for edge operation", e);
} catch (IllegalStateException e) {
    logger.error("Graph traversal error when processing edge query");
    throw new GraphicalException("Graph traversal error when processing edge query", e);
} catch (Exception e) {
    logger.error("Unexpected error during edge operation");
    throw new GraphicalException("Unexpected error during edge operation", e);
} finally {
    // Clean up resources to prevent memory leaks
    if (queryData instanceof AutoCloseable autoCloseable) {
        try {
            autoCloseable.close();
        } catch (Exception e) {
            logger.warn(TopologyGraphConstants.ERROR_CLOSING_QUERY_DATA_ITERATOR, e.getMessage());
        }
    }

    // Clear references to help garbage collection
    builder = null;
    queryData = null;
    edges = null;
}
}


    /**
     * Retrieves topology data for both edges and vertices operation mode.
     *
     * <p>
     * This method first retrieves vertices based on the query, then finds all edges
     * connected to those vertices, and finally combines them into a topology
     * representation.
     * </p>
     *
     * @param requestWrapper          the request containing query parameters, must
     *                                not be null
     * @param isExternalEdgesIncluded whether to include external edges in the
     *                                result
     * @return a Topology object containing the retrieved vertices and edges
     * @throws NullPointerException                if requestWrapper is null
     * @throws ScriptException                     if there is an error in script
     *                                             execution
     * @throws JanusOperationModeNotValidException if the operation mode is invalid
     * @throws HasTypeNotValidException            if the has-type condition is
     *                                             invalid
     * @throws JanusQueryTypeNotValidException     if the query type is invalid
     * @throws HasNotTypeNotValidException         if the has-not-type condition is
     *                                             invalid
     * @throws JanusQueryNotValidException         if the query is invalid
     * @throws InvalidInputException               if the query parameters are
     *                                             invalid
     * @throws GraphicalException                  if there is an error traversing
     *                                             the graph
     * @throws DataProcessingException             if there is an error processing
     *                                             the data
     */
    private Topology getTopologyDataOfOpModeBothEByV(TopologyRequestWrapper requestWrapper)
            throws ScriptException, JanusOperationModeNotValidException, HasTypeNotValidException,
            JanusQueryTypeNotValidException, HasNotTypeNotValidException, JanusQueryNotValidException {
        Objects.requireNonNull(requestWrapper, TopologyGraphConstants.REQUEST_WRAPPER_NOT_BE_NULL);

        JanusGraphQueryBuilder vertexQuerybuilder = null;
        JanusGraphQueryBuilder edgeQuerybuilder = null;
        Iterator<JanusGraphVertex> queryData = null;
        Map<JanusGraphVertex, Iterable<JanusGraphEdge>> edgeQueryData = null;
        List<Vertex> vertices = null;
        Set<Edge> edges = null;

        try {
            // Set operation mode for vertex query
            requestWrapper.getQuery().setOpMode(TopologyGraphConstants.VERTEX);

            // Build and execute vertex query
            vertexQuerybuilder = new JanusGraphQueryBuilder(requestWrapper.getQuery());
            queryData = (Iterator<JanusGraphVertex>) vertexQuerybuilder.run();
            vertices = JanusGraphQueryBuilderUtils.getVerticesByIterator(queryData);

            // Set operation mode for edge query
            requestWrapper.getQuery().setOpMode(TopologyGraphConstants.BOTH_E_BY_V);
            
            // Build and execute edge query
            edgeQuerybuilder = new JanusGraphQueryBuilder(requestWrapper.getQuery());
            edgeQuerybuilder.setVertices(vertices);
            edgeQueryData = (Map<JanusGraphVertex, Iterable<JanusGraphEdge>>) edgeQuerybuilder.run();

            edges = JanusGraphQueryBuilderUtils.getEdgesByMap(edgeQueryData);
            logger.info("Edges count: {}", edges.size());

            // Process vertices based on includeVertexWithNoEdge flag
            if (requestWrapper.getIncludeVertexWithNoEdge() != null && !requestWrapper.getIncludeVertexWithNoEdge()) {
                vertices.clear();
                vertices = new ArrayList<>(topologyGraphUtils.getVertexSetByEdges(edges));
            } else {
                vertices.addAll(new ArrayList<>(topologyGraphUtils.getVertexSetByEdges(edges)));
            }

            logger.info("Vertices count: {}", vertices.size());
            return convertVertexIntoWrapper(vertices);
            
        } catch (IllegalArgumentException e) {
            logger.error("Invalid query parameters for both edges and vertices operation");
            throw new InvalidInputException("Invalid query parameters for both edges and vertices operation", e);
        } catch (IllegalStateException e) {
            logger.error("Graph traversal error when processing both edges and vertices query");
            throw new GraphicalException("Graph traversal error when processing both edges and vertices query", e);
        } catch (NullPointerException e) {
            logger.error("Null element encountered during both edges and vertices processing");
            throw new DataProcessingException("Null element encountered during both edges and vertices processing", e);
        } finally {
            // Clean up resources to prevent memory leaks
            if (queryData != null) {
                try {
                    if (queryData instanceof AutoCloseable autocloseable) {
                        autocloseable.close();
                    }
                } catch (Exception e) {
                    logger.warn(TopologyGraphConstants.ERROR_CLOSING_QUERY_DATA_ITERATOR, e.getMessage());
                }
            }
            
            // Clear references to help garbage collection
            vertexQuerybuilder = null;
            edgeQuerybuilder = null;
            queryData = null;
            edgeQueryData = null;
            vertices = null;
            edges = null;
        }
    }


    /**
     * Creates an ElementInfoWrapper from a vertex property map.
     *
     * <p>
     * This method extracts properties from the provided map and populates an
     * ElementInfoWrapper object with the corresponding values.
     * </p>
     *
     * @param propertyMapByVertex the map containing vertex properties, must not be
     *                            null
     * @return an ElementInfoWrapper containing the vertex information
     * @throws NullPointerException    if propertyMapByVertex is null
     * @throws DataProcessingException if there is an error processing the vertex
     *                                 properties
     */
    public ElementInfoWrapper getElementInfoWrapperByVertex(Map<String, Object> propertyMapByVertex) {
        ElementInfoWrapper elementInfo = new ElementInfoWrapper();

        elementInfo.setElementName(getStringValue(propertyMapByVertex, ENAME));
        elementInfo.setNeId(getStringValue(propertyMapByVertex, EID));
        elementInfo.setEquipVendor(getStringValue(propertyMapByVertex, VENDOR));
        elementInfo.setEquipDomain(getStringValue(propertyMapByVertex, DOMAIN));
        elementInfo.setElementType(getStringValue(propertyMapByVertex, TopologyGraphConstants.ALIASE_ELEMENT_TYPE));
        elementInfo.setElementAliaseType(getStringValue(propertyMapByVertex, TopologyGraphConstants.ELEMENT_TYPE));
        elementInfo.setMwType(getStringValue(propertyMapByVertex, TopologyGraphConstants.MICROWAVE_TYPE));
        elementInfo.setGeographyL1(getStringValue(propertyMapByVertex, TopologyGraphConstants.GEOGRAPHY_LEVEL1));
        elementInfo.setGeographyL2(getStringValue(propertyMapByVertex, TopologyGraphConstants.GEOGRAPHY_LEVEL2));
        elementInfo.setGeographyL3(getStringValue(propertyMapByVertex, TopologyGraphConstants.GEOGRAPHY_LEVEL3));
        elementInfo.setLat(getDoubleValue(propertyMapByVertex, TopologyGraphConstants.LATITUDE));
        elementInfo.setLng(getDoubleValue(propertyMapByVertex, TopologyGraphConstants.LONGITUDE));
        elementInfo.setIpv4(getStringValue(propertyMapByVertex, TopologyGraphConstants.IPV4));
        elementInfo.setIpv6(getStringValue(propertyMapByVertex, TopologyGraphConstants.IPV6));
        elementInfo.setSiteId(getStringValue(propertyMapByVertex, TopologyGraphConstants.SITEID));
        elementInfo.setFibertakeSiteId(getStringValue(propertyMapByVertex, TopologyGraphConstants.FIBERTAKE_SITEID));
        elementInfo.setIsENBPresent(getStringValue(propertyMapByVertex, TopologyGraphConstants.IS_ENB_PRESENT));
        elementInfo.setGcName(getStringValue(propertyMapByVertex, TopologyGraphConstants.GC_NAME));
        elementInfo.setGeographyL1DName(getStringValue(propertyMapByVertex, TopologyGraphConstants.GEOGRAPHYL1_DNAME));
        elementInfo.setGeographyL2DName(getStringValue(propertyMapByVertex, TopologyGraphConstants.GEOGRAPHYL2_DNAME));
        elementInfo.setGeographyL3DName(getStringValue(propertyMapByVertex, TopologyGraphConstants.GEOGRAPHYL3_DNAME));
        elementInfo.setNodePriority(getStringValue(propertyMapByVertex, TopologyGraphConstants.NODE_PRIORITY));

        if (propertyMapByVertex.get(TopologyGraphConstants.ENABLED_ORIGIN_NW) != null) {
            List<String> origin = new ArrayList<>();
            origin.add(propertyMapByVertex.get(TopologyGraphConstants.ENABLED_ORIGIN_NW).toString());
            elementInfo.setEnabledOriginNW(origin);
        }

        return elementInfo;
    }

    /**
     * Retrieves connected edges for a vertex by element name and category.
     *
     * <p>
     * This method finds all edges connected to the specified vertex that match
     * the given category, and returns them along with their connected vertices.
     * </p>
     *
     * @param vertexId the element name of the vertex, must not be null or empty
     * @param category the category of edges to retrieve, must not be null
     * @return a map containing nodes and links connected to the specified vertex
     * @throws NullPointerException      if vertexId or category is null
     * @throws InvalidInputException     if vertexId is empty
     * @throws ResourceNotFoundException if the vertex is not found
     * @throws GraphicalException        if there is an error traversing the graph
     */
    @Override
    public Map<String, Object> getConnectedEdgesByEName(String vertexId, String category) {
        Objects.requireNonNull(vertexId, "Vertex ID cannot be null");
        Objects.requireNonNull(category, "Category cannot be null");

        if (StringUtils.isEmpty(vertexId)) {
            throw new InvalidInputException("Vertex ID cannot be null or empty");
        }

        Map<String, Object> topologyWrapper = new HashMap<>();
        List<Map<String, Object>> edgeList = new ArrayList<>();
        Set<Map<String, Object>> vertexSet = new HashSet<>();

        try {
            Set<Edge> connectedEdgesByVertexId = topologyGraphUtils.getConnectedEdgesByVeNameAndCategory(vertexId,
                    category);
            logger.error("getConnectedEdgesByVertexId connectedEdgesByVertexId size {}",
                    connectedEdgesByVertexId != null ? connectedEdgesByVertexId.size() : 0);

            if (connectedEdgesByVertexId != null && !connectedEdgesByVertexId.isEmpty()) {
                processConnectedEdges(connectedEdgesByVertexId, edgeList, vertexSet);
            }

            topologyWrapper.put(TopologyGraphConstants.NODES, vertexSet);
            topologyWrapper.put(TopologyGraphConstants.LINKS, edgeList);

        } catch (ResourceNotFoundException e) {
            logger.warn("Vertex not found: {}", vertexId);
            throw e;
        } catch (GraphicalException e) {
            logger.error("Graph traversal error for vertex {}", vertexId);
            throw e;
        }

        return topologyWrapper;
    }

    /**
     * Processes a set of connected edges to extract edge and vertex information.
     *
     * <p>
     * This helper method iterates through the provided edges, extracts their
     * properties,
     * and adds them to the edge list. It also processes the vertices connected to
     * each edge
     * and adds them to the vertex set.
     * </p>
     *
     * @param connectedEdgesByVertexId the set of edges to process, must not be null
     * @param edgeList                 the list to populate with edge property maps,
     *                                 must not be null
     * @param vertexSet                the set to populate with vertex property
     *                                 maps, must not be null
     * @throws NullPointerException if any parameter is null
     */
    private void processConnectedEdges(Set<Edge> connectedEdgesByVertexId,
                                       List<Map<String, Object>> edgeList,
                                       Set<Map<String, Object>> vertexSet) {
        Objects.requireNonNull(connectedEdgesByVertexId, "Connected edges set cannot be null");
        Objects.requireNonNull(edgeList, TopologyGraphConstants.EDGE_LIST_NOT_BE_NULL);
        Objects.requireNonNull(vertexSet, TopologyGraphConstants.VERTEX_SET_NOT_BE_NULL);

        for (Edge connectedEdge : connectedEdgesByVertexId) {
            Objects.requireNonNull(connectedEdge, TopologyGraphConstants.CONNECTED_EDGE_CANNOT_BE_NULL);

            try {
                Map<String, Object> propertyMapByEdge = iTopologyHbaseDao.getEdgeData(connectedEdge);
                addElementTypeAndAliasPortInEdgeProperty(propertyMapByEdge);

                if (isValidEdge(propertyMapByEdge)) {
                    edgeList.add(propertyMapByEdge);
                    processBothVertices(connectedEdge, vertexSet);
                }
            } catch (DataException e) {
                logger.warn("Could not process edge {}: {}", connectedEdge.id(), e.getMessage());
            }
        }
    }

    /**
     * Processes both vertices connected to an edge and adds them to the vertex set.
     *
     * <p>
     * This method retrieves both vertices connected to the specified edge,
     * fetches their data from the HBase data store, enriches the vertex data with
     * additional information, and adds them to the provided vertex set.
     * </p>
     *
     * @param connectedEdge the edge whose vertices are to be processed, must not be
     *                      null
     * @param vertexSet     the set to which processed vertex data will be added,
     *                      must not be null
     * @throws NullPointerException    if any parameter is null
     * @throws DataNotFoundException   if vertex data cannot be found in the data
     *                                 store
     * @throws DataProcessingException if there is an error processing the vertex
     *                                 data
     */
    private void processBothVertices(Edge connectedEdge, Set<Map<String, Object>> vertexSet) {
        Objects.requireNonNull(connectedEdge, TopologyGraphConstants.CONNECTED_EDGE_CANNOT_BE_NULL);
        Objects.requireNonNull(vertexSet, TopologyGraphConstants.VERTEX_SET_NOT_BE_NULL);

        Iterator<Vertex> bothVertices = connectedEdge.bothVertices();

        if (bothVertices != null) {
            while (bothVertices.hasNext()) {
                try {
                    Vertex vertex = bothVertices.next();
                    Map<String, Object> propertyMapByVertex = iTopologyHbaseDao.getVertexData(vertex);
                    enrichVertexData(propertyMapByVertex);
                    vertexSet.add(propertyMapByVertex);
                } catch (DataNotFoundException e) {
                    logger.warn("Vertex data not found for edge {}: {}", connectedEdge.id(), e.getMessage());
                }
            }
        }
    }

    /**
     * Enriches vertex data with additional information.
     *
     * <p>
     * This method adds alias names and replaces geography level properties
     * with their display values if available.
     * </p>
     *
     * @param propertyMapByVertex the vertex property map to enrich, must not be
     *                            null
     * @throws NullPointerException    if propertyMapByVertex is null
     * @throws DataProcessingException if there is an error during the enrichment
     *                                 process
     */
    private void enrichVertexData(Map<String, Object> propertyMapByVertex) {
        Objects.requireNonNull(propertyMapByVertex, TopologyGraphConstants.PROPERTY_MAP_BY_VERTEX_NOT_BE_NULL);

        try {
            enrichWithAliasName(propertyMapByVertex);
            replaceGeographyLevels(propertyMapByVertex);
        } catch (NullPointerException e) {
            throw new DataProcessingException("Null reference encountered while enriching vertex data", e);
        }
    }

    /**
     * Enriches vertex data with alias name information.
     *
     * <p>
     * This method retrieves the element info wrapper for the vertex's element name
     * and adds the source alias name to the vertex property map if available.
     * </p>
     *
     * @param propertyMapByVertex the vertex property map to enrich with alias name,
     *                            must not be null
     * @throws NullPointerException if propertyMapByVertex is null
     */
    private void enrichWithAliasName(Map<String, Object> propertyMapByVertex) {
        Objects.requireNonNull(propertyMapByVertex, TopologyGraphConstants.PROPERTY_MAP_BY_VERTEX_NOT_BE_NULL);

        if (propertyMapByVertex.get(TopologyGraphConstants.ELEMENT_NAME) != null) {
            String elementName = propertyMapByVertex.get(TopologyGraphConstants.ELEMENT_NAME).toString();
            ElementInfoWrapper elementInfoWrapper = getElementInfoWrapperByEName(elementName);

            if (elementInfoWrapper != null && elementInfoWrapper.getSrcEliaseName() != null) {
                propertyMapByVertex.put(TopologyGraphConstants.SOURCE_ALIASE_ENAME,
                        elementInfoWrapper.getSrcEliaseName());
            }
        }
    }

    /**
     * Replaces geography level properties with their display values if available.
     *
     * <p>
     * This method updates the geography level properties in the vertex property map
     * with their corresponding display values when available.
     * </p>
     *
     * @param propertyMapByVertex the vertex property map to update, must not be
     *                            null
     * @throws NullPointerException if propertyMapByVertex is null
     */
    private void replaceGeographyLevels(Map<String, Object> propertyMapByVertex) {
        Objects.requireNonNull(propertyMapByVertex, TopologyGraphConstants.PROPERTY_MAP_BY_VERTEX_NOT_BE_NULL);

        replaceProperty(propertyMapByVertex, TopologyGraphConstants.GEOGRAPHY_LEVEL1,
                TopologyGraphConstants.GEOGRAPHYL1_DNAME);
        replaceProperty(propertyMapByVertex, TopologyGraphConstants.GEOGRAPHY_LEVEL2,
                TopologyGraphConstants.GEOGRAPHYL2_DNAME);
        replaceProperty(propertyMapByVertex, TopologyGraphConstants.GEOGRAPHY_LEVEL3,
                TopologyGraphConstants.GEOGRAPHYL3_DNAME);
        replaceProperty(propertyMapByVertex, TopologyGraphConstants.GEOGRAPHY_LEVEL4,
                TopologyGraphConstants.GEOGRAPHYL4_DNAME);
    }

    /**
     * Replaces a property value with a fallback value if available.
     *
     * <p>
     * This helper method replaces the value of the primary key in the property map
     * with the value of the fallback key if the fallback key has a non-null value.
     * </p>
     *
     * @param propertyMap the property map to update, must not be null
     * @param primaryKey  the key whose value may be replaced, must not be null
     * @param fallbackKey the key whose value may be used as a replacement, must not
     *                    be null
     * @throws NullPointerException if any parameter is null
     */
    private void replaceProperty(Map<String, Object> propertyMap, String primaryKey, String fallbackKey) {
        Objects.requireNonNull(propertyMap, TopologyGraphConstants.PROPERTY_MAP_NOT_BE_NULL);
        Objects.requireNonNull(primaryKey, "Primary key cannot be null");
        Objects.requireNonNull(fallbackKey, "Fallback key cannot be null");

        propertyMap.replace(primaryKey,
                propertyMap.get(fallbackKey) != null ? propertyMap.get(fallbackKey) : propertyMap.get(primaryKey));
    }

    /**
     * Retrieves primary TOR information for a specified RIU.
     *
     * <p>
     * This method finds the primary TOR (Top of Rack) switch connected to the
     * specified RIU (Remote Interface Unit) and returns information about this
     * connection.
     * </p>
     *
     * @param riuName the name of the RIU to find primary TOR for, must not be null
     *                or empty
     * @return a set of maps containing RIU, primary TOR, and active port
     *         information
     * @throws NullPointerException      if riuName is null
     * @throws InvalidInputException     if riuName is empty
     * @throws ResourceNotFoundException if the specified RIU is not found
     * @throws DataProcessingException   if there is an error processing the data
     */
    @Override
    public Set<Map<String, Object>> getPrimaryTORByRIU(String riuName) {
        Objects.requireNonNull(riuName, "RIU name cannot be null");

        if (riuName.trim().isEmpty()) {
            throw new InvalidInputException("RIU name cannot be empty");
        }

        logger.info("Retrieving primary TOR for RIU: {}", riuName);
        Set<Map<String, Object>> elementSet = new HashSet<>();
        try {
            List<ElementConnectivityWrapper> elementConnectivityByElementNameMs = getElementConnectivityByElementName(
                    riuName, TopologyGraphConstants.CATEGORY_LOGICAL);
            logger.info("Element connectivity for RIU {}: {}", riuName,
                    elementConnectivityByElementNameMs != null ? elementConnectivityByElementNameMs.size() : 0);

            if (elementConnectivityByElementNameMs != null) {
                for (ElementConnectivityWrapper element : elementConnectivityByElementNameMs) {
                    Map<String, Object> elementMap = new HashMap<>();
                    logger.error("element is : {} ", element != null ? element : null);
                    if (element != null && TopologyGraphConstants.PRIMARY.equalsIgnoreCase(element.getNodePriority())) {
                        elementMap.put(TopologyGraphConstants.RIU, element.getSourceElementName());
                        elementMap.put(TopologyGraphConstants.PRIMARY_TOR_UHN, element.getDestElementName());
                        elementMap.put(TopologyGraphConstants.ACTIVE_PORT, element.getDestPort());
                        elementSet.add(elementMap);
                    }
                }
            }
            logger.info("Found {} primary TOR elements for RIU {}", elementSet.size(), riuName);
        } catch (ResourceNotFoundException e) {
            logger.warn("RIU not found: {}", riuName);
            throw e;
        } catch (DataProcessingException e) {
            logger.error("Error processing data for RIU {}", riuName);
            throw e;
        }
        return elementSet;
    }

    /**
     * Creates or updates edges in the specified graph instance.
     *
     * <p>
     * This method creates new edges or updates existing ones in the specified
     * Janus graph instance based on the provided edge data.
     * </p>
     *
     * @param edges         the list of edge data maps to create or update, must not
     *                      be null or empty
     * @param graphInstance the name of the graph instance to use, must not be null
     *                      or empty
     * @return a list of maps containing operation results
     * @throws NullPointerException  if edges or graphInstance is null
     * @throws InvalidInputException if edges is empty or graphInstance is empty
     * @throws GraphicalException    if there is an error during graph operations
     */
    @Override
    public List<Map<String, String>> createOrUpdateEdge(List<Map<String, Object>> edges, String graphInstance) {
        Objects.requireNonNull(edges, TopologyGraphConstants.EDGE_LIST_NOT_BE_NULL);
        Objects.requireNonNull(graphInstance, "Graph instance name cannot be null");

        if (edges.isEmpty()) {
            throw new InvalidInputException("Edge list cannot be empty");
        }
        if (StringUtils.isEmpty(graphInstance)) {
            throw new InvalidInputException("Graph instance name cannot be null or empty");
        }

        List<Map<String, String>> result = new ArrayList<>();
        try {
            JanusGraphInstance instance = JanusGraphInstance.valueOf(graphInstance);
            JanusGraph jGraph = JanusGraphInstanceManagerServiceImpl.getGraphByInstance(instance);
            VertexToEdgeRelationConfig config = iJanusGraphInstanceManagerService.getJanusConfigByInstance(instance);

            result = OperationUtils.createOrUpdateEdge(jGraph, edges, false, config);
        } catch (IllegalArgumentException e) {
            logger.error("Invalid graph instance name: {}", graphInstance);
            throw new InvalidInputException("Invalid graph instance: " + graphInstance, e);
        } catch (GraphicalException e) {
            logger.error("Graph operation error while creating/updating edges");
            throw e;
        } catch (Exception e) {
            logger.error("Error in createOrUpdateEdge : {}", e.getMessage());
            Map<String, String> res = new HashMap<>();
            res.put(JanusGraphUtilsConstants.RESULT_KEY, JanusGraphUtilsConstants.RESULT_FAILURE);
            res.put(JanusGraphUtilsConstants.MESSAGE_KEY, e.getMessage());
            result.clear();
            result.add(res);
        }
        return result;
    }

    /**
     * Creates or updates vertices in the specified graph instance.
     *
     * <p>
     * This method creates new vertices or updates existing ones in the specified
     * Janus graph instance based on the provided vertex data.
     * </p>
     *
     * @param vertices      the list of vertex data maps to create or update, must
     *                      not be null or empty
     * @param graphInstance the name of the graph instance to use, must not be null
     *                      or empty
     * @return a list of maps containing operation results
     * @throws NullPointerException    if vertices or graphInstance is null
     * @throws InvalidInputException   if vertices is empty or graphInstance is
     *                                 empty
     * @throws GraphicalException      if there is an error during graph operations
     * @throws VertexCreationException if there is an error creating vertices
     */
    @Override
    public List<Map<String, String>> createOrUpdateVertices(List<Map<String, Object>> vertices, String graphInstance) {
        Objects.requireNonNull(vertices, "Vertex list cannot be null");
        Objects.requireNonNull(graphInstance, "Graph instance name cannot be null");

        if (vertices.isEmpty()) {
            throw new InvalidInputException("Vertex list cannot be empty");
        }
        List<Map<String, String>> result = new ArrayList<>();

        if (vertices.isEmpty()) {
            throw new InvalidInputException("Vertex list cannot be null or empty");
        }
        if (StringUtils.isEmpty(graphInstance)) {
            throw new InvalidInputException("Graph instance name cannot be null or empty");
        }

        try {
            logger.info("graphInstance : {}", graphInstance);
            JanusGraphInstance instance = JanusGraphInstance.valueOf(graphInstance);
            logger.info("instance : {}", instance);
            JanusGraph jGraph = JanusGraphInstanceManagerServiceImpl.getGraphByInstance(instance);
            logger.info("jGraph : {}", jGraph);
            VertexToEdgeRelationConfig config = iJanusGraphInstanceManagerService.getJanusConfigByInstance(instance);

            result = OperationUtils.createOrUpdateVertex(jGraph, vertices, false, config);
        } catch (IllegalArgumentException e) {
            logger.error("Invalid graph instance name: {}", graphInstance);
            throw new InvalidInputException("Invalid graph instance: " + graphInstance, e);
        } catch (GraphicalException e) {
            logger.error("Graph operation error while creating/updating vertices");
            throw e;
        } catch (NullPointerException e) {
            logger.error("Null reference encountered during vertex creation");
            throw new VertexCreationException("Failed to create vertices due to null reference", e);
        } catch (Exception e) {
            logger.error("Error in createOrUpdateVertices: {}", e.getMessage());
            Map<String, String> res = new HashMap<>();
            res.put(JanusGraphUtilsConstants.RESULT_KEY, JanusGraphUtilsConstants.RESULT_FAILURE);
            res.put(JanusGraphUtilsConstants.MESSAGE_KEY, e.getMessage());
            result.clear();
            result.add(res);
        }
        return result;
    }

    /**
     * Retrieves connection information for a specified edge.
     *
     * <p>
     * This method fetches the edge with the given ID and returns a topology object
     * containing both the edge data and the data of vertices connected to it.
     * </p>
     *
     * @param edgeId the ID of the edge to retrieve, must not be null or empty
     * @return a Topology object containing edge and connected vertex data
     * @throws NullPointerException      if edgeId is null
     * @throws InvalidInputException     if edgeId is empty
     * @throws ResourceNotFoundException if the edge is not found
     * @throws GraphicalException        if there is an error during graph
     *                                   operations
     * @throws DataProcessingException   if there is an error processing the data
     */
    @Override
    public Topology getConnectionByEdgeMS(String edgeId) {
        Objects.requireNonNull(edgeId, "Edge ID cannot be null");

        if (StringUtils.isEmpty(edgeId)) {
            throw new InvalidInputException("Edge ID cannot be null or empty");
        }

        Topology topology = new Topology();
        List<Vertex> vertices = new ArrayList<>();

        if (StringUtils.isEmpty(edgeId)) {
            throw new InvalidInputException("Edge ID cannot be null or empty");
        }

        try {
            Edge edge = JanusGraphInstanceManagerServiceImpl
                    .getGraphByTraversalByInstance(JanusGraphInstance.TOPOLOGY_GRAPH).E(edgeId).next();
            topology.setLinks(Arrays.asList(iTopologyHbaseDao.getEdgeData(edge)));

            Iterator<Vertex> bothVertices = edge.bothVertices();
            while (bothVertices.hasNext()) {
                vertices.add(bothVertices.next());
            }
            topology.setNodes(iTopologyHbaseDao.getVertexData(vertices));
        } catch (NoSuchElementException e) {
            logger.error("Edge not found with ID: {}", edgeId);
            throw new ResourceNotFoundException("Edge not found with ID: " + edgeId, e);
        } catch (IllegalArgumentException e) {
            logger.error("Invalid edge ID format: {}", edgeId);
            throw new InvalidInputException("Invalid edge ID format: " + edgeId, e);
        } catch (GraphicalException e) {
            logger.error("Graph operation error while retrieving edge");
            throw e;
        } catch (Exception e) {
            logger.error("Error in getConnectionByEdge");
            throw new DataProcessingException("Failed to retrieve connection for edge: " + edgeId, e);
        }

        return topology;
    }

    /**
     * Retrieves link types associated with specified VNF types.
     *
     * <p>
     * This method finds all edges connected to vertices with the specified element
     * names
     * and returns the set of unique link types from those edges.
     * </p>
     *
     * @param vnfList the list of VNF element names to search for, must not be null
     *                or empty
     * @return a set of link type strings
     * @throws NullPointerException      if vnfList is null
     * @throws InvalidInputException     if vnfList is empty
     * @throws ResourceNotFoundException if the specified VNF elements are not found
     * @throws GraphicalException        if there is an error during graph
     *                                   operations
     * @throws DataProcessingException   if there is an error processing the data
     */
    @Override
    public Set<String> getLinkTypesByVNFTypesMS(List<String> vnfList) {
        Objects.requireNonNull(vnfList, "VNF list cannot be null");

        logger.info("VNF type list is : {}", vnfList);
        Set<String> linkTypeSet = new HashSet<>();

        if (vnfList.isEmpty()) {
            throw new InvalidInputException("VNF list cannot be null or empty");
        }

        try {
            List<Edge> edgeList = JanusGraphInstanceManagerServiceImpl
                    .getGraphByTraversalByInstance(JanusGraphInstance.TOPOLOGY_GRAPH).V()
                    .has(TopologyGraphConstants.ELEMENT_NAME, P.within(vnfList)).bothE().toList();
            List<Map<String, Object>> edgeDataList = iTopologyHbaseDao.getEdgeData(edgeList);
            if (edgeDataList != null && !edgeDataList.isEmpty()) {
                for (Map<String, Object> edgeData : edgeDataList) {
                    if (edgeData.get(TopologyGraphConstants.LINK_TYPE) != null)
                        linkTypeSet.add(edgeData.get(TopologyGraphConstants.LINK_TYPE).toString());
                }
            }
            logger.info("Link type list is : {}", linkTypeSet);
        } catch (NoSuchElementException e) {
            logger.error("VNF elements not found: {}", vnfList);
            throw new ResourceNotFoundException("VNF elements not found: " + vnfList, e);
        } catch (IllegalArgumentException e) {
            logger.error("Invalid VNF list format: {}", vnfList);
            throw new InvalidInputException("Invalid VNF list format", e);
        } catch (GraphicalException e) {
            logger.error("Graph operation error while retrieving link types");
            throw e;
        } catch (Exception e) {
            logger.error("Error in getLinkTypesByVNFTypes");
            throw new DataProcessingException("Failed to retrieve link types for VNF list", e);
        }
        return linkTypeSet;
    }

    /**
     * Retrieves protocols associated with specified VNF types and link types.
     *
     * <p>
     * This method finds all edges connected to vertices with the specified element
     * names
     * and optionally filtered by link types, then returns the set of unique
     * protocols from those edges.
     * </p>
     *
     * @param typeList a map containing VNF type list and optionally link type list,
     *                 must not be null or empty
     * @return a set of protocol strings
     * @throws NullPointerException      if typeList is null
     * @throws InvalidInputException     if typeList is empty or has invalid format
     * @throws ResourceNotFoundException if the specified elements are not found
     * @throws GraphicalException        if there is an error during graph
     *                                   operations
     * @throws DataProcessingException   if there is an error processing the data
     */
    @Override
    public Set<String> getProtocolByVNFTypesMS(Map<String, Object> typeList) {
        Objects.requireNonNull(typeList, "Type list map cannot be null");
        if (typeList.isEmpty()) {
            throw new InvalidInputException("Type list map cannot be empty");
        }

        logger.info("VNF type and link type list is : {}", typeList);
        Set<String> protocolSet = new HashSet<>();

        try {
            List<String> vnfTypeList = castToList(typeList.get(TopologyGraphConstants.VNF_TYPE_LIST));
            if (vnfTypeList == null || vnfTypeList.isEmpty()) {
                throw new InvalidInputException("VNF type list is required");
            }

            List<String> linkTypeList = castToList(typeList.get(TopologyGraphConstants.LINK_TYPE_LIST));

            List<Edge> edgeList = fetchEdgesByVNFAndLinkTypes(vnfTypeList, linkTypeList);

            List<Map<String, Object>> edgeDataList = iTopologyHbaseDao.getEdgeData(edgeList);

            if (edgeDataList != null) {
                for (Map<String, Object> edgeData : edgeDataList) {
                    Object protocol = edgeData.get(TopologyGraphConstants.PROTOCOL);
                    if (protocol != null) {
                        protocolSet.add(protocol.toString());
                    }
                }
            }
            logger.info("Protocol type list is : {}", protocolSet);

        } catch (ClassCastException e) {
            logger.error("Invalid type format in input map: {}", typeList);
            throw new InvalidInputException("Invalid type format in input map", e);
        } catch (NoSuchElementException e) {
            logger.error("VNF elements not found in type list: {}", typeList);
            throw new ResourceNotFoundException("VNF elements not found", e);
        } catch (GraphicalException e) {
            logger.error("Graph operation error while retrieving protocols");
            throw e;
        } catch (Exception e) {
            logger.error("Error in getProtocolByVNFTypes");
            throw new DataProcessingException("Failed to retrieve protocols for VNF types", e);
        }

        return protocolSet;
    }

    // Helper method to safely cast to List<String>
    @SuppressWarnings("unchecked")
    private List<String> castToList(Object obj) {
        if (obj instanceof List<?>) {
            return (List<String>) obj;
        }
        return Collections.emptyList();
    }

    // Helper method to fetch edges based on VNF and Link types
    private List<Edge> fetchEdgesByVNFAndLinkTypes(List<String> vnfTypeList, List<String> linkTypeList) {
        var traversal = JanusGraphInstanceManagerServiceImpl
                .getGraphByTraversalByInstance(JanusGraphInstance.TOPOLOGY_GRAPH).V()
                .has(TopologyGraphConstants.ELEMENT_NAME, P.within(vnfTypeList))
                .bothE();

        if (linkTypeList != null && !linkTypeList.isEmpty()) {
            traversal = traversal.has(TopologyGraphConstants.LINK_TYPE, P.within(linkTypeList));
        }
        return traversal.toList();
    }

    /**
     * Retrieves all data from the Janus graph.
     *
     * <p>
     * This method fetches all vertices and their connected edges from the Janus
     * graph
     * and organizes them into a structured map where keys are element names and
     * values
     * are lists of connected element information.
     * </p>
     *
     * @return a map of element names to lists of connected ElementInfoWrapper
     *         objects
     * @throws GraphicalException      if there is an error during graph traversal
     * @throws DataProcessingException if there is an error processing the graph
     *                                 data
     */
    @Override
    public Map<String, List<ElementInfoWrapper>> getAllJanusGraphData() {
        final String DELETED = "isDeleted";
        Map<String, ElementInfoWrapper> totalVertexMap = new HashMap<>();
        Map<String, List<ElementInfoWrapper>> graphDataMap = new HashMap<>();

        try (GraphTraversalSource g = JanusGraphInstanceManagerServiceImpl
                .getGraphByTraversalByInstance(JanusGraphInstance.TOPOLOGY_GRAPH)) {

            List<Vertex> vertices = g.V().has(DELETED, false).toList();
            logger.info("Vertex size: {}", vertices.size());

            for (Vertex v : vertices) {
                ElementInfoWrapper wrapper = getElementInfoWrapperByVertex(v);
                if (wrapper.getElementName() != null && wrapper.getNeId() != null) {
                    totalVertexMap.put(wrapper.getNeId(), wrapper);
                }
                if (v.id() != null) {
                    totalVertexMap.put(v.id().toString(), wrapper);
                }
            }

            List<Map<String, Object>> graphData = g.V().has(DELETED, false)
                    .as(VERTEX)
                    .bothE()
                    .as(CONNECTED)
                    .select(VERTEX, CONNECTED)
                    .toList();

            logger.info("Edges size: {}", graphData.size());

            graphDataMap = getGraphData(graphData, totalVertexMap);

        } catch (IllegalStateException e) {
            logger.error("Graph traversal error in getAllJanusGraphData");
            throw new GraphicalException("Graph traversal error while retrieving all graph data", e);
        } catch (Exception e) {
            logger.error("Exception inside getAllJanusGraphData");
            throw new DataProcessingException("Failed to retrieve all Janus graph data", e);
        }

        return graphDataMap;
    }

    /**
     * Retrieves affected nodes and their types based on a given node name,
     * termination type, and allowed types.
     * This method traverses the graph starting from the specified node and follows
     * paths that match the allowed
     * types until reaching nodes of the specified termination type.
     *
     * <p>
     * Example usage:
     * </p>
     *
     * <pre>
     * Set<Map<String, String>> affected = getAffectedNodesByNodeName("router1", "ROUTER", "SWITCH,ROUTER");
     * // Returns set of maps containing element names and types of affected
     * // nodes
     * </pre>
     *
     * @param eName           the name of the element to start traversal from
     * @param terminationType the element type that will terminate the path
     *                        traversal
     * @param allowType       comma-separated list of element types allowed in the
     *                        traversal path
     * @return a set of maps containing element names and types of affected nodes
     * @throws IllegalArgumentException  if any input parameter is null or empty
     * @throws ResourceNotFoundException if the specified element is not found
     * @throws BusinessException         if there is an error during graph traversal
     * @throws DataProcessingException   if there is a general error processing the
     *                                   data
     */
    @Override
    public Set<Map<String, String>> getAffectedNodesByNodeName(String eName, String terminationType, String allowType) {
        Objects.requireNonNull(eName, TopologyGraphConstants.ELEMENT_NAME_NOT_BE_NULL);
        Objects.requireNonNull(terminationType, "Termination type cannot be null");
        Objects.requireNonNull(allowType, "Allow type cannot be null");

        logger.info("Start getAffectedNodesByNodeName with eName: {}, terminationType: {}, allowType: {}", eName,
                terminationType, allowType);

        validateNotEmpty(eName, "Element name");
        validateNotEmpty(terminationType, "Termination type");
        validateNotEmpty(allowType, "Allow type");

        Set<Map<String, String>> affectedNodesAndTypes = new HashSet<>();

        try {
            Vertex next = JanusGraphInstanceManagerServiceImpl
                    .getGraphByTraversalByInstance(JanusGraphInstance.TOPOLOGY_GRAPH)
                    .V().has(TopologyGraphConstants.ELEMENT_NAME, eName)
                    .next();

            logger.info("Vertex found for element name: {}", eName);

            Set<Map<String, Object>> vertexData = iTopologyHbaseDao.getVertexData(Arrays.asList(next));
            logger.info("vertexData fetched: {}", vertexData);

            String gcName = extractGcName(vertexData);

            List<String> allowedTypesInPath = Arrays.asList(allowType.split(","));
            logger.info("allowedTypesInPath: {}", allowedTypesInPath);

            if (gcName != null && !allowedTypesInPath.isEmpty()) {
                processPaths(affectedNodesAndTypes,
                        getConnectedGraphByFilter(next, terminationType, gcName, allowedTypesInPath),
                        "vertexPathTraversed");

                Set<Path> pathTraversed = JanusGraphInstanceManagerServiceImpl
                        .getGraphByTraversalByInstance(JanusGraphInstance.TOPOLOGY_GRAPH).V().has(ENAME, eName)
                        .repeat(bothE().otherV().has(TopologyGraphConstants.ELEMENT_TYPE, P.within(allowedTypesInPath))
                                .has("gcName", gcName).simplePath())
                        .until(has(TopologyGraphConstants.ELEMENT_TYPE, terminationType))
                        .path().toSet();

                processPaths(affectedNodesAndTypes, pathTraversed, "pathTraversed");
            } else {
                logger.warn("gcName is null or allowedTypesInPath is empty; skipping path processing.");
            }

        } catch (NoSuchElementException e) {
            logger.error("Element not found for name: {}", eName);
            throw new ResourceNotFoundException(TopologyGraphConstants.ELEMENT_NOT_FOUND_WITH_NAME + eName, e);
        } catch (IllegalStateException e) {
            logger.error("Graph traversal error for element: {}", eName);
            throw new BusinessException("Graph traversal error while retrieving affected nodes", e);
        } catch (Exception e) {
            logger.error("Error while retrieving affected nodes for: {}", eName);
            throw new DataProcessingException("Failed to retrieve affected nodes for: " + eName, e);
        }

        logger.info("Returning affectedNodesAndTypes: {}", affectedNodesAndTypes);
        return affectedNodesAndTypes;
    }

    /**
     * Validates that a string value is not empty after trimming.
     *
     * @param value     the string to validate
     * @param fieldName the name of the field being validated (for error messages)
     * @throws IllegalArgumentException if the string is empty after trimming
     */
    private void validateNotEmpty(String value, String fieldName) {
        logger.info("Validating not empty for field: {}", fieldName);
        if (value.trim().isEmpty()) {
            logger.error("{} validation failed: empty string", fieldName);
            throw new IllegalArgumentException(fieldName + " cannot be empty");
        }
        logger.info("{} validation passed", fieldName);
    }

    /**
     * Extracts the graph context name (gcName) from vertex data.
     *
     * @param vertexData set of maps containing vertex properties
     * @return the extracted gcName value, or null if not found
     */
    private String extractGcName(Set<Map<String, Object>> vertexData) {
        logger.info("Extracting gcName from vertexData: {}", vertexData);
        for (Map<String, Object> vertex : vertexData) {
            Object gcNameValue = vertex.get(TopologyGraphConstants.GC_NAME);
            if (gcNameValue != null) {
                String gcName = gcNameValue.toString();
                logger.info("gcName extracted: {}", gcName);
                return gcName;
            }
        }
        logger.warn("gcName not found in vertexData");
        return null;
    }

    /**
     * Processes graph paths to extract and store affected node information.
     *
     * @param affectedNodes set to store the processed node information
     * @param paths         set of paths to process
     * @param logLabel      label to use in log messages for identifying the path
     *                      type
     */
    private void processPaths(Set<Map<String, String>> affectedNodes, Set<Path> paths, String logLabel) {
        if (paths == null) {
            logger.warn("{} is null, skipping processing.", logLabel);
            return;
        }
        logger.info("{} size: {}", logLabel, paths.size());
        for (Path path : paths) {
            logger.info("{} processing path: {}", logLabel, path);
            List<Map<String, Object>> vertexList = topologyGraphUtils.extractVertexFromPath(path);
            logger.info("{} -> extractVertexFromPath result: {}", logLabel, vertexList);
            for (Map<String, Object> vertex : vertexList) {
                Map<String, String> vertexData = new HashMap<>();
                vertexData.put(ENAME, String.valueOf(vertex.get(ENAME)));
                vertexData.put("eType", String.valueOf(vertex.get("eType")));
                logger.info("{} -> adding vertexData: {}", logLabel, vertexData);
                affectedNodes.add(vertexData);
            }
        }
        logger.info("{} processing completed. Total nodes added: {}", logLabel, affectedNodes.size());
    }

    /**
     * Retrieves connected graph paths based on specified filtering criteria.
     *
     * <p>
     * This method traverses the graph starting from the source node and follows
     * paths
     * that match the allowed types until reaching nodes of the specified
     * termination type.
     * </p>
     *
     * @param srcNode            the source vertex to start traversal from
     * @param terminationType    the element type that will terminate the path
     *                           traversal
     * @param gcName             the graph context name to filter nodes by
     * @param allowedTypesInPath list of element types allowed in the traversal path
     * @return a set of paths connecting the source node to nodes of the termination
     *         type
     * @throws IllegalArgumentException if any of the parameters are empty or
     *                                  invalid
     * @throws BusinessException        if there is an error during graph traversal
     * @throws DataProcessingException  if there is a general error processing the
     *                                  data
     */
    private Set<Path> getConnectedGraphByFilter(Vertex srcNode, String terminationType, String gcName,
                                                List<String> allowedTypesInPath) {
        Objects.requireNonNull(srcNode, "Source node cannot be null");
        Objects.requireNonNull(terminationType, "Termination type cannot be null");
        Objects.requireNonNull(gcName, "GC name cannot be null");
        Objects.requireNonNull(allowedTypesInPath, "Allowed types list cannot be null");

        if (terminationType.trim().isEmpty()) {
            throw new IllegalArgumentException("Termination type cannot be empty");
        }
        if (gcName.trim().isEmpty()) {
            throw new IllegalArgumentException("GC name cannot be empty");
        }
        if (allowedTypesInPath.isEmpty()) {
            throw new IllegalArgumentException("Allowed types list cannot be empty");
        }

        try {
            return JanusGraphInstanceManagerServiceImpl
                    .getGraphByTraversalByInstance(JanusGraphInstance.TOPOLOGY_GRAPH)
                    .V(srcNode.id())
                    .has(TopologyGraphConstants.GC_NAME, gcName)
                    .repeat(both()
                            .has(TopologyGraphConstants.ELEMENT_TYPE, P.within(allowedTypesInPath))
                            .simplePath())
                    .until(has(TopologyGraphConstants.ELEMENT_TYPE, terminationType))
                    .path()
                    .toSet();
        } catch (IllegalStateException e) {
            logger.error("Graph traversal error in getConnectedGraphByFilter");
            throw new BusinessException("Graph traversal error while retrieving connected graph", e);
        } catch (Exception e) {
            logger.error("Error in getConnectedGraphByFilter");
            throw new DataProcessingException("Failed to retrieve connected graph data", e);
        }
    }

    /**
     * Retrieves topology information connecting a server to its Top-of-Rack (TOR)
     * switches.
     *
     * <p>
     * This method builds a topology object containing nodes and links that
     * represent
     * the connections between a server and its associated TOR switches.
     * </p>
     *
     * @param eName the element name to retrieve topology information for
     * @return a Topology object containing nodes and links information
     * @throws IllegalArgumentException  if eName is empty
     * @throws ResourceNotFoundException if the specified element is not found
     * @throws DataProcessingException   if there is an error processing the data
     */
    @Override
    public Topology getServerToTorInformation(String eName) {
        Objects.requireNonNull(eName, TopologyGraphConstants.ELEMENT_NAME_NOT_BE_NULL);
        if (eName.trim().isEmpty()) {
            throw new IllegalArgumentException(TopologyGraphConstants.ELEMENT_NAME_CANNOT_BE_EMPTY);
        }

        logger.info("value of eName in getServerToTorInformation:{}", eName);
        Topology topology = new Topology();
        List<Map<String, Object>> edgeListFromVertex = new ArrayList<>();
        Set<Map<String, Object>> setOfVertex = new HashSet<>();

        try {
            List<Edge> edgeList = fetchEdgesForEName(eName);
            logger.info("value of edgeList in getServerToTorInformation is:{}", edgeList);

            if (!edgeList.isEmpty()) {
                populateEdgeData(edgeList, edgeListFromVertex);
                populateVertexData(edgeList, setOfVertex);
            }

            logger.info("value of edgeListFromVertex in getServerToTorInformation is:{} and vertexList:{}",
                    edgeListFromVertex.size(), setOfVertex.size());

            topology.setLinks(edgeListFromVertex.isEmpty() ? null : edgeListFromVertex);
            topology.setNodes(setOfVertex.isEmpty() ? null : setOfVertex);

            return topology;
        } catch (NoSuchElementException e) {
            logger.error("Element not found for name: {}", eName);
            throw new ResourceNotFoundException(TopologyGraphConstants.ELEMENT_NOT_FOUND_WITH_NAME + eName, e);
        } catch (Exception e) {
            logger.error("Error in getServerToTorInformation for element: {}", eName);
            throw new DataProcessingException("Failed to retrieve server to TOR information for: " + eName, e);
        }
    }

    /**
     * Fetches all edges connected to an element with the specified name.
     *
     * <p>
     * This method retrieves edges that represent SWITCH-SERVER connections
     * in the logical category for the specified element.
     * </p>
     *
     * @param eName the element name to fetch edges for
     * @return a list of edges connected to the specified element
     * @throws IllegalArgumentException if eName is empty
     * @throws BusinessException        if there is an error during graph traversal
     * @throws DataProcessingException  if there is a general error processing the
     *                                  data
     */
    private List<Edge> fetchEdgesForEName(String eName) {
        Objects.requireNonNull(eName, TopologyGraphConstants.ELEMENT_NAME_NOT_BE_NULL);
        if (eName.trim().isEmpty()) {
            throw new IllegalArgumentException(TopologyGraphConstants.ELEMENT_NAME_CANNOT_BE_EMPTY);
        }

        try {
            return JanusGraphInstanceManagerServiceImpl
                    .getGraphByTraversalByInstance(JanusGraphInstance.TOPOLOGY_GRAPH).V()
                    .has(TopologyGraphConstants.ELEMENT_NAME, eName)
                    .has(TopologyGraphConstants.DELETED, false)
                    .bothE()
                    .has(TopologyGraphConstants.CONNECTION_CATEGORY, "SWITCH-SERVER")
                    .has(TopologyGraphConstants.CATEGORY, TopologyGraphConstants.CATEGORY_LOGICAL)
                    .toList();
        } catch (IllegalStateException e) {
            logger.error("Graph traversal error in fetchEdgesForEName");
            throw new BusinessException("Graph traversal error while fetching edges", e);
        } catch (Exception e) {
            logger.error("Error in fetchEdgesForEName for element: {}", eName);
            throw new DataProcessingException("Failed to fetch edges for element: " + eName, e);
        }
    }

    /**
     * Populates edge data from the provided edge list into the target collection.
     *
     * <p>
     * This method retrieves detailed edge data from HBase and adds it to the
     * provided collection.
     * </p>
     *
     * @param edgeList           the list of edges to retrieve data for
     * @param edgeListFromVertex the target collection to populate with edge data
     * @throws DataProcessingException if there is an error retrieving or processing
     *                                 edge data
     */
    private void populateEdgeData(List<Edge> edgeList, List<Map<String, Object>> edgeListFromVertex) {
        Objects.requireNonNull(edgeList, TopologyGraphConstants.EDGE_LIST_NOT_BE_NULL);
        Objects.requireNonNull(edgeListFromVertex, "Edge list from vertex cannot be null");

        try {
            List<Map<String, Object>> edgeData = iTopologyHbaseDao.getEdgeData(edgeList);
            if (edgeData != null && !edgeData.isEmpty()) {
                for (Map<String, Object> propertyMapByEdge : edgeData) {
                    if (propertyMapByEdge != null && !propertyMapByEdge.isEmpty()) {
                        edgeListFromVertex.add(propertyMapByEdge);
                    }
                }
            }
            logger.info("value of edgeListFromVertex in getServerToTorInformation is:{}", edgeListFromVertex.size());
        } catch (Exception e) {
            logger.error("Error in populateEdgeData");
            throw new DataProcessingException("Failed to populate edge data", e);
        }
    }

    /**
     * Populates vertex data for all vertices connected by the provided edges.
     *
     * <p>
     * This method retrieves vertices connected by the edges, fetches their data
     * from HBase, and adds it to the provided set.
     * </p>
     *
     * @param edgeList    the list of edges to find connected vertices for
     * @param setOfVertex the target set to populate with vertex data
     * @throws BusinessException       if there is an error during graph traversal
     * @throws DataProcessingException if there is a general error processing the
     *                                 data
     */
    private void populateVertexData(List<Edge> edgeList, Set<Map<String, Object>> setOfVertex) {
        Objects.requireNonNull(edgeList, TopologyGraphConstants.EDGE_LIST_NOT_BE_NULL);
        Objects.requireNonNull(setOfVertex, TopologyGraphConstants.VERTEX_SET_NOT_BE_NULL);

        try {
            List<Vertex> vertexDataList = JanusGraphInstanceManagerServiceImpl
                    .getGraphByTraversalByInstance(JanusGraphInstance.TOPOLOGY_GRAPH).E(edgeList).bothV().toList();
            logger.info("vertexDataList values are :{}", vertexDataList);

            Set<Map<String, Object>> vertexDataSet = iTopologyHbaseDao.getVertexData(vertexDataList);
            if (vertexDataSet != null && !vertexDataSet.isEmpty()) {
                for (Map<String, Object> vertex : vertexDataSet) {
                    Map<String, Object> propertyMapByVertex = setMapForVertex(vertex);
                    logger.info("going to take propertyMapByVertex : {} ", propertyMapByVertex);
                    if (!propertyMapByVertex.isEmpty()) {
                        logger.info("Found vertex corresponding to the name : {}", propertyMapByVertex.get(ENAME));
                        setOfVertex.add(propertyMapByVertex);
                    }
                }
            }
        } catch (IllegalStateException e) {
            logger.error("Graph traversal error in populateVertexData");
            throw new BusinessException("Graph traversal error while populating vertex data", e);
        } catch (Exception e) {
            logger.error("Error in populateVertexData");
            throw new DataProcessingException("Failed to populate vertex data", e);
        }
    }

    /**
     * Creates a standardized map of vertex properties from the raw vertex data.
     *
     * <p>
     * This method extracts relevant properties from the vertex data and formats
     * them
     * consistently for use in the topology representation.
     * </p>
     *
     * @param propertyMapByVertex the raw vertex property map from HBase
     * @return a standardized map containing the vertex properties
     * @throws DataProcessingException if there is an error processing numeric
     *                                 values
     * @throws BusinessException       if there is an error with the vertex data
     *                                 format
     */
    private Map<String, Object> setMapForVertex(Map<String, Object> propertyMapByVertex) {
        Objects.requireNonNull(propertyMapByVertex, TopologyGraphConstants.PROPERTY_MAP_BY_VERTEX_NOT_BE_NULL);

        logger.info("Inside SetMapForVertex : {}", propertyMapByVertex);
        Map<String, Object> vertexMap = new HashMap<>();

        try {
            // Mandatory fields
            vertexMap.put(TopologyGraphConstants.ELEMENT_NAME,
                    propertyMapByVertex.get(TopologyGraphConstants.ELEMENT_NAME));
            vertexMap.put(TopologyGraphConstants.ELEMENT_TYPE,
                    propertyMapByVertex.get(TopologyGraphConstants.ELEMENT_TYPE));

            // Geography Levels using helper
            putGeographyLevel(vertexMap, propertyMapByVertex,
                    TopologyGraphConstants.GEOGRAPHYL1_DISPLAY, TopologyGraphConstants.GEOGRAPHY_LEVEL1);
            putGeographyLevel(vertexMap, propertyMapByVertex,
                    TopologyGraphConstants.GEOGRAPHYL2_DISPLAY, TopologyGraphConstants.GEOGRAPHY_LEVEL2);
            putGeographyLevel(vertexMap, propertyMapByVertex,
                    TopologyGraphConstants.GEOGRAPHYL3_DISPLAY, TopologyGraphConstants.GEOGRAPHY_LEVEL3);

            // Latitude & Longitude with decoding
            putDecodedIfPresent(vertexMap, propertyMapByVertex, TopologyGraphConstants.LATITUDE);
            putDecodedIfPresent(vertexMap, propertyMapByVertex, TopologyGraphConstants.LONGITUDE);

            // Optional fields - simple put if present
            putIfPresent(vertexMap, propertyMapByVertex, TopologyGraphConstants.OUTAGE);
            putIfPresent(vertexMap, propertyMapByVertex, TopologyGraphConstants.DETERIORATION);
            putIfPresent(vertexMap, propertyMapByVertex, TopologyGraphConstants.NORMAL);
            putIfPresent(vertexMap, propertyMapByVertex, TopologyGraphConstants.DP_NAME);

            // DOMAIN and VENDOR both must be present
            if (propertyMapByVertex.get(TopologyGraphConstants.DOMAIN) != null
                    && propertyMapByVertex.get(TopologyGraphConstants.VENDOR) != null) {
                vertexMap.put(TopologyGraphConstants.DOMAIN, propertyMapByVertex.get(TopologyGraphConstants.DOMAIN));
                vertexMap.put(TopologyGraphConstants.VENDOR, propertyMapByVertex.get(TopologyGraphConstants.VENDOR));
            }

            // Remaining optional fields
            putIfPresent(vertexMap, propertyMapByVertex, TopologyGraphConstants.ALIASE_ELEMENT_TYPE);
            putIfPresent(vertexMap, propertyMapByVertex, TopologyGraphConstants.RING_ID);
            putIfPresent(vertexMap, propertyMapByVertex, TopologyGraphConstants.GC_NAME);

            // NODE_PRIORITY as string
            Object nodePriority = propertyMapByVertex.get(TopologyGraphConstants.NODE_PRIORITY);
            if (nodePriority != null) {
                vertexMap.put(TopologyGraphConstants.NODE_PRIORITY, nodePriority.toString());
            }

        } catch (NumberFormatException e) {
            logger.error("Error parsing numeric value in setMapForVertex");
            throw new DataProcessingException("Failed to parse numeric value in vertex data", e);
        } catch (IllegalArgumentException e) {
            logger.error("Invalid argument in setMapForVertex");
            throw new BusinessException("Invalid vertex data format", e);
        } catch (NullPointerException e) {
            logger.error("Null reference in setMapForVertex");
            throw new DataProcessingException("Null reference encountered while processing vertex data", e);
        }

        return vertexMap;
    }

    // Helper methods

    private void putGeographyLevel(Map<String, Object> vertexMap, Map<String, Object> sourceMap,
                                   Object geographyl1Display, String levelKey) {
        Object value = sourceMap.get(geographyl1Display) != null ? sourceMap.get(geographyl1Display)
                : sourceMap.get(levelKey);
        if (value != null) {
            vertexMap.put(levelKey, value);
        }
    }

    private void putDecodedIfPresent(Map<String, Object> vertexMap, Map<String, Object> sourceMap, String key) {
        Object value = sourceMap.get(key);
        if (value != null) {
            vertexMap.put(key, topologyGraphUtils.decodeNumeric(value.toString()));
        }
    }

    private void putIfPresent(Map<String, Object> vertexMap, Map<String, Object> sourceMap, String key) {
        Object value = sourceMap.get(key);
        if (value != null) {
            vertexMap.put(key, value);
        }
    }

    /**
     * Retrieves property maps for a set of edges with additional vertex data.
     *
     * <p>
     * This method fetches edge data from HBase and enriches it with domain and
     * vendor
     * information from the source vertex.
     * </p>
     *
     * @param edgeList the set of edges to retrieve property maps for
     * @return a list of property maps for the edges with enriched data
     * @throws ResourceNotFoundException if a source element is not found
     * @throws GraphicalException        if there is an error traversing the graph
     * @throws BusinessException         if there is an error with the edge data
     *                                   format
     * @throws DataProcessingException   if there is a general error processing the
     *                                   data
     */
    public List<Map<String, Object>> getPropertyMapByEdgeUpdated(Set<Edge> edgeList) {
        Objects.requireNonNull(edgeList, TopologyGraphConstants.EDGE_LIST_NOT_BE_NULL);

        List<Map<String, Object>> propertyMap = new ArrayList<>();
        if (edgeList.isEmpty()) {
            return propertyMap;
        }

        try {
            List<Map<String, Object>> edgeDataSet = iTopologyHbaseDao.getEdgeData(edgeList);
            if (edgeDataSet == null || edgeDataSet.isEmpty()) {
                return propertyMap;
            }

            logger.info("Processing {} edges from HBase", edgeDataSet.size());

            for (Map<String, Object> edgeData : edgeDataSet) {
                processSingleEdge(edgeData, propertyMap);
            }

            logger.info("Successfully processed {} edges with vertex data", propertyMap.size());

        } catch (ResourceNotFoundException e) {
            throw e;
        } catch (IllegalStateException e) {
            logger.error("Graph traversal error in getPropertyMapByEdgeUpdated");
            throw new GraphicalException("Error traversing graph for edge data", e);
        } catch (IllegalArgumentException e) {
            logger.error("Invalid argument in getPropertyMapByEdgeUpdated");
            throw new BusinessException("Invalid edge data format", e);
        } catch (NullPointerException e) {
            logger.error("Null reference in getPropertyMapByEdgeUpdated");
            throw new DataProcessingException("Null reference encountered while processing edge data", e);
        }
        return propertyMap;
    }

    private void processSingleEdge(Map<String, Object> edgeData, List<Map<String, Object>> propertyMap) {
        String srcElementName = edgeData.get(TopologyGraphConstants.SOURCE_ELEMENT_NAME) != null
                ? edgeData.get(TopologyGraphConstants.SOURCE_ELEMENT_NAME).toString()
                : null;

        if (srcElementName == null) {
            logger.warn("Source element name is null in edge data: {}", edgeData);
            return; // skip processing this edge
        }

        try {
            Vertex vertex = JanusGraphInstanceManagerServiceImpl
                    .getGraphByTraversalByInstance(JanusGraphInstance.TOPOLOGY_GRAPH).V()
                    .has(TopologyGraphConstants.ELEMENT_NAME, srcElementName).next();

            Map<String, Object> vertexData = iTopologyHbaseDao.getVertexData(vertex);

            if (vertexData != null && !vertexData.isEmpty()) {
                edgeData.put(TopologyGraphConstants.DOMAIN, vertexData.get(TopologyGraphConstants.DOMAIN));
                edgeData.put(TopologyGraphConstants.VENDOR, vertexData.get(TopologyGraphConstants.VENDOR));
                propertyMap.add(edgeData);
            }
        } catch (NoSuchElementException ne) {
            logger.warn("Source element not found: {}", srcElementName);
            throw new ResourceNotFoundException("Source element not found: " + srcElementName, ne);
        }
    }

    /**
     * Retrieves element information wrapper for a specified element name.
     *
     * <p>
     * This method looks up an element by name and returns its information
     * in a wrapper object.
     * </p>
     *
     * @param eName the element name to retrieve information for
     * @return an ElementInfoWrapper containing the element information, or an empty
     *         wrapper if not found
     * @throws InvalidInputException if eName is null or empty
     */
    public ElementInfoWrapper getElementInfoWrapperByEName(String eName) {
        Objects.requireNonNull(eName, TopologyGraphConstants.ELEMENT_NAME_NOT_BE_NULL);
        if (StringUtils.isEmpty(eName)) {
            logger.warn("Element name is null or empty in getElementInfoWrapperByEName");
            throw new InvalidInputException(TopologyGraphConstants.ELEMENT_NAME_CANNOT_BE_NULL_OR_EMPTY);
        }

        try {
            logger.info("Retrieving element info for element: {}", eName);
            Vertex vertex = JanusGraphInstanceManagerServiceImpl
                    .getGraphByTraversalByInstance(JanusGraphInstance.TOPOLOGY_GRAPH).V()
                    .has(TopologyGraphConstants.ELEMENT_NAME, eName).next();
            return getElementInfoWrapperByVertex(vertex);
        } catch (Exception e) {
            logger.error("Error retrieving element info for {}: {}", eName, e.getMessage(), e);
            return new ElementInfoWrapper();
        }
    }

}
