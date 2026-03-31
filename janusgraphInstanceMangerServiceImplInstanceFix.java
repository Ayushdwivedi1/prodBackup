package com.enttribe.module.topology.core.graphinstancemgmt.service.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;
import com.enttribe.commons.Symbol;
import com.enttribe.commons.configuration.ConfigUtils;
import com.enttribe.module.topology.core.graphinstancemgmt.constants.JanusGraphInstanceConstants;
import com.enttribe.module.topology.core.graphinstancemgmt.enums.JanusGraphInstance;
import com.enttribe.module.topology.core.graphinstancemgmt.exceptions.GroovyScriptInitializationFailedException;
import com.enttribe.module.topology.core.graphinstancemgmt.service.JanusGraphInstanceManagerService;
import com.enttribe.module.topology.core.janusgraphutils.wrapper.VertexToEdgeRelationConfig;
import com.enttribe.platform.configuration.baseconfig.rest.BaseConfigurationRest;

/**
 * Implementation of the JanusGraph instance manager service that handles graph initialization,
 * traversal setup, and configuration management for topology services.
 *
 * <p>This service manages multiple JanusGraph instances, providing methods to initialize,
 * access, and configure graph databases used for topology management. It maintains
 * separate maps for graph instances, traversal sources, and Groovy script engines.</p>
 *
 * <p>Example usage:</p>
 * <pre>
 * JanusGraphInstanceManagerServiceImpl manager = new JanusGraphInstanceManagerServiceImpl();
 * Map<String, String> result = manager.initAllGraphs();
 * </pre>
 *
 * @author VisionWaves
 * @version 1.0
 * @since 1.0
 */
@Service("janusGraphInstanceManagerServiceImpl")
public class JanusGraphInstanceManagerServiceImpl implements JanusGraphInstanceManagerService {

    private static final Logger logger = LoggerFactory.getLogger(JanusGraphInstanceManagerServiceImpl.class);

    @Autowired
    private BaseConfigurationRest systemConfigRest;

    /**
     * Map containing all JanusGraph instances managed by this service.
     * Keys are instance names as strings, values are the corresponding JanusGraph objects.
     */
    private static Map<String, JanusGraph> graphs = new HashMap<>();

    /**
     * Map containing graph traversal sources for each JanusGraph instance.
     * Keys are instance names as strings, values are the corresponding GraphTraversalSource objects.
     */
    private static Map<String, GraphTraversalSource> traversals = new HashMap<>();

    /**
     * Map containing Gremlin Groovy script engines for each JanusGraph instance.
     * Keys are instance names as strings, values are the corresponding GremlinGroovyScriptEngine objects.
     */
    private static Map<String, GremlinGroovyScriptEngine> gremlinGroovyScriptEngines = new HashMap<>();

    /**
     * Retrieves a JanusGraph instance by its enum identifier.
     *
     * @param instance the JanusGraph instance enum to retrieve
     * @return the JanusGraph instance corresponding to the provided enum
     */
    public static JanusGraph getGraphByInstance(JanusGraphInstance instance) {
        Objects.requireNonNull(instance, JanusGraphInstanceConstants.INSTANCE_NOT_NULL);
        return graphs.get(instance.toString());
    }

    /**
     * Retrieves a GraphTraversalSource for a specific JanusGraph instance.
     *
     * @param instance the JanusGraph instance enum to get the traversal for
     * @return the GraphTraversalSource for the specified instance
     */
    public static GraphTraversalSource getGraphByTraversalByInstance(JanusGraphInstance instance) {
        Objects.requireNonNull(instance, JanusGraphInstanceConstants.INSTANCE_NOT_NULL);
        logger.info("getGraphByTraversalByInstance is");
        return traversals.get(instance.toString());
    }

    /**
     * Retrieves a GremlinGroovyScriptEngine for a specific JanusGraph instance.
     *
     * @param instance the JanusGraph instance enum to get the script engine for
     * @return the GremlinGroovyScriptEngine for the specified instance
     */
    public static GremlinGroovyScriptEngine getGremlinGroovyScriptEngineByInstance(JanusGraphInstance instance) {
        Objects.requireNonNull(instance, JanusGraphInstanceConstants.INSTANCE_NOT_NULL);
        return gremlinGroovyScriptEngines.get(instance.toString());
    }

    /**
     * Initializes all JanusGraph instances defined in the JanusGraphInstance enum.
     *
     * <p>This method iterates through all available JanusGraph instances and
     * initializes each one, collecting the results of each initialization.</p>
     *
     * @return a map containing initialization results for each graph instance
     */
    @Override
    public Map<String, String> initAllGraphs() {
        logger.info("initAllGraphs on date : {}", new Date());
        Map<String, String> result = new HashMap<>();

        JanusGraphInstance[] instances = JanusGraphInstance.values();
        logger.info("initAllGraphs instances list size : {}", instances.length);

        for (JanusGraphInstance instance : instances) {
            result.putAll(initGraphByInstance(instance));
        }
        return result;
    }

    /**
     * Initializes a specific JanusGraph instance with its configuration.
     *
     * <p>This method retrieves the properties for the specified instance,
     * validates them, and then initializes the graph, traversal, and
     * Gremlin script engine for that instance.</p>
     *
     * @param instance the JanusGraph instance to initialize
     * @return a map containing the initialization result for the specified instance
     */
    @Override
    public Map<String, String> initGraphByInstance(JanusGraphInstance instance) {
        Objects.requireNonNull(instance, JanusGraphInstanceConstants.INSTANCE_NOT_NULL);
        Map<String, String> result = new HashMap<>();

        logger.info("initGraphByInstance instance is");

        Map<String, String> propertiesMap = getJanusPropertiesByInstance(instance);
        logger.info("initGraphByInstance propertiesMap size is : {}", propertiesMap.size());


        try {
            if (validateProperties(propertiesMap, instance)) {
                String properties = propertiesMap.get(instance.toString() + JanusGraphInstanceConstants.TOPOLOGY_GRAPH_PROPERTIES_SUB_STRING);
                if (properties == null || properties.trim().isEmpty()) {
                    throw new IllegalArgumentException("Graph properties cannot be null or empty for instance: " + instance);
                }

                logger.info("initGraphByInstance properties is : {}", properties);
                initGraph(properties, instance);
                initTraversal(instance);
                initGremlinGroovyScriptEngine(instance);
                result.put(instance.toString(), JanusGraphInstanceConstants.GRAPH_INIT_SUCCESS_STRING);
            } else {
                result.put(instance.toString(), JanusGraphInstanceConstants.GRAPH_INIT_INVALID_CONFIG_OR_PROPERTIES);
            }
        } catch (Exception e) {
            logger.error("Exception in initAllGraphs : {} for instance : {}", e.getMessage(),
                    instance);
            result.put(instance.toString(), JanusGraphInstanceConstants.ERROR_MESSAGE_SUB_STRING + Symbol.SPACE
                    + Symbol.COLON + Symbol.SPACE + e.getMessage());
        }

        logger.info("initGraphByInstance result size is : {}", result.size());
        return result;
    }

    /**
     * Initializes a JanusGraph instance only if it is not already available.
     *
     * <p>This method checks if the specified instance is already initialized,
     * and if not, proceeds with initialization using the instance's properties.</p>
     *
     * @param instance the JanusGraph instance to initialize if not available
     * @return a map containing the initialization result or a message indicating the graph is already present
     */
    @Override
    public Map<String, String> initGraphByInstanceIfNotAvailable(JanusGraphInstance instance) {
        Objects.requireNonNull(instance, JanusGraphInstanceConstants.INSTANCE_NOT_NULL);
        Map<String, String> result = new HashMap<>();

        JanusGraph graphByInstance = getGraphByInstance(instance);

        if (graphByInstance == null) {
            Map<String, String> propertiesMap = getJanusPropertiesByInstance(instance);
            if (propertiesMap == null || propertiesMap.isEmpty()) {
                result.put(instance.toString(), "Failed to retrieve properties for instance: " + instance);
                return result;
            }

            try {
                if (validateProperties(propertiesMap, instance)) {
                    String properties = propertiesMap.get(instance.toString() + JanusGraphInstanceConstants.TOPOLOGY_GRAPH_PROPERTIES_SUB_STRING);
                    if (properties == null || properties.trim().isEmpty()) {
                        throw new IllegalArgumentException("Graph properties cannot be null or empty for instance: " + instance);
                    }

                    initGraph(properties, instance);
                    initTraversal(instance);
                    initGremlinGroovyScriptEngine(instance);
                    result.put(instance.toString(), JanusGraphInstanceConstants.GRAPH_INIT_SUCCESS_STRING);
                } else {
                    result.put(instance.toString(),
                            JanusGraphInstanceConstants.GRAPH_INIT_INVALID_CONFIG_OR_PROPERTIES);
                }
            } catch (Exception e) {
                logger.error("Exception in initAllGraphs : {} for instance : {}", e.getMessage(),
                        instance);
                result.put(instance.toString(), JanusGraphInstanceConstants.ERROR_MESSAGE_SUB_STRING + Symbol.SPACE
                        + Symbol.COLON + Symbol.SPACE + e.getMessage());
            }
        } else {
            result.put(instance.toString(), JanusGraphInstanceConstants.MESSAGE_GRAPH_ALREADY_PRESENT);
        }
        return result;
    }

    /**
     * Initializes a GremlinGroovyScriptEngine for the specified JanusGraph instance.
     *
     * <p>This method creates a new GremlinGroovyScriptEngine and associates it with
     * the graph traversal source for the specified instance.</p>
     *
     * @param instance the JanusGraph instance to initialize the script engine for
     * @throws GroovyScriptInitializationFailedException if initialization of the Groovy script engine fails
     */
    private void initGremlinGroovyScriptEngine(JanusGraphInstance instance) throws GroovyScriptInitializationFailedException {
        Objects.requireNonNull(instance, JanusGraphInstanceConstants.INSTANCE_NOT_NULL);

        if (isInstanceOpen(instance) && getGraphByTraversalByInstance(instance) != null) {
            GremlinGroovyScriptEngine gremlinGroovyScriptEngine = null;
            try {
                gremlinGroovyScriptEngine = new GremlinGroovyScriptEngine();
                gremlinGroovyScriptEngine.put(JanusGraphInstanceConstants.TOPOLOGY_GRAPH_TRAVERSAL,
                        getGraphByTraversalByInstance(instance));
                gremlinGroovyScriptEngines.put(instance.toString(), gremlinGroovyScriptEngine);
            } catch (Exception e) {
                logger.error("Exception in initGremlinGroovyScriptEngine");
                throw new GroovyScriptInitializationFailedException();
            }
        }
    }

    /**
     * Retrieves JanusGraph properties for the specified instance from the configuration system.
     *
     * <p>This method fetches the properties needed to initialize a JanusGraph instance
     * from the system configuration service.</p>
     *
     * @param instance the JanusGraph instance to get properties for
     * @return a map containing the properties for the specified instance
     */
    @Override
    public Map<String, String> getJanusPropertiesByInstance(JanusGraphInstance instance) {
        Objects.requireNonNull(instance, JanusGraphInstanceConstants.INSTANCE_NOT_NULL);
        Map<String, String> propertiesMap = new HashMap<>();

        try {
            List<String> valueListByNameAndType = new ArrayList<>();
            valueListByNameAndType.add(ConfigUtils.getString("TOPOLOGY_PROPERTIES"));
            logger.info("valueListByNameAndType size is : {}", valueListByNameAndType.size());
            if (!valueListByNameAndType.isEmpty()) {
                propertiesMap.put(
                        instance.toString() + JanusGraphInstanceConstants.TOPOLOGY_GRAPH_PROPERTIES_SUB_STRING,
                        valueListByNameAndType.get(0));
            }
            logger.info("propertiesMap size is : {}", propertiesMap.size());
        } catch (Exception e) {
            logger.error("Exception in getJanusPropertiesByInstance");
        }

        return propertiesMap;
    }

    /**
     * Retrieves the vertex-to-edge relation configuration for the specified JanusGraph instance.
     *
     * <p>This method fetches the configuration that defines how vertices and edges are related
     * in the graph database. The configuration includes mappings between source/destination vertices
     * and edge properties.</p>
     *
     * @param instance the JanusGraph instance to get configuration for
     * @return a VertexToEdgeRelationConfig object containing the configuration, or null if configuration cannot be retrieved
     */
    @Override
    public VertexToEdgeRelationConfig getJanusConfigByInstance(JanusGraphInstance instance) {
        Objects.requireNonNull(instance, JanusGraphInstanceConstants.INSTANCE_NOT_NULL);
        Map<String, String> configMap = new HashMap<>();

        try {
            List<String> valueListByNameAndType = new ArrayList<>();
            valueListByNameAndType.add("{\"edge\":{\"sourceVertexEdgeMapping\":\"srcEName:eName\",\"destVertexEdgeMapping\":\"destEName:eName\",\"edgeKeys\":\"rkPostfix\",\"label\":\"lebel\"},\"vertex\":{\"vertexKeys\":\"rkPostfix\",\"label\":\"eType\"}}");
            if (!valueListByNameAndType.isEmpty()) {
                logger.info("valueListByNameAndType size :: {} ", valueListByNameAndType.size());
                configMap.put(instance.toString() + JanusGraphInstanceConstants.TOPOLOGY_GRAPH_CONFIG_SUB_STRING,
                        valueListByNameAndType.get(0));

                logger.info("configMap size :: {} ", configMap.size());
                return getVertexToEdgeRelationConfig(configMap
                        .get(instance.toString() + JanusGraphInstanceConstants.TOPOLOGY_GRAPH_CONFIG_SUB_STRING));
            }
        } catch (Exception e) {
            logger.error("Exception in getJanusConfigByInstance");
        }
        return null;
    }

    /**
     * Parses a JSON string into a VertexToEdgeRelationConfig object.
     *
     * <p>This method converts the JSON configuration string into a structured object
     * that defines the relationships between vertices and edges in the graph.</p>
     *
     * @param conf the JSON configuration string to parse
     * @return a VertexToEdgeRelationConfig object representing the parsed configuration
     */
    private VertexToEdgeRelationConfig getVertexToEdgeRelationConfig(String conf) {
        if (conf == null || conf.trim().isEmpty()) {
            throw new IllegalArgumentException("Configuration string cannot be null or empty");
        }

        VertexToEdgeRelationConfig configurations = new VertexToEdgeRelationConfig();
        try {
            Gson gson = new Gson();
            configurations = gson.fromJson(conf, VertexToEdgeRelationConfig.class);
            logger.info("configurations");
        } catch (Exception e) {
            logger.error("Exception in getVertexToEdgeRelationConfig : {}", e.getMessage());
        }
        return configurations;
    }

    /**
     * Initializes a graph traversal for the specified JanusGraph instance.
     *
     * <p>This method creates a new graph traversal for the instance if the graph is open,
     * closing any existing traversal first to prevent resource leaks.</p>
     *
     * @param instance the JanusGraph instance to initialize traversal for
     * @throws Exception if there is an error closing an existing traversal or creating a new one
     */
    private void initTraversal(JanusGraphInstance instance) throws Exception {
        Objects.requireNonNull(instance, JanusGraphInstanceConstants.INSTANCE_NOT_NULL);

        if (isInstanceOpen(instance)) {

            closeTraversalIfOpen(instance);
            traversals.put(instance.toString(), graphs.get(instance.toString()).traversal());

        }
    }

    /**
     * Initializes a JanusGraph instance with the specified properties.
     *
     * <p>This method creates a new graph instance using the provided properties file,
     * closing any existing graph first to prevent resource leaks.</p>
     *
     * @param properties the path to the properties file for graph configuration
     * @param instance   the JanusGraph instance to initialize
     */
    private void initGraph(String properties, JanusGraphInstance instance) {
        Objects.requireNonNull(instance, JanusGraphInstanceConstants.INSTANCE_NOT_NULL);
        Objects.requireNonNull(properties, "Properties cannot be null");

        if (properties.trim().isEmpty()) {
            throw new IllegalArgumentException("Properties string cannot be empty");
        }

        if (isInstanceOpen(instance)) {
            graphs.get(instance.toString()).close();
        }
        graphs.put(instance.toString(), JanusGraphFactory.open(properties));
    }

    /**
     * Checks if the specified JanusGraph instance is open.
     *
     * <p>This method verifies whether the graph for the given instance exists and is in an open state.</p>
     *
     * @param instance the JanusGraph instance to check
     * @return true if the instance exists and is open, false otherwise
     */
    private boolean isInstanceOpen(JanusGraphInstance instance) {
        Objects.requireNonNull(instance, JanusGraphInstanceConstants.INSTANCE_NOT_NULL);

        if (graphs.get(instance.toString()) != null) {
            return graphs.get(instance.toString()).isOpen();
        }
        return false;
    }

    /**
     * Closes the graph traversal for the specified instance if it exists.
     *
     * <p>This method safely closes any existing traversal to prevent resource leaks.</p>
     *
     * @param instance the JanusGraph instance whose traversal should be closed
     * @throws Exception if there is an error closing the traversal
     */
    private void closeTraversalIfOpen(JanusGraphInstance instance) throws Exception {
        Objects.requireNonNull(instance, JanusGraphInstanceConstants.INSTANCE_NOT_NULL);

        if (traversals.get(instance.toString()) != null) {
            traversals.get(instance.toString()).close();
        }
    }

    /**
     * Validates that the properties for the specified instance exist and are not empty.
     *
     * <p>This method checks that the properties map contains a valid entry for the given instance.</p>
     *
     * @param properties the properties map to validate
     * @param instance   the JanusGraph instance to validate properties for
     * @return true if the properties are valid, false otherwise
     */
    private boolean validateProperties(Map<String, String> properties, JanusGraphInstance instance) {
        Objects.requireNonNull(instance, JanusGraphInstanceConstants.INSTANCE_NOT_NULL);
        return properties != null
                && !properties.isEmpty()
                && properties.get(instance.toString() + JanusGraphInstanceConstants.TOPOLOGY_GRAPH_PROPERTIES_SUB_STRING) != null
                && !properties.get(instance.toString() + JanusGraphInstanceConstants.TOPOLOGY_GRAPH_PROPERTIES_SUB_STRING)
                            .equalsIgnoreCase(Symbol.EMPTY_STRING);
    }
}
