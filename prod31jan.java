package com.enttribe.module.topology.core.corelation.service.impl;

import static org.apache.tinkerpop.gremlin.process.traversal.P.within;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.TextP;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.enttribe.commons.configuration.ConfigUtils;
import com.enttribe.core.generic.exceptions.application.BusinessException;
import com.enttribe.core.generic.utils.CustomTokenUtils;
import com.enttribe.core.generic.exceptions.application.ResourceNotFoundException;
import com.enttribe.lcm.model.NetworkElement;
import com.enttribe.module.topology.core.corelation.constants.AffectedNodesConstants;
import com.enttribe.module.topology.core.corelation.service.JanusgraphService;
import com.enttribe.platform.utility.notification.mail.model.NotificationAttachment;
import com.enttribe.platform.utility.notification.mail.rest.NotificationMailRest;
import com.enttribe.platform.utility.notification.mail.wrapper.NotificationMailWrapper;
import com.enttribe.module.topology.core.janusgraphutils.constants.JanusGraphUtilsConstants;
import com.enttribe.module.topology.core.querybuilder.exceptions.JanusGraphInitializationException;
import com.enttribe.module.topology.core.utils.exceptions.CsvDownloadException;
import com.enttribe.module.topology.core.utils.exceptions.DataProcessingException;
import com.enttribe.module.topology.core.utils.exceptions.InvalidParametersException;
import com.enttribe.module.topology.core.utils.exceptions.InvalidPayloadException;
import com.enttribe.module.topology.core.utils.exceptions.NetworkElementException;
import com.enttribe.module.topology.core.utils.exceptions.VertexCreationException;
import com.enttribe.module.topology.core.utils.exceptions.VertexNotFoundException;
import com.enttribe.module.topology.core.wrapper.NetworkElementWrapper;
import com.enttribe.network.module.fm.core.rest.AlarmRest;
import com.enttribe.network.module.fm.core.wrapper.AlarmMonitoringRequestFilterWrapper;
import com.enttribe.lcm.rest.NetworkElementRest;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.itextpdf.text.BaseColor;
import com.itextpdf.text.Document;
import com.itextpdf.text.Element;
import com.itextpdf.text.Font;
import com.itextpdf.text.FontFactory;
import com.itextpdf.text.PageSize;
import com.itextpdf.text.Paragraph;
import com.itextpdf.text.Phrase;
import com.itextpdf.text.pdf.PdfPCell;
import com.itextpdf.text.pdf.PdfPTable;
import com.itextpdf.text.pdf.PdfWriter;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

/**
 * Service implementation for managing JanusGraph operations in the topology
 * system.
 * This class provides functionality for creating, managing, and querying
 * vertices
 * and edges in the graph database.
 *
 * <p>
 * This implementation handles:
 * </p>
 * <ul>
 * <li>Graph initialization and connection management</li>
 * <li>Vertex creation and property management</li>
 * <li>Edge creation and relationship management</li>
 * <li>Network element operations</li>
 * <li>Data export and querying capabilities</li>
 * </ul>
 *
 * <p>
 * The class is thread-safe and manages its own graph connection lifecycle.
 * </p>
 *
 * @author Enttribe
 * @version 1.0
 * @since 1.0
 */
@Service
public class JanusgraphServiceImpl implements JanusgraphService {

    private static final Logger logger = LoggerFactory.getLogger(JanusgraphServiceImpl.class);

    /**
     * The JanusGraph instance used for all graph operations.
     * This field is initialized during bean construction and closed during
     * destruction.
     */
    private JanusGraph graph;

    /**
     * The graph traversal source used for querying the graph.
     * This provides access to Gremlin traversal methods.
     */
    private GraphTraversalSource g;

    /**
     * REST client for network element operations.
     * Used to retrieve network element information for edge creation.
     */
    @Autowired
    private NetworkElementRest networkElementRest;

    /**
     * REST client for alarm operations.
     * Used to retrieve network element information for alarm.
     */
    @Autowired
    private AlarmRest alarmRest;

    /**
     * REST client for notification operations.
     * Used to send notifications to the user.
     */
    @Autowired
    private NotificationMailRest notificationMailRest;

    // Store accumulated batch processing results for email notifications
    private BatchProcessingSummary accumulatedBatchSummary;

    // Store the last sent summary for CSV download (separate from active
    // accumulation)
    private BatchProcessingSummary lastSentSummary;

    // Scheduled executor service for delayed email sending
    private ScheduledExecutorService emailScheduler;

    // Scheduled future for the email task
    private ScheduledFuture<?> emailTask;

    // Lock to ensure thread-safe accumulation
    private final ReentrantLock accumulationLock = new ReentrantLock();

    // Delay in seconds before sending accumulated email (configurable, default 600
    // seconds - 10 minutes)
    private static final int EMAIL_DELAY_SECONDS = 600;

    // Flag to force all records in the current accumulation cycle to be treated as
    // NEW
    private boolean forceAllNewCycle = false;

    /**
     * Closes the JanusGraph connection when the bean is being destroyed.
     * This method ensures proper cleanup of graph resources and prevents memory
     * leaks.
     * The method is automatically called by Spring during application shutdown.
     *
     * <p>
     * This method is safe to call multiple times and handles null checks
     * to prevent {@link NullPointerException}.
     * </p>
     */
    @PreDestroy
    public void shutdown() {
        try {
            // Cancel any pending email task and send accumulated email if exists
            if (emailTask != null && !emailTask.isDone()) {
                emailTask.cancel(false);
                logger.info("Cancelled pending email task, sending accumulated email now");
            }

            // Send any accumulated email before shutdown
            sendAccumulatedEmailIfExists();

            // Shutdown email scheduler
            shutdownEmailScheduler();

            if (graph != null && graph.isOpen()) {
                logger.info("Shutting down JanusGraph...");
                // Close traversal first
                if (g != null) {
                    g.close();
                }
                graph.close();
                logger.info("JanusGraph shutdown completed.");
            }
        } catch (Exception e) {
            logger.error("Error occurred while shutting down JanusGraph: {}", e.getMessage());
        }
    }

    /**
     * Shuts down the email scheduler gracefully.
     * Waits for termination and forces shutdown if necessary.
     */
    private void shutdownEmailScheduler() {
        if (emailScheduler != null && !emailScheduler.isShutdown()) {
            emailScheduler.shutdown();
            try {
                if (!emailScheduler.awaitTermination(JanusGraphUtilsConstants.FIVE, TimeUnit.SECONDS)) {
                    emailScheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                emailScheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
            logger.info("Email scheduler shut down");
        }
    }

    /**
     * Initializes the JanusGraph connection after bean construction.
     * Reads configuration from environment variables and establishes the graph
     * connection.
     * This method is automatically called by Spring after dependency injection is
     * complete.
     *
     * @throws IllegalStateException if TOPOLOGY_PROPERTIES configuration is missing
     * @throws RuntimeException      if JanusGraph initialization fails
     */
    @PostConstruct
    public void initialize() {
        String topologyProperties = ConfigUtils.getString("TOPOLOGY_PROPERTIES");

        if (topologyProperties == null || topologyProperties.isEmpty()) {
            logger.error("TOPOLOGY_PROPERTIES is not set. JanusGraph cannot be initialized.");
            throw new IllegalStateException("Missing TOPOLOGY_PROPERTIES configuration.");
        }

        try {
            // Check if graph is already initialized
            if (this.graph != null && this.graph.isOpen()) {
                logger.info("JanusGraph is already initialized and open. Skipping initialization.");
                return;
            }

            this.graph = JanusGraphFactory.open(topologyProperties);
            this.g = graph.traversal();
            logger.info("Successfully connected to JanusGraph with properties: {}", topologyProperties);

            // Initialize email scheduler for batched email notifications
            this.emailScheduler = Executors.newScheduledThreadPool(1);
            this.accumulatedBatchSummary = new BatchProcessingSummary();
            logger.info("Email scheduler initialized for batched notifications");
        } catch (Exception e) {
            logger.error("Failed to initialize JanusGraph with the provided configuration");
            throw new JanusGraphInitializationException("JanusGraph initialization failed.", e);
        }
    }

    /**
     * Creates an edge between vertices based on network element information.
     * If the source or destination vertices don't exist, the operation fails.
     * If an edge already exists between the vertices, it updates the edge
     * properties.
     *
     * @param networkElement the network element wrapper containing connection
     *                       information
     * @return a map containing the result of the edge creation operation
     * @throws NullPointerException if networkElement is null
     */
    public Map<String, Object> createEdgeByNetworkElement(NetworkElementWrapper networkElement) {
        Objects.requireNonNull(networkElement, "NetworkElement cannot be null");

        Map<String, Object> response = new HashMap<>();
        JanusGraphTransaction tx = null;

        try {
            validateInput(networkElement);

            String sourceInterface = networkElement.getNeName();
            String destinationInterface = networkElement.getNeighbourNEName();
            String relation = networkElement.getRelation();
            String edgeLabel = networkElement.getNeType();

            logger.info("Creating edge for network element: {} to {}", sourceInterface, destinationInterface);

            String sourceNode = networkElement.getParentNE();
            String destinationNode = networkElement.getNeighbourParentNEName();

            tx = graph.newTransaction();
            GraphTraversalSource gtx = tx.traversal();

            Vertex sourceVertex = findVertex(gtx, sourceInterface);
            if (sourceVertex == null) {
                return buildErrorResponse(response, networkElement,
                        "Source vertex not found with eName: " + sourceInterface);
            }

            Vertex destinationVertex = findVertex(gtx, destinationInterface);
            if (destinationVertex == null) {
                return buildErrorResponse(response, networkElement,
                        "Destination vertex not found with eName: " + destinationInterface);
            }

            Edge existingEdge = findExistingEdge(gtx, sourceInterface, destinationInterface, relation);
            if (existingEdge != null) {
                logger.info("Updating existing edge between {} and {}", sourceInterface, destinationInterface);
                updateExistingEdgeProperties(existingEdge, networkElement);
            } else {
                logger.info("Creating new edge between {} and {}", sourceInterface, destinationInterface);
                Edge createdEdge = createNewEdge(destinationVertex, sourceVertex, edgeLabel, networkElement,
                        sourceNode, destinationNode,
                        networkElement.getCktId(), networkElement.getHostname(), networkElement.getNeCategory(),
                        networkElement.getDestCategory(), networkElement.getSrcCategory(), networkElement.getRelation(),
                        extractingId(sourceInterface), extractingId(destinationInterface),
                        networkElement.getNeId(), networkElement.getNeighbourNEId());
                response.put(JanusGraphUtilsConstants.CREATED_EDGE, createdEdge.properties());
                logger.info("Edge created successfully between {} and {}", sourceInterface, destinationInterface);
            }

            tx.commit();
            logger.info("Transaction committed successfully for edge creation");

        } catch (Exception e) {
            rollbackTransaction(tx);
            return handleException(e, response, networkElement);
        } finally {
            rollbackIfOpen(tx);
        }

        response.put(AffectedNodesConstants.EDGE + networkElement.getNeName(), AffectedNodesConstants.SUCCESS);
        return response;
    }

    private void validateInput(NetworkElementWrapper networkElement) {
        if (isNullOrEmpty(networkElement.getNeName()) ||
                isNullOrEmpty(networkElement.getParentNE()) ||
                isNullOrEmpty(networkElement.getNeighbourParentNEName()) ||
                isNullOrEmpty(networkElement.getNeType())) {
            throw new InvalidParametersException("Invalid request: eName, parentNeName, and edgeLabel are required.");
        }
    }

    private boolean isNullOrEmpty(String str) {
        return str == null || str.trim().isEmpty();
    }

    private Vertex findVertex(GraphTraversalSource g, String neName) {
        return g.V().has(AffectedNodesConstants.NENAME, neName).tryNext().orElse(null);
    }

    private Vertex findVertex(GraphTraversalSource g, String neName, String relation) {
        if (relation != null && !relation.trim().isEmpty()) {
            return g.V()
                    .has(AffectedNodesConstants.NENAME, neName)
                    .has(AffectedNodesConstants.RELATION, relation)
                    .tryNext()
                    .orElse(null);
        }
        return findVertex(g, neName);
    }

    private Edge findExistingEdge(GraphTraversalSource g, String src, String dest, String relation) {
        return g.E()
                .has(JanusGraphUtilsConstants.SRC_INTERFACE, src)
                .has(JanusGraphUtilsConstants.DEST_INTERFACE, dest)
                .has(JanusGraphUtilsConstants.RELATION, relation)
                .tryNext()
                .orElse(null);
    }

    /**
     * Finds an existing edge with support for ISIS relation linkType check.
     * For ISIS relations, also checks linkType to determine if edge should be
     * updated or created.
     * 
     * @param g        the graph traversal source
     * @param src      the source interface
     * @param dest     the destination interface
     * @param relation the relation
     * @param linkType the link type (normalized - empty string for null/empty,
     *                 required for ISIS relation)
     * @return the existing edge if found, null otherwise
     */
    private Edge findExistingEdgeWithLinkType(GraphTraversalSource g, String src, String dest, String relation,
            String linkType) {
        GraphTraversal<Edge, Edge> traversal = g.E()
                .has(JanusGraphUtilsConstants.SRC_INTERFACE, src)
                .has(JanusGraphUtilsConstants.DEST_INTERFACE, dest)
                .has(JanusGraphUtilsConstants.RELATION, relation);

        // For ISIS relation, ALWAYS check linkType to ensure exact match
        // linkType is normalized (empty string for null/empty)
        if ("ISIS".equalsIgnoreCase(relation)) {
            if (linkType != null && !linkType.isEmpty()) {
                // Check for exact linkType match
                traversal = traversal.has(AffectedNodesConstants.LINK_TYPE, linkType);
            } else {
                // If linkType is empty, only match edges that don't have linkType property
                traversal = traversal.hasNot(AffectedNodesConstants.LINK_TYPE);
            }
        }

        return traversal.tryNext().orElse(null);
    }

    private Map<String, Object> buildErrorResponse(Map<String, Object> response, NetworkElementWrapper ne,
            String message) {
        logger.warn(message);
        response.put(ne.getNeName(), AffectedNodesConstants.ERROR);
        response.put(AffectedNodesConstants.MSG, message);
        return response;
    }

    private String extractingId(String name) {
        return (name != null) ? name.replaceAll("^.*_", "") : null;
    }

    private void rollbackTransaction(JanusGraphTransaction tx) {
        if (tx != null && tx.isOpen()) {
            tx.rollback();
        }
    }

    private void rollbackIfOpen(JanusGraphTransaction tx) {
        if (tx != null && tx.isOpen()) {
            try {
                tx.rollback();
                logger.warn("Transaction rolled back in finally block");
            } catch (Exception e) {
                logger.error("Error during transaction rollback in finally block");
            }
        }
    }

    private Map<String, Object> handleException(Exception e, Map<String, Object> response, NetworkElementWrapper ne) {
        if (e instanceof VertexNotFoundException ||
                e instanceof InvalidParametersException ||
                e instanceof IllegalArgumentException) {
            logger.warn("Handled exception: {}", e.getMessage());
        } else {
            logger.error("Unhandled exception while creating edge for {}: {}", ne.getNeName(), e.getMessage());
        }

        response.put(ne.getNeName(), AffectedNodesConstants.ERROR);
        response.put(AffectedNodesConstants.MSG, (e instanceof IllegalArgumentException)
                ? JanusGraphUtilsConstants.INVALID + e.getMessage()
                : "Failed to create edge: " + e.getMessage());
        return response;
    }

    /**
     * Updates properties of an existing edge
     */
    private void updateExistingEdgeProperties(Edge existingEdge, NetworkElementWrapper networkElement) {
        existingEdge.property(JanusGraphUtilsConstants.DEST_LAT, networkElement.getNeighbourParentNELat());
        existingEdge.property(JanusGraphUtilsConstants.DEST_LONG, networkElement.getNeighbourParentNELong());
        existingEdge.property(JanusGraphUtilsConstants.NEIGHBOUR_VENDOR, networkElement.getNeighbourVendor());
        existingEdge.property(JanusGraphUtilsConstants.NEIGHBOUR_DOMAIN, networkElement.getNeighbourDomain());
        existingEdge.property(JanusGraphUtilsConstants.NEIGHBOUR_TECH, networkElement.getNeighbourTechnology());
        existingEdge.property(AffectedNodesConstants.DOMAIN, networkElement.getDomain());
        existingEdge.property(AffectedNodesConstants.VENDOR, networkElement.getVendor());
        existingEdge.property(AffectedNodesConstants.TECH, networkElement.getTechnology());
        existingEdge.property(JanusGraphUtilsConstants.SRC_LAT, networkElement.getLatitude());
        existingEdge.property(JanusGraphUtilsConstants.SRC_LONG, networkElement.getLongitude());
        existingEdge.property(JanusGraphUtilsConstants.MODIFIED_AT, System.currentTimeMillis());
        existingEdge.property(AffectedNodesConstants.ISDEL, networkElement.getIsDeleted());
    }

    /**
     * Creates a new edge with all required properties
     */
    @SuppressWarnings("java:S107")
    private Edge createNewEdge(Vertex destinationVertex, Vertex sourceVertex, String edgeLabel,
            NetworkElementWrapper networkElement, String sourceNode, String destinationNode,
            String cktId, String hostname, String neCategory, String sourceCategory,
            String destinationCategory, String relation, String sourceId, String destinationId,
            String sourceNodeId, String destinationNodeId) {
        return destinationVertex.addEdge(edgeLabel, sourceVertex,
                AffectedNodesConstants.START, sourceNode,
                AffectedNodesConstants.END, destinationNode,
                AffectedNodesConstants.CKTID, cktId,
                AffectedNodesConstants.LINE_NAME, hostname,
                AffectedNodesConstants.NECATEGORY, neCategory,
                AffectedNodesConstants.SRC_CATEGORY, sourceCategory,
                AffectedNodesConstants.DEST_CATEGORY, destinationCategory,
                AffectedNodesConstants.RELATION, relation,
                JanusGraphUtilsConstants.CREATED_AT, System.currentTimeMillis(),
                AffectedNodesConstants.STATUS, JanusGraphUtilsConstants.ACTIVE,
                JanusGraphUtilsConstants.SRC_INTERFACE, networkElement.getNeName(),
                JanusGraphUtilsConstants.DEST_INTERFACE, networkElement.getNeighbourNEName(),
                JanusGraphUtilsConstants.SRC_LAT, networkElement.getLatitude(),
                JanusGraphUtilsConstants.SRC_LONG, networkElement.getLongitude(),
                JanusGraphUtilsConstants.DEST_LAT, networkElement.getNeighbourParentNELat(),
                JanusGraphUtilsConstants.DEST_LONG, networkElement.getNeighbourParentNELong(),
                AffectedNodesConstants.NETYPE, edgeLabel,
                JanusGraphUtilsConstants.NEIGHBOUR_VENDOR, networkElement.getNeighbourVendor(),
                JanusGraphUtilsConstants.NEIGHBOUR_DOMAIN, networkElement.getNeighbourDomain(),
                JanusGraphUtilsConstants.START, sourceNode,
                "end", destinationNode,
                JanusGraphUtilsConstants.START_ID, sourceId,
                JanusGraphUtilsConstants.END_ID, destinationId,
                JanusGraphUtilsConstants.START_NEID, sourceNodeId,
                JanusGraphUtilsConstants.END_NEID, destinationNodeId,
                JanusGraphUtilsConstants.NEIGHBOUR_TECH, networkElement.getNeighbourTechnology(),
                AffectedNodesConstants.DOMAIN, networkElement.getDomain(),
                AffectedNodesConstants.VENDOR, networkElement.getVendor(),
                AffectedNodesConstants.TECH, networkElement.getTechnology(),
                AffectedNodesConstants.ISDEL, networkElement.getIsDeleted());
    }

    /**
     * Adds a vertex to the graph and links it to a parent vertex.
     * If the child vertex already exists, it will be used; otherwise, a new vertex
     * is created.
     * If the parent vertex doesn't exist, the operation fails.
     *
     * @param requestData a map containing element name, parent element name, and
     *                    edge label
     * @return a map containing the result of the operation
     * @throws NullPointerException if requestData is null
     */
    @Override
    public Map<String, Object> addVertexWithParentLink(Map<String, Object> requestData) {
        Objects.requireNonNull(requestData, "Request data cannot be null");

        Map<String, Object> response = new HashMap<>();

        try (JanusGraphTransaction tx = graph.newTransaction()) {
            String elementName = (String) requestData.get(AffectedNodesConstants.ENAME);
            String parentElementName = (String) requestData.get("parentNeName");
            String edgeLabel = (String) requestData.get("edgeLabel");

            logger.info("Adding vertex {} with parent link to {}", elementName, parentElementName);

            if (elementName == null || parentElementName == null || edgeLabel == null) {
                logger.warn("Missing required parameters for vertex creation with parent link");
                response.put(AffectedNodesConstants.STATUS, AffectedNodesConstants.ERROR);
                response.put(AffectedNodesConstants.MSG,
                        "Invalid request: elementName, parentElementName, and edgeLabel are required.");
                return response;
            }

            GraphTraversalSource gtx = tx.traversal();

            Vertex parentVertex = gtx.V()
                    .has(AffectedNodesConstants.ENAME, parentElementName)
                    .tryNext()
                    .orElse(null);

            if (parentVertex == null) {
                logger.warn("Parent vertex not found with elementName: {}", parentElementName);
                response.put(AffectedNodesConstants.STATUS, AffectedNodesConstants.ERROR);
                response.put(AffectedNodesConstants.MSG,
                        "Parent vertex not found with elementName: " + parentElementName);
                return response;
            }

            Vertex childVertex = gtx.V()
                    .has(AffectedNodesConstants.ENAME, elementName)
                    .tryNext()
                    .orElseGet(() -> {
                        Vertex newVertex = gtx.addV(AffectedNodesConstants.VERTICES)
                                .property(AffectedNodesConstants.ENAME, elementName)
                                .property("parentNeName", parentElementName)
                                .property(AffectedNodesConstants.STATUS, "LIVE")
                                .property("cTime", System.currentTimeMillis())
                                .next();
                        logger.info("Created new vertex with elementName: {}", elementName);
                        return newVertex;
                    });

            boolean edgeExists = gtx.E()
                    .hasLabel(edgeLabel)
                    .where(__.outV().has(AffectedNodesConstants.ENAME, elementName))
                    .where(__.inV().has(AffectedNodesConstants.ENAME, parentElementName))
                    .hasNext();

            if (!edgeExists) {
                logger.info("Creating edge from {} to {} with label {}", elementName, parentElementName, edgeLabel);
                childVertex.addEdge(edgeLabel, parentVertex,
                        AffectedNodesConstants.START, elementName,
                        AffectedNodesConstants.END, parentElementName,
                        JanusGraphUtilsConstants.CREATED_AT, System.currentTimeMillis(),
                        AffectedNodesConstants.STATUS, JanusGraphUtilsConstants.ACTIVE);
            } else {
                logger.info("Edge already exists between {} and {}", elementName, parentElementName);
            }

            tx.commit();
            logger.info("Successfully added vertex {} with parent link to {}", elementName, parentElementName);
            response.put(AffectedNodesConstants.STATUS, JanusGraphUtilsConstants.SUCCES);
            response.put(AffectedNodesConstants.START, elementName);
            response.put(AffectedNodesConstants.END, parentElementName);
            response.put("edgeLabel", edgeLabel);
            return response;

        } catch (VertexNotFoundException e) {
            logger.warn("Vertex not found: {}", e.getMessage());
            response.put(AffectedNodesConstants.STATUS, AffectedNodesConstants.ERROR);
            response.put(AffectedNodesConstants.MSG, e.getMessage());
            return response;

        } catch (InvalidParametersException e) {
            logger.warn("Invalid vertex link parameters: {}", e.getMessage());
            response.put(AffectedNodesConstants.STATUS, AffectedNodesConstants.ERROR);
            response.put(AffectedNodesConstants.MSG, e.getMessage());
            return response;

        } catch (IllegalArgumentException e) {
            logger.error("Invalid property value when creating vertex link: {}", e.getMessage());
            response.put(AffectedNodesConstants.STATUS, AffectedNodesConstants.ERROR);
            response.put(AffectedNodesConstants.MSG, JanusGraphUtilsConstants.INVALID + e.getMessage());
            return response;

        } catch (RuntimeException e) {
            logger.error("Failed to create vertex and link to parent: {}", e.getMessage());
            response.put(AffectedNodesConstants.STATUS, AffectedNodesConstants.ERROR);
            response.put(AffectedNodesConstants.MSG, "Failed to create vertex and link to parent: " + e.getMessage());
            return response;

        } catch (Exception e) {
            logger.error("Error closing transaction in finally block: {}", e.getMessage());
            response.put(AffectedNodesConstants.STATUS, AffectedNodesConstants.ERROR);
            response.put(AffectedNodesConstants.MSG, "Unexpected error occurred: " + e.getMessage());
            return response;
        }
    }

    /**
     * Adds a vertex to the graph based on a NetworkElementWrapper object.
     * This method creates a new vertex or updates an existing one with the
     * properties
     * from the provided network element, and creates appropriate edges.
     *
     * <p>
     * The method handles validation of input parameters and manages the
     * transaction,
     * committing on success and rolling back on failure.
     * </p>
     *
     * @param networkElement the network element wrapper containing vertex
     *                       properties, must not be null
     * @return a list of maps containing the results of vertex and edge creation
     *         operations
     * @throws VertexCreationException    if there is an error creating the vertex
     * @throws InvalidParametersException if required parameters are missing or
     *                                    invalid
     * @throws IllegalArgumentException   if property values are invalid
     */
    @Override
    public List<Map<String, Object>> addVertexByNetworkElement(NetworkElementWrapper networkElement) {
        List<Map<String, Object>> createdVertices = new ArrayList<>();
        JanusGraphTransaction tx = null;

        try {
            tx = graph.newTransaction();

            JanusGraphVertex existingVertex = (JanusGraphVertex) findVertex(
                    tx.traversal(),
                    networkElement.getNeName(),
                    networkElement.getRelation());

            Map<String, Object> vertexProperties = buildVerticesProperties(networkElement);

            JanusGraphVertex vertex = getOrCreateVertex(tx, existingVertex, networkElement);

            setVerticesProperties(vertex, vertexProperties);

            tx.commit();

            vertexProperties.put(AffectedNodesConstants.VERTEX + networkElement.getNeName(),
                    JanusGraphUtilsConstants.SUCCES);
            Map<String, Object> edgeResponse = createEdgeByNetworkElement(networkElement);

            createdVertices.add(vertexProperties);
            createdVertices.add(edgeResponse);

        } catch (VertexCreationException e) {
            handleException(tx, createdVertices, networkElement, e.getMessage());
        } catch (IllegalArgumentException e) {
            handleException(tx, createdVertices, networkElement, JanusGraphUtilsConstants.INVALID + e.getMessage());
        } catch (InvalidParametersException e) {
            String name = networkElement.getNeName() != null ? networkElement.getNeName() : "unknown";
            handleException(tx, createdVertices, name, e.getMessage());
        } catch (RuntimeException e) {
            if (tx != null) {
                tx.rollback();
            }
            logger.error(JanusGraphUtilsConstants.FAILED_VERTEX, networkElement.getNeName());
            throw new VertexCreationException(
                    AffectedNodesConstants.FAILED_TO_CREATE_VERTEX_FOR_NETWORK_ELEMENT + networkElement.getNeName(), e);
        } finally {
            if (tx != null && tx.isOpen()) {
                try {
                    tx.close();
                } catch (Exception e) {
                    logger.error("Error closing transaction in finally block: {}", e.getMessage());
                }
            }
        }

        return createdVertices;
    }

    // Helper Methods

    private JanusGraphVertex getOrCreateVertex(JanusGraphTransaction tx, JanusGraphVertex existingVertex,
            NetworkElementWrapper networkElement) {
        if (existingVertex != null) {
            logger.info(AffectedNodesConstants.USING_EXISTING_VERTEX_FOR_NETWORK_ELEMENT, networkElement.getNeName());
            return existingVertex;
        } else {
            JanusGraphVertex vertex = tx.addVertex(networkElement.getNeType());
            logger.info(AffectedNodesConstants.CREATED_NEW_VERTEX_FOR_NETWORK_ELEMENT, networkElement.getNeName());
            return vertex;
        }
    }

    private void setVerticesProperties(JanusGraphVertex vertex, Map<String, Object> vertexProperties) {
        for (Map.Entry<String, Object> entry : vertexProperties.entrySet()) {
            if (entry.getValue() != null) {
                vertex.property(entry.getKey(), entry.getValue());
            }
        }
    }

    private void handleException(JanusGraphTransaction tx, List<Map<String, Object>> createdVertices,
            NetworkElementWrapper networkElement, String message) {
        if (tx != null) {
            tx.rollback();
        }
        logger.error("Exception during vertex creation: {}", message);

        Map<String, Object> errorResponse = new HashMap<>();
        errorResponse.put(networkElement.getNeName(), AffectedNodesConstants.ERROR);
        errorResponse.put(AffectedNodesConstants.MSG, message);
        createdVertices.add(errorResponse);
    }

    private void handleException(JanusGraphTransaction tx, List<Map<String, Object>> createdVertices, String neName,
            String message) {
        if (tx != null) {
            tx.rollback();
        }
        logger.error("Exception during vertex creation: {}", message);

        Map<String, Object> errorResponse = new HashMap<>();
        errorResponse.put(neName, AffectedNodesConstants.ERROR);
        errorResponse.put(AffectedNodesConstants.MSG, message);
        createdVertices.add(errorResponse);
    }

    /**
     * Builds vertex properties map from NetworkElementWrapper.
     * This method extracts all properties from the network element and creates a
     * map
     * for efficient vertex property setting.
     *
     * @param networkElement the network element wrapper containing vertex
     *                       properties
     * @return map containing all vertex properties
     */
    private Map<String, Object> buildVerticesProperties(NetworkElementWrapper networkElement) {
        Map<String, Object> vertexProperties = new HashMap<>();

        vertexProperties.put(AffectedNodesConstants.NENAME, networkElement.getNeName());
        vertexProperties.put(AffectedNodesConstants.LAT, networkElement.getLatitude());
        vertexProperties.put(AffectedNodesConstants.LNG, networkElement.getLongitude());
        vertexProperties.put("geographyL1", networkElement.getL1GeoName());
        vertexProperties.put("geographyL2", networkElement.getL2GeoName());
        vertexProperties.put("geographyL3", networkElement.getL3GeoName());
        vertexProperties.put("geographyL4", networkElement.getL4GeoName());
        vertexProperties.put("createdTime",
                networkElement.getCreatedTime() != null
                        ? (int) (networkElement.getCreatedTime().getTime() / JanusGraphUtilsConstants.THOUSAND)
                        : null);
        vertexProperties.put(JanusGraphUtilsConstants.MODIFIED_TIME,
                networkElement.getModifiedTime() != null
                        ? (int) (networkElement.getModifiedTime().getTime() / JanusGraphUtilsConstants.THOUSAND)
                        : null);
        vertexProperties.put(AffectedNodesConstants.NE_SOURCE, networkElement.getNeSource());
        vertexProperties.put(AffectedNodesConstants.NETYPE,
                networkElement.getNeType() != null ? networkElement.getNeType().toUpperCase() : null);
        vertexProperties.put(AffectedNodesConstants.VENDOR, networkElement.getVendor());
        vertexProperties.put(AffectedNodesConstants.NE_FREQUENCY, networkElement.getNeFrequency());
        vertexProperties.put(AffectedNodesConstants.ISDEL, networkElement.getIsDeleted());
        vertexProperties.put(AffectedNodesConstants.NE_STATUS, networkElement.getNeStatus());
        vertexProperties.put(AffectedNodesConstants.TECH, networkElement.getTechnology());
        vertexProperties.put(AffectedNodesConstants.DOMAIN, networkElement.getDomain());
        vertexProperties.put(AffectedNodesConstants.SOFTWARE_VERSION, networkElement.getSoftwareVersion());
        vertexProperties.put("mnc", networkElement.getMnc());
        vertexProperties.put("mcc", networkElement.getMcc());
        vertexProperties.put("neId", networkElement.getNeId());
        vertexProperties.put("pmEmsId", networkElement.getPmEmsId());
        vertexProperties.put("fmEmsId", networkElement.getFmEmsId());
        vertexProperties.put("cmEmsId", networkElement.getCmEmsId());
        vertexProperties.put("ipv4", networkElement.getIpv4());
        vertexProperties.put("ipv6", networkElement.getIpv6());
        vertexProperties.put("model", networkElement.getModel());
        vertexProperties.put("duplex", networkElement.getDuplex());
        vertexProperties.put("radius", networkElement.getRadius());
        vertexProperties.put(AffectedNodesConstants.CATEGORY, networkElement.getCategory());
        vertexProperties.put(AffectedNodesConstants.HOSTNAME, networkElement.getHostname());
        vertexProperties.put("fqdn", networkElement.getFqdn());
        vertexProperties.put("uuid", networkElement.getUuid());
        vertexProperties.put("serialNumber", networkElement.getSerialNumber());
        vertexProperties.put("secured", networkElement.getSecured());
        vertexProperties.put("inServiceDate", networkElement.getInServiceDate());
        vertexProperties.put(AffectedNodesConstants.PARENT_NE, networkElement.getParentNE());
        vertexProperties.put(AffectedNodesConstants.NEIGHBOUR_NE, networkElement.getNeighbourNEName());
        vertexProperties.put(AffectedNodesConstants.NEIGHBOUR_NE_ID, networkElement.getNeighbourNEId());
        vertexProperties.put(AffectedNodesConstants.RELATION, networkElement.getRelation());
        vertexProperties.put(AffectedNodesConstants.CKTID, networkElement.getCktId());
        vertexProperties.put(AffectedNodesConstants.NECATEGORY, networkElement.getNeCategory());
        vertexProperties.put("id", networkElement.getId());
        return vertexProperties;
    }

    /**
     * Retrieves network elements based on the provided filters.
     *
     * <p>
     * This method fetches network elements from the REST service using the
     * specified filters.
     * If no filters are provided, all network elements will be fetched.
     * </p>
     *
     * @param filters list of filters to apply when fetching network elements, can
     *                be null
     * @return a list of maps containing the fetched network elements
     * @throws ResourceNotFoundException if no network elements are found
     * @throws BusinessException         if there is a business logic error during
     *                                   fetching
     * @throws NetworkElementException   if there is a general error fetching
     *                                   network elements
     */
    @Override
    public List<Map<String, Object>> getNetworkElement(List<com.enttribe.lcm.util.wrapper.Filter> filters) {
        List<Map<String, Object>> networkElementResponses = new ArrayList<>();
        try {
            if (filters == null) {
                logger.warn("No filters provided, fetching all network elements");
            }

            List<NetworkElement> networkElements = networkElementRest.getNE(filters);

            if (networkElements == null) {
                throw new ResourceNotFoundException("Network elements not found or null response received");
            }

            logger.info("Found {} network elements", networkElements.size());

            networkElementResponses = null;
        } catch (ResourceNotFoundException e) {
            logger.warn("Network elements not found");
            throw e;
        } catch (BusinessException e) {
            logger.error("Business error while fetching network elements");
            throw e;
        } catch (RuntimeException e) {
            logger.error("Failed to fetch network elements");
            throw new NetworkElementException("Failed to fetch network elements", e);
        }
        return networkElementResponses;
    }

    /**
     * Searches for network elements based on the provided name pattern.
     *
     * <p>
     * This method performs a case-insensitive regex search for network elements
     * whose names start with the provided pattern. Results can be sorted and
     * paginated.
     * 
     * neTypes is mandatory. Only one of neName, areaIpAddress, tag, popId, or popName can be provided at a time.
     * If areaIpAddress or tag is provided, this method will internally call
     * getLinksByAreaIpAddress to retrieve links and nodes based on the area IP address or tag
     * instead of performing a vertex search. If popId or popName is provided, it will search by POP ID or POP name.
     * The relation parameter is optional and filters results to only include vertices connected by edges with the specified relation.
     * </p>
     *
     * @param neTypes the network element types to search for (mandatory, must not be null or empty)
     * @param neName  the name pattern to search for (optional, can be null, but only one search parameter allowed)
     * @param areaIpAddress the area IP address to search for (optional, can be null, but only one search parameter allowed).
     *                      If provided, the method will search by area IP address instead
     * @param tag the tag to search for (optional, can be null, but only one search parameter allowed).
     *            If provided, the method will search by tag instead
     * @param popId the POP ID to search for (optional, can be null, but only one search parameter allowed).
     *              If provided, the method will search by POP ID (exact match)
     * @param popName the POP name to search for (optional, can be null, but only one search parameter allowed).
     *                If provided, the method will search by POP name (regex pattern)
     * @param relation the relation type to filter by (optional). If provided, only vertices connected by edges with this relation are returned
     * @return a list of maps containing the matching network elements with their
     *         properties. If areaIpAddress or tag is provided, returns list with links and nodes.
     * @throws InvalidParametersException if neTypes is empty, or if multiple search parameters are provided,
     *                                    or if none of the search parameters is provided
     * @throws BusinessException          if there is an error parsing the
     *                                    modifiedTime
     * @throws NetworkElementException    if there is a general error during the
     *                                    search
     */
    public List<Map<String, Object>> searchByNeTypeAndNeName(String neTypes, String neName, String areaIpAddress, String tag, String popId, String popName, String relation) {
        validateNeTypes(neTypes);
        boolean hasNeName = hasValidParameter(neName);
        boolean hasAreaIp = hasValidParameter(areaIpAddress);
        boolean hasTag = hasValidParameter(tag);
        boolean hasPopId = hasValidParameter(popId);
        boolean hasPopName = hasValidParameter(popName);

        validateSingleParameter(hasNeName, hasAreaIp, hasTag, hasPopId, hasPopName);

        if (hasAreaIp || hasTag) {
            return searchByAreaIpOrTag(areaIpAddress, tag, hasAreaIp, hasTag, relation);
        }

        if (hasNeName) {
            List<String> neTypeList = parseNeTypes(neTypes);
            return searchByNeTypeAndNeNameInternal(neName, neTypeList, relation);
        }

        if (hasPopId) {
            List<String> neTypeList = parseNeTypes(neTypes);
            return searchByPopIdInternal(popId, neTypeList, relation);
        }

        if (hasPopName) {
            List<String> neTypeList = parseNeTypes(neTypes);
            return searchByPopNameInternal(popName, neTypeList, relation);
        }

        throw new InvalidParametersException(
                "At least one of neName, areaIpAddress, tag, popId, or popName must be provided");
    }

    private void validateNeTypes(String neTypes) {
        Objects.requireNonNull(neTypes, "neTypes cannot be null");
        if (neTypes.trim().isEmpty()) {
            throw new InvalidParametersException("neTypes cannot be empty");
        }
    }

    private void validateSingleParameter(boolean hasNeName, boolean hasAreaIp, boolean hasTag, boolean hasPopId,
            boolean hasPopName) {
        int providedCount = (hasNeName ? 1 : 0) + (hasAreaIp ? 1 : 0) + (hasTag ? 1 : 0) + (hasPopId ? 1 : 0)
                + (hasPopName ? 1 : 0);
        if (providedCount > 1) {
            throw new InvalidParametersException(
                    "Give only one value at a time. Provide either neName, areaIpAddress, tag, popId, or popName, not multiple.");
        }
    }

    private List<Map<String, Object>> searchByAreaIpOrTag(String areaIpAddress, String tag, boolean hasAreaIp,
            boolean hasTag, String relation) {
        String filterDesc = hasAreaIp
                ? String.format("areaIpAddress: %s", areaIpAddress.trim())
                : String.format("tag: %s", tag.trim());
        logger.info("Area IP address or tag parameter provided: {}. Calling getLinksByAreaIpAddress with relation: {}", filterDesc, relation);
        try {
            return getLinksByAreaIpAddress(
                    hasAreaIp ? areaIpAddress.trim() : null,
                    hasTag ? tag.trim() : null,
                    relation);
        } catch (Exception e) {
            throw new NetworkElementException("Failed to get links by areaIpAddress/tag: " + e.getMessage(), e);
        }
    }

    private boolean hasValidParameter(String param) {
        return param != null && !param.trim().isEmpty() && !"null".equalsIgnoreCase(param.trim());
    }

    private List<String> parseNeTypes(String neTypes) {
        List<String> neTypeList = Arrays.stream(neTypes.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .toList();

        if (neTypeList.isEmpty()) {
            throw new InvalidParametersException("No valid neType values provided");
        }
        return neTypeList;
    }

    private List<String> parseRelations(String relation) {
        if (!hasValidParameter(relation)) {
            return Collections.emptyList();
        }
    
        return Arrays.stream(relation.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(String::toUpperCase)
                .toList();
    }    

    private List<Map<String, Object>> searchByNeTypeAndNeNameInternal(String neName, List<String> neTypeList, String relation) {
        try {
            logger.info("Searching for neName: {} inside neTypes: {} with relation: {}", neName, neTypeList.size(), relation);
            List<Map<Object, Object>> vertexResults = executeVertexQuery(neName, neTypeList, relation);
            return flattenVertexResults(vertexResults);
        } catch (Exception e) {
            logger.error("Error searching by neType and neName");
            throw new NetworkElementException("Failed to search by neType and neName: " + e.getMessage(), e);
        }
    }

    private List<Map<Object, Object>> executeVertexQuery(String neName, List<String> neTypeList, String relation) {
        String safeRegex = escapeRegexSpecialChars(neName);
        
        // If relation is provided, filter vertices that have the relation property OR edges with this relation
        if (hasValidParameter(relation)) {
            List<String> relationList = parseRelations(relation);
            logger.info("Filtering vertices and edges by relations: {} (checking both vertex property and edges)", relationList);
            
            if (relationList.size() == 1) {
                // Single relation - use exact match
                String relationValue = relationList.get(0);
                return g.V()
                        .has(AffectedNodesConstants.NETYPE, P.within(neTypeList))
                        .has(AffectedNodesConstants.NENAME, TextP.regex(AffectedNodesConstants.REGEX_PATTERN + safeRegex))
                        .has(AffectedNodesConstants.LAT, P.neq(null)).has(AffectedNodesConstants.LAT, P.neq(""))
                        .has(AffectedNodesConstants.LNG, P.neq(null)).has(AffectedNodesConstants.LNG, P.neq(""))
                        .or(
                            __.has(AffectedNodesConstants.RELATION, relationValue),
                            __.where(__.bothE().has(AffectedNodesConstants.RELATION, relationValue))
                        )
                        .elementMap()
                        .toList();
            } else {
                // Multiple relations - use P.within()
                return g.V()
                        .has(AffectedNodesConstants.NETYPE, P.within(neTypeList))
                        .has(AffectedNodesConstants.NENAME, TextP.regex(AffectedNodesConstants.REGEX_PATTERN + safeRegex))
                        .has(AffectedNodesConstants.LAT, P.neq(null)).has(AffectedNodesConstants.LAT, P.neq(""))
                        .has(AffectedNodesConstants.LNG, P.neq(null)).has(AffectedNodesConstants.LNG, P.neq(""))
                        .or(
                            __.has(AffectedNodesConstants.RELATION, P.within(relationList)),
                            __.where(__.bothE().has(AffectedNodesConstants.RELATION, P.within(relationList)))
                        )
                        .elementMap()
                        .toList();
            }
        }
        
        return g.V()
                .has(AffectedNodesConstants.NETYPE, P.within(neTypeList))
                .has(AffectedNodesConstants.NENAME, TextP.regex(AffectedNodesConstants.REGEX_PATTERN + safeRegex))
                .has(AffectedNodesConstants.LAT, P.neq(null)).has(AffectedNodesConstants.LAT, P.neq(""))
                .has(AffectedNodesConstants.LNG, P.neq(null)).has(AffectedNodesConstants.LNG, P.neq(""))
                .elementMap()
                .toList();
    }

    private List<Map<String, Object>> searchByPopIdInternal(String popId, List<String> neTypeList, String relation) {
        try {
            logger.info("Searching for popId: {} inside neTypes: {} with relation: {}", popId, neTypeList.size(), relation);
            List<Map<Object, Object>> vertexResults = executeVertexQueryByPopId(popId, neTypeList, relation);
            return flattenVertexResults(vertexResults);
        } catch (Exception e) {
            logger.error("Error searching by popId");
            throw new NetworkElementException("Failed to search by popId: " + e.getMessage(), e);
        }
    }

    private List<Map<String, Object>> searchByPopNameInternal(String popName, List<String> neTypeList, String relation) {
        try {
            logger.info("Searching for popName: {} inside neTypes: {} with relation: {}", popName, neTypeList.size(), relation);
            List<Map<Object, Object>> vertexResults = executeVertexQueryByPopName(popName, neTypeList, relation);
            return flattenVertexResults(vertexResults);
        } catch (Exception e) {
            logger.error("Error searching by popName");
            throw new NetworkElementException("Failed to search by popName: " + e.getMessage(), e);
        }
    }

    private List<Map<Object, Object>> executeVertexQueryByPopId(String popId, List<String> neTypeList, String relation) {
        try {
            Long popIdLong = Long.parseLong(popId.trim());
            
            // If relation is provided, filter vertices that have the relation property OR edges with this relation
            if (hasValidParameter(relation)) {
                List<String> relationList = parseRelations(relation);
                logger.info("Filtering vertices by relations: {} (checking both vertex property and edges)", relationList);
                
                if (relationList.size() == 1) {
                    // Single relation - use exact match
                    String relationValue = relationList.get(0);
                    return g.V()
                            .has(AffectedNodesConstants.NETYPE, P.within(neTypeList))
                            .has(AffectedNodesConstants.POP_ID, popIdLong)
                            .has(AffectedNodesConstants.LAT, P.neq(null))
                            .has(AffectedNodesConstants.LNG, P.neq(null))
                            .or(
                                __.has(AffectedNodesConstants.RELATION, relationValue),
                                __.where(__.bothE().has(AffectedNodesConstants.RELATION, relationValue))
                            )
                            .elementMap()
                            .toList();
                } else {
                    // Multiple relations - use P.within()
                    return g.V()
                            .has(AffectedNodesConstants.NETYPE, P.within(neTypeList))
                            .has(AffectedNodesConstants.POP_ID, popIdLong)
                            .has(AffectedNodesConstants.LAT, P.neq(null))
                            .has(AffectedNodesConstants.LNG, P.neq(null))
                            .or(
                                __.has(AffectedNodesConstants.RELATION, P.within(relationList)),
                                __.where(__.bothE().has(AffectedNodesConstants.RELATION, P.within(relationList)))
                            )
                            .elementMap()
                            .toList();
                }
            }
            
            return g.V()
                    .has(AffectedNodesConstants.NETYPE, P.within(neTypeList))
                    .has(AffectedNodesConstants.POP_ID, popIdLong)
                    .has(AffectedNodesConstants.LAT, P.neq(null))
                    .has(AffectedNodesConstants.LNG, P.neq(null))
                    .elementMap()
                    .toList();
        } catch (NumberFormatException e) {
            logger.warn(
                    "popId '{}' is not a valid integer. POP_ID property expects Integer type. Returning empty results.",
                    e.getMessage());
            return new ArrayList<>();
        }
    }

    private List<Map<Object, Object>> executeVertexQueryByPopName(String popName, List<String> neTypeList, String relation) {
        String safeRegex = escapeRegexSpecialChars(popName);
        
        // If relation is provided, filter vertices that have the relation property OR edges with this relation
        if (hasValidParameter(relation)) {
            List<String> relationList = parseRelations(relation);
            logger.info("Filtering vertices/edges by relations: {} (checking both vertex property and edges)", relationList);
            
            if (relationList.size() == 1) {
                // Single relation - use exact match
                String relationValue = relationList.get(0);
                return g.V()
                        .has(AffectedNodesConstants.NETYPE, P.within(neTypeList))
                        .has(AffectedNodesConstants.POP_NAME, TextP.regex(AffectedNodesConstants.REGEX_PATTERN + safeRegex))
                        .has(AffectedNodesConstants.LAT, P.neq(null)).has(AffectedNodesConstants.LAT, P.neq(""))
                        .has(AffectedNodesConstants.LNG, P.neq(null)).has(AffectedNodesConstants.LNG, P.neq(""))
                        .or(
                            __.has(AffectedNodesConstants.RELATION, relationValue),
                            __.where(__.bothE().has(AffectedNodesConstants.RELATION, relationValue))
                        )
                        .dedup()
                        .by(AffectedNodesConstants.POP_NAME)
                        .elementMap()
                        .toList();
            } else {
                // Multiple relations - use P.within()
                return g.V()
                        .has(AffectedNodesConstants.NETYPE, P.within(neTypeList))
                        .has(AffectedNodesConstants.POP_NAME, TextP.regex(AffectedNodesConstants.REGEX_PATTERN + safeRegex))
                        .has(AffectedNodesConstants.LAT, P.neq(null)).has(AffectedNodesConstants.LAT, P.neq(""))
                        .has(AffectedNodesConstants.LNG, P.neq(null)).has(AffectedNodesConstants.LNG, P.neq(""))
                        .or(
                            __.has(AffectedNodesConstants.RELATION, P.within(relationList)),
                            __.where(__.bothE().has(AffectedNodesConstants.RELATION, P.within(relationList)))
                        )
                        .dedup()
                        .by(AffectedNodesConstants.POP_NAME)
                        .elementMap()
                        .toList();
            }
        }
        
        return g.V()
                .has(AffectedNodesConstants.NETYPE, P.within(neTypeList))
                .has(AffectedNodesConstants.POP_NAME, TextP.regex(AffectedNodesConstants.REGEX_PATTERN + safeRegex))
                .has(AffectedNodesConstants.LAT, P.neq(null)).has(AffectedNodesConstants.LAT, P.neq(""))
                .has(AffectedNodesConstants.LNG, P.neq(null)).has(AffectedNodesConstants.LNG, P.neq(""))
                .dedup()
                .by(AffectedNodesConstants.POP_NAME)
                .elementMap()
                .toList();
    }

    private List<Map<String, Object>> flattenVertexResults(List<Map<Object, Object>> vertexResults) {
        List<Map<String, Object>> results = new ArrayList<>();
        for (Map<Object, Object> objMap : vertexResults) {
            results.add(flattenVertexMap(objMap));
        }
        return results;
    }

    private String escapeRegexSpecialChars(String input) {
        return input.replaceAll("([\\\\.*+\\[\\]{}()|^$?])", "\\\\$1");
    }

    /**
     * Retrieves the count of network elements grouped by geographic regions at the
     * specified level.
     *
     * <p>
     * This method counts network elements grouped by the specified geography level
     * (L1-L4)
     * and returns the counts along with a representative latitude and longitude for
     * each region.
     * </p>
     *
     * @param geographyLevel the geography level to group by (L1, L2, L3, or L4)
     * @return a list of maps containing the region name, count, and coordinates
     * @throws InvalidParametersException if the geography level is invalid or empty
     * @throws NetworkElementException    if there is an error during the counting
     *                                    process
     */
    @Override
    public List<Map<String, Object>> stateWiseCount(String geographyLevel) {
        Objects.requireNonNull(geographyLevel, "Geography level cannot be null");

        if (geographyLevel.trim().isEmpty()) {
            throw new InvalidParametersException("Geography level cannot be empty");
        }

        logger.info("Fetching count for geographyLevel: {}", geographyLevel);

        final String geographyField;
        final String geographyType;

        switch (geographyLevel) {
            case "L1":
                geographyField = "geographyL1Name";
                geographyType = "region";
                break;
            case "L2":
                geographyField = "geographyL2Name";
                geographyType = "state";
                break;
            case "L3":
                geographyField = "geographyL3Name";
                geographyType = "city";
                break;
            case "L4":
                geographyField = "geographyL4Name";
                geographyType = "zone";
                break;
            default:
                throw new InvalidParametersException("Invalid geography level: " + geographyLevel);
        }

        List<Map<String, Object>> resultList = new ArrayList<>();

        try (JanusGraphTransaction tx = graph.newTransaction()) {
            GraphTraversalSource gtx = tx.traversal();

            Map<Object, Long> groupedCount = gtx.V()
                    .has(geographyField)
                    .groupCount()
                    .by(geographyField)
                    .next();

            for (Map.Entry<Object, Long> entry : groupedCount.entrySet()) {
                Object geographyName = entry.getKey();
                Long count = entry.getValue();

                Map<String, Object> result = new HashMap<>();
                result.put(geographyType, geographyName.toString());
                result.put(AffectedNodesConstants.COUNT, count);

                List<Map<String, Object>> latLonList = gtx.V()
                        .has(geographyField, geographyName.toString())
                        .has("lat")
                        .has("lng")
                        .project("lat", "lng")
                        .by("lat")
                        .by("lng")
                        .limit(1)
                        .toList();

                if (!latLonList.isEmpty()) {
                    Map<String, Object> latLon = latLonList.get(0);
                    result.put("lat", latLon.get("lat"));
                    result.put("lng", latLon.get("lng"));
                }

                resultList.add(result);
            }

            tx.commit();
            logger.info("Fetched state count result with {} entries", resultList.size());
            return resultList;

        } catch (InvalidParametersException e) {
            logger.error("Invalid geography level");
            throw e;
        } catch (IllegalArgumentException e) {
            logger.error("Invalid argument when fetching state count: {}", e.getMessage());
            throw new InvalidParametersException("Invalid parameters for state count", "ERR-GEO-1002");
        } catch (RuntimeException e) {
            logger.error("Error fetching state count: {}", e.getMessage());
            throw new NetworkElementException("Failed to fetch state count for geography level: " + geographyLevel,
                    "ERR-GEO-1003");
        }
    }

    @Override
    public List<Map<String, Object>> neTypeWiseCount(Map<String, Object> requestPayload) {
        Objects.requireNonNull(requestPayload, "Request payload cannot be null");

        List<Map<String, Object>> results = new ArrayList<>();

        try (JanusGraphTransaction tx = graph.newTransaction()) {
            List<String> neTypes = (List<String>) requestPayload.get("neTypes");
            String geographyLevel = (String) requestPayload.get("geographicType");
            validateGeographyLevel(geographyLevel);

            BoundingBox boundingBox = extractBoundingBox((Map<String, Double>) requestPayload.get("boundingBox"));
            GeographyContext geographyContext = resolveGeographyContext(geographyLevel);

            GraphTraversalSource gtx = tx.traversal();

            if (neTypes == null || neTypes.isEmpty()) {
                Long totalCount = gtx.V().has(AffectedNodesConstants.NETYPE).count().next();
                logger.info("Total count of neTypes: {}", totalCount);

                Map<String, Object> resultMap = new HashMap<>();
                resultMap.put(AffectedNodesConstants.COUNT, totalCount);
                results.add(resultMap);

                tx.commit();
                return results;
            }

            logger.info("Fetching neTypeWiseCount for neTypes: {}, geographyLevel: {}", neTypes.size(), geographyLevel);

            GraphTraversal<Vertex, Vertex> query = buildQuery(gtx, neTypes, geographyContext.getField(), boundingBox);
            Map<Object, Long> groupedCount = query.groupCount().by(geographyContext.getField()).next();

            logger.info("Found {} distinct geography entries for level {}", groupedCount.size(), geographyLevel);

            groupedCount.forEach((geoName, count) -> {
                Map<String, Object> result = new HashMap<>();
                result.put(geographyContext.getType(), geoName.toString());
                result.put(AffectedNodesConstants.COUNT, count);

                List<Map<String, Object>> latLonList = fetchLatLongForGeo(
                        gtx, geographyContext.getField(), geoName.toString(), neTypes, boundingBox);

                if (!latLonList.isEmpty()) {
                    Map<String, Object> latLon = latLonList.get(0);
                    result.put("lat", latLon.get("lat"));
                    result.put("lng", latLon.get("lng"));
                }

                results.add(result);
            });

            tx.commit();

            logger.info("Fetched neTypeWiseCount results with {} entries", results.size());
            return results;

        } catch (InvalidParametersException e) {
            logger.error("Invalid parameters for neTypeWiseCount");
            throw e;
        } catch (ClassCastException e) {
            logger.error("Invalid payload format: {}", e.getMessage());
            throw new InvalidPayloadException("Invalid payload format for neTypeWiseCount", "ERR-PAYLOAD-1001");
        } catch (IllegalArgumentException e) {
            logger.error("Invalid argument when fetching neTypeWiseCount: {}", e.getMessage());
            throw new InvalidParametersException("Invalid parameters for neTypeWiseCount", "ERR-GEO-2003");
        } catch (RuntimeException e) {
            logger.error("Error fetching neTypeWiseCount: {}", e.getMessage());
            throw new NetworkElementException("Failed to fetch network element type count", "ERR-NE-1001");
        }
    }

    private void validateGeographyLevel(String geographyLevel) {
        if (geographyLevel == null || geographyLevel.trim().isEmpty()) {
            throw new InvalidParametersException("Geography type is mandatory");
        }
    }

    private GeographyContext resolveGeographyContext(String level) {
        return switch (level) {
            case "L1" -> new GeographyContext("geographyL1Name", "region");
            case "L2" -> new GeographyContext("geographyL2Name", "state");
            case "L3" -> new GeographyContext("geographyL3Name", "city");
            case "L4" -> new GeographyContext("geographyL4Name", "zone");
            default -> throw new InvalidParametersException("Invalid geography level: " + level);
        };
    }

    private BoundingBox extractBoundingBox(Map<String, Double> box) {
        if (box == null)
            return null;

        Double swLat = box.get("swLat");
        Double swLng = box.get("swLng");
        Double neLat = box.get("neLat");
        Double neLng = box.get("neLng");

        if (swLat == null || swLng == null || neLat == null || neLng == null) {
            throw new InvalidParametersException("Incomplete bounding box coordinates provided");
        }

        return new BoundingBox(swLat, swLng, neLat, neLng);
    }

    private GraphTraversal<Vertex, Vertex> buildQuery(GraphTraversalSource gtx, List<String> neTypes, String geoField,
            BoundingBox box) {
        GraphTraversal<Vertex, Vertex> query = gtx.V().has(geoField).has(AffectedNodesConstants.NETYPE,
                within(neTypes));

        if (box != null) {
            query = query.has("lat", P.gt(box.swLat)).has("lat", P.lt(box.neLat))
                    .has("lng", P.gt(box.swLng)).has("lng", P.lt(box.neLng));
            logger.info("Applied bounding box filters: SW({},{}) NE({},{})",
                    box.swLat, box.swLng, box.neLat, box.neLng);
        }

        return query;
    }

    private List<Map<String, Object>> fetchLatLongForGeo(GraphTraversalSource gtx, String geoField, String geoName,
            List<String> neTypes, BoundingBox box) {
        GraphTraversal<Vertex, Vertex> latLonQuery = gtx.V()
                .has(geoField, geoName)
                .has(AffectedNodesConstants.NETYPE, within(neTypes));

        if (box != null) {
            latLonQuery = latLonQuery.has("lat", P.gt(box.swLat)).has("lat", P.lt(box.neLat))
                    .has("lng", P.gt(box.swLng)).has("lng", P.lt(box.neLng));
        }

        return latLonQuery.project("lat", "lng")
                .by("lat")
                .by("lng")
                .limit(1)
                .toList();
    }

    /**
     * Creates an edge within an existing transaction using pre-fetched data for
     * better performance.
     * This method maintains the exact same logic as createEdgeByNetworkElements
     * but uses pre-fetched vertices and edges to avoid individual database queries.
     *
     * @param data                the edge data
     * @param gtx                 the graph traversal source
     * @param allRequiredVertices map of pre-fetched vertices
     * @param existingEdges       map of pre-fetched edges
     * @return response map for the edge creation
     */
    private Map<String, Object> createEdgeByNetworkElements(Map<String, Object> data,
            GraphTraversalSource gtx,
            Map<String, JanusGraphVertex> allRequiredVertices,
            Map<String, Edge> existingEdges) {
        Map<String, Object> response = new HashMap<>();

        try {
            validateInputForMap(data);

            String sourceInterface = (String) data.get(AffectedNodesConstants.NENAME);
            String destinationInterface = (String) data.get(AffectedNodesConstants.NEIGHBOUR_NENAME);
            String edgeLabel = (String) data.get(AffectedNodesConstants.NETYPE);
            String relation = (String) data.get(AffectedNodesConstants.RELATION);

            String sourceNode = resolveParentNodeName(data, AffectedNodesConstants.PARENT_NE);
            String destinationNode = resolveParentNodeName(data, JanusGraphUtilsConstants.NEIGHBOUR_PARENT_NENAME);

            Vertex sourceVertex = findOrLookupVertex(gtx, allRequiredVertices, sourceInterface, response, data,
                    "Source vertex not found with eName: ");
            if (sourceVertex == null) {
                return response;
            }

            Vertex destinationVertex = findOrLookupVertex(gtx, allRequiredVertices, destinationInterface, response,
                    data,
                    "Destination vertex not found with eName: ");
            if (destinationVertex == null) {
                return response;
            }

            // Get linkType for ISIS relation check
            String linkType = (String) data.get(AffectedNodesConstants.LINK_TYPE);
            // Normalize linkType: use empty string for null/empty to match createEdgeKey logic
            String normalizedLinkType = (linkType != null && !linkType.trim().isEmpty()) ? linkType.trim() : "";

            Edge existingEdge = findOrLookupEdge(gtx, existingEdges, sourceInterface, destinationInterface, relation,
                    normalizedLinkType);

            if (existingEdge != null) {
                logger.info(
                        "Found existing edge for ISIS relation - updating. neName: {}, neighbourNEName: {}, linkType: '{}'",
                        sourceInterface, destinationInterface, normalizedLinkType);
                updateExistingEdgePropertiesForMap(existingEdge, data);
                // Add updated edge properties to response
                response.put(JanusGraphUtilsConstants.CREATED_EDGE, existingEdge.properties());
            } else {
                logger.info("Creating new edge for ISIS relation. neName: {}, neighbourNEName: {}, linkType: '{}'",
                        sourceInterface, destinationInterface, normalizedLinkType);
                createAndAddNewEdge(destinationVertex, sourceVertex, edgeLabel, data, sourceNode, destinationNode,
                        response);
            }

        } catch (Exception e) {
            return handleExceptionForMap(e, response, data);
        }

        response.put(AffectedNodesConstants.EDGE + data.get(AffectedNodesConstants.NENAME),
                AffectedNodesConstants.SUCCESS);
        return response;
    }

    /**
     * Resolves parent node name from data map only.
     * Returns null if not present in data map (no fallback extraction).
     * 
     * @param data       the data map
     * @param dataMapKey the key in data map for parent node name
     * @return the parent node name from data map, or null if not present
     */
    private String resolveParentNodeName(Map<String, Object> data, String dataMapKey) {
        // Use data map value only, no fallback extraction
        String parentNode = (String) data.get(dataMapKey);
        return (parentNode != null && !parentNode.trim().isEmpty()) ? parentNode : null;
    }

    /**
     * Finds vertex from pre-fetched map or performs individual lookup.
     * 
     * @param gtx                 the graph traversal source
     * @param allRequiredVertices the pre-fetched vertices map
     * @param interfaceName       the interface name to find
     * @param response            the response map for error handling
     * @param data                the data map for error response
     * @param errorMessagePrefix  the error message prefix
     * @return the vertex if found, null if not found (error response already set)
     */
    private Vertex findOrLookupVertex(GraphTraversalSource gtx, Map<String, JanusGraphVertex> allRequiredVertices,
            String interfaceName, Map<String, Object> response, Map<String, Object> data, String errorMessagePrefix) {
        Vertex vertex = allRequiredVertices.get(interfaceName);
        if (vertex == null) {
            vertex = findVertex(gtx, interfaceName);
            if (vertex == null) {
                buildErrorResponseForMap(response, data, errorMessagePrefix + interfaceName);
                return null;
            }
        }
        return vertex;
    }

    /**
     * Creates an edge key for lookup, including linkType for ISIS relations.
     * 
     * @param sourceInterface      the source interface name
     * @param destinationInterface the destination interface name
     * @param relation             the relation
     * @param linkType             the link type (required for ISIS relation)
     * @return the edge key string
     */
    private String createEdgeKey(String sourceInterface, String destinationInterface, String relation,
            String linkType) {
        String edgeKey = sourceInterface + "|" + destinationInterface + "|" + relation;
        // For ISIS relation, include linkType in the key (normalize to handle
        // null/empty)
        if ("ISIS".equalsIgnoreCase(relation)) {
            String normalizedLinkType = (linkType != null && !linkType.trim().isEmpty()) ? linkType.trim() : "";
            edgeKey = edgeKey + "|" + normalizedLinkType;
        }
        return edgeKey;
    }

    /**
     * Finds edge from pre-fetched map or performs individual lookup.
     * For ISIS relations, also checks linkType to determine if edge should be
     * updated or created.
     * 
     * @param gtx                  the graph traversal source
     * @param existingEdges        the pre-fetched edges map
     * @param sourceInterface      the source interface name
     * @param destinationInterface the destination interface name
     * @param relation             the relation
     * @param linkType             the link type (required for ISIS relation)
     * @return the edge if found, null otherwise
     */
    private Edge findOrLookupEdge(GraphTraversalSource gtx, Map<String, Edge> existingEdges,
            String sourceInterface, String destinationInterface, String relation, String linkType) {
        String edgeKey = createEdgeKey(sourceInterface, destinationInterface, relation, linkType);
        Edge existingEdge = existingEdges.get(edgeKey);
        if (existingEdge == null) {
            existingEdge = findExistingEdgeWithLinkType(gtx, sourceInterface, destinationInterface, relation, linkType);
        }
        return existingEdge;
    }

    /**
     * Creates a new edge and adds it to the response.
     * 
     * @param destinationVertex the destination vertex
     * @param sourceVertex      the source vertex
     * @param edgeLabel         the edge label
     * @param data              the data map
     * @param sourceNode        the source node name
     * @param destinationNode   the destination node name
     * @param response          the response map
     */
    private void createAndAddNewEdge(Vertex destinationVertex, Vertex sourceVertex, String edgeLabel,
            Map<String, Object> data, String sourceNode, String destinationNode, Map<String, Object> response) {
        String sourceInterface = (String) data.get(AffectedNodesConstants.NENAME);
        String destinationInterface = (String) data.get(AffectedNodesConstants.NEIGHBOUR_NENAME);

        String cktId = data.get(AffectedNodesConstants.CKTID) != null
                ? data.get(AffectedNodesConstants.CKTID).toString()
                : null;
        Edge createdEdge = createNewEdgeForMap(destinationVertex, sourceVertex, edgeLabel, data,
                sourceNode, destinationNode,
                cktId,
                (String) data.get(AffectedNodesConstants.HOSTNAME),
                (String) data.get(AffectedNodesConstants.NECATEGORY),
                (String) data.get(AffectedNodesConstants.SRC_CATEGORY),
                (String) data.get(AffectedNodesConstants.DEST_CATEGORY),
                (String) data.get(AffectedNodesConstants.RELATION),
                extractingId(sourceInterface), extractingId(destinationInterface),
                (String) data.get(JanusGraphUtilsConstants.NEIGHBOUR_NE_ID), (String) data.get("neId"));
        response.put(JanusGraphUtilsConstants.CREATED_EDGE, createdEdge.properties());
    }

    private void validateInputForMap(Map<String, Object> data) {
        if (isNullOrEmpty((String) data.get(AffectedNodesConstants.NENAME)) ||
                isNullOrEmpty((String) data.get(AffectedNodesConstants.PARENT_NE)) ||
                isNullOrEmpty((String) data.get(JanusGraphUtilsConstants.NEIGHBOUR_PARENT_NENAME)) ||
                isNullOrEmpty((String) data.get(AffectedNodesConstants.NETYPE))) {
            throw new InvalidParametersException("Invalid request: eName, parentNeName, and edgeLabel are required.");
        }
    }

    private Map<String, Object> buildErrorResponseForMap(Map<String, Object> response, Map<String, Object> data,
            String message) {
        logger.warn(message);
        response.put((String) data.get(AffectedNodesConstants.NENAME), AffectedNodesConstants.ERROR);
        response.put(AffectedNodesConstants.MSG, message);
        return response;
    }

    private void updateExistingEdgePropertiesForMap(Edge existingEdge, Map<String, Object> data) {
        // Use parent node names directly from data map only (no fallback extraction)
        String sourceNode = (String) data.get(AffectedNodesConstants.PARENT_NE);
        String destinationNode = (String) data.get(JanusGraphUtilsConstants.NEIGHBOUR_PARENT_NENAME);

        // Update start and end properties only if values are present in data map
        if (sourceNode != null && !sourceNode.trim().isEmpty()) {
            existingEdge.property(AffectedNodesConstants.START, sourceNode);
        }
        if (destinationNode != null && !destinationNode.trim().isEmpty()) {
            existingEdge.property(AffectedNodesConstants.END, destinationNode);
        }

        existingEdge.property(JanusGraphUtilsConstants.DEST_LAT,
                data.get(AffectedNodesConstants.NEIGHBOUR_PARENT_NE_LAT));
        existingEdge.property(JanusGraphUtilsConstants.DEST_LONG,
                data.get(AffectedNodesConstants.NEIGHBOUR_PARENT_NE_LNG));
        existingEdge.property(JanusGraphUtilsConstants.NEIGHBOUR_VENDOR,
                data.get(AffectedNodesConstants.NEIGHBOUR_VENDOR));
        existingEdge.property(JanusGraphUtilsConstants.NEIGHBOUR_DOMAIN,
                data.get(AffectedNodesConstants.NEIGHBOUR_DOMAIN));
        existingEdge.property(JanusGraphUtilsConstants.NEIGHBOUR_TECH,
                data.get(AffectedNodesConstants.NEIGHBOUR_TECHNOLOGY));
        existingEdge.property(JanusGraphUtilsConstants.SRC_LAT, data.get(AffectedNodesConstants.LAT));
        existingEdge.property(JanusGraphUtilsConstants.SRC_LONG, data.get(AffectedNodesConstants.LNG));
        existingEdge.property(JanusGraphUtilsConstants.LAST_DISCOVERY_TIME, data.get(JanusGraphUtilsConstants.LAST_DISCOVERY_TIME));
        existingEdge.property(AffectedNodesConstants.ISDEL, data.get(AffectedNodesConstants.ISDEL));
        // Add missing properties for edge updates
        existingEdge.property(JanusGraphUtilsConstants.SRC_VENDOR, data.get(JanusGraphUtilsConstants.SRC_VENDOR));
        existingEdge.property(JanusGraphUtilsConstants.DEST_VENDOR, data.get(JanusGraphUtilsConstants.DEST_VENDOR));
        existingEdge.property(JanusGraphUtilsConstants.SRC_TECHNOLOGY,
                data.get(JanusGraphUtilsConstants.SRC_TECHNOLOGY));
        existingEdge.property(JanusGraphUtilsConstants.DEST_TECHNOLOGY,
                data.get(JanusGraphUtilsConstants.DEST_TECHNOLOGY));
        existingEdge.property(JanusGraphUtilsConstants.SRC_TYPE, data.get(JanusGraphUtilsConstants.SRC_TYPE));
        existingEdge.property(JanusGraphUtilsConstants.DEST_TYPE, data.get(JanusGraphUtilsConstants.DEST_TYPE));
        existingEdge.property(JanusGraphUtilsConstants.SRC_DOMAIN, data.get(JanusGraphUtilsConstants.SRC_DOMAIN));
        existingEdge.property(JanusGraphUtilsConstants.DEST_DOMAIN, data.get(JanusGraphUtilsConstants.DEST_DOMAIN));
        // Add missing category properties for edge updates
        existingEdge.property(AffectedNodesConstants.SRC_CATEGORY, data.get(AffectedNodesConstants.SRC_CATEGORY));
        existingEdge.property(AffectedNodesConstants.DEST_CATEGORY, data.get(AffectedNodesConstants.DEST_CATEGORY));
        // Update interface properties correctly with new values
        existingEdge.property(JanusGraphUtilsConstants.SRC_INTERFACE, data.get(AffectedNodesConstants.NENAME));
        existingEdge.property(JanusGraphUtilsConstants.DEST_INTERFACE,
                data.get(AffectedNodesConstants.NEIGHBOUR_NENAME));
        // Update vertex neId properties - startNeId from destination vertex (neighbourNEId) and endNeId from source vertex (neId)
        // Note: Edge is created from destinationVertex to sourceVertex, so START_NEID should be neighbourNEId and END_NEID should be neId
        String sourceNeId = (String) data.get("neId");
        String destNeId = (String) data.get(JanusGraphUtilsConstants.NEIGHBOUR_NE_ID);
        if (destNeId != null && !destNeId.trim().isEmpty()) {
            existingEdge.property(JanusGraphUtilsConstants.START_NEID, destNeId);
        }
        if (sourceNeId != null && !sourceNeId.trim().isEmpty()) {
            existingEdge.property(JanusGraphUtilsConstants.END_NEID, sourceNeId);
        }
    }

    @SuppressWarnings("java:S107")
    private Edge createNewEdgeForMap(Vertex destinationVertex, Vertex sourceVertex, String edgeLabel,
            Map<String, Object> data, String sourceNode, String destinationNode,
            String cktId, String hostname, String neCategory, String sourceCategory,
            String destinationCategory, String relation, String sourceId, String destinationId,
            String sourceNodeId, String destinationNodeId) {
        // Create edge with all properties except START and END (which are conditional)
        Edge edge = destinationVertex.addEdge(edgeLabel, sourceVertex,
                AffectedNodesConstants.CKTID, cktId,
                AffectedNodesConstants.LINE_NAME, hostname,
                AffectedNodesConstants.SRC_CATEGORY, sourceCategory,
                AffectedNodesConstants.DEST_CATEGORY, destinationCategory,
                AffectedNodesConstants.RELATION, relation,
                JanusGraphUtilsConstants.CREATED_AT, System.currentTimeMillis(),
                AffectedNodesConstants.STATUS, JanusGraphUtilsConstants.ACTIVE,
                JanusGraphUtilsConstants.SRC_INTERFACE, data.get(AffectedNodesConstants.NENAME),
                JanusGraphUtilsConstants.DEST_INTERFACE, data.get(AffectedNodesConstants.NEIGHBOUR_NENAME),
                JanusGraphUtilsConstants.SRC_LAT, data.get(AffectedNodesConstants.LAT),
                JanusGraphUtilsConstants.SRC_LONG, data.get(AffectedNodesConstants.LNG),
                JanusGraphUtilsConstants.DEST_LAT, data.get(AffectedNodesConstants.NEIGHBOUR_PARENT_NE_LAT),
                JanusGraphUtilsConstants.DEST_LONG, data.get(AffectedNodesConstants.NEIGHBOUR_PARENT_NE_LNG),
                JanusGraphUtilsConstants.SRC_TYPE, data.get(JanusGraphUtilsConstants.SRC_TYPE),
                JanusGraphUtilsConstants.NEIGHBOUR_VENDOR, data.get(AffectedNodesConstants.NEIGHBOUR_VENDOR),
                JanusGraphUtilsConstants.NEIGHBOUR_DOMAIN, data.get(AffectedNodesConstants.NEIGHBOUR_DOMAIN),
                JanusGraphUtilsConstants.START_ID, sourceId,
                JanusGraphUtilsConstants.END_ID, destinationId,
                JanusGraphUtilsConstants.START_NEID, sourceNodeId,
                JanusGraphUtilsConstants.END_NEID, destinationNodeId,
                JanusGraphUtilsConstants.NEIGHBOUR_TECH, data.get(AffectedNodesConstants.NEIGHBOUR_TECHNOLOGY),
                AffectedNodesConstants.ISDEL, data.get(AffectedNodesConstants.ISDEL),
                AffectedNodesConstants.SRC_BANDWIDTH, data.get(AffectedNodesConstants.SRC_BANDWIDTH),
                AffectedNodesConstants.DEST_BANDWIDTH, data.get(AffectedNodesConstants.DEST_BANDWIDTH),
                AffectedNodesConstants.LINK_TYPE, data.get(AffectedNodesConstants.LINK_TYPE),
                AffectedNodesConstants.AREA_IP_ADDRESS, data.get(AffectedNodesConstants.AREA_IP_ADDRESS),
                AffectedNodesConstants.TAG, data.get(AffectedNodesConstants.TAG),
                // Add missing properties for edge response
                JanusGraphUtilsConstants.SRC_VENDOR, data.get(JanusGraphUtilsConstants.SRC_VENDOR),
                JanusGraphUtilsConstants.DEST_VENDOR, data.get(JanusGraphUtilsConstants.DEST_VENDOR),
                JanusGraphUtilsConstants.SRC_TECHNOLOGY, data.get(JanusGraphUtilsConstants.SRC_TECHNOLOGY),
                JanusGraphUtilsConstants.DEST_TECHNOLOGY, data.get(JanusGraphUtilsConstants.DEST_TECHNOLOGY),
                JanusGraphUtilsConstants.DEST_TYPE, data.get(JanusGraphUtilsConstants.DEST_TYPE),
                JanusGraphUtilsConstants.SRC_DOMAIN, data.get(JanusGraphUtilsConstants.SRC_DOMAIN),
                JanusGraphUtilsConstants.DEST_DOMAIN, data.get(JanusGraphUtilsConstants.DEST_DOMAIN),
                // Add missing category properties for edge response
                AffectedNodesConstants.SRC_CATEGORY, data.get(AffectedNodesConstants.SRC_CATEGORY),
                AffectedNodesConstants.DEST_CATEGORY, data.get(AffectedNodesConstants.DEST_CATEGORY));

        // Conditionally add START and END properties only if parentNE values are
        // present
        if (sourceNode != null && !sourceNode.trim().isEmpty()) {
            edge.property(AffectedNodesConstants.START, sourceNode);
        }
        if (destinationNode != null && !destinationNode.trim().isEmpty()) {
            edge.property(AffectedNodesConstants.END, destinationNode);
        }

        // Set lastDiscoveryTime from data if available
        edge.property(JanusGraphUtilsConstants.LAST_DISCOVERY_TIME, data.get(JanusGraphUtilsConstants.LAST_DISCOVERY_TIME));

        return edge;
    }

    private Map<String, Object> handleExceptionForMap(Exception e, Map<String, Object> response,
            Map<String, Object> data) {
        if (e instanceof VertexNotFoundException ||
                e instanceof InvalidParametersException ||
                e instanceof IllegalArgumentException) {
            logger.warn("Handled exception: {}", e.getMessage());
        } else {
            logger.error("Unhandled exception while creating edge for {}: {}", data.get(AffectedNodesConstants.NENAME),
                    e.getMessage());
        }

        response.put((String) data.get(AffectedNodesConstants.NENAME), AffectedNodesConstants.ERROR);
        response.put(AffectedNodesConstants.MSG, (e instanceof IllegalArgumentException)
                ? JanusGraphUtilsConstants.INVALID + e.getMessage()
                : "Failed to create edge: " + e.getMessage());
        return response;
    }

    /**
     * Creates multiple vertices and edges in a two-phase process.
     * Phase 1: Creates all vertices first without creating edges.
     * Phase 2: Creates all edges after vertices have been created.
     * This ensures all required vertices exist before edge creation.
     *
     * @param payloadList the list of vertex data to process
     * @return list of response maps for the batch
     */
    @Override
    public List<Map<String, Object>> createMultipleVertices(List<Map<String, Object>> payloadList) {
        List<Map<String, Object>> finalResponseList = new ArrayList<>();

        if (payloadList == null || payloadList.isEmpty()) {
            logger.warn("Empty payload list provided to createMultipleVertices");
            return finalResponseList;
        }

        logger.info("Processing {} vertices in two-phase mode", payloadList.size());

        // Determine if this cycle should force all records as NEW
        boolean graphWasEmptyAtStart = isGraphEmpty();
        if (graphWasEmptyAtStart) {
            forceAllNewCycle = true;
        }

        long startTime = System.currentTimeMillis();

        // Phase 1: Create all vertices first
        List<Map<String, Object>> vertexResults = createAllVerticesFirst(payloadList);
        finalResponseList.addAll(vertexResults);

        // Phase 2: Create all edges in separate batches (only for child records)
        List<Map<String, Object>> edgeResults = createAllEdgesSecond(payloadList);
        finalResponseList.addAll(edgeResults);

        long endTime = System.currentTimeMillis();
        logger.info("Completed two-phase processing {} vertices in {} ms ({} ms per recordItem)",
                payloadList.size(), (endTime - startTime),
                !payloadList.isEmpty() ? (endTime - startTime) / payloadList.size() : 0);

        // Accumulate results instead of sending notification immediately
        if (!finalResponseList.isEmpty()) {
            try {
                BatchProcessingSummary tempSummary = analyzeBatchResults(finalResponseList, payloadList,
                        forceAllNewCycle);
                if (!tempSummary.getNewRecords().isEmpty() || !tempSummary.getUpdatedRecords().isEmpty()
                        || !tempSummary.getFailedRecords().isEmpty()) {
                    tempSummary.setProcessingTime((endTime - startTime) + " ms");
                    accumulateAndScheduleNotification(tempSummary);
                }
            } catch (Exception e) {
                logger.warn("Failed to accumulate batch processing results: {}", e.getMessage());
            }
        }
        return finalResponseList;
    }

    /**
     * Phase 1: Creates all vertices first without creating edges.
     * This ensures all vertices exist before attempting edge creation.
     *
     * @param payloadList the list of vertex data to process
     * @return list of vertex creation results
     */
    private List<Map<String, Object>> createAllVerticesFirst(List<Map<String, Object>> payloadList) {
        List<Map<String, Object>> vertexResults = new ArrayList<>();

        logger.info("Phase 1: Creating {} vertices", payloadList.size());

        // Process vertices in batches - increased batch size for better performance
        int batchSize = AffectedNodesConstants.VERTEX_BATCH_SIZE; // Much larger batch size for vertex-only operations
        int totalBatches = (payloadList.size() + batchSize - 1) / batchSize;

        for (int batchIndex = 0; batchIndex < totalBatches; batchIndex++) {
            int startIndex = batchIndex * batchSize;
            int endIndex = Math.min(startIndex + batchSize, payloadList.size());
            List<Map<String, Object>> batch = payloadList.subList(startIndex, endIndex);

            long batchStartTime = System.currentTimeMillis();
            logger.info("Phase 1: Processing vertex batch {}/{} with {} records", batchIndex + 1, totalBatches,
                    batch.size());

            try {
                List<Map<String, Object>> batchResults = processVertexOnlyBatch(batch);
                vertexResults.addAll(batchResults);

                long batchEndTime = System.currentTimeMillis();
                logger.info("Phase 1: Completed vertex batch {}/{} in {} ms", batchIndex + 1, totalBatches,
                        (batchEndTime - batchStartTime));

            } catch (Exception e) {
                logger.error("Phase 1: Error processing vertex batch {}/{}: {}", batchIndex + 1, totalBatches,
                        e.getMessage());
                // Create error responses for all records in the failed batch
                for (Map<String, Object> data : batch) {
                    Map<String, Object> errorResponse = createErrorResponse(data,
                            "Vertex creation failed: " + e.getMessage());
                    vertexResults.add(errorResponse);
                }
            }

            // Force garbage collection between batches
            if (batchIndex < totalBatches - 1) {
                System.gc(); // NOSONAR - intentional GC call between batches
            }
        }

        logger.info("Phase 1: Completed creating {} vertices", payloadList.size());
        return vertexResults;
    }

    /**
     * Phase 2: Creates all edges after vertices have been created.
     * This ensures all required vertices exist before edge creation.
     * Only processes records that need edge creation (child records).
     *
     * @param payloadList the list of data to create edges from
     * @return list of edge creation results
     */
    private List<Map<String, Object>> createAllEdgesSecond(List<Map<String, Object>> payloadList) {
        List<Map<String, Object>> edgeResults = new ArrayList<>();

        // Filter only records that need edge creation (child records)
        List<Map<String, Object>> edgeRecords = payloadList.stream()
                .filter(data -> {
                    String neName = (String) data.get(AffectedNodesConstants.NENAME);
                    String neighbourName = (String) data.get(AffectedNodesConstants.NEIGHBOUR_NENAME);
                    String relation = (String) data.get(AffectedNodesConstants.RELATION);
                    // Only create edges if we have both source and destination and relation
                    return neName != null && neighbourName != null && relation != null &&
                            !neName.trim().isEmpty() && !neighbourName.trim().isEmpty() && !relation.trim().isEmpty();
                })
                .toList();

        logger.info("Phase 2: Creating edges for {} records (filtered from {} total)", edgeRecords.size(),
                payloadList.size());

        if (edgeRecords.isEmpty()) {
            logger.info("Phase 2: No edges to create, skipping edge creation phase");
            return new ArrayList<>();
        }

        // Process edges in optimized batches for better performance
        int batchSize = AffectedNodesConstants.EDGE_BATCH_SIZE; // Increased batch size for edge operations
        int totalBatches = (edgeRecords.size() + batchSize - 1) / batchSize;

        for (int batchIndex = 0; batchIndex < totalBatches; batchIndex++) {
            int startIndex = batchIndex * batchSize;
            int endIndex = Math.min(startIndex + batchSize, edgeRecords.size());
            List<Map<String, Object>> batch = edgeRecords.subList(startIndex, endIndex);

            long batchStartTime = System.currentTimeMillis();
            logger.info("Phase 2: Processing edge batch {}/{} with {} records", batchIndex + 1, totalBatches,
                    batch.size());

            try {
                List<Map<String, Object>> batchResults = processEdgeOnlyBatch(batch);
                edgeResults.addAll(batchResults);

                long batchEndTime = System.currentTimeMillis();
                logger.info("Phase 2: Completed edge batch {}/{} in {} ms", batchIndex + 1, totalBatches,
                        (batchEndTime - batchStartTime));

            } catch (Exception e) {
                logger.error("Phase 2: Error processing edge batch {}/{}: {}", batchIndex + 1, totalBatches,
                        e.getMessage());
                // Create error responses for all records in the failed batch
                for (Map<String, Object> data : batch) {
                    Map<String, Object> errorResponse = createErrorResponse(data,
                            "Edge creation failed: " + e.getMessage());
                    edgeResults.add(errorResponse);
                }
            }

            // Force garbage collection between batches
            if (batchIndex < totalBatches - 1) {
                System.gc(); // NOSONAR - intentional GC call between batches
            }
        }

        logger.info("Phase 2: Completed creating edges for {} records", edgeRecords.size());
        return edgeResults;
    }

    /**
     * Phase 1: Processes a batch of vertices only (no edge creation).
     * This method creates vertices efficiently without attempting edge creation.
     *
     * @param batch the batch of vertex data to process
     * @return list of response maps for the batch
     */
    private List<Map<String, Object>> processVertexOnlyBatch(List<Map<String, Object>> batch) {
        List<Map<String, Object>> batchResults = new ArrayList<>();

        // Pre-validate all records in the batch
        Map<String, Map<String, Object>> validRecords = new HashMap<>();
        for (Map<String, Object> data : batch) {
            try {
                String neName = (String) data.get(AffectedNodesConstants.NENAME);
                String neType = (String) data.get(AffectedNodesConstants.NETYPE);
                validateMandatoryFields(neName, neType);
                validRecords.put(neName, data);
            } catch (Exception e) {
                // Create error response for invalid records
                Map<String, Object> errorResponse = createErrorResponse(data, e.getMessage());
                batchResults.add(errorResponse);
            }
        }

        if (validRecords.isEmpty()) {
            return batchResults;
        }

        // Process valid records in a single transaction for better performance
        try (JanusGraphTransaction tx = graph.newTransaction()) {
            GraphTraversalSource gtx = tx.traversal();

            // Batch fetch existing vertices to reduce database queries
            Map<String, JanusGraphVertex> existingVertices = batchFetchExistingVerticesOptimized(gtx,
                    validRecords.keySet());

            // Process vertices in parallel for better performance
            List<Map<String, Object>> responses = validRecords.values().parallelStream()
                    .map(data -> processVertexOnlyInTransaction(data, gtx, existingVertices))
                    .toList();

            batchResults.addAll(responses);

            tx.commit();
            logger.info("Phase 1: Successfully committed vertex batch of {} vertices", validRecords.size());

        } catch (Exception e) {
            logger.error("Phase 1: Error processing vertex batch: {}", e.getMessage());
            // Create error responses for all records in the batch
            for (Map<String, Object> data : validRecords.values()) {
                Map<String, Object> errorResponse = createErrorResponse(data,
                        "Vertex batch processing failed: " + e.getMessage());
                batchResults.add(errorResponse);
            }
        }

        return batchResults;
    }

    /**
     * Phase 2: Processes a batch of edges only (vertices already exist).
     * This method creates edges efficiently assuming all vertices exist.
     *
     * @param batch the batch of edge data to process
     * @return list of response maps for the batch
     */
    private List<Map<String, Object>> processEdgeOnlyBatch(List<Map<String, Object>> batch) {
        List<Map<String, Object>> batchResults = new ArrayList<>();

        // Pre-validate all records in the batch
        // Use composite key (neName + neighbourNEName + relation + linkType for ISIS) to avoid overwriting records with same neName but different neighbourNEName or linkType
        Map<String, Map<String, Object>> validRecords = new HashMap<>();
        for (Map<String, Object> data : batch) {
            try {
                String neName = (String) data.get(AffectedNodesConstants.NENAME);
                String neType = (String) data.get(AffectedNodesConstants.NETYPE);
                validateMandatoryFields(neName, neType);

                // Create composite key to handle multiple edges with same neName but different
                // neighbourNEName/linkType
                String neighbourNEName = (String) data.get(AffectedNodesConstants.NEIGHBOUR_NENAME);
                String relation = (String) data.get(AffectedNodesConstants.RELATION);
                String linkType = (String) data.get(AffectedNodesConstants.LINK_TYPE);
                String normalizedLinkType = (linkType != null && !linkType.trim().isEmpty()) ? linkType.trim() : "";

                // Create unique key for each edge record
                String recordKey = createEdgeKey(neName,
                        neighbourNEName != null ? neighbourNEName : "",
                        relation != null ? relation : "",
                        normalizedLinkType);

                validRecords.put(recordKey, data);
            } catch (Exception e) {
                // Create error response for invalid records
                Map<String, Object> errorResponse = createErrorResponse(data, e.getMessage());
                batchResults.add(errorResponse);
            }
        }

        if (validRecords.isEmpty()) {
            return batchResults;
        }

        // Process valid records in a single transaction for better performance
        try (JanusGraphTransaction tx = graph.newTransaction()) {
            GraphTraversalSource gtx = tx.traversal();

            // Batch fetch all required vertices for edge creation
            Map<String, JanusGraphVertex> allRequiredVertices = batchFetchAllRequiredVertices(gtx, validRecords);

            // Batch fetch existing edges to avoid individual queries
            Map<String, Edge> existingEdges = batchFetchExistingEdges(gtx, validRecords);

            // Process edges in parallel for better performance
            List<Map<String, Object>> responses = validRecords.values().parallelStream()
                    .map(data -> processEdgeOnlyInTransaction(data, gtx, allRequiredVertices, existingEdges))
                    .toList();

            batchResults.addAll(responses);

            tx.commit();
            logger.info("Phase 2: Successfully committed edge batch of {} edges", validRecords.size());

        } catch (Exception e) {
            logger.error("Phase 2: Error processing edge batch: {}", e.getMessage());
            // Create error responses for all records in the batch
            for (Map<String, Object> data : validRecords.values()) {
                Map<String, Object> errorResponse = createErrorResponse(data,
                        "Edge batch processing failed: " + e.getMessage());
                batchResults.add(errorResponse);
            }
        }

        return batchResults;
    }

    /**
     * Processes a single vertex data within an existing transaction (vertex
     * creation only).
     * This method creates vertices without attempting edge creation.
     *
     * @param data             the vertex data to process
     * @param gtx              the graph traversal source
     * @param existingVertices map of existing vertices
     * @return response map for the vertex
     */
    private Map<String, Object> processVertexOnlyInTransaction(
            Map<String, Object> data,
            GraphTraversalSource gtx,
            Map<String, JanusGraphVertex> existingVertices) {

        Map<String, Object> finalResponse = new HashMap<>();
        Map<String, Object> vertexStatus = initVertexStatus(data);

        try {
            // Determine update or create operation
            String neName = String.valueOf(data.get(AffectedNodesConstants.NENAME));
            Object relation = data.get(AffectedNodesConstants.RELATION);

            // Normalize relation to string for consistent key creation (same as in
            // batchFetchExistingVerticesOptimized)
            String relationStr = (relation != null) ? relation.toString().trim() : null;
            String vertexKey = createVertexKey(neName, relationStr);
            JanusGraphVertex existingVertex = existingVertices.get(vertexKey);

            // Additional safety check: verify relation matches if vertex exists
            if (existingVertex != null) {
                Object existingRelation = existingVertex.property(AffectedNodesConstants.RELATION).orElse(null);
                String existingRelationStr = (existingRelation != null) ? existingRelation.toString().trim() : null;

                // If relations don't match, don't use this vertex (treat as new)
                if (!areRelationsEqual(relationStr, existingRelationStr)) {
                    logger.warn(
                            "Found existing vertex with different relation. neName: {}, existingRelation: {}, newRelation: {}. Creating new vertex instead of updating.",
                            neName, existingRelationStr, relationStr);
                    existingVertex = null;
                } else {
                    logger.info("Found matching vertex for update. neName: {}, relation: {}", neName, relationStr);
                }
            } else {
                logger.info("No existing vertex found. Creating new vertex. neName: {}, relation: {}", neName,
                        relationStr);
            }

            boolean shouldUpdate = shouldUpdateVertex(existingVertex, data);

            List<String> updatedProps = new ArrayList<>();
            JanusGraphVertex vertex = getOrCreateVertex(gtx, data, existingVertex, shouldUpdate, updatedProps);

            // Set vertex properties
            setVertexProperties(vertex, data);

            // Update edges if required
            updateEdgesIfNeeded(gtx, data, vertexStatus, shouldUpdate, updatedProps);

            vertexStatus.put(AffectedNodesConstants.STATUS_KEY, AffectedNodesConstants.SUCCESS);

        } catch (Exception e) {
            vertexStatus.put(AffectedNodesConstants.STATUS_KEY, AffectedNodesConstants.ERROR);
            vertexStatus.put(AffectedNodesConstants.ERROR_MSG, e.getMessage());
            logger.warn("Phase 1: Error processing vertex {}: {}", data.get(AffectedNodesConstants.NENAME),
                    e.getMessage());
        }

        finalResponse.put(AffectedNodesConstants.VERTEX_STATUS, vertexStatus);
        return finalResponse;
    }

    /**
     * Checks if two relation values are equal, handling null cases properly.
     * 
     * @param relation1 first relation value (can be String or Object)
     * @param relation2 second relation value (can be String or Object)
     * @return true if relations are equal, false otherwise
     */
    private boolean areRelationsEqual(Object relation1, Object relation2) {
        if (relation1 == null && relation2 == null) {
            return true;
        }
        if (relation1 == null || relation2 == null) {
            return false;
        }
        // Normalize both to strings and compare
        String rel1Str = relation1.toString().trim();
        String rel2Str = relation2.toString().trim();
        return rel1Str.equals(rel2Str);
    }

    private Map<String, Object> initVertexStatus(Map<String, Object> data) {
        Map<String, Object> status = new HashMap<>();
        status.put(AffectedNodesConstants.NENAME, data.get(AffectedNodesConstants.NENAME));
        status.put(AffectedNodesConstants.NETYPE, data.get(AffectedNodesConstants.NETYPE));
        return status;
    }

    private boolean shouldUpdateVertex(JanusGraphVertex existingVertex, Map<String, Object> data) {
        if (existingVertex == null) {
            return false;
        }

        Object relation = data.get(AffectedNodesConstants.RELATION);
        Object existingRelation = existingVertex.property(AffectedNodesConstants.RELATION).orElse(null);

        // Use normalized comparison to ensure consistent matching
        return areRelationsEqual(relation, existingRelation);
    }

    private JanusGraphVertex getOrCreateVertex(
            GraphTraversalSource gtx,
            Map<String, Object> data,
            JanusGraphVertex existingVertex,
            boolean shouldUpdate,
            List<String> updatedProps) {

        if (shouldUpdate) {
            updatedProps.addAll(getUpdatedVertexProperties(existingVertex, data));
            return existingVertex;
        }

        return (JanusGraphVertex) gtx.addV((String) data.get(AffectedNodesConstants.NETYPE)).next();
    }

    private void setVertexProperties(JanusGraphVertex vertex, Map<String, Object> data) {
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            if (entry.getValue() != null && !isSourceDestinationProperty(entry.getKey())) {
                vertex.property(entry.getKey(), entry.getValue());
            }
        }
    }

    private void updateEdgesIfNeeded(
            GraphTraversalSource gtx,
            Map<String, Object> data,
            Map<String, Object> vertexStatus,
            boolean shouldUpdate,
            List<String> updatedProps) {

        if (shouldUpdate && !updatedProps.isEmpty()) {
            String neName = (String) data.get(AffectedNodesConstants.NENAME);
            List<String> edgeUpdatedProps = updateEdgesIfVertexUpdated(gtx, neName, updatedProps, data);

            if (!edgeUpdatedProps.isEmpty()) {
                vertexStatus.put(AffectedNodesConstants.MESSAGE, "Edge updated for neName: " + neName + " ");
            }
        }
    }

    /**
     * Processes a single edge data within an existing transaction (edge creation
     * only).
     * This method creates edges assuming all vertices already exist.
     *
     * @param data                the edge data to process
     * @param gtx                 the graph traversal source
     * @param allRequiredVertices map of pre-fetched vertices
     * @param existingEdges       map of pre-fetched edges
     * @return response map for the edge
     */
    private Map<String, Object> processEdgeOnlyInTransaction(Map<String, Object> data,
            GraphTraversalSource gtx,
            Map<String, JanusGraphVertex> allRequiredVertices,
            Map<String, Edge> existingEdges) {
        Map<String, Object> finalResponse = new HashMap<>();
        Map<String, Object> edgeStatus = new HashMap<>();

        String neName = (String) data.get(AffectedNodesConstants.NENAME);

        try {
            Map<String, Object> edgeResponse = createEdgeByNetworkElements(data, gtx, allRequiredVertices,
                    existingEdges);
            edgeStatus.putAll(edgeResponse);
        } catch (Exception e) {
            edgeStatus.put(AffectedNodesConstants.EDGE + neName, AffectedNodesConstants.ERROR);
            edgeStatus.put(AffectedNodesConstants.ERROR_MSG, e.getMessage());
            logger.warn("Phase 2: Error processing edge for {}: {}", neName, e.getMessage());
        }

        finalResponse.put(AffectedNodesConstants.EDGE_STATUS, edgeStatus);
        return finalResponse;
    }

    /**
     * Batch fetches existing vertices to reduce database round trips.
     * Fetches vertices with both neName and relation to ensure correct matching.
     *
     * @param gtx     the graph traversal source
     * @param neNames set of network element names to fetch
     * @return map of composite key (neName + relation) to existing vertex
     */
    private Map<String, JanusGraphVertex> batchFetchExistingVerticesOptimized(GraphTraversalSource gtx,
            Set<String> neNames) {
        Map<String, JanusGraphVertex> existingVertices = new HashMap<>();

        if (neNames.isEmpty()) {
            return existingVertices;
        }

        // Use batch query to fetch all existing vertices at once
        // Fetch vertices that have neName in the set
        List<Vertex> vertices = gtx.V()
                .has(AffectedNodesConstants.NENAME, P.within(neNames))
                .toList();

        for (Vertex vertex : vertices) {
            String neName = vertex.value(AffectedNodesConstants.NENAME);
            Object relation = vertex.property(AffectedNodesConstants.RELATION).orElse(null);

            // Normalize relation to string for consistent key creation
            String relationStr = (relation != null) ? relation.toString().trim() : null;
            String key = createVertexKey(neName, relationStr);

            // Only store if key doesn't already exist to avoid overwriting
            // This ensures that if multiple vertices have same neName but different relations,they are all stored correctly
            if (!existingVertices.containsKey(key)) {
                existingVertices.put(key, (JanusGraphVertex) vertex);
                logger.info("Cached vertex with key: {}, neName: {}, relation: {}", key, neName, relationStr);
            } else {
                // Log warning if duplicate key is found (shouldn't happen with proper relation filtering)
                logger.warn(
                        "Duplicate vertex key found: {} for neName: {}, relation: {}. This may indicate data inconsistency.",
                        key, neName, relationStr);
            }
        }

        logger.info("Batch fetched {} vertices for {} neNames, created {} unique keys",
                vertices.size(), neNames.size(), existingVertices.size());
        return existingVertices;
    }

    /**
     * Creates a composite key for vertex lookup using neName and relation.
     *
     * @param neName   the network element name
     * @param relation the relation value (can be null)
     * @return composite key string
     */
    private String createVertexKey(String neName, Object relation) {
        if (relation != null && !relation.toString().trim().isEmpty()) {
            return neName + "|" + relation.toString();
        }
        return neName;
    }

    /**
     * Batch fetches all required vertices for edge creation (source and destination
     * vertices).
     * This reduces individual vertex lookups from potentially hundreds to just a
     * few queries.
     *
     * @param gtx          the graph traversal source
     * @param validRecords map of valid records to process
     * @return map of vertex identifier to vertex
     */
    private Map<String, JanusGraphVertex> batchFetchAllRequiredVertices(GraphTraversalSource gtx,
            Map<String, Map<String, Object>> validRecords) {
        Map<String, JanusGraphVertex> allVertices = new HashMap<>();

        if (validRecords.isEmpty()) {
            return allVertices;
        }

        // Collect all unique vertex names (source and destination)
        Set<String> allVertexNames = new HashSet<>();
        for (Map<String, Object> data : validRecords.values()) {
            String sourceInterface = (String) data.get(AffectedNodesConstants.NENAME);
            String destinationInterface = (String) data.get(AffectedNodesConstants.NEIGHBOUR_NENAME);

            if (sourceInterface != null && !sourceInterface.trim().isEmpty()) {
                allVertexNames.add(sourceInterface);
            }
            if (destinationInterface != null && !destinationInterface.trim().isEmpty()) {
                allVertexNames.add(destinationInterface);
            }
        }

        if (allVertexNames.isEmpty()) {
            return allVertices;
        }

        // Batch fetch all vertices in a single query
        List<Vertex> vertices = gtx.V()
                .has(AffectedNodesConstants.NENAME, P.within(allVertexNames))
                .toList();

        for (Vertex vertex : vertices) {
            String neName = vertex.value(AffectedNodesConstants.NENAME);
            allVertices.put(neName, (JanusGraphVertex) vertex);
        }

        logger.info("Batch fetched {} vertices for edge creation", allVertices.size());
        return allVertices;
    }

    /**
     * Batch fetches existing edges to avoid individual edge lookups.
     * This reduces edge queries from potentially hundreds to just a few queries.
     *
     * @param gtx          the graph traversal source
     * @param validRecords map of valid records to process
     * @return map of edge key to edge
     */
    private Map<String, Edge> batchFetchExistingEdges(GraphTraversalSource gtx,
            Map<String, Map<String, Object>> validRecords) {

        Map<String, Edge> existingEdges = new HashMap<>();
        if (validRecords.isEmpty()) {
            return existingEdges;
        }

        // Step 1 → Prepare edge keys
        Set<String> edgeKeys = collectEdgeKeys(validRecords);
        if (edgeKeys.isEmpty()) {
            return existingEdges;
        }

        // Step 2 → Fetch all edges
        List<Edge> allEdges = fetchAllEdges(gtx);

        // Step 3 → Filter & match edges
        filterMatchingEdges(edgeKeys, allEdges, existingEdges);

        logger.info("Batch fetched {} existing edges", existingEdges.size());
        return existingEdges;
    }

    private Set<String> collectEdgeKeys(Map<String, Map<String, Object>> validRecords) {
        Set<String> edgeKeys = new HashSet<>();

        for (Map<String, Object> data : validRecords.values()) {
            String sourceInterface = (String) data.get(AffectedNodesConstants.NENAME);
            String destinationInterface = (String) data.get(AffectedNodesConstants.NEIGHBOUR_NENAME);
            String relation = (String) data.get(AffectedNodesConstants.RELATION);
            String linkType = (String) data.get(AffectedNodesConstants.LINK_TYPE);

            String normalizedLinkType = normalizeLinkType(linkType);

            if (sourceInterface != null && destinationInterface != null && relation != null) {
                edgeKeys.add(createEdgeKey(sourceInterface, destinationInterface, relation, normalizedLinkType));
            }
        }
        return edgeKeys;
    }

    private List<Edge> fetchAllEdges(GraphTraversalSource gtx) {
        return gtx.E()
                .has(JanusGraphUtilsConstants.SRC_INTERFACE)
                .has(JanusGraphUtilsConstants.DEST_INTERFACE)
                .has(JanusGraphUtilsConstants.RELATION)
                .toList();
    }

    private void filterMatchingEdges(Set<String> edgeKeys, List<Edge> allEdges, Map<String, Edge> existingEdges) {

        for (Edge edge : allEdges) {
            String src = edge.value(JanusGraphUtilsConstants.SRC_INTERFACE);
            String dest = edge.value(JanusGraphUtilsConstants.DEST_INTERFACE);
            String rel = edge.value(JanusGraphUtilsConstants.RELATION);

            String linkType = extractLinkType(edge);
            String normalizedLinkType = normalizeLinkType(linkType);

            if (src != null && dest != null && rel != null) {
                String edgeKey = createEdgeKey(src, dest, rel, normalizedLinkType);

                if (edgeKeys.contains(edgeKey)) {
                    existingEdges.put(edgeKey, edge);
                    logger.info("Matched existing edge with key: {} (src: {}, dest: {}, relation: {}, linkType: '{}')",
                            edgeKey, src, dest, rel, normalizedLinkType);
                }
            }
        }
    }

    private String normalizeLinkType(String linkType) {
        return (linkType != null && !linkType.trim().isEmpty()) ? linkType.trim() : "";
    }

    private String extractLinkType(Edge edge) {
        try {
            Object linkTypeObj = edge.property(AffectedNodesConstants.LINK_TYPE).orElse(null);
            return (linkTypeObj != null) ? linkTypeObj.toString() : null;
        } catch (Exception e) {
            try {
                return edge.value(AffectedNodesConstants.LINK_TYPE);
            } catch (Exception ex) {
                return null;
            }
        }
    }

    /**
     * Creates an error response for failed vertex processing.
     *
     * @param data         the original data that failed
     * @param errorMessage the error message
     * @return error response map
     */
    private Map<String, Object> createErrorResponse(Map<String, Object> data, String errorMessage) {
        Map<String, Object> finalResponse = new HashMap<>();
        Map<String, Object> vertexStatus = new HashMap<>();
        Map<String, Object> edgeStatus = new HashMap<>();

        String neName = (String) data.get(AffectedNodesConstants.NENAME);
        String neType = (String) data.get(AffectedNodesConstants.NETYPE);

        vertexStatus.put(AffectedNodesConstants.NENAME, neName);
        vertexStatus.put(AffectedNodesConstants.NETYPE, neType);
        vertexStatus.put(AffectedNodesConstants.STATUS_KEY, AffectedNodesConstants.ERROR);
        vertexStatus.put(AffectedNodesConstants.ERROR_MSG, errorMessage);

        edgeStatus.put(AffectedNodesConstants.EDGE + neName, AffectedNodesConstants.ERROR);
        edgeStatus.put(AffectedNodesConstants.MSG, errorMessage);

        finalResponse.put(AffectedNodesConstants.VERTEX_STATUS, vertexStatus);
        finalResponse.put(AffectedNodesConstants.EDGE_STATUS, edgeStatus);
        return finalResponse;
    }

    private void validateMandatoryFields(String neName, String neType) {
        if (neName == null || neName.trim().isEmpty()) {
            throw new InvalidParametersException("neName is required");
        }
        if (neType == null || neType.trim().isEmpty()) {
            throw new InvalidParametersException("neType is required");
        }
    }

    /**
     * Checks if a property key is related to source or destination properties that
     * should be excluded from vertex.
     * 
     * @param propertyKey the property key to check
     * @return true if the property should be excluded from vertex, false otherwise
     */
    private boolean isSourceDestinationProperty(String propertyKey) {
        if (propertyKey == null) {
            return false;
        }

        // List of source and destination related properties that should not be in
        // vertex properties
        return propertyKey.equals(JanusGraphUtilsConstants.SRC_VENDOR) ||
                propertyKey.equals(JanusGraphUtilsConstants.DEST_VENDOR) ||
                propertyKey.equals(JanusGraphUtilsConstants.SRC_TECHNOLOGY) ||
                propertyKey.equals(JanusGraphUtilsConstants.DEST_TECHNOLOGY) ||
                propertyKey.equals(JanusGraphUtilsConstants.SRC_TYPE) ||
                propertyKey.equals(JanusGraphUtilsConstants.DEST_TYPE) ||
                propertyKey.equals(JanusGraphUtilsConstants.SRC_DOMAIN) ||
                propertyKey.equals(JanusGraphUtilsConstants.DEST_DOMAIN) ||
                propertyKey.equals(AffectedNodesConstants.SRC_CATEGORY) ||
                propertyKey.equals(AffectedNodesConstants.DEST_CATEGORY) ||
                propertyKey.equals(JanusGraphUtilsConstants.NEIGHBOUR_NENAME) ||
                propertyKey.equals(JanusGraphUtilsConstants.NEIGHBOUR_NE_ID) ||
                propertyKey.equals(JanusGraphUtilsConstants.NEIGHBOUR_PARENT_NENAME) ||
                propertyKey.equals("neighbourParentNELat") ||
                propertyKey.equals("neighbourParentNELong") ||
                propertyKey.equals("neighbourNELat") ||
                propertyKey.equals("neighbourNELong") ||
                propertyKey.equals(JanusGraphUtilsConstants.CKTID);
    }

    /**
     * Updates edges if vertex properties are updated using ultra-optimized Gremlin
     * queries.
     * This method uses the fastest possible approach with zero loops for production
     * performance.
     *
     * @param g            the graph traversal source
     * @param neName       the name of the vertex
     * @param updatedProps the updated properties
     * @param data         the new vertex data
     * @return list of updated edge properties
     */
    private List<String> updateEdgesIfVertexUpdated(GraphTraversalSource g, String neName, List<String> updatedProps,
            Map<String, Object> data) {
        List<String> updatedInEdge = new ArrayList<>();
        if (updatedProps == null || updatedProps.isEmpty() || neName == null || neName.trim().isEmpty()) {
            return updatedInEdge;
        }

        try {
            // Get relation from data to filter edges by relation
            Object relation = data.get(AffectedNodesConstants.RELATION);
            String relationStr = (relation != null) ? relation.toString().trim() : null;

            updatedInEdge = updateEdgePropertiesBasedOnVertex(g, neName, updatedProps, data, relationStr);

        } catch (Exception e) {
            logger.error("Error updating edges for vertex {}: {}", neName, e.getMessage());
        }

        return updatedInEdge;
    }

    /**
     * Ultra-optimized edge updates using single Gremlin operations for maximum
     * production performance.
     * This method eliminates all loops and uses the fastest possible Gremlin bulk
     * operations.
     * Only updates edges that have the same relation as the vertex being updated.
     *
     * @param g            the graph traversal source
     * @param neName       the name of the vertex
     * @param updatedProps the updated properties
     * @param data         the new vertex data
     * @param relationStr  the relation string to filter edges (only edges with this relation will be updated)
     * @return list of updated edge properties
     */
    private List<String> updateEdgePropertiesBasedOnVertex(GraphTraversalSource g, String neName,
            List<String> updatedProps,
            Map<String, Object> data, String relationStr) {
        List<String> updatedInEdge = new ArrayList<>();

        try {
            List<String> sourceUpdates = updateSourceEdges(g, neName, updatedProps, data, relationStr);
            updatedInEdge.addAll(sourceUpdates);

            List<String> destUpdates = updateDestinationEdges(g, neName, updatedProps, data, relationStr);
            updatedInEdge.addAll(destUpdates);

            updateEdgeModifiedTimestamp(g, neName, relationStr, data);

        } catch (Exception e) {
            logger.error("Error in ultra-optimized Gremlin edge updates for vertex {}: {}", neName, e.getMessage());
        }

        return updatedInEdge;
    }

    /**
     * Updates source edges using Gremlin bulk operations.
     * Uses direct property mapping without loops for maximum performance.
     * Only updates edges that have the same relation as the vertex being updated.
     */
    private List<String> updateSourceEdges(GraphTraversalSource g, String neName, List<String> updatedProps,
            Map<String, Object> data, String relationStr) {
        List<String> updatedEdgeProps = new ArrayList<>();

        if (updatedProps == null || updatedProps.isEmpty()) {
            return updatedEdgeProps;
        }

        try {
            // Filter edges by srcInterface AND relation to ensure we only update edges with
            // matching relation
            GraphTraversal<Edge, Edge> traversal = g.E()
                    .has(JanusGraphUtilsConstants.SRC_INTERFACE, neName);

            // Add relation filter if relation is provided
            if (relationStr != null && !relationStr.isEmpty()) {
                traversal = traversal.has(AffectedNodesConstants.RELATION, relationStr);
                logger.info("Filtering source edges by relation: {} for neName: {}", relationStr, neName);
            } else {
                // If no relation, only update edges that also have no relation
                traversal = traversal.hasNot(AffectedNodesConstants.RELATION);
                logger.info("Filtering source edges with no relation for neName: {}", neName);
            }

            // Helper list to store property mappings
            List<PropertyMappingForSource> propertyMappings = Arrays.asList(
                    new PropertyMappingForSource(AffectedNodesConstants.LAT, JanusGraphUtilsConstants.SRC_LAT),
                    new PropertyMappingForSource(AffectedNodesConstants.LNG, JanusGraphUtilsConstants.SRC_LONG),
                    new PropertyMappingForSource(AffectedNodesConstants.ISDEL, AffectedNodesConstants.ISDEL),
                    new PropertyMappingForSource(AffectedNodesConstants.CATEGORY, AffectedNodesConstants.SRC_CATEGORY),
                    new PropertyMappingForSource(AffectedNodesConstants.HOSTNAME, AffectedNodesConstants.LINE_NAME),
                    new PropertyMappingForSource(AffectedNodesConstants.NE_STATUS, AffectedNodesConstants.STATUS),
                    new PropertyMappingForSource("neId", JanusGraphUtilsConstants.START_NEID),
                    new PropertyMappingForSource(AffectedNodesConstants.PARENT_NE, AffectedNodesConstants.START),
                    // Update SRC_INTERFACE with the new neName value
                    new PropertyMappingForSource(AffectedNodesConstants.NENAME, JanusGraphUtilsConstants.SRC_INTERFACE),
                    new PropertyMappingForSource(JanusGraphUtilsConstants.CKTID, AffectedNodesConstants.CKTID),
                    new PropertyMappingForSource(JanusGraphUtilsConstants.RELATION, AffectedNodesConstants.RELATION),
                    new PropertyMappingForSource(JanusGraphUtilsConstants.LAST_DISCOVERY_TIME,
                            JanusGraphUtilsConstants.LAST_DISCOVERY_TIME),
                    // Add missing source-specific properties with correct source keys
                    new PropertyMappingForSource(JanusGraphUtilsConstants.SRC_VENDOR,
                            JanusGraphUtilsConstants.SRC_VENDOR),
                    new PropertyMappingForSource(JanusGraphUtilsConstants.SRC_TECHNOLOGY,
                            JanusGraphUtilsConstants.SRC_TECHNOLOGY),
                    new PropertyMappingForSource(JanusGraphUtilsConstants.SRC_DOMAIN,
                            JanusGraphUtilsConstants.SRC_DOMAIN),
                    new PropertyMappingForSource(JanusGraphUtilsConstants.SRC_TYPE, JanusGraphUtilsConstants.SRC_TYPE),
                    new PropertyMappingForSource(AffectedNodesConstants.SRC_CATEGORY,
                            AffectedNodesConstants.SRC_CATEGORY));

            boolean hasUpdates = false;

            for (PropertyMappingForSource mapping : propertyMappings) {
                if (updatedProps.contains(mapping.getSourceKey()) && data.get(mapping.getSourceKey()) != null) {
                    traversal = traversal.property(mapping.getTargetKey(), data.get(mapping.getSourceKey()));
                    updatedEdgeProps.add(mapping.getTargetKey());
                    hasUpdates = true;
                }
            }

            // Always update lastDiscoveryTime if present in data, regardless of updatedProps
            Object lastDiscoveryTime = data.get(JanusGraphUtilsConstants.LAST_DISCOVERY_TIME);
            if (lastDiscoveryTime != null) {
                traversal = traversal.property(JanusGraphUtilsConstants.LAST_DISCOVERY_TIME, lastDiscoveryTime);
                if (!updatedEdgeProps.contains(JanusGraphUtilsConstants.LAST_DISCOVERY_TIME)) {
                    updatedEdgeProps.add(JanusGraphUtilsConstants.LAST_DISCOVERY_TIME);
                }
                hasUpdates = true;
            }

            if (hasUpdates) {
                traversal.iterate();
            }

        } catch (Exception e) {
            logger.error("Error updating source edges for vertex {}: {}", neName, e.getMessage());
        }

        return updatedEdgeProps;
    }

    /**
     * Helper inner class to store property mappings.
     */
    private static class PropertyMappingForSource {
        private final String sourceKey;
        private final String targetKey;

        PropertyMappingForSource(String sourceKey, String targetKey) {
            this.sourceKey = sourceKey;
            this.targetKey = targetKey;
        }

        String getSourceKey() {
            return sourceKey;
        }

        String getTargetKey() {
            return targetKey;
        }
    }

    /**
     * Updates destination edges using Gremlin bulk operations.
     * Uses direct property mapping without loops for maximum performance.
     * Only updates edges that have the same relation as the vertex being updated.
     */
    private List<String> updateDestinationEdges(GraphTraversalSource g, String neName, List<String> updatedProps,
            Map<String, Object> data, String relationStr) {
        List<String> updatedEdgeProps = new ArrayList<>();

        if (updatedProps == null || updatedProps.isEmpty()) {
            return updatedEdgeProps;
        }

        try {
            // Filter edges by destInterface AND relation to ensure we only update edges
            // with matching relation
            GraphTraversal<Edge, Edge> traversal = g.E()
                    .has(JanusGraphUtilsConstants.DEST_INTERFACE, neName);

            // Add relation filter if relation is provided
            if (relationStr != null && !relationStr.isEmpty()) {
                traversal = traversal.has(AffectedNodesConstants.RELATION, relationStr);
                logger.info("Filtering destination edges by relation: {} for neName: {}", relationStr, neName);
            } else {
                // If no relation, only update edges that also have no relation
                traversal = traversal.hasNot(AffectedNodesConstants.RELATION);
                logger.info("Filtering destination edges with no relation for neName: {}", neName);
            }

            // Define all property mappings (source → destination key)
            // For destination edges, we need to use the vertex's own properties since it's
            // the destination
            List<PropertyMappingForDestination> propertyMappings = Arrays.asList(
                    new PropertyMappingForDestination(AffectedNodesConstants.LAT, JanusGraphUtilsConstants.DEST_LAT),
                    new PropertyMappingForDestination(AffectedNodesConstants.LNG, JanusGraphUtilsConstants.DEST_LONG),
                    new PropertyMappingForDestination(AffectedNodesConstants.ISDEL, AffectedNodesConstants.ISDEL),
                    new PropertyMappingForDestination(AffectedNodesConstants.CATEGORY,
                            AffectedNodesConstants.DEST_CATEGORY),
                    new PropertyMappingForDestination(AffectedNodesConstants.HOSTNAME,
                            AffectedNodesConstants.LINE_NAME),
                    new PropertyMappingForDestination(AffectedNodesConstants.NE_STATUS, AffectedNodesConstants.STATUS),
                    new PropertyMappingForDestination(AffectedNodesConstants.PARENT_NE, AffectedNodesConstants.END),
                    // Update DEST_INTERFACE with the new neName value
                    new PropertyMappingForDestination(AffectedNodesConstants.NENAME,
                            JanusGraphUtilsConstants.DEST_INTERFACE),
                    new PropertyMappingForDestination(JanusGraphUtilsConstants.NEIGHBOUR_NE_ID,
                            JanusGraphUtilsConstants.END_NEID),
                    new PropertyMappingForDestination(JanusGraphUtilsConstants.CKTID, AffectedNodesConstants.CKTID),
                    new PropertyMappingForDestination(JanusGraphUtilsConstants.RELATION,
                            AffectedNodesConstants.RELATION),
                    new PropertyMappingForDestination(JanusGraphUtilsConstants.LAST_DISCOVERY_TIME,
                            JanusGraphUtilsConstants.LAST_DISCOVERY_TIME),
                    // For destination edges, the vertex being updated is the destination, so we
                    // update both DESTINATION and SOURCE properties
                    // The vertex's properties should update both destination and source properties
                    // of edges where it's the destination
                    new PropertyMappingForDestination(JanusGraphUtilsConstants.DEST_VENDOR,
                            JanusGraphUtilsConstants.DEST_VENDOR),
                    new PropertyMappingForDestination(JanusGraphUtilsConstants.DEST_TECHNOLOGY,
                            JanusGraphUtilsConstants.DEST_TECHNOLOGY),
                    new PropertyMappingForDestination(JanusGraphUtilsConstants.DEST_DOMAIN,
                            JanusGraphUtilsConstants.DEST_DOMAIN),
                    new PropertyMappingForDestination(JanusGraphUtilsConstants.DEST_TYPE,
                            JanusGraphUtilsConstants.DEST_TYPE),
                    new PropertyMappingForDestination(AffectedNodesConstants.DEST_CATEGORY,
                            AffectedNodesConstants.DEST_CATEGORY),
                    // Also update source properties since the vertex contains source information
                    new PropertyMappingForDestination(JanusGraphUtilsConstants.SRC_VENDOR,
                            JanusGraphUtilsConstants.SRC_VENDOR),
                    new PropertyMappingForDestination(JanusGraphUtilsConstants.SRC_TECHNOLOGY,
                            JanusGraphUtilsConstants.SRC_TECHNOLOGY),
                    new PropertyMappingForDestination(JanusGraphUtilsConstants.SRC_DOMAIN,
                            JanusGraphUtilsConstants.SRC_DOMAIN),
                    new PropertyMappingForDestination(JanusGraphUtilsConstants.SRC_TYPE,
                            JanusGraphUtilsConstants.SRC_TYPE),
                    new PropertyMappingForDestination(AffectedNodesConstants.SRC_CATEGORY,
                            AffectedNodesConstants.SRC_CATEGORY));

            boolean hasUpdates = false;

            // Iterate over all mappings instead of repetitive if blocks
            for (PropertyMappingForDestination mapping : propertyMappings) {
                String sourceKey = mapping.getSourceKey();
                String targetKey = mapping.getTargetKey();
                Object value = data.get(sourceKey);

                if (updatedProps.contains(sourceKey) && value != null) {
                    traversal = traversal.property(targetKey, value);
                    updatedEdgeProps.add(targetKey);
                    hasUpdates = true;
                }
            }

            // Always update lastDiscoveryTime if present in data, regardless of updatedProps
            Object lastDiscoveryTime = data.get(JanusGraphUtilsConstants.LAST_DISCOVERY_TIME);
            if (lastDiscoveryTime != null) {
                traversal = traversal.property(JanusGraphUtilsConstants.LAST_DISCOVERY_TIME, lastDiscoveryTime);
                if (!updatedEdgeProps.contains(JanusGraphUtilsConstants.LAST_DISCOVERY_TIME)) {
                    updatedEdgeProps.add(JanusGraphUtilsConstants.LAST_DISCOVERY_TIME);
                }
                hasUpdates = true;
            }

            if (hasUpdates) {
                traversal.iterate();
            }

        } catch (Exception e) {
            logger.error("Error updating destination edges for vertex {}: {}", neName, e.getMessage());
        }

        return updatedEdgeProps;
    }

    /**
     * Helper inner class to map property keys.
     */
    private static class PropertyMappingForDestination {
        private final String sourceKey;
        private final String targetKey;

        PropertyMappingForDestination(String sourceKey, String targetKey) {
            this.sourceKey = sourceKey;
            this.targetKey = targetKey;
        }

        String getSourceKey() {
            return sourceKey;
        }

        String getTargetKey() {
            return targetKey;
        }
    }

    /**
     * Updates the modified timestamp for all edges connected to a vertex using
     * Gremlin. Only updates edges that have the same relation as the vertex being
     * updated.
     */
    private void updateEdgeModifiedTimestamp(GraphTraversalSource g, String neName, String relationStr, Map<String, Object> data) {
        try {
            GraphTraversal<Edge, Edge> traversal = g.E()
                    .or(
                            __.has(JanusGraphUtilsConstants.SRC_INTERFACE, neName),
                            __.has(JanusGraphUtilsConstants.DEST_INTERFACE, neName));

            // Add relation filter if relation is provided
            if (relationStr != null && !relationStr.isEmpty()) {
                traversal = traversal.has(AffectedNodesConstants.RELATION, relationStr);
            } else {
                // If no relation, only update edges that also have no relation
                traversal = traversal.hasNot(AffectedNodesConstants.RELATION);
            }

            // Use LAST_DISCOVERY_TIME from data if available
            Object lastDiscoveryTime = data != null ? data.get(JanusGraphUtilsConstants.LAST_DISCOVERY_TIME) : null;
            if (lastDiscoveryTime != null) {
                traversal.property(JanusGraphUtilsConstants.LAST_DISCOVERY_TIME, lastDiscoveryTime)
                        .iterate();
            }
        } catch (Exception e) {
            logger.warn("Error updating edge modified timestamp for vertex {} with relation {}: {}",
                    neName, relationStr, e.getMessage());
        }
    }

    /**
     * Gets the updated vertex properties from the existing vertex and the new data.
     *
     * @param existingVertex the existing vertex
     * @param newData        the new data
     * @return the updated vertex properties
     */
    private List<String> getUpdatedVertexProperties(JanusGraphVertex existingVertex, Map<String, Object> newData) {
        List<String> updatedProps = new ArrayList<>();
        if (existingVertex == null)
            return updatedProps;

        for (Map.Entry<String, Object> entry : newData.entrySet()) {
            String key = entry.getKey();
            Object newValue = entry.getValue();
            Object oldValue = existingVertex.property(key).orElse(null);

            // Special handling for modification time properties to ensure they are always
            // updated
            if (isModificationTimeProperty(key)) {
                if (newValue != null) {
                    updatedProps.add(key);
                    logger.info("Modification time property {} will be updated with value: {}", key, newValue);
                }
            } else if ((oldValue == null && newValue != null) || (newValue != null && !newValue.equals(oldValue))) {
                updatedProps.add(key);
            }
        }
        return updatedProps;
    }

    /**
     * Checks if a property key is a modification time property that should always
     * be updated.
     *
     * @param propertyKey the property key to check
     * @return true if it's a modification time property
     */
    private boolean isModificationTimeProperty(String propertyKey) {
        if (propertyKey == null) {
            return false;
        }

        return propertyKey.equals(JanusGraphUtilsConstants.NE_MODIFY_TIME) ||
                propertyKey.equals(JanusGraphUtilsConstants.LLDP_MODIFY_TIME) ||
                propertyKey.equals(JanusGraphUtilsConstants.OSPF_MODIFY_TIME) ||
                propertyKey.equals(JanusGraphUtilsConstants.LLDP_INTERFACE_MODIFY_TIME) ||
                propertyKey.equals(JanusGraphUtilsConstants.OSPF_INTERFACE_MODIFY_TIME);
    }

    /**
     * Exports all JanusGraph data in the specified format and operation mode.
     * This method retrieves vertices and/or edges from the graph database and
     * formats them according to the requested output format.
     *
     * <p>
     * Supported formats:
     * </p>
     * <ul>
     * <li>{@code "json"} - Exports data as formatted JSON</li>
     * <li>{@code "csv"} - Exports data as comma-separated values</li>
     * <li>{@code "pdf"} - Exports data as a formatted PDF document</li>
     * </ul>
     *
     * <p>
     * Operation modes:
     * </p>
     * <ul>
     * <li>{@code "vertex"} - Exports only vertex data</li>
     * <li>{@code "edge"} - Exports only edge data</li>
     * <li>{@code "both"} - Exports both vertex and edge data</li>
     * </ul>
     *
     * <p>
     * Note: Only vertices with latitude and longitude coordinates are included in
     * the export.
     * </p>
     *
     * @param format the output format for the exported data, must not be null
     * @param opmode the operation mode specifying what data to export, must not be
     *               null
     * @return byte array containing the exported data
     * @throws IllegalArgumentException if format is not supported or parameters are
     *                                  null
     * @throws DataProcessingException  if no data is found or export fails
     */
    @Override
    public byte[] downloadAllData(String format, String opmode) {
        try {
            logger.info("Exporting JanusGraph data in format: {}, opmode: {}", format, opmode);

            List<Map<String, Object>> vertices = new ArrayList<>();
            List<Map<String, Object>> edges = new ArrayList<>();

            if (AffectedNodesConstants.VERTICES.equalsIgnoreCase(opmode) || "both".equalsIgnoreCase(opmode)) {
                vertices = g.V().elementMap().toList().stream()
                        .map(this::flattenVertexMap)
                        .filter(v -> v.containsKey(AffectedNodesConstants.LAT)
                                && v.containsKey(AffectedNodesConstants.LNG))
                        .toList();
            }

            if ("edge".equalsIgnoreCase(opmode) || "both".equalsIgnoreCase(opmode)) {
                edges = g.E().elementMap().toList().stream()
                        .map(this::flattenEdgeMap)
                        .toList();
            }

            if (vertices.isEmpty() && edges.isEmpty()) {
                throw new DataProcessingException("No data found in JanusGraph for opmode: " + opmode);
            }

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

            switch (format.toLowerCase()) {
                case "json":
                    writeJsonCombined(vertices, edges, outputStream);
                    break;
                case "csv":
                    writeCsvCombined(vertices, edges, outputStream);
                    break;
                case "pdf":
                    writePdfCombined(vertices, edges, outputStream);
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported format: " + format);
            }

            return outputStream.toByteArray();

        } catch (Exception e) {
            logger.error("Export failed");
            throw new DataProcessingException("Data export failed: " + e.getMessage(), e);
        }
    }

    /**
     * Flattens a vertex element map by removing internal JanusGraph properties.
     * This method delegates to the common flattening logic.
     *
     * @param objMap the vertex element map to flatten
     * @return flattened map containing only user-defined properties
     */
    private Map<String, Object> flattenVertexMap(Map<Object, Object> objMap) {
        return flattenElementMap(objMap);
    }

    /**
     * Flattens an edge element map by removing internal JanusGraph properties.
     * This method delegates to the common flattening logic.
     *
     * @param objMap the edge element map to flatten
     * @return flattened map containing only user-defined properties
     */
    private Map<String, Object> flattenEdgeMap(Map<Object, Object> objMap) {
        return flattenElementMap(objMap);
    }

    /**
     * Flattens a JanusGraph element map by removing internal properties and
     * extracting the first value from list properties.
     *
     * <p>
     * This method removes the following internal properties:
     * </p>
     * <ul>
     * <li>{@code "id"} - JanusGraph internal identifier</li>
     * <li>{@code "label"} - Element label</li>
     * <li>{@code "IN"} - Incoming vertex reference</li>
     * <li>{@code "OUT"} - Outgoing vertex reference</li>
     * </ul>
     *
     * <p>
     * For list properties, only the first element is retained.
     * </p>
     *
     * @param objMap the element map to flatten
     * @return flattened map with internal properties removed
     */
    private Map<String, Object> flattenElementMap(Map<Object, Object> objMap) {
        Map<String, Object> flatMap = new HashMap<>();
        for (Map.Entry<Object, Object> entry : objMap.entrySet()) {
            String key = entry.getKey().toString();
            Object value = entry.getValue();

            // Skip internal JanusGraph properties
            if ("id".equals(key) || AffectedNodesConstants.LABEL.equals(key) ||
                    "IN".equals(key) || "OUT".equals(key)) {
                continue;
            }

            if (value instanceof List<?> list && !list.isEmpty()) {
                flatMap.put(key, list.get(0));
            } else {
                flatMap.put(key, value);
            }
        }
        return flatMap;
    }

    /**
     * Writes vertex and edge data to an output stream in JSON format.
     * The data is structured with separate "vertices" and "edges" sections.
     *
     * <p>
     * The JSON output includes proper date formatting and pretty printing
     * for readability.
     * </p>
     *
     * @param vertices list of vertex data maps
     * @param edges    list of edge data maps
     * @param os       the output stream to write to
     * @throws IOException if writing to the output stream fails
     */
    private void writeJsonCombined(List<Map<String, Object>> vertices, List<Map<String, Object>> edges, OutputStream os)
            throws IOException {
        Map<String, Object> combined = new HashMap<>();
        combined.put("vertices", vertices);
        combined.put("edges", edges);

        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        mapper.writerWithDefaultPrettyPrinter().writeValue(os, combined);
    }

    /**
     * Writes vertex and edge data to an output stream in CSV format.
     * Each section (vertices and edges) is clearly labeled and separated.
     *
     * <p>
     * The CSV output includes:
     * </p>
     * <ul>
     * <li>Section headers for vertices and edges</li>
     * <li>Column headers from the first data row</li>
     * <li>Properly escaped values with double quotes</li>
     * <li>Empty string for null values</li>
     * </ul>
     *
     * @param vertices list of vertex data maps
     * @param edges    list of edge data maps
     * @param os       the output stream to write to
     */
    private void writeCsvCombined(List<Map<String, Object>> vertices, List<Map<String, Object>> edges,
            OutputStream os) {
        try (PrintWriter writer = new PrintWriter(os)) {
            if (!vertices.isEmpty()) {
                writer.println("  [JANUSGRAPH DATA FOR VERTICES] ");
                Set<String> headers = vertices.get(0).keySet();
                writer.println(String.join(",", headers));
                for (Map<String, Object> row : vertices) {
                    List<String> values = headers.stream()
                            .map(h -> Optional.ofNullable(row.get(h)).orElse("").toString())
                            .map(v -> "\"" + v.replace("\"", "\"\"") + "\"")
                            .toList();
                    writer.println(String.join(",", values));
                }
                writer.println();
            }

            if (!edges.isEmpty()) {
                writer.println(" [JANUSGRAPH DATA FOR EDGES] ");
                Set<String> headers = edges.get(0).keySet();
                writer.println(String.join(",", headers));
                for (Map<String, Object> row : edges) {
                    List<String> values = headers.stream()
                            .map(h -> Optional.ofNullable(row.get(h)).orElse("").toString())
                            .map(v -> "\"" + v.replace("\"", "\"\"") + "\"")
                            .toList();
                    writer.println(String.join(",", values));
                }
            }
        }
    }

    /**
     * Writes vertex and edge data to an output stream in PDF format.
     * Creates a professionally formatted document with styled tables.
     *
     * <p>
     * The PDF includes:
     * </p>
     * <ul>
     * <li>Professional color scheme with navy blue headers</li>
     * <li>Alternating row colors for better readability</li>
     * <li>Centered titles and headers</li>
     * <li>Proper spacing and borders</li>
     * </ul>
     *
     * @param vertices list of vertex data maps
     * @param edges    list of edge data maps
     * @param os       the output stream to write to
     * @throws DataProcessingException if PDF generation fails
     */
    private void writePdfCombined(List<Map<String, Object>> vertices, List<Map<String, Object>> edges,
            OutputStream os) {
        try {
            // Professional color palette
            final BaseColor primaryColor = new BaseColor(44, 62, 80); // Navy blue
            final BaseColor headerBgColor = primaryColor;
            final BaseColor headerTextColor = BaseColor.WHITE;
            final BaseColor rowBgOdd = new BaseColor(245, 245, 245); // Very light gray
            final BaseColor rowBgEven = BaseColor.WHITE;
            final BaseColor borderColor = new BaseColor(200, 221, 242); // Light blue-gray
            final BaseColor cellTextColor = new BaseColor(51, 51, 51); // Charcoal

            Document document = new Document(PageSize.A4, AffectedNodesConstants.FORTY, AffectedNodesConstants.FORTY,
                    AffectedNodesConstants.FIFTY, AffectedNodesConstants.FIFTY);
            PdfWriter.getInstance(document, os);
            document.open();

            // Fonts
            Font titleFont = FontFactory.getFont(FontFactory.HELVETICA_BOLD, AffectedNodesConstants.TWELVE,
                    primaryColor);
            Font headerFont = FontFactory.getFont(FontFactory.HELVETICA_BOLD, AffectedNodesConstants.SIX,
                    headerTextColor);
            Font cellFont = FontFactory.getFont(FontFactory.HELVETICA, AffectedNodesConstants.FIVE, cellTextColor); // Smaller
                                                                                                                    // font
                                                                                                                    // for
                                                                                                                    // data
                                                                                                                    // rows

            if (!vertices.isEmpty()) {
                Paragraph vertexTitle = new Paragraph("All Vertices - Topology", titleFont);
                vertexTitle.setAlignment(Element.ALIGN_CENTER);
                vertexTitle.setSpacingBefore(AffectedNodesConstants.TENF);
                vertexTitle.setSpacingAfter(AffectedNodesConstants.TENF);
                document.add(vertexTitle);

                PdfPTable table = createStyledPdfTable(vertices, headerFont, cellFont, headerBgColor, rowBgOdd,
                        rowBgEven, borderColor);
                document.add(table);
            }

            if (!edges.isEmpty()) {
                Paragraph edgeTitle = new Paragraph("All Edges - Topology", titleFont);
                edgeTitle.setAlignment(Element.ALIGN_CENTER);
                edgeTitle.setSpacingBefore(AffectedNodesConstants.TWENTYF);
                edgeTitle.setSpacingAfter(AffectedNodesConstants.TENF);
                document.add(edgeTitle);

                PdfPTable table = createStyledPdfTable(edges, headerFont, cellFont, headerBgColor, rowBgOdd, rowBgEven,
                        borderColor);
                document.add(table);
            }

            document.close();
        } catch (Exception e) {
            logger.error("Failed to generate PDF file.");
            throw new DataProcessingException("Failed to generate PDF file. " + e.getMessage(), e);
        }
    }

    private PdfPTable createStyledPdfTable(
            List<Map<String, Object>> data,
            Font headerFont,
            Font cellFont,
            BaseColor headerBg,
            BaseColor rowBgOdd,
            BaseColor rowBgEven,
            BaseColor borderColor) {

        if (data == null || data.isEmpty()) {
            return new PdfPTable(1);
        }
        Set<String> columns = data.get(0).keySet();
        PdfPTable table = new PdfPTable(columns.size());
        table.setWidthPercentage(AffectedNodesConstants.HUNDRED);
        table.setSpacingBefore(AffectedNodesConstants.TENF);
        table.setSpacingAfter(AffectedNodesConstants.TENF);

        // Header
        for (String col : columns) {
            PdfPCell headerCell = new PdfPCell(new Phrase(col, headerFont));
            headerCell.setBackgroundColor(headerBg);
            headerCell.setHorizontalAlignment(Element.ALIGN_CENTER); // Header centered
            headerCell.setVerticalAlignment(Element.ALIGN_MIDDLE);
            headerCell.setBorderColor(borderColor);
            headerCell.setPaddingTop(AffectedNodesConstants.SIXF);
            headerCell.setPaddingBottom(AffectedNodesConstants.SIXF);
            table.addCell(headerCell);
        }

        // Data rows with alternate background, left-aligned
        int rowIndex = 0;
        for (Map<String, Object> row : data) {
            BaseColor rowColor = (rowIndex % AffectedNodesConstants.TWO == 0) ? rowBgEven : rowBgOdd;
            for (String col : columns) {
                Object value = row.get(col);
                PdfPCell cell = new PdfPCell(new Phrase(value != null ? value.toString() : "", cellFont));
                cell.setBackgroundColor(rowColor);
                cell.setHorizontalAlignment(Element.ALIGN_LEFT); // Left-aligned data[1][3][7]
                cell.setVerticalAlignment(Element.ALIGN_MIDDLE);
                cell.setBorderColor(borderColor);
                cell.setPaddingTop(AffectedNodesConstants.ID);
                cell.setPaddingBottom(AffectedNodesConstants.ID);
                table.addCell(cell);
            }
            rowIndex++;
        }
        return table;
    }

    /**
     * Gets the count of topology data based on the operation mode and filters.
     *
     * @param opmode  the operation mode (vertex or edge)
     * @param filters map containing filter properties and their values
     * @return the count of topology data
     * @throws IllegalArgumentException if operation mode is null or empty
     * @throws DataProcessingException  if count retrieval fails
     */
    @Override
    public Long getTopologyDataCount(String opmode, Map<String, Object> filters) {
        logger.info("Getting count for opmode: {} with filters: {}", opmode, filters);

        validateOperationMode(opmode);
        String normalizedOpmode = opmode.toLowerCase().trim();

        try {
            return switch (normalizedOpmode) {
                case AffectedNodesConstants.VERTICES -> getVertexCount(filters);
                case "edge" -> getEdgeCount(filters);
                default -> {
                    logger.warn("Invalid operation mode provided: {}. Supported modes are 'vertex' and 'edge'", opmode);
                    throw new IllegalArgumentException(
                            "Invalid operation mode: '" + opmode + "'. Supported modes are 'vertex' and 'edge'");
                }
            };
        } catch (Exception e) {
            logger.error("Error getting count for opmode");
            throw new DataProcessingException("Failed to get count for opmode: " + opmode, e);
        }
    }

    private void validateOperationMode(String opmode) {
        if (opmode == null || opmode.trim().isEmpty()) {
            throw new IllegalArgumentException(JanusGraphUtilsConstants.OPERATION_MODE_CANNOT_BE_NULL_OR_EMPTY);
        }
    }

    private Long getVertexCount(Map<String, Object> filters) {
        try (JanusGraphTransaction tx = graph.newTransaction()) {
            GraphTraversalSource gtx = tx.traversal();
            GraphTraversal<Vertex, Vertex> vertexTraversal = createVertexTraversal(gtx);
            vertexTraversal = applyFiltersIfNeeded(vertexTraversal, filters);

            long totalCount = vertexTraversal.count().next();
            tx.commit();
            logger.info("Total vertices count: {}", totalCount);
            return totalCount;
        } catch (Exception e) {
            logger.error("Error getting vertex count");
            throw new DataProcessingException("Failed to get vertex count", e);
        }
    }

    private Long getEdgeCount(Map<String, Object> filters) {
        try (JanusGraphTransaction tx = graph.newTransaction()) {
            GraphTraversalSource gtx = tx.traversal();
            GraphTraversal<Edge, Edge> edgeTraversal = gtx.E();
            edgeTraversal = applyFiltersIfNeeded(edgeTraversal, filters);

            long totalCount = edgeTraversal.count().next();
            tx.commit();
            logger.info("Total edges count: {}", totalCount);
            return totalCount;
        } catch (Exception e) {
            logger.error("Error getting edge count");
            throw new DataProcessingException("Failed to get edge count", e);
        }
    }

    private GraphTraversal<Vertex, Vertex> createVertexTraversal(GraphTraversalSource gtx) {
        return gtx.V()
                .has(AffectedNodesConstants.LAT)
                .has(AffectedNodesConstants.LNG)
                .filter(traversal -> {
                    try {
                        Object lat = traversal.get().value(AffectedNodesConstants.LAT);
                        Object lng = traversal.get().value(AffectedNodesConstants.LNG);

                        return areValidCoordinates(lat, lng);
                    } catch (Exception e) {
                        logger.info("Error checking latitude/longitude values: {}", e.getMessage());
                        return false;
                    }
                });
    }

    private boolean areValidCoordinates(Object lat, Object lng) {
        if (lat == null || lng == null) {
            return false;
        }

        String latStr = lat.toString().trim();
        String lngStr = lng.toString().trim();

        if (latStr.isEmpty() || lngStr.isEmpty()) {
            return false;
        }

        return canParseAsDouble(latStr) && canParseAsDouble(lngStr);
    }

    private boolean canParseAsDouble(String value) {
        try {
            Double.parseDouble(value);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private <T> GraphTraversal<T, T> applyFiltersIfNeeded(GraphTraversal<T, T> traversal, Map<String, Object> filters) {
        if (filters != null && !filters.isEmpty()) {
            return applyJsonFilters(traversal, filters);
        }
        return traversal;
    }

    /**
     * Applies JSON filters to the graph traversal.
     *
     * @param traversal the graph traversal to apply filters to
     * @param filters   map containing filter properties and their values
     * @return the filtered traversal
     */
    private <T> GraphTraversal<T, T> applyJsonFilters(GraphTraversal<T, T> traversal, Map<String, Object> filters) {
        if (filters == null || filters.isEmpty()) {
            return traversal;
        }

        for (Map.Entry<String, Object> entry : filters.entrySet()) {
            String property = entry.getKey();
            Object value = entry.getValue();

            if (isValidFilter(property, value)) {
                String stringValue = value.toString().trim();
                if (stringValue.contains(",")) {
                    traversal = applyMultiValueFilter(traversal, property, stringValue);
                } else {
                    traversal = applySingleValueFilter(traversal, property, stringValue);
                }
            }
        }

        return traversal;
    }

    private boolean isValidFilter(String property, Object value) {
        return property != null && !property.trim().isEmpty()
                && value != null && !value.toString().trim().isEmpty();
    }

    private <T> GraphTraversal<T, T> applyMultiValueFilter(GraphTraversal<T, T> traversal, String property,
            String stringValue) {
        String[] values = stringValue.split(",");
        List<String> trimmedValues = new ArrayList<>();
        for (String val : values) {
            String trimmedVal = val.trim();
            if (!trimmedVal.isEmpty()) {
                trimmedValues.add(trimmedVal);
            }
        }

        if (!trimmedValues.isEmpty()) {
            traversal = traversal.has(property, P.within(trimmedValues));
            logger.info("Applied multi-value filter: {} = {}", property, trimmedValues);
        }

        return traversal;
    }

    private <T> GraphTraversal<T, T> applySingleValueFilter(GraphTraversal<T, T> traversal, String property,
            String stringValue) {
        traversal = traversal.has(property, stringValue);
        logger.info("Applied single-value filter: {} = {}", property, stringValue);
        return traversal;
    }

    /**
     * Searches for edges and vertices based on network element names with optional
     * filter.
     * If filter is null or blank, behaves like {@link #getFilterData(List)}.
     * Currently supported filter values:
     * - "parent": return only nodes that exactly match the provided neNames and
     * links among them.
     * - "lldp": return edges with relation=LLDP and related nodes (default behavior
     * with relation filter)
     * - "ospf": return edges with relation=OSPF and related nodes (default behavior
     * with relation filter)
     */
    @Override
    public List<Map<String, Object>> getFilterData(List<String> neNames, String filter) {
        return buildGraphForNeNames(neNames, filter);
    }

    // Unified builder to handle both default and filtered (e.g., parent, lldp,
    // ospf) cases in
    // one place
    private List<Map<String, Object>> buildGraphForNeNames(List<String> neNames, String filter) {
        List<Map<String, Object>> finalResponseList = new ArrayList<>();
        if (neNames == null || neNames.isEmpty()) {
            logger.info("Empty payload provided to getFilterData");
            return finalResponseList;
        }

        String normalized = filter == null ? null : filter.trim().toLowerCase();

        // Step 1: Build links via traversal
        List<Map<String, Object>> links;
        if (JanusGraphUtilsConstants.PARENT.equals(normalized)) {
            // Only edges whose start/end properties are both within provided neNames
            try (JanusGraphTransaction tx = graph.newTransaction()) {
                GraphTraversalSource gtx = tx.traversal();
                links = gtx.V()
                        .has(AffectedNodesConstants.PARENT_NE, P.within(neNames))
                        .bothE()
                        .has(AffectedNodesConstants.START, P.within(neNames))
                        .has(AffectedNodesConstants.END, P.within(neNames))
                        .dedup()
                        .elementMap()
                        .toList()
                        .stream()
                        .map(this::flattenEdgeMap)
                        .toList();
                tx.commit();
            } catch (Exception e) {
                logger.error("Error building filtered links");
                throw new DataProcessingException("Failed to build filtered links: " + e.getMessage(), e);
            }
        } else if ("lldp".equals(normalized) || "ospf".equals(normalized)) {

            links = edgeCriteriaWithRelation(neNames, normalized);
        } else {
            // Default behavior: all edges connected to provided parents (dedup at
            // traversal)
            links = edgeCriteria(neNames);
        }

        // Step 2: Build nodes using simple helper(s)
        List<Map<String, Object>> nodes;
        if (JanusGraphUtilsConstants.PARENT.equals(normalized)) {
            nodes = fetchVerticesByNames(neNames);
        } else {

            nodes = vertexCriteria(neNames, links);
        }

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("links", links);
        response.put("nodes", nodes);
        finalResponseList.add(response);
        return finalResponseList;
    }

    // Fetch edges connected to provided parentNEs with relation filtering
    private List<Map<String, Object>> edgeCriteriaWithRelation(List<String> neNames, String relationFilter) {
        List<Map<String, Object>> edges = new ArrayList<>();
        if (neNames == null || neNames.isEmpty()) {
            return edges;
        }
        try (JanusGraphTransaction tx = graph.newTransaction()) {
            GraphTraversalSource gtx = tx.traversal();
            String relationValue = relationFilter.toUpperCase();
            edges = gtx.V()
                    .has(AffectedNodesConstants.PARENT_NE, P.within(neNames))
                    .inE()
                    .has(AffectedNodesConstants.RELATION, relationValue)
                    .dedup()
                    .elementMap()
                    .toList()
                    .stream()
                    .map(this::flattenEdgeMap)
                    .toList();
            tx.commit();

            // Remove bidirectional duplicates (A→B and B→A, keep only one)
            List<Map<String, Object>> dedupedEdges = removeBidirectionalDuplicates(edges);

            logger.info("Found {} edges with relation filter: {}", dedupedEdges.size(), relationValue);
            return dedupedEdges;
        } catch (Exception e) {
            logger.error("Error in edgeCriteriaWithRelation");
            throw new DataProcessingException("Failed to search edges with relation filter: " + e.getMessage(), e);
        }
    }

    /**
     * Removes bidirectional duplicate edges based on start and end parent NE names.
     * For example, if there's an edge A→B and B→A, only one will be kept.
     *
     * @param edges the list of edges to deduplicate
     * @return deduplicated list of edges
     */
    private List<Map<String, Object>> removeBidirectionalDuplicates(List<Map<String, Object>> edges) {
        Set<String> seenPairs = new HashSet<>();

        return edges.stream()
                .filter(edge -> {
                    String start = (String) edge.get(AffectedNodesConstants.START);
                    String end = (String) edge.get(AffectedNodesConstants.END);

                    if (start == null || end == null) {
                        return true;
                    }

                    String reverseKey = end + "|" + start;

                    // Skip if reverse pair already exists
                    if (seenPairs.contains(reverseKey)) {
                        return false;
                    }

                    seenPairs.add(start + "|" + end);
                    return true;
                })
                .toList();
    }

    // Fetch edges connected to provided parentNEs
    private List<Map<String, Object>> edgeCriteria(List<String> neNames) {
        List<Map<String, Object>> edges = new ArrayList<>();
        if (neNames == null || neNames.isEmpty()) {
            return edges;
        }
        try (JanusGraphTransaction tx = graph.newTransaction()) {
            GraphTraversalSource gtx = tx.traversal();
            edges = gtx.V()
                    .has(AffectedNodesConstants.PARENT_NE, P.within(neNames))
                    .bothE()
                    .dedup()
                    .elementMap()
                    .toList()
                    .stream()
                    .map(this::flattenEdgeMap)
                    .toList();
            tx.commit();
            return edges;
        } catch (Exception e) {
            logger.error("Error in edgeCriteria");
            throw new DataProcessingException("Failed to search edges: " + e.getMessage(), e);
        }
    }

    // Fetch vertices for provided names plus names discovered in links
    private List<Map<String, Object>> vertexCriteria(List<String> neNames, List<Map<String, Object>> links) {
        Set<String> nodeNames = new HashSet<>();
        if (neNames != null)
            nodeNames.addAll(neNames);
        for (Map<String, Object> link : links) {
            String linkStart = (String) link.get(AffectedNodesConstants.START);
            String linkEnd = (String) link.get(AffectedNodesConstants.END);
            if (notBlank(linkStart))
                nodeNames.add(linkStart);
            if (notBlank(linkEnd))
                nodeNames.add(linkEnd);
        }
        if (nodeNames.isEmpty())
            return new ArrayList<>();

        try (JanusGraphTransaction tx = graph.newTransaction()) {
            GraphTraversalSource gtx = tx.traversal();
            List<Map<String, Object>> vertices = gtx.V()
                    .has(AffectedNodesConstants.NENAME, P.within(nodeNames))
                    // Exclude vertices that don't have latitude and longitude or have null/empty values
                    .not(__.or(
                            __.not(__.has(AffectedNodesConstants.LAT)),
                            __.not(__.has(AffectedNodesConstants.LNG)),
                            __.has(AffectedNodesConstants.LAT, within(null, "")),
                            __.has(AffectedNodesConstants.LNG, within(null, ""))))
                    .elementMap()
                    .toList()
                    .stream()
                    .map(this::flattenVertexMap)
                    .toList();
            tx.commit();
            return vertices;
        } catch (Exception e) {
            logger.error("Error in vertexCriteria");
            throw new DataProcessingException("Failed to search vertices: " + e.getMessage(), e);
        }
    }

    // Fetch exactly the provided vertices (used for parent filter)
    private List<Map<String, Object>> fetchVerticesByNames(List<String> neNames) {
        if (neNames == null || neNames.isEmpty())
            return new ArrayList<>();
        try (JanusGraphTransaction tx = graph.newTransaction()) {
            GraphTraversalSource gtx = tx.traversal();
            List<Map<String, Object>> vertices = gtx.V()
                    .has(AffectedNodesConstants.NENAME, P.within(new HashSet<>(neNames)))
                    // Exclude vertices that don't have latitude and longitude or have null/empty values
                    .not(__.or(
                            __.not(__.has(AffectedNodesConstants.LAT)),
                            __.not(__.has(AffectedNodesConstants.LNG)),
                            __.has(AffectedNodesConstants.LAT, within(null, "")),
                            __.has(AffectedNodesConstants.LNG, within(null, ""))))
                    .elementMap()
                    .toList()
                    .stream()
                    .map(this::flattenVertexMap)
                    .toList();
            tx.commit();
            return vertices;
        } catch (Exception e) {
            logger.error("Error in fetchVerticesByNames");
            throw new DataProcessingException("Failed to search vertices: " + e.getMessage(), e);
        }
    }

    private boolean notBlank(String str) {
        return str != null && !str.trim().isEmpty();
    }

    /**
     * Retrieves the maximum modification time for vertices.
     * Returns max values for neModificationTime, lldpModificationTime,
     * ospfModificationTime, and interfaceModificationTime
     * formatted as yyyy-MM-dd HH:mm:ss (UTC).
     * If none present, returns empty string for that key.
     * 
     * @return a map containing the maximum modification time for all modification
     *         time keys
     */
    @Override
    public Map<String, String> getMaxModificationTime() {
        try {
            Map<String, String> result = new HashMap<>();

            // Get max neModificationTime
            Object neMax = g.V().values(JanusGraphUtilsConstants.NE_MODIFY_TIME).max().tryNext().orElse(null);
            Long neTime = coerceToLong(neMax);
            result.put(JanusGraphUtilsConstants.NE_MODIFY_TIME, formatTimestamp(neTime));

            // Get max lldpModificationTime
            Object lldpMax = g.V().values(JanusGraphUtilsConstants.LLDP_MODIFY_TIME).max().tryNext().orElse(null);
            Long lldpTime = coerceToLong(lldpMax);
            result.put(JanusGraphUtilsConstants.LLDP_MODIFY_TIME, formatTimestamp(lldpTime));

            // Get max ospfModificationTime
            Object ospfMax = g.V().values(JanusGraphUtilsConstants.OSPF_MODIFY_TIME).max().tryNext().orElse(null);
            Long ospfTime = coerceToLong(ospfMax);
            result.put(JanusGraphUtilsConstants.OSPF_MODIFY_TIME, formatTimestamp(ospfTime));

            // Get max interfaceModificationTime
            Object lldpinterfaceMax = g.V().values(JanusGraphUtilsConstants.LLDP_INTERFACE_MODIFY_TIME).max().tryNext()
                    .orElse(null);
            Long lldpinterfaceTime = coerceToLong(lldpinterfaceMax);
            result.put(JanusGraphUtilsConstants.LLDP_INTERFACE_MODIFY_TIME, formatTimestamp(lldpinterfaceTime));

            // Get max interfaceModificationTime
            Object ospfinterfaceMax = g.V().values(JanusGraphUtilsConstants.OSPF_INTERFACE_MODIFY_TIME).max().tryNext()
                    .orElse(null);
            Long ospfinterfaceTime = coerceToLong(ospfinterfaceMax);
            result.put(JanusGraphUtilsConstants.OSPF_INTERFACE_MODIFY_TIME, formatTimestamp(ospfinterfaceTime));

            return result;
        } catch (Exception ex) {
            logger.error("Failed to compute max modification times");
            throw new DataProcessingException("Failed to compute max modification times: " + ex.getMessage(), ex);
        }
    }

    /**
     * Formats a timestamp value into the required format.
     * 
     * @param timestamp the timestamp value to format
     * @return formatted timestamp string or empty string if null
     */
    private String formatTimestamp(Long timestamp) {
        if (timestamp == null) {
            return "";
        }

        // Format epoch millis into the required timestamp format in UTC
        Instant instant = Instant.ofEpochMilli(timestamp);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                .withZone(ZoneOffset.UTC);

        return formatter.format(instant);
    }

    private Long coerceToLong(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number number) {
            return number.longValue();
        }
        try {
            return Long.parseLong(value.toString());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * Fetches alarm-based nodes by calling the FM service for active alarms,
     * extracting their {@code entityName} as {@code neName}, and retrieving
     * corresponding node details from JanusGraph via {@code getFilterData}.
     * Returns an empty list if no alarms or valid nodes are found.
     *
     * @param wrapper optional filter for alarms (nullable)
     * @return list of alarm-based node details from JanusGraph
     */
    @Override
    public List<Map<String, Object>> getAlarmBasedNodes(AlarmMonitoringRequestFilterWrapper wrapper) {
        try {
            // Step 1: Hit FM service via Feign Client
            List<String> neNames = alarmRest.getDistinctNes(wrapper);

            if (neNames == null || neNames.isEmpty()) {
                logger.info("No alarms found from FM service");
                return Collections.emptyList();
            }

            logger.info("Fetching graph data for neNames from FM: {}", neNames);

            // Step 3: Use unified method with no filter
            return getFilterData(neNames, JanusGraphUtilsConstants.PARENT);

        } catch (Exception e) {
            logger.error("Error in getAlarmBasedNodes");
            throw new DataProcessingException("Failed to get Alarm Based Nodes: " + e.getMessage(), e);
        }
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
     * @param opmode  the operation mode specifying what data to retrieve ("vertex",
     *                "edge", or "both")
     * @param filters map containing filter properties and their values
     * @param lLimit  lower limit for pagination (optional, null for no limit)
     * @param uLimit  upper limit for pagination (optional, null for no limit)
     * @return list of maps containing the topology detail data
     */
    @Override
    public List<Map<String, Object>> getTopologyDetail(String opmode, Map<String, Object> filters, Integer lLimit,
            Integer uLimit) {
        logger.info("Getting topology detail with opmode: {}, filters: {}, lLimit: {}, uLimit: {}", opmode, filters,
                lLimit, uLimit);

        if (opmode == null || opmode.trim().isEmpty()) {
            throw new InvalidParametersException(JanusGraphUtilsConstants.OPERATION_MODE_CANNOT_BE_NULL_OR_EMPTY);
        }

        String normalizedOpmode = opmode.trim().toLowerCase();
        if (!JanusGraphUtilsConstants.MODE_VERTEX.equals(normalizedOpmode) && !"edge".equals(normalizedOpmode)) {
            throw new InvalidParametersException("Operation mode must be 'vertex' or 'edge'");
        }

        // Validate pagination parameters
        if (lLimit != null && lLimit < 0) {
            throw new InvalidParametersException("Lower limit (lLimit) cannot be negative");
        }
        if (uLimit != null && uLimit < 0) {
            throw new InvalidParametersException("Upper limit (uLimit) cannot be negative");
        }
        if (lLimit != null && uLimit != null && lLimit > uLimit) {
            throw new InvalidParametersException("Lower limit (lLimit) cannot be greater than upper limit (uLimit)");
        }

        List<Map<String, Object>> result = new ArrayList<>();

        try (JanusGraphTransaction tx = graph.newTransaction()) {
            GraphTraversalSource gtx = tx.traversal();

            // Get vertices if requested
            if (JanusGraphUtilsConstants.MODE_VERTEX.equals(normalizedOpmode)) {
                List<Map<String, Object>> vertices = getFilteredVertices(gtx, filters, lLimit, uLimit);
                result.addAll(vertices);
                logger.info("Retrieved {} vertices", vertices.size());
            }

            // Get edges if requested
            if ("edge".equals(normalizedOpmode)) {
                List<Map<String, Object>> edges = getFilteredEdges(gtx, filters, lLimit, uLimit);
                result.addAll(edges);
                logger.info("Retrieved {} edges", edges.size());
            }

            tx.commit();
            logger.info("Successfully retrieved topology detail with {} total items", result.size());

        } catch (Exception e) {
            logger.error("Error in getTopologyDetail");
            throw new DataProcessingException("Failed to get topology detail: " + e.getMessage(), e);
        }

        return result;
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
     * @param opmode  the operation mode specifying what data to count ("vertex",
     *                "edge", or "both")
     * @param filters map containing filter properties and their values
     * @return the count as a Long value
     */
    @Override
    public Long getTopologyDetailCount(String opmode, Map<String, Object> filters) {
        logger.info("Getting topology detail count with opmode: {} and filters: {}", opmode, filters);

        if (opmode == null || opmode.trim().isEmpty()) {
            throw new InvalidParametersException(JanusGraphUtilsConstants.OPERATION_MODE_CANNOT_BE_NULL_OR_EMPTY);
        }

        String normalizedOpmode = opmode.trim().toLowerCase();
        if (!JanusGraphUtilsConstants.MODE_VERTEX.equals(normalizedOpmode) && !"edge".equals(normalizedOpmode)) {
            throw new InvalidParametersException("Operation mode must be 'vertex' or 'edge'");
        }

        long count = 0L;

        try (JanusGraphTransaction tx = graph.newTransaction()) {
            GraphTraversalSource gtx = tx.traversal();

            // Count vertices if requested
            if (JanusGraphUtilsConstants.MODE_VERTEX.equals(normalizedOpmode)) {
                count = getFilteredVertexCount(gtx, filters);
                logger.info("Retrieved {} vertices", count);
            }

            // Count edges if requested
            else if ("edge".equals(normalizedOpmode)) {
                count = getFilteredEdgeCount(gtx, filters);
                logger.info("Retrieved {} edges", count);
            }

            tx.commit();
            logger.info("Successfully retrieved topology detail count: {}", count);

        } catch (Exception e) {
            logger.error("Error in getTopologyDetailCount");
            throw new DataProcessingException("Failed to get topology detail count: " + e.getMessage(), e);
        }

        return count;
    }

    /**
     * Retrieves filtered vertices based on the provided filters.
     * Supports comma-separated values (OR logic) and semicolon-separated values
     * (AND logic).
     * 
     * @param gtx     the graph traversal source
     * @param filters map containing filter properties and their values
     * @param lLimit  lower limit for pagination (optional, null for no limit)
     * @param uLimit  upper limit for pagination (optional, null for no limit)
     * @return list of filtered vertex data
     */
    private List<Map<String, Object>> getFilteredVertices(GraphTraversalSource gtx, Map<String, Object> filters,
            Integer lLimit, Integer uLimit) {
        GraphTraversal<Vertex, Vertex> traversal = gtx.V();

        // Add latitude and longitude validation using direct Gremlin query
        // Exclude vertices that don't have lat/lng or have null/empty values
        traversal = traversal
                .not(__.or(
                        __.not(__.has(JanusGraphUtilsConstants.LATITUDE)),
                        __.not(__.has(JanusGraphUtilsConstants.LONGITUDE)),
                        __.has(JanusGraphUtilsConstants.LATITUDE, within(null, "")),
                        __.has(JanusGraphUtilsConstants.LONGITUDE, within(null, ""))));

        if (filters != null && !filters.isEmpty()) {
            traversal = applyEnhancedFilters(traversal, filters);
        }

        // Apply pagination if limits are provided
        if (lLimit != null) {
            traversal = traversal.range(lLimit, uLimit);
        } else if (uLimit != null) {
            traversal = traversal.limit(uLimit);
        }

        return traversal.elementMap()
                .toList()
                .stream()
                .map(this::flattenVertexMap)
                .toList();
    }

    /**
     * Retrieves filtered edges based on the provided filters.
     * Supports comma-separated values (OR logic) and semicolon-separated values
     * (AND logic).
     * 
     * @param gtx     the graph traversal source
     * @param filters map containing filter properties and their values
     * @param lLimit  lower limit for pagination (optional, null for no limit)
     * @param uLimit  upper limit for pagination (optional, null for no limit)
     * @return list of filtered edge data
     */
    private List<Map<String, Object>> getFilteredEdges(GraphTraversalSource gtx, Map<String, Object> filters,
            Integer lLimit, Integer uLimit) {
        GraphTraversal<Edge, Edge> traversal = gtx.E();

        if (filters != null && !filters.isEmpty()) {
            traversal = applyEnhancedFilters(traversal, filters);
        }

        // Apply pagination if limits are provided
        if (lLimit != null) {
            traversal = traversal.range(lLimit, uLimit != null ? uLimit : Integer.MAX_VALUE);
        } else if (uLimit != null) {
            traversal = traversal.limit(uLimit);
        }

        return traversal.elementMap()
                .toList()
                .stream()
                .map(this::flattenEdgeMap)
                .toList();
    }

    /**
     * Counts filtered vertices based on the provided filters.
     * 
     * @param gtx     the graph traversal source
     * @param filters map containing filter properties and their values
     * @return count of filtered vertices
     */
    private long getFilteredVertexCount(GraphTraversalSource gtx, Map<String, Object> filters) {
        GraphTraversal<Vertex, Vertex> traversal = gtx.V();

        // Add latitude and longitude validation using direct Gremlin query
        // Exclude vertices that don't have lat/lng or have null/empty values
        traversal = traversal
                .not(__.or(
                        __.not(__.has(JanusGraphUtilsConstants.LATITUDE)),
                        __.not(__.has(JanusGraphUtilsConstants.LONGITUDE)),
                        __.has(JanusGraphUtilsConstants.LATITUDE, within(null, "")),
                        __.has(JanusGraphUtilsConstants.LONGITUDE, within(null, ""))));

        if (filters != null && !filters.isEmpty()) {
            traversal = applyEnhancedFilters(traversal, filters);
        }

        return traversal.count().next();
    }

    /**
     * Counts filtered edges based on the provided filters.
     * 
     * @param gtx     the graph traversal source
     * @param filters map containing filter properties and their values
     * @return count of filtered edges
     */
    private long getFilteredEdgeCount(GraphTraversalSource gtx, Map<String, Object> filters) {
        GraphTraversal<Edge, Edge> traversal = gtx.E();

        if (filters != null && !filters.isEmpty()) {
            traversal = applyEnhancedFilters(traversal, filters);
        }

        return traversal.count().next();
    }

    /**
     * Applies enhanced filtering logic supporting both comma (OR) and semicolon
     * (AND) separators.
     * 
     * @param traversal the graph traversal to apply filters to
     * @param filters   map containing filter properties and their values
     * @return the filtered traversal
     */
    private <T> GraphTraversal<T, T> applyEnhancedFilters(GraphTraversal<T, T> traversal, Map<String, Object> filters) {
        if (filters == null || filters.isEmpty()) {
            return traversal;
        }

        for (Map.Entry<String, Object> entry : filters.entrySet()) {
            String property = entry.getKey();
            Object value = entry.getValue();

            if (isValidFilter(property, value)) {
                String stringValue = value.toString().trim();

                // Check for comma (OR logic)
                if (stringValue.contains(",")) {
                    traversal = applyOrFilters(traversal, property, stringValue);
                }
                // Single value
                else {
                    traversal = traversal.has(property, stringValue);
                    logger.info("Applied single-value filter: {} = {}", property, stringValue);
                }
            }
        }

        return traversal;
    }

    /**
     * Applies OR logic filters using comma separator.
     * 
     * @param traversal   the graph traversal
     * @param property    the property name
     * @param stringValue the comma-separated values
     * @return the filtered traversal
     */
    private <T> GraphTraversal<T, T> applyOrFilters(GraphTraversal<T, T> traversal, String property,
            String stringValue) {
        String[] values = stringValue.split(",");
        List<String> trimmedValues = new ArrayList<>();

        for (String val : values) {
            String trimmedVal = val.trim();
            if (!trimmedVal.isEmpty()) {
                trimmedValues.add(trimmedVal);
            }
        }

        if (!trimmedValues.isEmpty()) {
            traversal = traversal.has(property, P.within(trimmedValues));
            logger.info("Applied OR filters: {} = {}", property, trimmedValues);
        }

        return traversal;
    }

    /**
     * Wrapper class for email configuration
     */
    private static class EmailConfigWrapper {
        private String emailSubject;
        private String emailMessage;
        private List<String> emailList;
        private boolean enableAttachment;
        private Map<String, String> attachments;
        private int totalRecords;
        private String processingTime;
        private int newRecordsCount;
        private int updatedRecordsCount;
        private int failedRecordsCount;

        public String getEmailSubject() {
            return emailSubject;
        }

        public void setEmailSubject(String emailSubject) {
            this.emailSubject = emailSubject;
        }

        public String getEmailMessage() {
            return emailMessage;
        }

        public void setEmailMessage(String emailMessage) {
            this.emailMessage = emailMessage;
        }

        public List<String> getEmailList() {
            return emailList;
        }

        public void setEmailList(List<String> emailList) {
            this.emailList = emailList;
        }

        public boolean getEnableAttachment() {
            return enableAttachment;
        }

        public void setEnableAttachment(boolean enableAttachment) {
            this.enableAttachment = enableAttachment;
        }

        public Map<String, String> getAttachments() {
            return attachments;
        }

        public void setAttachments(Map<String, String> attachments) {
            this.attachments = attachments;
        }

        public int getTotalRecords() {
            return totalRecords;
        }

        public void setTotalRecords(int totalRecords) {
            this.totalRecords = totalRecords;
        }

        public String getProcessingTime() {
            return processingTime;
        }

        public void setProcessingTime(String processingTime) {
            this.processingTime = processingTime;
        }

        public int getNewRecordsCount() {
            return newRecordsCount;
        }

        public void setNewRecordsCount(int newRecordsCount) {
            this.newRecordsCount = newRecordsCount;
        }

        public int getUpdatedRecordsCount() {
            return updatedRecordsCount;
        }

        public void setUpdatedRecordsCount(int updatedRecordsCount) {
            this.updatedRecordsCount = updatedRecordsCount;
        }

        public int getFailedRecordsCount() {
            return failedRecordsCount;
        }

        public void setFailedRecordsCount(int failedRecordsCount) {
            this.failedRecordsCount = failedRecordsCount;
        }
    }

    /**
     * Accumulates batch processing summary and schedules email notification after a
     * delay.
     * This ensures that multiple API calls are combined into a single email.
     * 
     * @param summary the batch processing summary to accumulate
     */
    private void accumulateAndScheduleNotification(BatchProcessingSummary summary) {
        accumulationLock.lock();
        try {
            // Merge the new summary into the accumulated summary
            mergeBatchSummaries(accumulatedBatchSummary, summary);

            // Cancel any existing scheduled email task
            if (emailTask != null && !emailTask.isDone()) {
                emailTask.cancel(false);
                logger.info("Cancelled previous email task to reschedule");
            }

            // Schedule email to be sent after delay
            String delaySecondsStr = ConfigUtils.getString("TOPOLOGY_EMAIL_DELAY_SECONDS");
            int delaySeconds = EMAIL_DELAY_SECONDS;
            if (delaySecondsStr != null && !delaySecondsStr.trim().isEmpty()) {
                try {
                    delaySeconds = Integer.parseInt(delaySecondsStr.trim());
                } catch (NumberFormatException e) {
                    logger.warn("Invalid TOPOLOGY_EMAIL_DELAY_SECONDS value: {}, using default: {}", delaySecondsStr,
                            EMAIL_DELAY_SECONDS);
                }
            }
            emailTask = emailScheduler.schedule(this::sendAccumulatedEmailIfExists, delaySeconds, TimeUnit.SECONDS);

            logger.info("Accumulated batch results. Email will be sent in {} seconds if no new requests arrive",
                    delaySeconds);
        } finally {
            accumulationLock.unlock();
        }
    }

    /**
     * Merges a new batch summary into the accumulated summary.
     * Combines all new records, updated records, failed records, and totals.
     * 
     * @param accumulated the accumulated summary to merge into
     * @param newSummary  the new summary to merge
     */
    private void mergeBatchSummaries(BatchProcessingSummary accumulated, BatchProcessingSummary newSummary) {
        if (accumulated == null) {
            accumulated = new BatchProcessingSummary();
            accumulatedBatchSummary = accumulated;
        }

        // Merge records lists
        accumulated.getNewRecords().addAll(newSummary.getNewRecords());
        accumulated.getUpdatedRecords().addAll(newSummary.getUpdatedRecords());
        accumulated.getFailedRecords().addAll(newSummary.getFailedRecords());

        // Update totals
        accumulated.setTotalRecords(accumulated.getTotalRecords() + newSummary.getTotalRecords());

        // Combine processing time (sum of all processing times)
        long currentProcessingTime = parseProcessingTime(accumulated.getProcessingTime());
        long newProcessingTime = parseProcessingTime(newSummary.getProcessingTime());
        accumulated.setProcessingTime((currentProcessingTime + newProcessingTime) + " ms");
    }

    /**
     * Parses processing time string to long milliseconds.
     * 
     * @param processingTime the processing time string (e.g., "1000 ms")
     * @return processing time in milliseconds
     */
    private long parseProcessingTime(String processingTime) {
        if (processingTime == null || processingTime.trim().isEmpty()) {
            return 0L;
        }
        try {
            String numericPart = processingTime.replaceAll("\\D", "");
            return Long.parseLong(numericPart);
        } catch (NumberFormatException e) {
            logger.warn("Failed to parse processing time: {}", processingTime);
            return 0L;
        }
    }

    /**
     * Sends the accumulated email if there is accumulated data.
     * This method is thread-safe and ensures only one email is sent.
     */
    private void sendAccumulatedEmailIfExists() {
        accumulationLock.lock();
        try {
            if (accumulatedBatchSummary == null ||
                    (accumulatedBatchSummary.getNewRecords().isEmpty() &&
                            accumulatedBatchSummary.getUpdatedRecords().isEmpty() &&
                            accumulatedBatchSummary.getFailedRecords().isEmpty())) {
                logger.info("No accumulated data to send email");
                return;
            }

            logger.info("Sending accumulated email with {} new, {} updated, {} failed records",
                    accumulatedBatchSummary.getNewRecords().size(),
                    accumulatedBatchSummary.getUpdatedRecords().size(),
                    accumulatedBatchSummary.getFailedRecords().size());

            // Create a copy for email sending
            BatchProcessingSummary summaryToSend = copyBatchSummary(accumulatedBatchSummary);

            // Reset accumulated summary BEFORE sending email to ensure clean state
            accumulatedBatchSummary = new BatchProcessingSummary();

            // Store summary for CSV download (separate from active accumulation)
            lastSentSummary = summaryToSend;

            // Reset the cycle flag so subsequent hits use normal logic
            forceAllNewCycle = false;

            // Send the email
            sendNotificationEmail(summaryToSend);

        } finally {
            accumulationLock.unlock();
        }
    }

    /**
     * Creates a deep copy of BatchProcessingSummary.
     * 
     * @param original the original summary to copy
     * @return a copy of the summary
     */
    private BatchProcessingSummary copyBatchSummary(BatchProcessingSummary original) {
        BatchProcessingSummary copy = new BatchProcessingSummary();
        copy.setTotalRecords(original.getTotalRecords());
        copy.setProcessingTime(original.getProcessingTime());

        // Deep copy lists
        copy.getNewRecords().addAll(original.getNewRecords());
        copy.getUpdatedRecords().addAll(original.getUpdatedRecords());
        copy.getFailedRecords().addAll(original.getFailedRecords());

        return copy;
    }

    /**
     * Sends a notification email with the given batch processing summary.
     * This method sends CSV files for each category via email notification using
     * platform
     * team's approach.
     * 
     * @param summary the batch processing summary to send
     */
    private void sendNotificationEmail(BatchProcessingSummary summary) {
        try {
            logger.info("Sending notification for accumulated batch processing results using CSV download API");

            // Note: lastSentSummary is already set before calling this method for CSV
            // download

            // Create EmailConfigWrapper following platform team's pattern
            EmailConfigWrapper emailConfigWrapper = new EmailConfigWrapper();
            emailConfigWrapper.setEmailSubject("Topology Batch Processing Results");

            // Create detailed email message using summary information
            StringBuilder messageBuilder = new StringBuilder();
            messageBuilder.append("Batch processing completed successfully!\n\n");
            messageBuilder.append("Processing Summary:\n");
            messageBuilder.append("- Total Records Processed: ").append(summary.getTotalRecords()).append("\n");
            messageBuilder.append("- New Records: ").append(summary.getNewRecords().size()).append("\n");
            messageBuilder.append("- Updated Records: ").append(summary.getUpdatedRecords().size()).append("\n");
            messageBuilder.append("- Failed Records: ").append(summary.getFailedRecords().size()).append("\n");
            messageBuilder.append("- Processing Time: ").append(summary.getProcessingTime()).append("\n\n");
            messageBuilder.append("Please find the attached CSV files for detailed information.\n\n");
            messageBuilder.append("Best regards,\nTopology Management System");

            emailConfigWrapper.setEmailMessage(messageBuilder.toString());

            // Set summary information in EmailConfigWrapper
            emailConfigWrapper.setTotalRecords(summary.getTotalRecords());
            emailConfigWrapper.setProcessingTime(summary.getProcessingTime());
            emailConfigWrapper.setNewRecordsCount(summary.getNewRecords().size());
            emailConfigWrapper.setUpdatedRecordsCount(summary.getUpdatedRecords().size());
            emailConfigWrapper.setFailedRecordsCount(summary.getFailedRecords().size());

            // Set email recipients from configuration
            List<String> emailList = new ArrayList<>();
            String notificationEmail = ConfigUtils.getString("TOPOLOGY_NOTIFICATION_EMAIL",
                    "ayush.dwivedi@visionwaves.com");
            emailList.add(notificationEmail);
            emailConfigWrapper.setEmailList(emailList);

            // Enable attachments and set attachment files using CSV download API
            emailConfigWrapper.setEnableAttachment(true);
            Map<String, String> attachments = new HashMap<>();

            // Generate timestamp for unique filenames
            String timestamp = getCurrentTimestamp();

            // Include all relevant CSV files in attachments
            // Priority: failed > updated > new
            if (!summary.getNewRecords().isEmpty()) {
                attachments.put("NAME", AffectedNodesConstants.NEW_RECORDS + timestamp + ".csv");
            }
            if (!summary.getUpdatedRecords().isEmpty() && summary.getFailedRecords().isEmpty()) {
                attachments.put("NAME", AffectedNodesConstants.UPDATED_RECORDS + timestamp + ".csv");
            }
            if (!summary.getFailedRecords().isEmpty()) {
                attachments.put("NAME", AffectedNodesConstants.FAILED_RECORDS + timestamp + ".csv");
            }
            emailConfigWrapper.setAttachments(attachments);

            // Send email using emailConfigWrapper
            sendEmailWithTemplateByEmailConfig(emailConfigWrapper, null);
            logger.info("Email notification sent successfully for batch processing results with CSV download API");

        } catch (Exception e) {
            logger.error("Failed to send notification for batch processing results: {}", e.getMessage(), e);
        }
    }

    /**
     * Gets current timestamp for CSV filenames.
     */
    private String getCurrentTimestamp() {
        return Instant.now().atOffset(ZoneOffset.UTC)
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH-mm-ss-SSS'Z'"));
    }

    /**
     * Sends email with template using EmailConfigWrapper following platform team's
     * approach.
     * This method is based on the platform team's reference implementation.
     * 
     * @param emailConfigWrapper the email configuration wrapper
     * @param customerId         the customer ID (can be null)
     * @return result string indicating success or failure
     */
    private String sendEmailWithTemplateByEmailConfig(EmailConfigWrapper emailConfigWrapper, Integer customerId) {
        logger.info("Inside method sendEmailWithTemplateByEmailConfig() emailConfigWrapper : {} and customerId : {}",
                emailConfigWrapper, customerId);
        String result = null;
        try {
            // Get template name from configuration
            String templateName = ConfigUtils.getString("TOPOLOGY_SERVICE");
            if (templateName != null && templateName.startsWith("base64:")) {
                templateName = new String(
                        Base64.getUrlDecoder().decode(templateName.substring(AffectedNodesConstants.SEVEN)));
            }
            logger.info("sendMail notificationTemplateRest templateName===> {}", templateName);

            String subject = emailConfigWrapper.getEmailSubject();
            String eMessage = emailConfigWrapper.getEmailMessage();

            logger.info("eMessage is {}", eMessage);

            List<String> emailList = emailConfigWrapper.getEmailList();
            HashSet<String> hashSet = new HashSet<>(emailList);

            logger.info("emailList emailListString=====>: {}", hashSet);

            NotificationMailRest emailRest = notificationMailRest;
            NotificationMailWrapper emailWrapper = new NotificationMailWrapper();

            // Set from email address
            String fromMail = ConfigUtils.getString("EMAIL_DEFAULT_ADDRESS_FROM", "noreply@visionwaves.com");
            emailWrapper.setFromEmail(fromMail);

            emailWrapper.setSubject(subject);
            emailWrapper.setToEmailIds(hashSet);
            emailWrapper.setTemplateName(templateName);

            logger.info("emailConfigWrapper is {}", emailConfigWrapper.getAttachments());

            // Handle attachments if enabled
            if (emailConfigWrapper.getEnableAttachment() && !emailConfigWrapper.getAttachments().isEmpty()) {
                logger.info("Attachments enabled with files: {}", emailConfigWrapper.getAttachments());

                String attachmentName = emailConfigWrapper.getAttachments().getOrDefault("NAME",
                        "topology_insertion_report.csv");

                String attachmentPath = "http://topology-service/topology/graphdb/downloadCsvFile?fileName="
                        + attachmentName;

                NotificationAttachment notificationAttachment = new NotificationAttachment();
                notificationAttachment.setName(attachmentName);
                notificationAttachment.setPath(attachmentPath);
                notificationAttachment.setIsPublic(true);
                emailWrapper.setAttachment(Arrays.asList(notificationAttachment));
                logger.info("Attachment Name: {}, Path: {}", attachmentName, attachmentPath);
            }

            logger.info("NotificationMailWrapper email wrapper ===>:{}", emailWrapper.getToEmailIds());

            logger.info("emailWrapper is for sending email ======>: {}", emailWrapper);
            logger.info("Attachment is for sending email ======>: {}", emailWrapper.getAttachment());

            emailWrapper.setIsVariableResolve(false);
            Map<String, String> resolveVariable = new HashMap<>();
            resolveVariable.put("ANALYTICS_NOTIFICATION_MESSAGE", eMessage);
            resolveVariable.put("ANALYTICS_NOTIFICATION_SUBJECT", subject);

            // Add additional variables for email template
            resolveVariable.put("TOTAL_RECORDS", String.valueOf(emailConfigWrapper.getTotalRecords()));
            resolveVariable.put("PROCESSING_TIME", emailConfigWrapper.getProcessingTime());
            resolveVariable.put("NEW_RECORDS_COUNT", String.valueOf(emailConfigWrapper.getNewRecordsCount()));
            resolveVariable.put("UPDATED_RECORDS_COUNT", String.valueOf(emailConfigWrapper.getUpdatedRecordsCount()));
            resolveVariable.put("FAILED_RECORDS_COUNT", String.valueOf(emailConfigWrapper.getFailedRecordsCount()));
            resolveVariable.put("TIMESTAMP", Instant.now().toString());

            emailWrapper.setResolveVariable(resolveVariable);

            String userId = ConfigUtils.getPlainString("USER_ID");
            String userName = ConfigUtils.getPlainString("USER_NAME");
            String customerName = ConfigUtils.getPlainString("CUSTOMER_NAME");
            String systemCustomerId = ConfigUtils.getPlainString("CUSTOMER_ID");
            String customerUUID = ConfigUtils.getPlainString("CUSTOMER_UUID");

            String token = CustomTokenUtils.generateCustomToken(userId, userName, customerName,
                    Integer.parseInt(systemCustomerId), customerUUID);
            logger.info("token is {}", token);
            result = emailRest.sendEmail(emailWrapper, true);
            logger.info("mail send successfully : result======>: {}", result);

        } catch (Exception e) {
            logger.error("Error Occurred in sendEmailWithTemplate ====>: {}", e.getMessage(), e);
        }
        return result;
    }

    /**
     * Analyzes batch processing results to categorize them into new records,
     * updated records, and failed records.
     * 
     * @param batchResults    the results from createMultipleVertices
     * @param originalPayload the original payload
     * @return BatchProcessingSummary containing categorized results
     */
    private BatchProcessingSummary analyzeBatchResults(List<Map<String, Object>> batchResults,
            List<Map<String, Object>> originalPayload, boolean forceAllNew) {
        BatchProcessingSummary summary = new BatchProcessingSummary();
        summary.setTotalRecords(originalPayload.size());

        // If forced for this cycle, classify all as new records
        if (forceAllNew) {
            for (Map<String, Object> originalData : originalPayload) {
                Map<String, Object> recordData = new HashMap<>();
                recordData.put(AffectedNodesConstants.NENAME, originalData.get(AffectedNodesConstants.NENAME));
                recordData.put(AffectedNodesConstants.NETYPE, originalData.get(AffectedNodesConstants.NETYPE));
                recordData.put(AffectedNodesConstants.STATUS, AffectedNodesConstants.SUCCESS);
                recordData.put(AffectedNodesConstants.ORIGINAL_DATA, originalData);
                summary.getNewRecords().add(recordData);
            }
            return summary;
        }

        Map<String, Map<String, Object>> originalDataMap = buildOriginalDataMap(originalPayload);
        Set<String> processedRecords = new HashSet<>();

        for (Map<String, Object> result : batchResults) {
            processVertexStatus(result, summary, originalDataMap, processedRecords);
            processEdgeStatus(result, summary, originalDataMap);
        }

        return summary;
    }

    private Map<String, Map<String, Object>> buildOriginalDataMap(List<Map<String, Object>> originalPayload) {
        Map<String, Map<String, Object>> map = new HashMap<>();
        for (Map<String, Object> data : originalPayload) {
            String neName = (String) data.get(AffectedNodesConstants.NENAME);
            if (neName != null) {
                map.put(neName, data);
            }
        }
        return map;
    }

    @SuppressWarnings("unchecked")
    private void processVertexStatus(Map<String, Object> result,
            BatchProcessingSummary summary,
            Map<String, Map<String, Object>> originalDataMap,
            Set<String> processedRecords) {
        Map<String, Object> vertexStatus = (Map<String, Object>) result.get(AffectedNodesConstants.VERTEX_STATUS);
        if (vertexStatus == null)
            return;

        String neName = (String) vertexStatus.get(AffectedNodesConstants.NENAME);
        String status = (String) vertexStatus.get(AffectedNodesConstants.STATUS_KEY);

        if (AffectedNodesConstants.ERROR.equals(status)) {
            handleVertexError(summary, vertexStatus, originalDataMap, neName);
        } else if (AffectedNodesConstants.SUCCESS.equals(status)) {
            handleVertexSuccess(summary, vertexStatus, originalDataMap, processedRecords, neName);
        }
    }

    private void handleVertexError(BatchProcessingSummary summary,
            Map<String, Object> vertexStatus,
            Map<String, Map<String, Object>> originalDataMap,
            String neName) {
        Map<String, Object> failedRecord = new HashMap<>();
        failedRecord.put(AffectedNodesConstants.NENAME, neName);
        failedRecord.put(AffectedNodesConstants.NETYPE, vertexStatus.get(AffectedNodesConstants.NETYPE));
        failedRecord.put(AffectedNodesConstants.ERROR_MSG, vertexStatus.get(AffectedNodesConstants.ERROR_MSG));
        failedRecord.put(AffectedNodesConstants.ORIGINAL_DATA, originalDataMap.get(neName));
        summary.getFailedRecords().add(failedRecord);
    }

    private void handleVertexSuccess(BatchProcessingSummary summary,
            Map<String, Object> vertexStatus,
            Map<String, Map<String, Object>> originalDataMap,
            Set<String> processedRecords,
            String neName) {
        processedRecords.add(neName);
        Map<String, Object> originalData = originalDataMap.get(neName);
        String message = (String) vertexStatus.get(AffectedNodesConstants.MESSAGE);
        boolean isUpdate = message != null && message.contains("Edge updated");

        Map<String, Object> recordData = new HashMap<>();
        recordData.put(AffectedNodesConstants.NENAME, neName);
        recordData.put(AffectedNodesConstants.NETYPE, vertexStatus.get(AffectedNodesConstants.NETYPE));
        recordData.put(AffectedNodesConstants.STATUS, AffectedNodesConstants.SUCCESS);
        recordData.put(AffectedNodesConstants.ORIGINAL_DATA, originalData);
        recordData.put("isUpdate", isUpdate);

        if (isUpdate) {
            summary.getUpdatedRecords().add(recordData);
        } else {
            summary.getNewRecords().add(recordData);
        }
    }

    @SuppressWarnings("unchecked")
    private void processEdgeStatus(Map<String, Object> result,
            BatchProcessingSummary summary,
            Map<String, Map<String, Object>> originalDataMap) {
        Map<String, Object> edgeStatus = (Map<String, Object>) result.get(AffectedNodesConstants.EDGE_STATUS);
        if (edgeStatus == null)
            return;

        for (Map.Entry<String, Object> entry : edgeStatus.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(AffectedNodesConstants.EDGE)) {
                String neName = key.substring(AffectedNodesConstants.EDGE.length());
                String status = entry.getValue().toString();

                if (AffectedNodesConstants.ERROR.equals(status)) {
                    handleEdgeError(summary, edgeStatus, originalDataMap, neName);
                }
            }
        }
    }

    private void handleEdgeError(BatchProcessingSummary summary,
            Map<String, Object> edgeStatus,
            Map<String, Map<String, Object>> originalDataMap,
            String neName) {
        Map<String, Object> failedEdge = new HashMap<>();
        failedEdge.put(AffectedNodesConstants.NENAME, neName);
        failedEdge.put(AffectedNodesConstants.ERROR_MSG, edgeStatus.get(AffectedNodesConstants.MSG));
        failedEdge.put(AffectedNodesConstants.ORIGINAL_DATA, originalDataMap.get(neName));
        summary.getFailedRecords().add(failedEdge);
    }

    /**
     * Escapes CSV values to handle commas, quotes, and newlines.
     * 
     * @param value the value to escape
     * @return escaped CSV value
     */
    private String escapeCsvValue(Object value) {
        if (value == null) {
            return "";
        }

        String stringValue = value.toString();

        // If the value contains comma, quote, or newline, wrap it in quotes and escape
        // internal quotes
        if (stringValue.contains(",") || stringValue.contains("\"") || stringValue.contains("\n")
                || stringValue.contains("\r")) {
            return "\"" + stringValue.replace("\"", "\"\"") + "\"";
        }

        return stringValue;
    }

    @Override
    public byte[] downloadCsvFile(String fileName) {
        if (fileName == null || fileName.trim().isEmpty()) {
            throw new IllegalArgumentException("File name cannot be null or empty");
        }

        try {
            logger.info("Download request for CSV file: {}", fileName);

            // Generate CSV content based on the filename using accumulated batch processing
            // data
            String csvContent = generateCsvContentByFileName(fileName);

            if (csvContent == null || csvContent.isEmpty()) {
                throw new ResourceNotFoundException("CSV file not found: " + fileName);
            }

            return csvContent.getBytes(StandardCharsets.UTF_8);

        } catch (Exception e) {
            logger.error("Failed to download CSV file");
            throw new CsvDownloadException("Failed to download CSV file: " + fileName, e);
        }
    }

    /**
     * Generates CSV content based on the filename using accumulated batch
     * processing data.
     * 
     * @param fileName the filename to generate content for
     * @return CSV content as string, or null if file type is not supported
     */
    private String generateCsvContentByFileName(String fileName) {
        // Use lastSentSummary for CSV download (the data that was actually sent in the
        // email)
        BatchProcessingSummary summaryForCsv = lastSentSummary;

        if (summaryForCsv == null) {
            logger.warn("No last sent summary available for CSV generation: {}", fileName);
            return generateFallbackCsvContent(fileName);
        }

        if (fileName.startsWith(AffectedNodesConstants.NEW_RECORDS)) {
            return generateNewRecordsCsvContent(summaryForCsv.getNewRecords());
        } else if (fileName.startsWith(AffectedNodesConstants.UPDATED_RECORDS)) {
            return generateUpdatedRecordsCsvContent(summaryForCsv.getUpdatedRecords());
        } else if (fileName.startsWith(AffectedNodesConstants.FAILED_RECORDS)) {
            return generateFailedRecordsCsvContent(summaryForCsv.getFailedRecords());
        } else {
            logger.warn("Unsupported CSV file type: {}", fileName);
            return null;
        }
    }

    /**
     * Generates fallback CSV content when no current batch data is available.
     */
    private String generateFallbackCsvContent(String fileName) {
        logger.info("Generating fallback CSV content for: {}", fileName);

        if (fileName.startsWith(AffectedNodesConstants.NEW_RECORDS)) {
            return generateNewRecordsCsvContent(new ArrayList<>());
        } else if (fileName.startsWith(AffectedNodesConstants.UPDATED_RECORDS)) {
            return generateUpdatedRecordsCsvContent(new ArrayList<>());
        } else if (fileName.startsWith(AffectedNodesConstants.FAILED_RECORDS)) {
            return generateFailedRecordsCsvContent(new ArrayList<>());
        }

        return "No data available";
    }

    /**
     * Generates CSV content for new records using actual data.
     */
    private String generateNewRecordsCsvContent(List<Map<String, Object>> newRecords) {
        StringBuilder csvContent = new StringBuilder();
        appendCsvHeader(csvContent);

        if (newRecords.isEmpty()) {
            csvContent.append("No new records found\n");
            return csvContent.toString();
        }

        for (Map<String, Object> recordItem : newRecords) {
            appendRecord(csvContent, recordItem);
        }

        return csvContent.toString();
    }

    private void appendCsvHeader(StringBuilder csvContent) {
        csvContent.append(
                "neName,neType,status,parentNE,neighbourNEName,relation,neCategory,domain,vendor,technology,latitude,longitude\n");
    }

    @SuppressWarnings("unchecked")
    private void appendRecord(StringBuilder csvContent, Map<String, Object> recordItem) {
        Map<String, Object> originalData = (Map<String, Object>) recordItem.get(AffectedNodesConstants.ORIGINAL_DATA);

        appendFieldForNewRecords(csvContent, recordItem.get(AffectedNodesConstants.NENAME));
        appendFieldForNewRecords(csvContent, recordItem.get(AffectedNodesConstants.NETYPE));
        appendFieldForNewRecords(csvContent, recordItem.get(AffectedNodesConstants.STATUS));
        appendFieldForNewRecords(csvContent,
                getOriginalFieldForNewRecords(originalData, AffectedNodesConstants.PARENT_NE));
        appendFieldForNewRecords(csvContent,
                getOriginalFieldForNewRecords(originalData, AffectedNodesConstants.NEIGHBOUR_NENAME));
        appendFieldForNewRecords(csvContent,
                getOriginalFieldForNewRecords(originalData, AffectedNodesConstants.RELATION));
        appendFieldForNewRecords(csvContent,
                getOriginalFieldForNewRecords(originalData, AffectedNodesConstants.NECATEGORY));
        appendFieldForNewRecords(csvContent,
                getOriginalFieldForNewRecords(originalData, AffectedNodesConstants.DOMAIN));
        appendFieldForNewRecords(csvContent,
                getOriginalFieldForNewRecords(originalData, AffectedNodesConstants.VENDOR));
        appendFieldForNewRecords(csvContent, getOriginalFieldForNewRecords(originalData, AffectedNodesConstants.TECH));
        appendFieldForNewRecords(csvContent, getOriginalFieldForNewRecords(originalData, AffectedNodesConstants.LAT));
        appendLastFieldForNewRecords(csvContent,
                getOriginalFieldForNewRecords(originalData, AffectedNodesConstants.LNG));
    }

    private void appendFieldForNewRecords(StringBuilder csvContent, Object value) {
        csvContent.append(escapeCsvValue(value)).append(",");
    }

    private void appendLastFieldForNewRecords(StringBuilder csvContent, Object value) {
        csvContent.append(escapeCsvValue(value)).append("\n");
    }

    private Object getOriginalFieldForNewRecords(Map<String, Object> originalData, String key) {
        return originalData != null ? originalData.get(key) : "";
    }

    /**
     * Generates CSV content for updated records using actual data.
     */
    private String generateUpdatedRecordsCsvContent(List<Map<String, Object>> updatedRecords) {
        StringBuilder csvContent = new StringBuilder();
        appendUpdatedCsvHeader(csvContent);

        if (updatedRecords.isEmpty()) {
            csvContent.append("No updated records found\n");
            return csvContent.toString();
        }

        for (Map<String, Object> recordItem : updatedRecords) {
            appendUpdatedRecord(csvContent, recordItem);
        }

        return csvContent.toString();
    }

    private void appendUpdatedCsvHeader(StringBuilder csvContent) {
        csvContent.append(
                "neName,neType,status,parentNE,neighbourNEName,relation,neCategory,domain,vendor,technology,latitude,longitude\n");
    }

    @SuppressWarnings("unchecked")
    private void appendUpdatedRecord(StringBuilder csvContent, Map<String, Object> recordItem) {
        Map<String, Object> originalData = (Map<String, Object>) recordItem.get(AffectedNodesConstants.ORIGINAL_DATA);

        appendFieldForUpdatedRecords(csvContent, recordItem.get(AffectedNodesConstants.NENAME));
        appendFieldForUpdatedRecords(csvContent, recordItem.get(AffectedNodesConstants.NETYPE));
        appendFieldForUpdatedRecords(csvContent, recordItem.get(AffectedNodesConstants.STATUS));
        appendFieldForUpdatedRecords(csvContent,
                getOriginalFieldForUpdatedRecords(originalData, AffectedNodesConstants.PARENT_NE));
        appendFieldForUpdatedRecords(csvContent,
                getOriginalFieldForUpdatedRecords(originalData, AffectedNodesConstants.NEIGHBOUR_NENAME));
        appendFieldForUpdatedRecords(csvContent,
                getOriginalFieldForUpdatedRecords(originalData, AffectedNodesConstants.RELATION));
        appendFieldForUpdatedRecords(csvContent,
                getOriginalFieldForUpdatedRecords(originalData, AffectedNodesConstants.NECATEGORY));
        appendFieldForUpdatedRecords(csvContent,
                getOriginalFieldForUpdatedRecords(originalData, AffectedNodesConstants.DOMAIN));
        appendFieldForUpdatedRecords(csvContent,
                getOriginalFieldForUpdatedRecords(originalData, AffectedNodesConstants.VENDOR));
        appendFieldForUpdatedRecords(csvContent,
                getOriginalFieldForUpdatedRecords(originalData, AffectedNodesConstants.TECH));
        appendFieldForUpdatedRecords(csvContent,
                getOriginalFieldForUpdatedRecords(originalData, AffectedNodesConstants.LAT));
        appendLastFieldForUpdatedRecords(csvContent,
                getOriginalFieldForUpdatedRecords(originalData, AffectedNodesConstants.LNG));
    }

    private void appendFieldForUpdatedRecords(StringBuilder csvContent, Object value) {
        csvContent.append(escapeCsvValue(value)).append(",");
    }

    private void appendLastFieldForUpdatedRecords(StringBuilder csvContent, Object value) {
        csvContent.append(escapeCsvValue(value)).append("\n");
    }

    private Object getOriginalFieldForUpdatedRecords(Map<String, Object> originalData, String key) {
        return originalData != null ? originalData.get(key) : "";
    }

    /**
     * Generates CSV content for failed records using actual data.
     */
    private String generateFailedRecordsCsvContent(List<Map<String, Object>> failedRecords) {
        StringBuilder csvContent = new StringBuilder();
        appendFailedCsvHeader(csvContent);

        if (failedRecords.isEmpty()) {
            csvContent.append("No failed records found\n");
            return csvContent.toString();
        }

        for (Map<String, Object> recordItem : failedRecords) {
            appendFailedRecord(csvContent, recordItem);
        }

        return csvContent.toString();
    }

    private void appendFailedCsvHeader(StringBuilder csvContent) {
        csvContent.append(
                "neName,neType,status,parentNE,neighbourNEName,relation,neCategory,domain,vendor,technology,latitude,longitude,errorMessage\n");
    }

    @SuppressWarnings("unchecked")
    private void appendFailedRecord(StringBuilder csvContent, Map<String, Object> recordItem) {
        Map<String, Object> originalData = (Map<String, Object>) recordItem.get(AffectedNodesConstants.ORIGINAL_DATA);

        appendFieldForFailedRecords(csvContent, recordItem.get(AffectedNodesConstants.NENAME));
        appendFieldForFailedRecords(csvContent, recordItem.get(AffectedNodesConstants.NETYPE));
        appendFieldForFailedRecords(csvContent, recordItem.get(AffectedNodesConstants.STATUS));
        appendFieldForFailedRecords(csvContent,
                getOriginalFieldForFailedRecords(originalData, AffectedNodesConstants.PARENT_NE));
        appendFieldForFailedRecords(csvContent,
                getOriginalFieldForFailedRecords(originalData, AffectedNodesConstants.NEIGHBOUR_NENAME));
        appendFieldForFailedRecords(csvContent,
                getOriginalFieldForFailedRecords(originalData, AffectedNodesConstants.RELATION));
        appendFieldForFailedRecords(csvContent,
                getOriginalFieldForFailedRecords(originalData, AffectedNodesConstants.NECATEGORY));
        appendFieldForFailedRecords(csvContent,
                getOriginalFieldForFailedRecords(originalData, AffectedNodesConstants.DOMAIN));
        appendFieldForFailedRecords(csvContent,
                getOriginalFieldForFailedRecords(originalData, AffectedNodesConstants.VENDOR));
        appendFieldForFailedRecords(csvContent,
                getOriginalFieldForFailedRecords(originalData, AffectedNodesConstants.TECH));
        appendFieldForFailedRecords(csvContent,
                getOriginalFieldForFailedRecords(originalData, AffectedNodesConstants.LAT));
        appendFieldForFailedRecords(csvContent,
                getOriginalFieldForFailedRecords(originalData, AffectedNodesConstants.LNG));
        appendLastFieldForFailedRecords(csvContent, recordItem.get(AffectedNodesConstants.ERROR_MSG));
    }

    private void appendFieldForFailedRecords(StringBuilder csvContent, Object value) {
        csvContent.append(escapeCsvValue(value)).append(",");
    }

    private void appendLastFieldForFailedRecords(StringBuilder csvContent, Object value) {
        csvContent.append(escapeCsvValue(value)).append("\n");
    }

    private Object getOriginalFieldForFailedRecords(Map<String, Object> originalData, String key) {
        return originalData != null ? originalData.get(key) : "";
    }

    /**
     * Inner class to hold batch processing summary information.
     */
    private static class BatchProcessingSummary {
        private int totalRecords;
        private List<Map<String, Object>> newRecords = new ArrayList<>();
        private List<Map<String, Object>> failedRecords = new ArrayList<>();
        private List<Map<String, Object>> updatedRecords = new ArrayList<>();
        private String processingTime;

        // Getters and setters
        public int getTotalRecords() {
            return totalRecords;
        }

        public void setTotalRecords(int totalRecords) {
            this.totalRecords = totalRecords;
        }

        public List<Map<String, Object>> getNewRecords() {
            return newRecords;
        }

        public List<Map<String, Object>> getFailedRecords() {
            return failedRecords;
        }

        public List<Map<String, Object>> getUpdatedRecords() {
            return updatedRecords;
        }

        public String getProcessingTime() {
            return processingTime;
        }

        public void setProcessingTime(String processingTime) {
            this.processingTime = processingTime;
        }
    }

    private boolean isGraphEmpty() {
        try {
            Long cnt = g.V().limit(1).count().tryNext().orElse(0L);
            return cnt == 0L;
        } catch (Exception e) {
            logger.warn("Failed to check graph emptiness: {}", e.getMessage());
            return false;
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
        return getLinksByAreaIpAddress(areaIpAddress, tag, null);
    }

    private List<Map<String, Object>> getLinksByAreaIpAddress(String areaIpAddress, String tag, String relation) {
        boolean hasAreaIp = hasValidParameter(areaIpAddress);
        boolean hasTag = hasValidParameter(tag);

        // Validate that only one parameter is provided at a time
        if (hasAreaIp && hasTag) {
            throw new InvalidParametersException(
                    "Give only one value at a time. Provide either areaIpAddress or tag, not both.");
        }

        if (!hasAreaIp && !hasTag) {
            return buildLinksResponse(Collections.emptyList(), Collections.emptyList());
        }

        String filterDescription;
        try (JanusGraphTransaction tx = graph.newTransaction()) {
            GraphTraversalSource gtx = tx.traversal();
            List<Map<String, Object>> links;

            if (hasAreaIp) {
                String trimmedAreaIp = areaIpAddress.trim();
                filterDescription = String.format("areaIpAddress: %s", trimmedAreaIp);
                if (hasValidParameter(relation)) {
                    filterDescription += String.format(", relation: %s", relation.trim().toUpperCase());
                }
                logger.info("Fetching links and nodes with {}", filterDescription);
                links = fetchLinksByAreaIp(gtx, trimmedAreaIp, relation);
            } else {
                String trimmedTag = tag.trim();
                filterDescription = String.format("tag: %s", trimmedTag);
                if (hasValidParameter(relation)) {
                    filterDescription += String.format(", relation: %s", relation.trim().toUpperCase());
                }
                logger.info("Fetching links and nodes with {}", filterDescription);
                links = fetchLinksByTag(gtx, trimmedTag, relation);
            }

            List<Map<String, Object>> nodes = fetchNodesForLinks(gtx, links);
            tx.commit();

            logger.info("Successfully retrieved {} links and {} nodes with {}",
                    links.size(), nodes.size(), filterDescription);
            return buildLinksResponse(links, nodes);
        } catch (Exception e) {
            throw new DataProcessingException("Failed to get links: " + e.getMessage(), e);
        }
    }

    private List<Map<String, Object>> fetchLinksByAreaIp(GraphTraversalSource gtx, String trimmedAreaIp, String relation) {
        // If relation is provided, filter edges by relation(s)
        if (hasValidParameter(relation)) {
            List<String> relationList = parseRelations(relation);
            logger.info("Filtering edges by relations: {}", relationList);
            
            if (relationList.size() == 1) {
                // Single relation - use exact match
                String relationValue = relationList.get(0);
                List<Map<String, Object>> links = gtx.E()
                        .has(AffectedNodesConstants.AREA_IP_ADDRESS, trimmedAreaIp)
                        .has(AffectedNodesConstants.RELATION, relationValue)
                        .dedup()
                        .elementMap()
                        .toList()
                        .stream()
                        .map(this::flattenEdgeMap)
                        .toList();
                logger.info("Found {} distinct links with areaIpAddress: {} and relation: {}", links.size(), trimmedAreaIp, relationValue);
                return links;
            } else {
                // Multiple relations - use P.within()
                List<Map<String, Object>> links = gtx.E()
                        .has(AffectedNodesConstants.AREA_IP_ADDRESS, trimmedAreaIp)
                        .has(AffectedNodesConstants.RELATION, P.within(relationList))
                        .dedup()
                        .elementMap()
                        .toList()
                        .stream()
                        .map(this::flattenEdgeMap)
                        .toList();
                logger.info("Found {} distinct links with areaIpAddress: {} and relations: {}", links.size(), trimmedAreaIp, relationList);
                return links;
            }
        }
        
        List<Map<String, Object>> links = gtx.E()
                .has(AffectedNodesConstants.AREA_IP_ADDRESS, trimmedAreaIp)
                .dedup()
                .elementMap()
                .toList()
                .stream()
                .map(this::flattenEdgeMap)
                .toList();
        logger.info("Found {} distinct links with areaIpAddress: {}", links.size(), trimmedAreaIp);
        return links;
    }

    private List<Map<String, Object>> fetchLinksByTag(GraphTraversalSource gtx, String trimmedTag, String relation) {
        // If relation is provided, filter edges by relation(s)
        if (hasValidParameter(relation)) {
            List<String> relationList = parseRelations(relation);
            logger.info("Filtering edges by relations: {}", relationList);
            
            if (relationList.size() == 1) {
                // Single relation - use exact match
                String relationValue = relationList.get(0);
                List<Map<String, Object>> links = gtx.E()
                        .has(AffectedNodesConstants.TAG, trimmedTag)
                        .has(AffectedNodesConstants.RELATION, relationValue)
                        .dedup()
                        .elementMap()
                        .toList()
                        .stream()
                        .map(this::flattenEdgeMap)
                        .toList();
                logger.info("Found {} distinct links with tag: {} and relation: {}", links.size(), trimmedTag, relationValue);
                return links;
            } else {
                // Multiple relations - use P.within()
                List<Map<String, Object>> links = gtx.E()
                        .has(AffectedNodesConstants.TAG, trimmedTag)
                        .has(AffectedNodesConstants.RELATION, P.within(relationList))
                        .dedup()
                        .elementMap()
                        .toList()
                        .stream()
                        .map(this::flattenEdgeMap)
                        .toList();
                logger.info("Found {} distinct links with tag: {} and relations: {}", links.size(), trimmedTag, relationList);
                return links;
            }
        }
        
        List<Map<String, Object>> links = gtx.E()
                .has(AffectedNodesConstants.TAG, trimmedTag)
                .dedup()
                .elementMap()
                .toList()
                .stream()
                .map(this::flattenEdgeMap)
                .toList();
        logger.info("Found {} distinct links with tag: {}", links.size(), trimmedTag);
        return links;
    }

    private List<Map<String, Object>> fetchNodesForLinks(GraphTraversalSource gtx, List<Map<String, Object>> links) {
        Set<String> nodeNames = extractNodeNamesFromLinks(links);
        if (nodeNames.isEmpty()) {
            return new ArrayList<>();
        }
        List<Map<String, Object>> nodes = gtx.V()
                .has(AffectedNodesConstants.NENAME, P.within(nodeNames))
                .elementMap()
                .toList()
                .stream()
                .map(this::flattenVertexMap)
                .toList();
        logger.info("Found {} distinct nodes", nodes.size());
        return nodes;
    }

    private Set<String> extractNodeNamesFromLinks(List<Map<String, Object>> links) {
        Set<String> nodeNames = new HashSet<>();
        for (Map<String, Object> link : links) {
            String start = (String) link.get(AffectedNodesConstants.START);
            String end = (String) link.get(AffectedNodesConstants.END);
            if (notBlank(start)) {
                nodeNames.add(start);
            }
            if (notBlank(end)) {
                nodeNames.add(end);
            }
        }
        return nodeNames;
    }

    private List<Map<String, Object>> buildLinksResponse(List<Map<String, Object>> links,
            List<Map<String, Object>> nodes) {
        List<Map<String, Object>> finalResponseList = new ArrayList<>();
        Map<String, Object> response = new LinkedHashMap<>();
        response.put(AffectedNodesConstants.LINKS, links);
        response.put(AffectedNodesConstants.NODES, nodes);
        finalResponseList.add(response);
        return finalResponseList;
    }

    /**
     * Deletes vertices and edges based on current date.
     * 
     * <p>
     * This method automatically uses the current date (yyyy-MM-dd) to delete all vertices and edges
     * that have a lastDiscoveryTime property value less than the current date.
     * Deletes vertices first, commits, then deletes edges and commits again.
     * </p>
     * 
     * <p>
     * Example: If current date is "2026-01-15", it will delete all vertices and edges
     * where lastDiscoveryTime < "2026-01-15 00:00:00".
     * </p>
     * 
     * @return a map containing the status of the deletion operation
     * @throws DataProcessingException if there is an error during deletion
     */
    @Override
    public Map<String, Object> deleteByLastDiscoveryTime() {
        // Calculate date 7 days ago to keep only last 7 days of data
        String date7DaysAgo = getDate7DaysAgo();
        String currentDate = getCurrentDate();
        logger.info("Keeping last 7 days of data. Current date: {}, Deleting data before: {}", currentDate, date7DaysAgo);

        Map<String, Object> response = new HashMap<>();
        JanusGraphTransaction vertexTx = null;
        JanusGraphTransaction edgeTx = null;

        try {
            logger.info("Deleting vertices and edges with lastDiscoveryTime date less than: {} (keeping last 7 days)", date7DaysAgo);

            vertexTx = graph.newTransaction();
            GraphTraversalSource vertexGtx = vertexTx.traversal();

            vertexGtx.V()
                    .has(JanusGraphUtilsConstants.LAST_DISCOVERY_TIME, P.lt(date7DaysAgo))
                    .drop()
                    .iterate();

            vertexTx.commit();
            logger.info("Successfully deleted and committed vertices with lastDiscoveryTime date < {} (kept last 7 days)", date7DaysAgo);

            edgeTx = graph.newTransaction();
            GraphTraversalSource edgeGtx = edgeTx.traversal();

            edgeGtx.E()
                    .has(JanusGraphUtilsConstants.LAST_DISCOVERY_TIME, P.lt(date7DaysAgo))
                    .drop()
                    .iterate();

            edgeTx.commit();
            logger.info("Successfully deleted and committed edges with lastDiscoveryTime date < {} (kept last 7 days)", date7DaysAgo);

            response.put("status", "success");
            response.put("currentDate", currentDate);
            response.put("deletedBeforeDate", date7DaysAgo);
            response.put("daysKept", AffectedNodesConstants.SEVEN);
            response.put(AffectedNodesConstants.MESSAGE, String.format("Successfully deleted vertices and edges older than %s. Kept last 7 days of data (from %s to %s)", 
                    date7DaysAgo, date7DaysAgo, currentDate));

        } catch (InvalidParametersException e) {
            rollbackTransaction(vertexTx);
            rollbackTransaction(edgeTx);
            throw e;
        } catch (Exception e) {
            rollbackTransaction(vertexTx);
            rollbackTransaction(edgeTx);
            logger.error("Error deleting vertices and edges older than {}", date7DaysAgo);
            throw new DataProcessingException("Failed to delete vertices and edges: " + e.getMessage(), e);
        } finally {
            rollbackIfOpen(vertexTx);
            rollbackIfOpen(edgeTx);
        }

        return response;
    }

    /**
     * Gets the current date in the format "yyyy-MM-dd".
     * 
     * @return current date as string in format "yyyy-MM-dd"
     */
    private String getCurrentDate() {
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        return now.format(formatter);
    }

    /**
     * Gets the date 7 days ago from today in the format "yyyy-MM-dd".
     * This is used to keep only the last 7 days of data.
     * 
     * @return date 7 days ago as string in format "yyyy-MM-dd"
     */
    private String getDate7DaysAgo() {
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime date7DaysAgo = now.minusDays(AffectedNodesConstants.SEVEN);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        return date7DaysAgo.format(formatter);
    }
}
