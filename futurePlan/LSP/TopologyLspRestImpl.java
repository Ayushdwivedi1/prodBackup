package com.enttribe.module.topology.core.corelation.rest.impl;

import com.enttribe.module.topology.core.corelation.rest.TopologyLspRest;
import com.enttribe.module.topology.core.corelation.service.TopologyLspService;
import com.enttribe.module.topology.core.utils.exceptions.EdgeSearchException;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Primary;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.util.List;
import java.util.Map;

/**
 * REST controller implementation for Topology LSP data operations.
 *
 * <p>
 * This controller provides endpoints for retrieving MPLS LSP link data from PM
 * Cassandra keyspace (pm.mpls_lsp_link).
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
@Primary
@RequestMapping(produces = MediaType.APPLICATION_JSON_VALUE, path = "lsp")
@RestController
public class TopologyLspRestImpl implements TopologyLspRest {

    private static final Logger logger = LoggerFactory.getLogger(TopologyLspRestImpl.class);

    @Autowired
    private TopologyLspService topologyLspService;

    @Override
    public Object getTopologyLspData(String filter, String start, String end) {
        logger.info("Received request for getTopologyLspData, filter={}, start={}, end={}", filter, start, end);
        try {
            return topologyLspService.getTopologyLspData(filter, start, end);
        } catch (RuntimeException e) {
            logger.error("Unexpected error while fetching topology LSP data from Cassandra");
            throw new EdgeSearchException("Failed to fetch topology LSP data: " + e.getMessage(), e);
        }
    }

    @Override
    public Object getLspData(String start, String end, String lspName, Integer lLimit,
            Integer uLimit, String orderBy, String orderType, String metaInfoFilters) {
        logger.info(
                "Received request for getLspData, start={}, end={}, lspName={}, lLimit={}, uLimit={}, orderBy={}, orderType={}, metaInfoFilters={}",
                start, end, lspName, lLimit, uLimit, orderBy, orderType, metaInfoFilters);
        try {
            return topologyLspService.getLspData(start, end, lspName, lLimit, uLimit, orderBy, orderType,
                    metaInfoFilters);
        } catch (RuntimeException e) {
            logger.error("Unexpected error while fetching LSP links data");
            throw new EdgeSearchException("Failed to fetch LSP data: " + e.getMessage(), e);
        }
    }

    @Override
    public String getLspDataCount(String start, String end, String lspName, String metaInfoFilters) {
        logger.info("Received request for getLspDataCount, start={}, end={}, lspName={}, metaInfoFilters={}", start,
                end, lspName, metaInfoFilters);
        try {
            return topologyLspService.getLspDataCount(start, end, lspName, metaInfoFilters);
        } catch (RuntimeException e) {
            logger.error("Unexpected error while fetching LSP links count");
            throw new EdgeSearchException("Failed to fetch LSP count: " + e.getMessage(), e);
        }
    }

    @Override
    public Object getLspStatus(Map<String, Object> payload) {
        logger.info("Received request for getLspStatus, payload={}", payload);
        try {
            return topologyLspService.getLspStatus(payload);
        } catch (RuntimeException e) {
            logger.error("Unexpected error while fetching getLspStatus data");
            throw new EdgeSearchException("Failed to fetch LSP status data: " + e.getMessage(), e);
        }
    }

    @Override
    public List<Map<String, Object>> lspSearch(String neTypes, String region, String filter) {
        logger.info("Received lspSearch neTypes={}, region={}, filter={}", neTypes, region, filter);
        try {
            return topologyLspService.lspSearch(neTypes, region, filter);
        } catch (RuntimeException e) {
            logger.error("Unexpected error during lspSearch");
            throw new EdgeSearchException("Failed LSP search (Cassandra): " + e.getMessage(), e);
        }
    }

    @Override
    public List<Map<String, Object>> getLspFilterData(List<String> payload, String filter) {
        logger.info("Received getLspFilterData filter={}, payloadSize={}", filter,
                payload == null ? null : payload.size());
        try {
            return topologyLspService.getLspFilterData(payload, filter);
        } catch (RuntimeException e) {
            logger.error("Unexpected error during getLspFilterData");
            throw new EdgeSearchException("Failed to build LSP filter data from Cassandra: " + e.getMessage(), e);
        }
    }
}
