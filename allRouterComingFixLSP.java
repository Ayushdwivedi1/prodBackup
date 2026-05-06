package com.enttribe.module.topology.core.corelation.service.impl;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.enttribe.commons.configuration.ConfigUtils;
import com.enttribe.core.generic.utils.CustomTokenUtils;
import com.enttribe.module.topology.core.corelation.constants.AffectedNodesConstants;
import com.enttribe.module.topology.core.pm.cassandra.MplsLspLinkDto;
import com.enttribe.module.topology.core.pm.cassandra.MplsLspLinkService;
import com.enttribe.module.topology.core.corelation.service.TopologyLspService;
import com.enttribe.network.module.fm.core.rest.AlarmRest;
import com.enttribe.product.security.spring.userdetails.CustomerInfo;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Service implementation for retrieving MPLS LSP link data from PM Cassandra
 * keyspace (pm.mpls_lsp_link).
 * This class provides functionality for retrieving MPLS LSP link data from PM
 * Cassandra keyspace (pm.mpls_lsp_link).
 *
 * <p>
 * This implementation handles:
 * </p>
 * <ul>
 * <li>Retrieving MPLS LSP link data from PM Cassandra keyspace
 * (pm.mpls_lsp_link)</li>
 * <li>Building aggregated response for LSP data</li>
 * <li>Building default response for LSP data</li>
 * <li>Filtering nodes by source and destination</li>
 * <li>Building links from nodes</li>
 * <li>Adding aliases to link details</li>
 * </ul>
 *
 * <p>
 * The class is thread-safe and manages its own PM Cassandra connection
 * lifecycle.
 * </p>
 *
 * @author Enttribe
 * @version 1.0
 * @since 1.0
 */

@Service
public class TopologyLspServiceImpl implements TopologyLspService {

    @Autowired
    private MplsLspLinkService mplsLspLinkService;

    @Autowired
    private EntityManager entityManager;

    @Autowired
    private AlarmRest alarmRest;

    @Autowired
    private CustomerInfo customerInfo;

    private static final Logger logger = LoggerFactory.getLogger(TopologyLspServiceImpl.class);
    private static final Pattern META_INFO_FILTER_PATTERN = Pattern.compile(
            "^\\s*(\\w+)\\s*(>=|<=|!=|=|>|<)\\s*(\\S(?:.*\\S)?)\\s*$");
    private static final String LSP_CONFIG_SQL = """
            SELECT MIN(CONFIG_VALUE) AS LSP_CONFIG_DATA
            FROM PLATFORM.BASE_CONFIGURATION
            WHERE CONFIG_KEY IN (
              'JUNIPER_MPLS_LSP_LINK_CREATION',
              'CISCO_MPLS_LSP_LINK_CREATION',
              'NOKIA_MPLS_LSP_LINK_CREATION',
              'EDGECORE_MPLS_LSP_LINK_CREATION'
            )
            """;
    private static final String LSP_DOWN_ALARM_CONFIG_KEY = "TOPOLOGY_LSP_ALARM_NAMES";
    private static final String LSP_DOWN_ALARM_SQL = """
            SELECT CONFIG_VALUE
            FROM PLATFORM.BASE_CONFIGURATION
            WHERE CONFIG_KEY = :configKey
            """;
    private static final String LSP_ALARMS_FIELD = "lspAlarms";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /**
     * Retrieves MPLS LSP link data from PM Cassandra keyspace (pm.mpls_lsp_link).
     * When filter is "LSP", returns a single map with "nodes" and "links".
     * Otherwise returns list of records.
     *
     * @param filter optional; when "LSP" returns single object { nodes, links };
     *               when LSP name filters by that name
     * @return when filter=LSP: single Map with "nodes", "links" and "timestamp";
     *         otherwise List of maps with sourceIp, destinationIp, lspName, links,
     *         nodes, timestamp
     */
    @Override
    public Object getTopologyLspData(String filter, String start, String end) {
        String timestamp = getLspConfigData();
        if (isInvalidTimestamp(timestamp)) {
            return buildMissingLspConfigError();
        }

        if (isServiceUnavailable()) {
            return new ArrayList<>();
        }

        String trimmedFilter = trimFilter(filter);
        boolean aggregateResponse = isAggregateResponse(trimmedFilter);
        boolean filterByLspName = isFilterByLspName(trimmedFilter, aggregateResponse);

        List<MplsLspLinkDto> list = fetchLspData(timestamp, filterByLspName, trimmedFilter);

        list = applyStartEndFilterIfNeeded(list, filterByLspName, start, end);

        if (aggregateResponse) {
            return handleAggregatedResponse(list, trimmedFilter, filterByLspName, start, end);
        }

        return buildDefaultResponse(list);
    }

    private boolean isInvalidTimestamp(String timestamp) {
        return timestamp == null || timestamp.isBlank();
    }

    private Map<String, String> buildMissingLspConfigError() {
        Map<String, String> response = new LinkedHashMap<>();
        response.put(AffectedNodesConstants.MESSAGE,
                "LSP config data not found in PLATFORM.BASE_CONFIGURATION");
        return response;
    }

    private String getLspConfigData() {
        try {
            Query query = entityManager.createNativeQuery(LSP_CONFIG_SQL);
            Object result = query.getSingleResult();
            if (result == null) {
                logger.info("getLspConfigData query response is null");
                return null;
            }
            String configValue = result.toString().trim();
            if (configValue.isEmpty()) {
                logger.info("getLspConfigData query response is blank");
                return null;
            }
            logger.info("getLspConfigData query response (LSP_CONFIG_DATA): {}", configValue);
            return configValue;
        } catch (Exception e) {
            logger.error("Failed to fetch LSP config data from BASE_CONFIGURATION: {}", e.getMessage(), e);
            return null;
        }
    }

    private boolean isServiceUnavailable() {
        if (mplsLspLinkService == null) {
            logger.info("MplsLspLinkService not available; returning empty list for getTopologyLspData");
            return true;
        }
        return false;
    }

    private String trimFilter(String filter) {
        return filter != null ? filter.trim() : null;
    }

    private boolean isAggregateResponse(String trimmedFilter) {
        return trimmedFilter != null;
    }

    private boolean isFilterByLspName(String trimmedFilter, boolean aggregateResponse) {
        return aggregateResponse && !"LSP".equalsIgnoreCase(trimmedFilter);
    }

    private List<MplsLspLinkDto> fetchLspData(String timestamp, boolean filterByLspName, String trimmedFilter) {
        Instant filterTimestamp = parseTimestampFilter(timestamp);
        return mplsLspLinkService.getAllMplsLspLinks(
                filterTimestamp,
                filterByLspName ? trimmedFilter : null);
    }

    private List<MplsLspLinkDto> applyStartEndFilterIfNeeded(
            List<MplsLspLinkDto> list,
            boolean filterByLspName,
            String start,
            String end) {

        if (filterByLspName && hasStartOrEnd(start, end)) {
            String startNorm = normalizeFilter(start);
            String endNorm = normalizeFilter(end);

            return list.stream()
                    .filter(dto -> dtoMatchesStartEnd(dto, startNorm, endNorm))
                    .toList();
        }
        return list;
    }

    private Object handleAggregatedResponse(
            List<MplsLspLinkDto> list,
            String trimmedFilter,
            boolean filterByLspName,
            String start,
            String end) {

        Map<String, Object> aggregated = buildAggregatedResponse(list, trimmedFilter, filterByLspName);

        if (!filterByLspName && hasStartOrEnd(start, end)) {
            return filterAggregatedResponseByStartEnd(
                    aggregated,
                    normalizeFilter(start),
                    normalizeFilter(end));
        }
        return aggregated;
    }

    private boolean hasStartOrEnd(String start, String end) {
        return (start != null && !start.isBlank()) || (end != null && !end.isBlank());
    }

    /*
     * 
     * @param start optional; include only links whose start contains this value
     * 
     * @param end optional; include only links whose end contains this value
     * 
     * @param lspName optional; include only links whose lspName starts with this
     * value
     * 
     * @param lLimit optional; lower limit (default 0)
     * 
     * @param uLimit optional; upper limit (default 25)
     * 
     * @param orderBy optional; order by key
     * 
     * @param orderType optional; order type (asc or desc)
     * 
     * @return map with "links" only (same as getTopologyDetail edge response), or
     * "message" when LSP config data is unavailable
     */
    @SuppressWarnings({ "java:S107", "unchecked" })
    @Override
    public Object getLspData(String start, String end, String lspName, Integer lLimit,
            Integer uLimit, String orderBy, String orderType, String metaInfoFilters) {

        Map<String, String> validationError = validateInputs(lLimit, uLimit);
        if (validationError != null && !validationError.isEmpty()) {
            return validationError;
        }

        Object full = getTopologyLspData("LSP", null, null);
        Map<String, Object> fullMap = validateTopologyResponse(full);
        if (fullMap != null && fullMap.containsKey(AffectedNodesConstants.MESSAGE)) {
            return fullMap;
        }
        if (!(full instanceof Map)) {
            return emptyLinks();
        }

        Object linksObj = ((Map<String, Object>) full).get(AffectedNodesConstants.LINKS);

        int fromIndex = lLimit != null ? Math.max(0, lLimit) : 0;
        int pageSize;
        if (uLimit != null) {
            // uLimit is inclusive (last 0-based index to include), so pageSize = count of
            // indices [fromIndex..uLimit]
            pageSize = uLimit - fromIndex + 1;
        } else {
            pageSize = AffectedNodesConstants.TWENTYFIVE;
        }

        if (pageSize <= 0) {
            return emptyLinks();
        }

        String startNorm = normalizeFilter(start);
        String endNorm = normalizeFilter(end);
        String lspNameNorm = normalizeFilter(lspName);
        List<MetaInfoComparisonFilter> parsedMetaFilters = parseMetaInfoFilters(metaInfoFilters);

        List<Map<String, Object>> pagedLinks;
        if (orderBy == null || orderBy.isBlank()) {
            // Preserve old behavior (no sorting, streaming pagination) unless orderBy is
            // provided.
            pagedLinks = filterAndPaginateLinks(linksObj, startNorm, endNorm, lspNameNorm, parsedMetaFilters,
                    fromIndex, pageSize);
        } else {
            pagedLinks = filterSortAndPaginateLinks(linksObj, startNorm, endNorm, lspNameNorm, parsedMetaFilters,
                    fromIndex, pageSize, orderBy, orderType);
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put(AffectedNodesConstants.LINKS, pagedLinks);
        return result;
    }

    private Map<String, String> validateInputs(Integer lLimit, Integer uLimit) {

        if (lLimit != null && lLimit < 0) {
            return error("Lower limit (lLimit) cannot be negative");
        }

        if (uLimit != null && uLimit < 0) {
            return error("Upper limit (uLimit) cannot be negative");
        }

        if (lLimit != null && uLimit != null && lLimit > uLimit) {
            return error("Lower limit (lLimit) cannot be greater than upper limit (uLimit)");
        }

        return Collections.emptyMap();
    }

    private Map<String, String> error(String message) {
        Map<String, String> response = new LinkedHashMap<>();
        response.put(AffectedNodesConstants.MESSAGE, message);
        return response;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> validateTopologyResponse(Object full) {

        if (!(full instanceof Map)) {
            return emptyLinks();
        }

        Map<String, Object> fullMap = (Map<String, Object>) full;

        if (fullMap.containsKey(AffectedNodesConstants.MESSAGE)) {
            return fullMap;
        }

        return Collections.emptyMap();
    }

    private Map<String, Object> emptyLinks() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put(AffectedNodesConstants.LINKS, new ArrayList<>());
        return result;
    }

    private List<Map<String, Object>> filterAndPaginateLinks(
            Object linksObj,
            String startNorm,
            String endNorm,
            String lspNameNorm,
            List<MetaInfoComparisonFilter> metaFilters,
            int fromIndex,
            int pageSize) {

        List<Map<String, Object>> pagedLinks = new ArrayList<>(
                Math.min(pageSize, AffectedNodesConstants.TWO_HUNDRED_FIFTY_SIX));

        if (!(linksObj instanceof List)) {
            return pagedLinks;
        }

        List<?> rawList = (List<?>) linksObj;
        int matchedCount = 0;

        for (Object item : rawList) {

            Map<String, Object> link = extractLink(item);

            if (link != null && matchesLinkFilterWithNormalized(link, startNorm, endNorm, lspNameNorm, metaFilters)) {

                if (shouldAddLink(matchedCount, fromIndex, pagedLinks, pageSize, link)) {
                    break;
                }

                matchedCount++;
            }
        }

        return pagedLinks;
    }

    /**
     * Same filtering semantics as {@link #filterAndPaginateLinks} but additionally
     * sorts
     * the matched links by the requested orderBy key before applying pagination.
     * Activated only when orderBy is provided, to avoid impacting old logic.
     */
    @SuppressWarnings({ "java:S107" })
    private List<Map<String, Object>> filterSortAndPaginateLinks(
            Object linksObj,
            String startNorm,
            String endNorm,
            String lspNameNorm,
            List<MetaInfoComparisonFilter> metaFilters,
            int fromIndex,
            int pageSize,
            String orderBy,
            String orderType) {

        if (!(linksObj instanceof List)) {
            return new ArrayList<>();
        }

        List<?> rawList = (List<?>) linksObj;
        List<Map<String, Object>> matched = new ArrayList<>();

        for (Object item : rawList) {
            Map<String, Object> link = extractLink(item);
            if (link != null && matchesLinkFilterWithNormalized(link, startNorm, endNorm, lspNameNorm, metaFilters)) {
                matched.add(link);
            }
        }

        Comparator<Map<String, Object>> comparator = buildLinkComparator(orderBy, orderType);
        if (comparator != null) {
            matched.sort(comparator);
        }

        if (fromIndex >= matched.size() || pageSize <= 0) {
            return new ArrayList<>();
        }

        int toExclusive = Math.min(matched.size(), fromIndex + pageSize);
        return new ArrayList<>(matched.subList(fromIndex, toExclusive));
    }

    private Comparator<Map<String, Object>> buildLinkComparator(String orderBy, String orderType) {
        if (orderBy == null || orderBy.isBlank()) {
            return null;
        }

        boolean desc = orderType != null && "desc".equalsIgnoreCase(orderType.trim());
        String key = orderBy.trim();

        return (a, b) -> {
            Object av = getOrderValue(a, key);
            Object bv = getOrderValue(b, key);

            int cmp = compareOrderValues(av, bv);
            if (cmp == 0) {
                // stable-ish tie-breaker so pagination is deterministic
                cmp = compareOrderValues(getOrderValue(a, AffectedNodesConstants.START),
                        getOrderValue(b, AffectedNodesConstants.START));
                if (cmp == 0) {
                    cmp = compareOrderValues(getOrderValue(a, AffectedNodesConstants.END),
                            getOrderValue(b, AffectedNodesConstants.END));
                }
            }
            return desc ? -cmp : cmp;
        };
    }

    @SuppressWarnings("unchecked")
    private Object getOrderValue(Map<String, Object> link, String orderBy) {
        if (link == null || orderBy == null || orderBy.isBlank()) {
            return null;
        }

        String key = orderBy;
        String metaKey = null;

        int dotIdx = orderBy.indexOf('.');
        if (dotIdx > 0) {
            String prefix = orderBy.substring(0, dotIdx);
            if (AffectedNodesConstants.META_INFO.equals(prefix)) {
                metaKey = orderBy.substring(dotIdx + 1);
            }
        }

        if (metaKey != null) {
            Object meta = link.get(AffectedNodesConstants.META_INFO);
            if (meta instanceof Map) {
                return ((Map<String, Object>) meta).get(metaKey);
            }
            return null;
        }

        if (link.containsKey(key)) {
            return link.get(key);
        }

        // Convenience: allow ordering by metaInfo fields directly, e.g.
        // orderBy=mplsLspAge
        Object meta = link.get(AffectedNodesConstants.META_INFO);
        if (meta instanceof Map) {
            Map<String, Object> metaMap = (Map<String, Object>) meta;
            if (metaMap.containsKey(key)) {
                return metaMap.get(key);
            }
        }

        return null;
    }

    private int compareOrderValues(Object a, Object b) {
        if (a == b) {
            return 0;
        }
        if (a == null) {
            return 1; // nulls last
        }
        if (b == null) {
            return -1;
        }

        if (a instanceof Number an && b instanceof Number bn) {
            return Double.compare(an.doubleValue(), bn.doubleValue());
        }

        Double an = tryParseDouble(a);
        Double bn = tryParseDouble(b);
        if (an != null && bn != null) {
            return Double.compare(an, bn);
        }

        return a.toString().compareToIgnoreCase(b.toString());
    }

    private Double tryParseDouble(Object v) {
        if (v == null) {
            return null;
        }
        if (v instanceof Number number) {
            return number.doubleValue();
        }
        try {
            String s = v.toString().trim();
            if (s.isEmpty()) {
                return null;
            }
            return Double.parseDouble(s);
        } catch (Exception e) {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> extractLink(Object item) {
        return (item instanceof Map) ? (Map<String, Object>) item : null;
    }

    private boolean shouldAddLink(int matchedCount, int fromIndex,
            List<Map<String, Object>> pagedLinks,
            int pageSize,
            Map<String, Object> link) {

        if (matchedCount >= fromIndex) {
            pagedLinks.add(link);
            return pagedLinks.size() >= pageSize;
        }
        return false;
    }

    /*
     * 
     * @param start optional; include only links whose start contains this value
     * 
     * @param end optional; include only links whose end contains this value
     * 
     * @param lspName optional; include only links whose lspName starts with this
     * value
     * 
     * @return count of matching links as plain text (e.g. "20")
     */
    @Override
    public String getLspDataCount(String start, String end, String lspName, String metaInfoFilters) {
        Object full = getTopologyLspData("LSP", null, null);
        if (!(full instanceof Map)) {
            return "0";
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> fullMap = (Map<String, Object>) full;
        if (fullMap.containsKey(AffectedNodesConstants.MESSAGE)) {
            return "0";
        }
        Object linksObj = fullMap.get(AffectedNodesConstants.LINKS);
        String startNorm = normalizeFilter(start);
        String endNorm = normalizeFilter(end);
        String lspNameNorm = normalizeFilter(lspName);
        List<MetaInfoComparisonFilter> parsedMetaFilters = parseMetaInfoFilters(metaInfoFilters);
        int count = 0;
        if (linksObj instanceof List) {
            for (Object item : (List<?>) linksObj) {
                if (!(item instanceof Map)) {
                    continue;
                }
                @SuppressWarnings("unchecked")
                Map<String, Object> link = (Map<String, Object>) item;
                if (matchesLinkFilterWithNormalized(link, startNorm, endNorm, lspNameNorm, parsedMetaFilters)) {
                    count++;
                }
            }
        }
        return String.valueOf(count);
    }

    @Override
    public Object getLspStatus(Map<String, Object> payload) {
        Map<String, Object> response = new LinkedHashMap<>();
        String entityName = extractEntityName(payload);
        if (entityName == null || entityName.isBlank()) {
            response.put(AffectedNodesConstants.MESSAGE, "entityName is required");
            return response;
        }

        int totalLsp = safeParseCount(getLspDataCount(entityName, null, null, null));
        int downLsp = fetchDownLspCount(entityName);
        int activeLsp = Math.max(totalLsp - downLsp, 0);

        response.put("totalLsp", totalLsp);
        response.put("downLsp", downLsp);
        response.put("activeLsp", activeLsp);
        return response;
    }

    @Override
    public List<Map<String, Object>> getLspFilterData(List<String> payload, String filter) {
    
        Set<String> filterSet = buildFilterSet(payload);
        if (filterSet.isEmpty()) {
            return emptyResult();
        }
    
        DirectionFilter directionFilter = parseDirectionFilter(filter);
    
        Map<String, Object> fullMap = fetchFullTopology();
        if (fullMap == null) {
            return emptyResult();
        }
    
        Object timestamp = fullMap.get(AffectedNodesConstants.TIMESTAMP);
    
        List<Map<String, Object>> matchedLinks =
                filterLinks(fullMap.get(AffectedNodesConstants.LINKS), filterSet, directionFilter);
    
        Set<String> matchedNodeNames = collectNodeNames(matchedLinks);
    
        List<Map<String, Object>> matchedNodes =
                filterNodes(fullMap.get(AffectedNodesConstants.NODES), matchedNodeNames);
    
        return List.of(buildLinksNodesResult(matchedLinks, matchedNodes, timestamp));
    }

    private Set<String> buildFilterSet(List<String> payload) {
        if (payload == null || payload.isEmpty()) {
            logger.info("Empty payload provided to getLspFilterData");
            return Set.of();
        }
    
        Set<String> filterSet = new HashSet<>();
        for (String name : payload) {
            if (name != null && !name.isBlank()) {
                filterSet.add(name.trim().toLowerCase());
            }
        }
        return filterSet;
    }
    
    @SuppressWarnings("unchecked")
    private Map<String, Object> fetchFullTopology() {
        Object full = getTopologyLspData("LSP", null, null);
        if (!(full instanceof Map<?, ?>)) {
            return Collections.emptyMap();
        }
        return (Map<String, Object>) full;
    }
    
    private List<Map<String, Object>> filterLinks(
            Object linksObj,
            Set<String> filterSet,
            DirectionFilter directionFilter) {
    
        List<Map<String, Object>> matchedLinks = new ArrayList<>();
    
        if (linksObj instanceof List<?>) {
            for (Object item : (List<?>) linksObj) {
                if (!(item instanceof Map<?, ?>)) {
                    continue;
                }
    
                Map<String, Object> link = (Map<String, Object>) item;
                String start = objectToLower(link.get(AffectedNodesConstants.START));
                String end = objectToLower(link.get(AffectedNodesConstants.END));
    
                if (matchesDirectionFilter(start, end, filterSet, directionFilter)) {
                    matchedLinks.add(link);
                }
            }
        }
        return matchedLinks;
    }
    
    private Set<String> collectNodeNames(List<Map<String, Object>> matchedLinks) {
        Set<String> names = new HashSet<>();
    
        for (Map<String, Object> link : matchedLinks) {
            String start = objectToLower(link.get(AffectedNodesConstants.START));
            String end = objectToLower(link.get(AffectedNodesConstants.END));
    
            if (start != null) names.add(start);
            if (end != null) names.add(end);
        }
        return names;
    }
    
    private List<Map<String, Object>> filterNodes(
            Object nodesObj,
            Set<String> matchedNodeNames) {
    
        List<Map<String, Object>> matchedNodes = new ArrayList<>();
    
        if (nodesObj instanceof List<?>) {
            for (Object item : (List<?>) nodesObj) {
                if (!(item instanceof Map<?, ?>)) {
                    continue;
                }
    
                Map<String, Object> node = (Map<String, Object>) item;
                String neName = objectToLower(node.get(AffectedNodesConstants.NENAME));
    
                if (neName != null && matchedNodeNames.contains(neName)) {
                    matchedNodes.add(node);
                }
            }
        }
        return matchedNodes;
    }
    
    private List<Map<String, Object>> emptyResult() {
        return List.of(buildLinksNodesResult(new ArrayList<>(), new ArrayList<>(), null));
    }

    private DirectionFilter parseDirectionFilter(String filter) {
        if (filter == null || filter.isBlank()) {
            return DirectionFilter.OUT;
        }
        String normalized = filter.trim().toUpperCase();
        return switch (normalized) {
            case "OUT" -> DirectionFilter.OUT;
            case "IN" -> DirectionFilter.IN;
            case "BOTH" -> DirectionFilter.BOTH;
            default -> DirectionFilter.OUT;
        };
    }

    private boolean matchesDirectionFilter(String start, String end, Set<String> filterSet, DirectionFilter direction) {
        boolean startMatch = start != null && filterSet.contains(start);
        boolean endMatch = end != null && filterSet.contains(end);
        return switch (direction) {
            case OUT -> startMatch;
            case IN -> endMatch;
            case BOTH -> startMatch || endMatch;
        };
    }

    private enum DirectionFilter {
        OUT, IN, BOTH
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<Map<String, Object>> getLspSearch(String region) {
        if (region == null || region.isBlank()) {
            logger.info("Empty region provided to getLspSearch");
            return List.of(buildLinksNodesResult(new ArrayList<>(), new ArrayList<>(), null));
        }

        String regionNorm = region.trim().toLowerCase();
        Object full = getTopologyLspData("LSP", null, null);
        if (!(full instanceof Map<?, ?>)) {
            return List.of(buildLinksNodesResult(new ArrayList<>(), new ArrayList<>(), null));
        }

        Map<String, Object> fullMap = (Map<String, Object>) full;
        Object nodesObj = fullMap.get(AffectedNodesConstants.NODES);
        List<String> regionMatchedNodeNames = new ArrayList<>();
        if (nodesObj instanceof List<?>) {
            for (Object item : (List<?>) nodesObj) {
                if (!(item instanceof Map<?, ?>)) {
                    continue;
                }
                Map<String, Object> node = (Map<String, Object>) item;
                Object neNameObj = node.get(AffectedNodesConstants.NENAME);
                String neName = objectToLower(neNameObj);
                String geographyL1Name = objectToLower(node.get(AffectedNodesConstants.GEO_L1_NAME));
                if (neName != null && geographyL1Name != null && geographyL1Name.contains(regionNorm)) {
                    regionMatchedNodeNames.add(neNameObj.toString());
                }
            }
        }

        if (regionMatchedNodeNames.isEmpty()) {
            return List.of(buildLinksNodesResult(new ArrayList<>(), new ArrayList<>(), null));
        }

        // Delegate final links/nodes assembly to getLspFilterData for consistent output.
        return getLspFilterData(regionMatchedNodeNames, "OUT");
    }

    private String objectToLower(Object value) {
        if (value == null) {
            return null;
        }
        String s = value.toString().trim();
        return s.isEmpty() ? null : s.toLowerCase();
    }

    private Map<String, Object> buildLinksNodesResult(List<Map<String, Object>> links, List<Map<String, Object>> nodes,
            Object timestamp) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put(AffectedNodesConstants.LINKS, links);
        result.put(AffectedNodesConstants.NODES, nodes);
        result.put(AffectedNodesConstants.TIMESTAMP, timestamp);
        return result;
    }

    private String extractEntityName(Map<String, Object> payload) {
        if (payload == null) {
            return null;
        }
        Object entityName = payload.get("entityName");
        return entityName != null ? entityName.toString().trim() : null;
    }

    private int fetchDownLspCount(String entityName) {
        try {
            String filter = buildLspDownFilter(entityName);
            if (filter == null) {
                logger.info("No LSP down alarm names configured for config key={}", LSP_DOWN_ALARM_CONFIG_KEY);
                return 0;
            }
            String token = getToken();
            logger.info("Token generated for user is {}", token);
            Long fmCount = alarmRest.getActiveAlarmsCount(filter);
            return safeParseCount(fmCount);
        } catch (Exception e) {
            logger.error("Failed to fetch down LSP count from FM for entityName={}", entityName, e);
            return 0;
        }
    }

    private String buildLspDownFilter(String entityName) {
        List<String> alarmNames = getLspDownAlarmNamesFromConfig();
        if (alarmNames.isEmpty()) {
            return null;
        }

        StringJoiner alarmNameJoiner = new StringJoiner(",");
        for (String alarmName : alarmNames) {
            alarmNameJoiner.add("alarmname=='" + escapeFilterValue(alarmName) + "'");
        }

        return "((" + alarmNameJoiner + ");entityName=='" + escapeFilterValue(entityName)
                + "';(alarmStatus=='OPEN',alarmStatus=='REOPEN'))";
    }

    private List<String> getLspDownAlarmNamesFromConfig() {
        try {
            Query query = entityManager.createNativeQuery(LSP_DOWN_ALARM_SQL);
            query.setParameter("configKey", LSP_DOWN_ALARM_CONFIG_KEY);
            Object result = query.getSingleResult();
            if (result == null) {
                return Collections.emptyList();
            }

            String configValue = result.toString().trim();
            if (configValue.isEmpty()) {
                return Collections.emptyList();
            }

            JsonNode rootNode = OBJECT_MAPPER.readTree(configValue);
            JsonNode lspAlarmsNode = rootNode.get(LSP_ALARMS_FIELD);
            if (lspAlarmsNode == null || !lspAlarmsNode.isArray()) {
                logger.info("Invalid LSP alarms config format for key={}, expected JSON with array field '{}'",
                        LSP_DOWN_ALARM_CONFIG_KEY, LSP_ALARMS_FIELD);
                return Collections.emptyList();
            }

            List<String> alarmNames = new ArrayList<>();
            for (JsonNode alarmNode : lspAlarmsNode) {
                if (alarmNode != null && alarmNode.isTextual()) {
                    String value = alarmNode.asText().trim();
                    if (!value.isEmpty()) {
                        alarmNames.add(value);
                    }
                }
            }

            if (alarmNames.isEmpty()) {
                logger.info("No valid alarm names found in '{}' for key={}", LSP_ALARMS_FIELD,
                        LSP_DOWN_ALARM_CONFIG_KEY);
            }
            return alarmNames;
        } catch (Exception e) {
            logger.error("Failed to fetch/parse LSP down alarm names from BASE_CONFIGURATION for key={}",
                    LSP_DOWN_ALARM_CONFIG_KEY, e);
            return Collections.emptyList();
        }
    }

    private String escapeFilterValue(String value) {
        return value == null ? "" : value.replace("'", "''");
    }

    private int safeParseCount(Object value) {
        if (value == null) {
            return 0;
        }
        try {
            if (value instanceof Number number) {
                return number.intValue();
            }
            String parsed = value.toString().trim();
            if (parsed.isEmpty()) {
                return 0;
            }
            return Integer.parseInt(parsed);
        } catch (Exception e) {
            logger.info("Unable to parse count value '{}'", value);
            return 0;
        }
    }

    /**
     * Returns trimmed lowercase string, or null if input is null or blank.
     * Used to normalize filter strings once before iterating links.
     */
    private static String normalizeFilter(String filter) {
        if (filter == null || filter.isBlank()) {
            return null;
        }
        return filter.trim().toLowerCase();
    }

    /**
     * Returns true if the DTO matches optional start/end filters (source and
     * destination).
     * startNorm matches source IP or first node's neName/hostname (contains,
     * case-insensitive).
     * endNorm matches destination IP or last node's neName/hostname (contains,
     * case-insensitive).
     */
    private boolean dtoMatchesStartEnd(
            MplsLspLinkDto dto,
            String startNorm,
            String endNorm) {

        if (startNorm != null) {
            String sourceIp = dto.getSourceIp();
            boolean sourceIpMatch = sourceIp != null && sourceIp.toLowerCase().contains(startNorm);
            boolean sourceNodeMatch = nodeDetailsContains(dto.getNodeDetails(), 0, startNorm);
            if (!sourceIpMatch && !sourceNodeMatch) {
                return false;
            }
        }
        if (endNorm != null) {
            String destIp = dto.getDestinationIp();
            boolean destIpMatch = destIp != null && destIp.toLowerCase().contains(endNorm);
            List<Map<String, Object>> nodes = dto.getNodeDetails();
            int lastIdx = nodes != null ? nodes.size() - 1 : -1;
            boolean destNodeMatch = nodeDetailsContains(dto.getNodeDetails(), lastIdx, endNorm);
            if (!destIpMatch && !destNodeMatch) {
                return false;
            }
        }
        return true;
    }

    private boolean nodeDetailsContains(List<Map<String, Object>> nodeDetails, int index, String normalized) {
        if (nodeDetails == null || index < 0 || index >= nodeDetails.size()) {
            return false;
        }
        Map<String, Object> node = nodeDetails.get(index);
        if (node == null) {
            return false;
        }
        for (String key : new String[] { "neName", "hostname", "neId", "ipv4" }) {
            Object val = node.get(key);
            if (val != null && val.toString().toLowerCase().contains(normalized)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Applies start/end/lspName filters using pre-normalized filter strings.
     * Call normalizeFilter() once per parameter before the loop to avoid repeated
     * trim/toLowerCase.
     */
    private boolean matchesLinkFilterWithNormalized(Map<String, Object> link, String startNorm, String endNorm,
            String lspNameNorm, List<MetaInfoComparisonFilter> metaFilters) {
        if (startNorm != null) {
            Object startVal = link.get(AffectedNodesConstants.START);
            if (startVal == null || !startVal.toString().toLowerCase().contains(startNorm)) {
                return false;
            }
        }
        if (endNorm != null) {
            Object endVal = link.get(AffectedNodesConstants.END);
            if (endVal == null || !endVal.toString().toLowerCase().contains(endNorm)) {
                return false;
            }
        }
        if (lspNameNorm != null) {
            Object lspVal = link.get(AffectedNodesConstants.LSP_NAME);
            if (lspVal == null || !lspVal.toString().trim().toLowerCase().startsWith(lspNameNorm)) {
                return false;
            }
        }
        return matchesMetaInfoFilters(link, metaFilters);
    }

    @SuppressWarnings("unchecked")
    private boolean matchesMetaInfoFilters(Map<String, Object> link, List<MetaInfoComparisonFilter> metaFilters) {
        if (metaFilters == null || metaFilters.isEmpty()) {
            return true;
        }
        Object metaObj = link.get(AffectedNodesConstants.META_INFO);
        if (!(metaObj instanceof Map<?, ?>)) {
            return false;
        }
        Map<String, Object> metaMap = (Map<String, Object>) metaObj;
        for (MetaInfoComparisonFilter filter : metaFilters) {
            if (!matchesSingleMetaFilter(metaMap, filter)) {
                return false;
            }
        }
        return true;
    }

    private boolean matchesSingleMetaFilter(Map<String, Object> metaMap, MetaInfoComparisonFilter filter) {
        Object currentValue = metaMap.get(filter.key());
        if (currentValue == null) {
            return false;
        }
        Double currentNum = tryParseDouble(currentValue);
        Double filterNum = tryParseDouble(filter.value());

        if (currentNum != null && filterNum != null) {
            return compareNumbers(currentNum, filterNum, filter.operator());
        }
        return compareStrings(currentValue.toString(), filter.value(), filter.operator());
    }

    private boolean compareNumbers(double left, double right, String operator) {
        return switch (operator) {
            case ">" -> left > right;
            case ">=" -> left >= right;
            case "<" -> left < right;
            case "<=" -> left <= right;
            case "!=" -> Double.compare(left, right) != 0;
            case "=" -> Double.compare(left, right) == 0;
            default -> false;
        };
    }

    private boolean compareStrings(String left, String right, String operator) {
        int cmp = left.compareToIgnoreCase(right);
        return switch (operator) {
            case "!=" -> cmp != 0;
            case "=" -> cmp == 0;
            case ">" -> cmp > 0;
            case ">=" -> cmp >= 0;
            case "<" -> cmp < 0;
            case "<=" -> cmp <= 0;
            default -> false;
        };
    }

    private List<MetaInfoComparisonFilter> parseMetaInfoFilters(String metaInfoFilters) {
        if (isBlank(metaInfoFilters)) {
            return Collections.emptyList();
        }

        List<MetaInfoComparisonFilter> filters = new ArrayList<>();
        String[] expressions = metaInfoFilters.split(",");

        for (String expr : expressions) {
            MetaInfoComparisonFilter filter = parseExpression(expr);
            if (filter != null) {
                filters.add(filter);
            }
        }

        return filters;
    }

    private MetaInfoComparisonFilter parseExpression(String expr) {
        if (isBlank(expr)) {
            return null;
        }

        Matcher matcher = META_INFO_FILTER_PATTERN.matcher(expr);
        if (matcher.matches()) {
            String key = matcher.group(1).trim();
            String operator = matcher.group(AffectedNodesConstants.TWO).trim();
            String value = matcher.group(AffectedNodesConstants.THREE).trim();
            return new MetaInfoComparisonFilter(key, operator, value);
        }

        logger.info("Ignoring invalid metaInfo filter expression: {}", expr);
        return null;
    }

    private boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    private record MetaInfoComparisonFilter(String key, String operator, String value) {
    }

    /**
     * Parses compact timestamp string yyyyMMddHHmm (e.g. 202603050730) to Instant
     * in UTC (e.g. 2026-03-05T07:30:00Z).
     * Returns null if input is null/blank or invalid.
     */
    private Instant parseTimestampFilter(String timestamp) {
        if (timestamp == null || timestamp.isBlank()) {
            return null;
        }
        try {
            return LocalDateTime.parse(timestamp.trim(), DateTimeFormatter.ofPattern("yyyyMMddHHmm"))
                    .atOffset(ZoneOffset.UTC)
                    .toInstant();
        } catch (Exception e) {
            logger.info("Invalid timestamp filter '{}', expected yyyyMMddHHmm: {}", timestamp, e.getMessage());
            return null;
        }
    }

    private Map<String, Object> buildAggregatedResponse(
            List<MplsLspLinkDto> list,
            String trimmedFilter,
            boolean filterByLspName) {

        Map<String, Map<String, Object>> nodesById = new LinkedHashMap<>();
        List<Map<String, Object>> allLinks = new ArrayList<>();
        Instant latestTimestamp = null;
        Map<String, Object> metaInfo = null;

        for (com.enttribe.module.topology.core.pm.cassandra.MplsLspLinkDto dto : list) {

            if (shouldSkip(dto, trimmedFilter, filterByLspName)) {
                continue;
            }

            latestTimestamp = updateLatestTimestamp(dto, latestTimestamp);
            if (dto.getMetaInfo() != null) {
                metaInfo = dto.getMetaInfo();
            }

            processLsp(dto, filterByLspName, nodesById, allLinks);
        }

        if (filterByLspName) {
            addAliasesToLspLinkDetails(allLinks);
        }

        return prepareAggregatedResponse(nodesById, allLinks, latestTimestamp, metaInfo, filterByLspName);
    }

    private Map<String, Object> filterAggregatedResponseByStartEnd(
            Map<String, Object> aggregated,
            String startNorm,
            String endNorm) {

        if (aggregated == null) {
            return new LinkedHashMap<>();
        }

        List<?> rawLinks = getListSafely(aggregated.get(AffectedNodesConstants.LINKS));
        List<?> rawNodes = getListSafely(aggregated.get(AffectedNodesConstants.NODES));

        if (rawLinks == null || rawNodes == null) {
            return aggregated;
        }

        Set<String> matchedNodeNames = new HashSet<>();
        List<Map<String, Object>> matchedLinks = extractMatchedLinks(rawLinks, startNorm, endNorm, matchedNodeNames);

        List<Map<String, Object>> matchedNodes = extractMatchedNodes(rawNodes, matchedNodeNames);

        return buildFilteredResponse(aggregated, matchedNodes, matchedLinks);
    }

    private List<?> getListSafely(Object obj) {
        return (obj instanceof List<?>) ? (List<?>) obj : null;
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> extractMatchedLinks(
            List<?> rawLinks,
            String startNorm,
            String endNorm,
            Set<String> matchedNodeNames) {

        List<Map<String, Object>> matchedLinks = new ArrayList<>();

        for (Object item : rawLinks) {
            if (item instanceof Map<?, ?>) {

                Map<String, Object> link = (Map<String, Object>) item;

                if (matchesLinkFilterWithNormalized(link, startNorm, endNorm, null, Collections.emptyList())) {
                    matchedLinks.add(link);
                    collectNodeNames(link, matchedNodeNames);
                }
            }
        }

        return matchedLinks;
    }

    private void collectNodeNames(
            Map<String, Object> link,
            Set<String> matchedNodeNames) {

        Object startVal = link.get(AffectedNodesConstants.START);
        Object endVal = link.get(AffectedNodesConstants.END);

        if (startVal != null) {
            matchedNodeNames.add(startVal.toString().toLowerCase());
        }
        if (endVal != null) {
            matchedNodeNames.add(endVal.toString().toLowerCase());
        }
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> extractMatchedNodes(
            List<?> rawNodes,
            Set<String> matchedNodeNames) {

        List<Map<String, Object>> matchedNodes = new ArrayList<>();

        for (Object nodeObj : rawNodes) {
            if (!(nodeObj instanceof Map<?, ?>)) {
                continue;
            }

            Map<String, Object> node = (Map<String, Object>) nodeObj;
            Object neNameVal = node.get(AffectedNodesConstants.NENAME);

            if (neNameVal != null &&
                    matchedNodeNames.contains(neNameVal.toString().toLowerCase())) {
                matchedNodes.add(node);
            }
        }

        return matchedNodes;
    }

    private Map<String, Object> buildFilteredResponse(
            Map<String, Object> aggregated,
            List<Map<String, Object>> matchedNodes,
            List<Map<String, Object>> matchedLinks) {

        Map<String, Object> filtered = new LinkedHashMap<>();
        filtered.put(AffectedNodesConstants.NODES, matchedNodes);
        filtered.put(AffectedNodesConstants.LINKS, matchedLinks);
        filtered.put(AffectedNodesConstants.TIMESTAMP,
                aggregated.get(AffectedNodesConstants.TIMESTAMP));

        return filtered;
    }

    private Instant updateLatestTimestamp(
            MplsLspLinkDto dto,
            Instant latestTimestamp) {

        if (dto.getTimestamp() != null &&
                (latestTimestamp == null || dto.getTimestamp().isAfter(latestTimestamp))) {
            return dto.getTimestamp();
        }
        return latestTimestamp;
    }

    private void processLsp(
            MplsLspLinkDto dto,
            boolean filterByLspName,
            Map<String, Map<String, Object>> nodesById,
            List<Map<String, Object>> allLinks) {

        if (filterByLspName) {
            processFilteredLsp(dto, nodesById, allLinks);
        } else {
            processGenericLsp(dto, nodesById, allLinks);
        }
    }

    private Map<String, Object> prepareAggregatedResponse(
            Map<String, Map<String, Object>> nodesById,
            List<Map<String, Object>> allLinks,
            Instant latestTimestamp,
            Map<String, Object> metaInfo,
            boolean filterByLspName) {

        Map<String, Object> aggregated = new LinkedHashMap<>();
        aggregated.put(AffectedNodesConstants.NODES, new ArrayList<>(nodesById.values()));

        if (filterByLspName) {
            for (Map<String, Object> linkMap : allLinks) {
                aggregated.putAll(linkMap);
            }
            Object metaInfoFields = extractMetaInfoFields(metaInfo);
            if (metaInfoFields != null) {
                aggregated.put(AffectedNodesConstants.META_INFO, metaInfoFields);
            }
        } else {
            aggregated.put(AffectedNodesConstants.LINKS, allLinks);
        }

        aggregated.put(AffectedNodesConstants.TIMESTAMP,
                latestTimestamp != null ? latestTimestamp.toString() : null);

        return aggregated;
    }

    /**
     * Extracts only the "fields" map from meta_info (metrics like mplsLspAge,
     * mplsPathBandwidth, etc.).
     * Excludes name, tags, and timestamp from the response.
     */
    private Object extractMetaInfoFields(Map<String, Object> metaInfo) {
        if (metaInfo == null) {
            return null;
        }
        Object fields = metaInfo.get("fields");
        return (fields instanceof Map) ? fields : null;
    }

    private boolean shouldSkip(
            MplsLspLinkDto dto,
            String trimmedFilter,
            boolean filterByLspName) {

        return filterByLspName && !trimmedFilter.equals(dto.getMplsLspName());
    }

    private void processFilteredLsp(
            MplsLspLinkDto dto,
            Map<String, Map<String, Object>> nodesById,
            List<Map<String, Object>> allLinks) {

        List<Map<String, Object>> nodeDetails = dto.getNodeDetails() != null
                ? dto.getNodeDetails()
                : Collections.emptyList();

        addNodes(nodesById, nodeDetails);

        if (dto.getLinkDetails() != null) {
            allLinks.addAll(dto.getLinkDetails());
        }
    }

    private void processGenericLsp(
            MplsLspLinkDto dto,
            Map<String, Map<String, Object>> nodesById,
            List<Map<String, Object>> allLinks) {

        String sourceIp = dto.getSourceIp();
        String destinationIp = dto.getDestinationIp();
        String lspName = dto.getMplsLspName();

        List<Map<String, Object>> filteredNodes = filterNodesBySourceAndDestination(
                dto.getNodeDetails(),
                sourceIp,
                destinationIp,
                lspName);

        addNodes(nodesById, filteredNodes);

        Object metaInfoFields = extractMetaInfoFields(dto.getMetaInfo());
        allLinks.addAll(buildLspLinksFromNodes(filteredNodes, lspName, metaInfoFields));
    }

    private void addNodes(
            Map<String, Map<String, Object>> nodesById,
            List<Map<String, Object>> nodes) {

        for (Map<String, Object> node : nodes) {
            Object neIdObj = node.get("neId");
            String neId = neIdObj != null ? neIdObj.toString() : null;

            if (neId != null) {
                nodesById.computeIfAbsent(neId, k -> node);
            }
        }
    }

    private List<Map<String, Object>> buildDefaultResponse(
            List<MplsLspLinkDto> list) {

        List<Map<String, Object>> result = new ArrayList<>();

        for (MplsLspLinkDto dto : list) {

            Map<String, Object> map = new LinkedHashMap<>();

            map.put("sourceIp", dto.getSourceIp());
            map.put("destinationIp", dto.getDestinationIp());
            map.put(AffectedNodesConstants.LSP_NAME, dto.getMplsLspName());
            map.put(AffectedNodesConstants.LINKS, dto.getLinkDetails());
            map.put(AffectedNodesConstants.NODES, dto.getNodeDetails());
            Object metaInfoFields = extractMetaInfoFields(dto.getMetaInfo());
            if (metaInfoFields != null) {
                map.put(AffectedNodesConstants.META_INFO, metaInfoFields);
            }
            map.put(AffectedNodesConstants.TIMESTAMP,
                    dto.getTimestamp() != null
                            ? dto.getTimestamp().toString()
                            : null);

            result.add(map);
        }

        return result;
    }

    private List<Map<String, Object>> filterNodesBySourceAndDestination(
            List<Map<String, Object>> nodes,
            String sourceIp,
            String destinationIp,
            String lspName) {

        if (nodes == null || nodes.isEmpty()) {
            return new ArrayList<>();
        }

        List<Map<String, Object>> out = new ArrayList<>(AffectedNodesConstants.INITIAL_CAPACITY);

        for (Map<String, Object> node : nodes) {
            String neId = getNeId(node);

            if (sourceIp.equals(neId) || destinationIp.equals(neId)) {
                Map<String, Object> nodeCopy = new LinkedHashMap<>(node);
                nodeCopy.put(AffectedNodesConstants.LSP_NAME, lspName);
                out.add(nodeCopy);
            }
        }

        out.sort((a, b) -> compareNodes(a, b, sourceIp));
        return out;
    }

    private int compareNodes(Map<String, Object> a, Map<String, Object> b, String sourceIp) {

        String aId = getNeId(a);
        String bId = getNeId(b);

        boolean aIsSource = sourceIp.equals(aId);
        boolean bIsSource = sourceIp.equals(bId);

        if (aIsSource && !bIsSource) {
            return -1;
        }

        if (!aIsSource && bIsSource) {
            return 1;
        }

        return 0;
    }

    private String getNeId(Map<String, Object> node) {
        Object neIdObj = node.get("neId");
        return neIdObj != null ? neIdObj.toString() : "";
    }

    /**
     * Builds graph-ready links from LSP filtered nodes. Each link references nodes
     * by neName only (start/end).
     * Does not duplicate full node data inside links.
     */
    private List<Map<String, Object>> buildLspLinksFromNodes(List<Map<String, Object>> nodes, String lspName,
            Object metaInfoFields) {
        List<Map<String, Object>> links = new ArrayList<>();
        if (nodes == null || nodes.size() < AffectedNodesConstants.INITIAL_CAPACITY) {
            return links;
        }
        String startNeName = nodes.get(0).get(AffectedNodesConstants.NENAME) != null
                ? nodes.get(0).get(AffectedNodesConstants.NENAME).toString()
                : null;
        String endNeName = nodes.get(1).get(AffectedNodesConstants.NENAME) != null
                ? nodes.get(1).get(AffectedNodesConstants.NENAME).toString()
                : null;
        if (startNeName != null && endNeName != null) {
            Map<String, Object> link = new LinkedHashMap<>();
            link.put(AffectedNodesConstants.START, startNeName);
            link.put("end", endNeName);
            link.put("relation", "LSP");
            link.put(AffectedNodesConstants.LSP_NAME, lspName);
            if (metaInfoFields instanceof Map && !((Map<?, ?>) metaInfoFields).isEmpty()) {
                link.put(AffectedNodesConstants.META_INFO, metaInfoFields);
            }
            links.add(link);
        }
        return links;
    }

    private static final String EXPLICIT_ROUTE_LINK_DETAILS = "explicitRouteLinkDetails";
    private static final String RECORD_ROUTE_LINK_DETAILS = "recordRouteLinkDetails";

    /**
     * Adds alias keys to link detail objects inside the links list.
     * Handles both flat link maps and nested structure (explicitRouteLinkDetails,
     * recordRouteLinkDetails).
     */
    private void addAliasesToLspLinkDetails(List<Map<String, Object>> allLinks) {
        if (allLinks == null) {
            return;
        }

        for (Map<String, Object> linkMap : allLinks) {

            processAliasList(linkMap.get(EXPLICIT_ROUTE_LINK_DETAILS));
            processAliasList(linkMap.get(RECORD_ROUTE_LINK_DETAILS));

            if (linkMap.containsKey("mplsLspName") || linkMap.containsKey("srcParentNeName")) {
                addLspLinkDetailAliases(linkMap);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void processAliasList(Object listObj) {
        if (listObj instanceof List) {
            for (Object item : (List<?>) listObj) {
                if (item instanceof Map) {
                    addLspLinkDetailAliases((Map<String, Object>) item);
                }
            }
        }
    }

    private void addLspLinkDetailAliases(Map<String, Object> map) {
        putAlias(map, "mplsLspName", AffectedNodesConstants.LSP_NAME);
        putAlias(map, "srcNeId", "startNeId");
        putAlias(map, "destNeId", "endNeId");
        putAlias(map, "srcNeName", "srcInterface");
        putAlias(map, "destNeName", "destInterface");
        putAlias(map, "srcParentNeName", AffectedNodesConstants.START);
        putAlias(map, "destParentNeName", "end");
        putAlias(map, "srcCktId", "cktId");
        putAlias(map, "srcNeType", "srcType");
        putAlias(map, "destNeType", "destType");
        putAlias(map, "destNeCategory", "destCategory");
        putAlias(map, "srcNeCategory", "srcCategory");
    }

    private void putAlias(Map<String, Object> map, String sourceKey, String aliasKey) {
        if (map.containsKey(sourceKey)) {
            map.put(aliasKey, map.get(sourceKey));
            map.remove(sourceKey);
        }
    }
    /**
     * Generates a token for the user.
     *
     * <p>This method generates a token for the user based on the system user ID, system user name, system user group, and system customer ID.</p>
     *
     * @return the token for the user
     */
    public String getToken() {
        String token = null;
        String systemUserId = null;
        String systemUserName = null;
        String systemUserGroup = null;
        Integer systemCustomerId = null;
        String customerUUID = null;
        String newCustomerUUID = null;
        try {
            systemUserId = ConfigUtils.getPlainString("SYSTEM_USER_ID");
            systemUserName = ConfigUtils.getPlainString("SYSTEM_USER_NAME");
            systemUserGroup = ConfigUtils.getPlainString("SYSTEM_USER_GROUP");
            systemCustomerId = Integer.parseInt(ConfigUtils.getPlainString("SYSTEM_CUSTOMER_ID"));
            customerUUID = ConfigUtils.getPlainString("CUSTOMER_UUID");
            newCustomerUUID = customerInfo.getCustomerWrapper().getCustomerUUID();
            logger.info("newCustomerUUID is {}", newCustomerUUID);
            Integer userId = customerInfo.getUserId();
            String userName = customerInfo.getUsername();
            token = CustomTokenUtils.generateCustomToken(
                userId + "",userName, systemUserGroup, systemCustomerId, newCustomerUUID);
            logger.info("Token generated for user {} is {} with newCustomerUUID {} and systemUserId {} and systemUserName {} and systemUserGroup {} and systemCustomerId {} and customerUUID {}", userName, token, newCustomerUUID, systemUserId, systemUserName, systemUserGroup, systemCustomerId, customerUUID);
            if (token == null) {
                logger.info("Token is null for user {}, generating token for system user {}", userName, systemUserName);
                token = CustomTokenUtils.generateCustomToken(systemUserId, systemUserName, systemUserGroup,
                        systemCustomerId, customerUUID);
                    logger.info("Token generated for system user {} is {}", systemUserName, token);
            }
        } catch (Exception e) {
            logger.error("Exception in generating custom token {}", e.getMessage());
            token = CustomTokenUtils.generateCustomToken(systemUserId, systemUserName, systemUserGroup,
                    systemCustomerId, customerUUID);
        }
        return token;
    }
}
