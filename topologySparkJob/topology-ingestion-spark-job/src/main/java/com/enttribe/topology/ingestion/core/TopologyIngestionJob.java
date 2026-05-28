package com.enttribe.topology.ingestion.core;

import com.enttribe.topology.ingestion.utils.ConfigLoader;
import com.enttribe.topology.ingestion.utils.EdgeWriter;
import com.enttribe.topology.ingestion.utils.GraphInitializer;
import com.enttribe.topology.ingestion.utils.JdbcStageRunner;
import com.enttribe.topology.ingestion.utils.VertexWriter;
import org.apache.spark.sql.SparkSession;
import org.janusgraph.core.JanusGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Orchestrates the topology ingestion pipeline. For each enabled stage:
 * <ol>
 *   <li>Run the inventory SQL query against MySQL.</li>
 *   <li>Phase 1 — write vertices to JanusGraph (composite key {@code neName|relation},
 *       update-or-create, batched).</li>
 *   <li>Phase 2 — write edges where {@code neName}, {@code neighbourNEName} and
 *       {@code relation} are all non-empty.</li>
 * </ol>
 *
 * <p>Stages run sequentially in the same order as the original NiFi processors:
 * LLDP/OSPF → ISIS → SWITCH. The whole job uses a single JanusGraph instance
 * that lives for the JVM lifetime and is closed in a {@code finally}.
 */
public final class TopologyIngestionJob {

    private static final Logger logger = LoggerFactory.getLogger(TopologyIngestionJob.class);

    private TopologyIngestionJob() {
    }

    public static boolean startTopologyIngestionJob(SparkSession sparkSession, String domain) {
        if (sparkSession == null) {
            throw new IllegalArgumentException("SparkSession cannot be null");
        }

        logger.info("=".repeat(70));
        logger.info("    TOPOLOGY INGESTION JOB - STARTING");
        logger.info("    Domain: {}", domain);
        logger.info("=".repeat(70));

        long jobStartTime = System.currentTimeMillis();
        boolean stopOnFailure = ConfigLoader.getBoolean("INGESTION_STOP_ON_STAGE_FAILURE", false);

        JanusGraph graph = null;
        try {
            graph = GraphInitializer.open();
            VertexWriter vertexWriter = new VertexWriter(graph);
            EdgeWriter edgeWriter = new EdgeWriter(graph);
            JdbcStageRunner runner = new JdbcStageRunner(vertexWriter, edgeWriter);

            List<StageDef> stages = buildStageList();
            List<StageOutcome> outcomes = new ArrayList<>(stages.size());
            boolean overallSuccess = true;

            for (StageDef s : stages) {
                if (!s.enabled) {
                    logger.info("[{}] skipped (disabled via {})", s.name, s.toggleKey);
                    continue;
                }
                JdbcStageRunner.StageResult r = runner.run(s.name, s.query);
                outcomes.add(new StageOutcome(s.name, r));
                if (!r.success) {
                    overallSuccess = false;
                    if (stopOnFailure) {
                        logger.error("[{}] failed and INGESTION_STOP_ON_STAGE_FAILURE=true — aborting", s.name);
                        break;
                    }
                }
            }

            long elapsed = System.currentTimeMillis() - jobStartTime;
            logSummary(outcomes, overallSuccess, elapsed);
            return overallSuccess;
        } catch (Exception e) {
            logger.error("Topology ingestion job failed: {}", e.getMessage(), e);
            return false;
        } finally {
            GraphInitializer.close(graph);
        }
    }

    private static List<StageDef> buildStageList() {
        List<StageDef> stages = new ArrayList<>();

        // LLDP / OSPF
        stages.add(new StageDef(
                "lldpOspf.parentVertexForAll",
                "STAGE_LLDP_OSPF_PARENT_VERTEX",
                TopologyQueries.lldpOspfParentVertex()));
        stages.add(new StageDef(
                "lldpOspf.neighbourInterfaceForLLDP",
                "STAGE_LLDP_OSPF_NEIGHBOUR_INTERFACE_LLDP",
                TopologyQueries.neighbourInterfaceForLldp()));
        stages.add(new StageDef(
                "lldpOspf.childInterfaceForLLDP",
                "STAGE_LLDP_OSPF_CHILD_INTERFACE_LLDP",
                TopologyQueries.childInterfaceForLldp()));
        stages.add(new StageDef(
                "lldpOspf.neighbourInterfaceForOSPF",
                "STAGE_LLDP_OSPF_NEIGHBOUR_INTERFACE_OSPF",
                TopologyQueries.neighbourInterfaceForOspf()));
        stages.add(new StageDef(
                "lldpOspf.childInterfaceForOSPF",
                "STAGE_LLDP_OSPF_CHILD_INTERFACE_OSPF",
                TopologyQueries.childInterfaceForOspf()));

        // ISIS
        stages.add(new StageDef(
                "isis.parentVertexForAll",
                "STAGE_ISIS_PARENT_VERTEX",
                TopologyQueries.isisParentVertex()));
        stages.add(new StageDef(
                "isis.neighbourInterfaceForISIS",
                "STAGE_ISIS_NEIGHBOUR_INTERFACE",
                TopologyQueries.neighbourInterfaceForIsis()));
        stages.add(new StageDef(
                "isis.childInterfaceForISIS",
                "STAGE_ISIS_CHILD_INTERFACE",
                TopologyQueries.childInterfaceForIsis()));

        // SWITCH
        stages.add(new StageDef(
                "switch.parentVertexForAll",
                "STAGE_SWITCH_PARENT_VERTEX",
                TopologyQueries.switchParentVertex()));
        stages.add(new StageDef(
                "switch.neighbourInterfaceForSWITCH",
                "STAGE_SWITCH_NEIGHBOUR_INTERFACE",
                TopologyQueries.neighbourInterfaceForSwitch()));
        stages.add(new StageDef(
                "switch.childInterfaceForSWITCH",
                "STAGE_SWITCH_CHILD_INTERFACE",
                TopologyQueries.childInterfaceForSwitch()));

        return stages;
    }

    private static void logSummary(List<StageOutcome> outcomes, boolean overallSuccess, long elapsedMs) {
        logger.info("=".repeat(70));
        logger.info("    TOPOLOGY INGESTION JOB - SUMMARY");
        logger.info("=".repeat(70));
        int totalVCreated = 0, totalVUpdated = 0, totalVSkipped = 0;
        int totalECreated = 0, totalEUpdated = 0, totalESkipped = 0;
        for (StageOutcome o : outcomes) {
            logger.info("  {} | success={} | rows={} | "
                            + "V[c={},u={},s={},fb={}] | E[c={},u={},s={},fb={}] | {} ms",
                    o.name, o.result.success, o.result.rows,
                    o.result.vCreated, o.result.vUpdated, o.result.vSkipped, o.result.vFailedBatches,
                    o.result.eCreated, o.result.eUpdated, o.result.eSkipped, o.result.eFailedBatches,
                    o.result.elapsedMs);
            totalVCreated += o.result.vCreated;
            totalVUpdated += o.result.vUpdated;
            totalVSkipped += o.result.vSkipped;
            totalECreated += o.result.eCreated;
            totalEUpdated += o.result.eUpdated;
            totalESkipped += o.result.eSkipped;
        }
        logger.info("    Totals: V[created={}, updated={}, skipped={}], E[created={}, updated={}, skipped={}]",
                totalVCreated, totalVUpdated, totalVSkipped, totalECreated, totalEUpdated, totalESkipped);
        logger.info("    Overall success: {} | Total elapsed: {} ms", overallSuccess, elapsedMs);
        logger.info("=".repeat(70));
    }

    private static final class StageDef {
        final String name;
        final String toggleKey;
        final String query;
        final boolean enabled;

        StageDef(String name, String toggleKey, String query) {
            this.name = name;
            this.toggleKey = toggleKey;
            this.query = query;
            this.enabled = ConfigLoader.getBoolean(toggleKey, true);
        }
    }

    private static final class StageOutcome {
        final String name;
        final JdbcStageRunner.StageResult result;

        StageOutcome(String name, JdbcStageRunner.StageResult result) {
            this.name = name;
            this.result = result;
        }
    }
}
