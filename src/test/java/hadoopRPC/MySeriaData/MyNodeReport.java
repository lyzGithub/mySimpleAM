package hadoopRPC.MySeriaData;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.Records;

import java.util.Set;

/**
 * Created by ubuntu2 on 6/20/17.
 */
public class MyNodeReport {
    @InterfaceAudience.Private
    @InterfaceStability.Unstable
    public static NodeReport newInstance(NodeId nodeId, NodeState nodeState,
                                         String httpAddress, String rackName, Resource used, Resource capability,
                                         int numContainers, String healthReport, long lastHealthReportTime) {
        return newInstance(nodeId, nodeState, httpAddress, rackName, used,
                capability, numContainers, healthReport, lastHealthReportTime, null);
    }

    @InterfaceAudience.Private
    @InterfaceStability.Unstable
    public static NodeReport newInstance(NodeId nodeId, NodeState nodeState,
                                         String httpAddress, String rackName, Resource used, Resource capability,
                                         int numContainers, String healthReport, long lastHealthReportTime,
                                         Set<String> nodeLabels) {
        NodeReport nodeReport = Records.newRecord(NodeReport.class);
        nodeReport.setNodeId(nodeId);
        nodeReport.setNodeState(nodeState);
        nodeReport.setHttpAddress(httpAddress);
        nodeReport.setRackName(rackName);
        nodeReport.setUsed(used);
        nodeReport.setCapability(capability);
        nodeReport.setNumContainers(numContainers);
        nodeReport.setHealthReport(healthReport);
        nodeReport.setLastHealthReportTime(lastHealthReportTime);
        nodeReport.setNodeLabels(nodeLabels);
        return nodeReport;
    }
}
