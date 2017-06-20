package hadoopRPC.MySeriaData;

import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.util.Records;

import java.io.Serializable;
import java.util.List;

/**
 * Created by ubuntu2 on 6/20/17.
 */
public class MyAllocateResponse implements Serializable{
    public static AllocateResponse newInstance(int responseId,
                                               List<ContainerStatus> completedContainers,
                                               List<Container> allocatedContainers, List<NodeReport> updatedNodes,
                                               Resource availResources, AMCommand command, int numClusterNodes,
                                               PreemptionMessage preempt, List<NMToken> nmTokens) {
        AllocateResponse response = Records.newRecord(AllocateResponse.class);
        response.setNumClusterNodes(numClusterNodes);
        response.setResponseId(responseId);
        response.setCompletedContainersStatuses(completedContainers);
        response.setAllocatedContainers(allocatedContainers);
        response.setUpdatedNodes(updatedNodes);
        response.setAvailableResources(availResources);
        response.setAMCommand(command);
        response.setPreemptionMessage(preempt);
        response.setNMTokens(nmTokens);
        return response;
    }
    
}
