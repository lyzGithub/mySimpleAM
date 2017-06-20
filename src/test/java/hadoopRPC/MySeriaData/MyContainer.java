package hadoopRPC.MySeriaData;

import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.util.Records;

import java.io.Serializable;

/**
 * Created by ubuntu2 on 6/20/17.
 */
public class MyContainer implements Serializable {
    public static Container newInstance(ContainerId containerId, NodeId nodeId,
                                        String nodeHttpAddress, Resource resource, Priority priority,
                                        Token containerToken) {
        Container container = Records.newRecord(Container.class);
        container.setId(containerId);
        container.setNodeId(nodeId);
        container.setNodeHttpAddress(nodeHttpAddress);
        container.setResource(resource);
        container.setPriority(priority);
        container.setContainerToken(containerToken);
        return container;
    }
}
