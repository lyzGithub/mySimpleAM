package hadoopRPC.MySeriaData;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.util.Records;

import java.io.Serializable;

/**
 * Created by ubuntu2 on 6/20/17.
 */
public class MyContainerStatus implements Serializable {
    public static ContainerStatus newInstance(ContainerId containerId,
                                              ContainerState containerState, String diagnostics, int exitStatus) {
        ContainerStatus containerStatus = Records.newRecord(ContainerStatus.class);
        containerStatus.setState(containerState);
        containerStatus.setContainerId(containerId);
        containerStatus.setDiagnostics(diagnostics);
        containerStatus.setExitStatus(exitStatus);
        return containerStatus;
    }
}
