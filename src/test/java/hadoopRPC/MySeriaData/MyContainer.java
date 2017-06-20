package hadoopRPC.MySeriaData;

import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.util.Records;

import java.io.Serializable;

/**
 * Created by ubuntu2 on 6/20/17.
 */
public class MyContainer implements Serializable {

    private MyContainerID myContainerID;
    private MyNodeId myNodeId;
    private String nodeHttpAddress;
    private MyResource myResource;
    private int priority;
    private MyToken containerToken;
    public static MyContainer newInstance(ContainerId containerId, NodeId nodeId,
                                        String nodeHttpAddress, Resource resource, Priority priority,
                                        Token containerToken) {
        MyContainer container = new MyContainer();
        container.setId(containerId);
        container.setNodeId(nodeId);
        container.setNodeHttpAddress(nodeHttpAddress);
        container.setResource(resource);
        container.setPriority(priority);
        container.setContainerToken(containerToken);
        return container;
    }

    public  MyContainerID getId(){
        return this.myContainerID;
    }

    public  void setId(ContainerId id){
        this.myContainerID = new MyContainerID(id);
    }

    public  MyNodeId getNodeId(){
        return this.myNodeId;
    }

    public  void setNodeId(NodeId nodeId){
        this.myNodeId = MyNodeId.newInstance(nodeId.getHost(),nodeId.getPort());
    }

    public  String getNodeHttpAddress(){
        return this.nodeHttpAddress;
    }

    public  void setNodeHttpAddress(String nodeHttpAddress){
        this.nodeHttpAddress = nodeHttpAddress;
    }


    public  MyResource getResource(){
        return this.myResource;
    }

    public  void setResource(Resource resource){
        this.myResource = MyResource.newInstance(resource.getMemory(),resource.getVirtualCores());
    }

    public  Priority getPriority(){
        return Priority.newInstance(this.priority);
    }

    public  void setPriority(Priority priority){
        this.priority = priority.getPriority();
    }


    public  MyToken getContainerToken(){
        return this.containerToken;
    }

    public  void setContainerToken(Token containerToken){
        this.containerToken = MyToken.newInstance(
                containerToken.getIdentifier().array(),
                containerToken.getKind(),
                containerToken.getPassword().array(),
                containerToken.getService());
    }

    public Container tansBack(){
        return Container.newInstance(
                this.myContainerID.tansBack(),
                this.myNodeId.tansBack(),
                this.nodeHttpAddress,
                this.getResource().tansBack(),
                this.getPriority(),
                this.containerToken.tansBack());
    }

}
