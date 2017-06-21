package com.application.MySeriaData;

import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;

import java.io.Serializable;
import java.util.Set;

/**
 * Created by ubuntu2 on 6/20/17.
 */
public class MyNodeReport implements Serializable{
    //public enum NodeState
    private String nodeState;
    private MyNodeId myNodeId;
    private String httpAddress;
    private String rackName;
    private MyResource usedRsource;
    private MyResource resourceCapability;
    private int numContainers;
    private String healthReport;
    private long lastHealthReportTime;
    private Set<String> nodeLabels;
    public static MyNodeReport newInstance(NodeId nodeId, NodeState nodeState,
                                         String httpAddress, String rackName, Resource used, Resource capability,
                                         int numContainers, String healthReport, long lastHealthReportTime) {
        return newInstance(nodeId, nodeState, httpAddress, rackName, used,
                capability, numContainers, healthReport, lastHealthReportTime, null);
    }


    public static MyNodeReport newInstance(NodeId nodeId, NodeState nodeState,
                                         String httpAddress, String rackName, Resource used, Resource capability,
                                         int numContainers, String healthReport, long lastHealthReportTime,
                                         Set<String> nodeLabels) {
        MyNodeReport nodeReport = new MyNodeReport();
        nodeReport.setNodeId(nodeId);
        nodeReport.setNodeState(nodeState);
        nodeReport.setHttpAddress(httpAddress);
        nodeReport.setRackName(rackName);
        nodeReport.setUsedResource(used);
        nodeReport.setResourceCapability(capability);
        nodeReport.setNumContainers(numContainers);
        nodeReport.setHealthReport(healthReport);
        nodeReport.setLastHealthReportTime(lastHealthReportTime);
        nodeReport.setNodeLabels(nodeLabels);
        return nodeReport;
    }
    
    public  NodeId getNodeId(){
        return this.myNodeId.tansBack();
    }

    public  void setNodeId(NodeId nodeId){
        this.myNodeId = MyNodeId.newInstance(nodeId.getHost(),nodeId.getPort());
    }

    public  NodeState getNodeState(){
       return   (this.nodeState == null)?null:NodeState.valueOf(this.nodeState);
    }

   
    public  void setNodeState(NodeState nodeState){

        this.nodeState = (nodeState == null)?null:nodeState.toString();
    }

    public  String getHttpAddress(){
        return this.httpAddress;
    }

    
    public  void setHttpAddress(String httpAddress){
        this.httpAddress = httpAddress;
    }

   
    public  String getRackName(){
        return this.rackName;
    }

    public  void setRackName(String rackName){
        this.rackName = rackName;
    }

   
    public  Resource getUsedResource(){
        return this.usedRsource.tansBack();
    }

    public  void setUsedResource(Resource used){
        this.usedRsource = MyResource.newInstance(used.getMemory(),used.getVirtualCores());
    }

   
    public  Resource getResourceCapability(){
        return this.resourceCapability.tansBack();
    }

    public  void setResourceCapability(Resource capability){
        this.resourceCapability = MyResource.newInstance(capability.getMemory(),capability.getVirtualCores());
    }

   
    public  int getNumContainers(){
        return this.numContainers;
    }

    public  void setNumContainers(int numContainers){
        this.numContainers = numContainers;

    }


    
    public  String getHealthReport(){
        return this.healthReport;
    }

    public  void setHealthReport(String healthReport){
        this.healthReport = healthReport;
    }

   
    public  long getLastHealthReportTime(){
        return this.lastHealthReportTime;
    }

    public  void setLastHealthReportTime(long lastHealthReport){
        this.lastHealthReportTime = lastHealthReport;
    }

    public  Set<String> getNodeLabels(){
        return this.nodeLabels;
    }

    public  void setNodeLabels(Set<String> nodeLabels){
        this.nodeLabels = nodeLabels;
    }

    public NodeReport tansBack(){
        return NodeReport.newInstance(
                this.myNodeId.tansBack(),
                NodeState.valueOf(this.nodeState),
                this.httpAddress,
                this.rackName,
                this.usedRsource.tansBack(),
                this.resourceCapability.tansBack(),
                this.numContainers,
                this.healthReport,
                this.lastHealthReportTime,
                this.nodeLabels
        );
    }

}
