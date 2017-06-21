package hadoopRPC.MySeriaData;

import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.util.Records;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by ubuntu2 on 6/20/17.
 */
public class MyAllocateResponse implements Serializable{
    public static MyAllocateResponse newInstance(int responseId,
                                               List<ContainerStatus> completedContainers,
                                               List<Container> allocatedContainers, List<NodeReport> updatedNodes,
                                               Resource availResources, AMCommand command, int numClusterNodes,
                                               PreemptionMessage preempt, List<NMToken> nmTokens) {
        MyAllocateResponse response = new MyAllocateResponse();
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

    public static MyAllocateResponse newInstance(int responseId,
                                               List<ContainerStatus> completedContainers,
                                               List<Container> allocatedContainers, List<NodeReport> updatedNodes,
                                               Resource availResources, AMCommand command, int numClusterNodes,
                                               PreemptionMessage preempt, List<NMToken> nmTokens,
                                               List<ContainerResourceIncrease> increasedContainers,
                                               List<ContainerResourceDecrease> decreasedContainers) {
        MyAllocateResponse response = newInstance(responseId, completedContainers,
                allocatedContainers, updatedNodes, availResources, command,
                numClusterNodes, preempt, nmTokens);
        response.setIncreasedContainers(increasedContainers);
        response.setDecreasedContainers(decreasedContainers);
        return response;
    }


    public static MyAllocateResponse newInstance(int responseId,
                                               List<ContainerStatus> completedContainers,
                                               List<Container> allocatedContainers, List<NodeReport> updatedNodes,
                                               Resource availResources, AMCommand command, int numClusterNodes,
                                               PreemptionMessage preempt, List<NMToken> nmTokens, Token amRMToken,
                                               List<ContainerResourceIncrease> increasedContainers,
                                               List<ContainerResourceDecrease> decreasedContainers) {
        MyAllocateResponse response =
                newInstance(responseId, completedContainers, allocatedContainers,
                        updatedNodes, availResources, command, numClusterNodes, preempt,
                        nmTokens, increasedContainers, decreasedContainers);
        response.setAMRMToken(amRMToken);
        return response;
    }
    private int  responseId;
    private int numClusterNodes;
    private MyResource availResources;
    private AMCommand amCommand;
    private MyPreemptionMessage myPreemptionMessage;
    private MyCompletedContainers myCompletedContainers;
    private MyAllocatedContainers myAllocatedContainers;
    private MyUpdateNodes myUpdateNodes;
    private MyNMTokenList myNMTokenList;
    private MyToken myAmRmToken;

    private MyContainerResourceIncreaseList myContainerResourceIncreaseList;
    private MyContainerResourceDecreaseList myContainerResourceDecreaseList;

    public  AMCommand getAMCommand(){
        return this.amCommand;
    }
    public  void setAMCommand(AMCommand command){
        this.amCommand = command;
    }
    public  int getResponseId(){
        return this.responseId;
    }
    public  void setResponseId(int responseId){
        this.responseId = responseId;
    }

   
    public  List<Container> getAllocatedContainers(){

        return (this.myAllocatedContainers == null) ? null:this.myAllocatedContainers.getAllocatedContainers();
    }
    public  void setAllocatedContainers(List<Container> containers){
        if(containers == null) {
            this.myAllocatedContainers = null;
        }
        else{
            this.myAllocatedContainers = new MyAllocatedContainers(containers);
        }
    }


    public  Resource getAvailableResources(){
        return  (this.availResources == null) ? null:this.availResources.tansBack();
    }


    public  void setAvailableResources(Resource limit){
        this.availResources = (limit == null) ? null:MyResource.newInstance(limit.getMemory(),limit.getVirtualCores());
    }


    public  List<ContainerStatus> getCompletedContainersStatuses(){
        return (this.myCompletedContainers == null) ? null:this.myCompletedContainers.getMyContainerStatusList();
    }


    public  void setCompletedContainersStatuses(List<ContainerStatus> containers){
        this.myCompletedContainers = (containers == null) ? null:new MyCompletedContainers(containers);
    }


    public   List<NodeReport> getUpdatedNodes(){
        return (this.myUpdateNodes == null) ? null:this.myUpdateNodes.getUpdateNodes();
    }

    public  void setUpdatedNodes(final List<NodeReport> updatedNodes){
        this.myUpdateNodes = (updatedNodes == null)?null:new MyUpdateNodes(updatedNodes);
    }


    public  int getNumClusterNodes(){
        return this.numClusterNodes;
    }

    public  void setNumClusterNodes(int numNodes){
        this.numClusterNodes = numNodes;
    }


    public  PreemptionMessage getPreemptionMessage(){
        return (this.myPreemptionMessage == null)?null:myPreemptionMessage.tansBack();
    }


    public  void setPreemptionMessage(PreemptionMessage request){
        this.myPreemptionMessage = (request == null)?null:MyPreemptionMessage.newInstance(
                request.getStrictContract(),
                request.getContract());
    }


    public  List<NMToken> getNMTokens(){
        return (this.myNMTokenList == null)?null:this.myNMTokenList.getNMToken();
    }


    public  void setNMTokens(List<NMToken> nmTokens){
        this.myNMTokenList = (nmTokens == null)?null:new MyNMTokenList(nmTokens);
    }


    public  List<ContainerResourceIncrease> getIncreasedContainers(){
        return (this.myContainerResourceIncreaseList == null)?null:this.myContainerResourceIncreaseList.getIncreasedContainers();
    }


    public  void setIncreasedContainers(
            List<ContainerResourceIncrease> increasedContainers){
        this.myContainerResourceIncreaseList = (increasedContainers == null)?null:new MyContainerResourceIncreaseList(increasedContainers);
    }

    public  List<ContainerResourceDecrease> getDecreasedContainers(){
        return (this.myContainerResourceDecreaseList ==null)?null:this.myContainerResourceDecreaseList.getDecreasedContainers();
    }


    public  void setDecreasedContainers(
            List<ContainerResourceDecrease> decreasedContainers){
        this.myContainerResourceDecreaseList =(decreasedContainers == null)? null: new MyContainerResourceDecreaseList(decreasedContainers);
    }


    public  Token getAMRMToken(){
        return (this.myAmRmToken == null)?null:this.myAmRmToken.tansBack();
    }

    public  void setAMRMToken(Token amRMToken){
        this.myAmRmToken = (amRMToken == null)?null:MyToken.newInstance(
                amRMToken.getIdentifier().array(),
                amRMToken.getKind(),
                amRMToken.getPassword().array(),
                amRMToken.getService());
    }

    private class MyContainerResourceDecreaseList implements  Serializable{
        private List<MyContainerResourceDecrease> myContainerResourceDecreaseList;

        public MyContainerResourceDecreaseList(List<ContainerResourceDecrease> decreasedContainers) {
            this.myContainerResourceDecreaseList = new ArrayList<MyContainerResourceDecrease>();
            for (Iterator it2 = myContainerResourceDecreaseList.iterator(); it2.hasNext(); ) {
                ContainerResourceDecrease temp = (ContainerResourceDecrease) it2.next();
                myContainerResourceDecreaseList.add(MyContainerResourceDecrease.newInstance(
                        temp.getContainerId(),
                        temp.getCapability()
                ));
            }
        }

        public List<ContainerResourceDecrease> getDecreasedContainers() {

            List<ContainerResourceDecrease> decreasedContainers = new ArrayList<ContainerResourceDecrease>();
            for (Iterator it2 = this.myContainerResourceDecreaseList.iterator(); it2.hasNext(); ) {
                ContainerResourceDecrease temp = ((MyContainerResourceDecrease) it2.next()).tansBack();
                decreasedContainers.add(temp);
            }
            return decreasedContainers;
        }
    }

    private class MyContainerResourceIncreaseList implements  Serializable{
        private List<MyContainerResourceIncrease> myContainerResourceIncreaseList;

        public MyContainerResourceIncreaseList(List<ContainerResourceIncrease> increasedContainers) {
            this.myContainerResourceIncreaseList = new ArrayList<MyContainerResourceIncrease>();
            for (Iterator it2 = myContainerResourceIncreaseList.iterator(); it2.hasNext(); ) {
                ContainerResourceIncrease temp = (ContainerResourceIncrease) it2.next();
                myContainerResourceIncreaseList.add(MyContainerResourceIncrease.newInstance(
                        temp.getContainerId(),
                        temp.getCapability(),
                        temp.getContainerToken()
                ));
            }
        }

        public List<ContainerResourceIncrease> getIncreasedContainers() {
            List<ContainerResourceIncrease> increasedContainers = new ArrayList<ContainerResourceIncrease>();
            for (Iterator it2 = this.myContainerResourceIncreaseList.iterator(); it2.hasNext(); ) {
                ContainerResourceIncrease temp = ((MyContainerResourceIncrease) it2.next()).tansBack();
                increasedContainers.add(temp);
            }
            return increasedContainers;
        }
    }

    private class MyNMTokenList implements  Serializable{
        private List<MyNMToken> myNmTokens;

        public MyNMTokenList(List<NMToken> nmTokens) {
            this.myNmTokens = new ArrayList<MyNMToken>();
            for (Iterator it2 = nmTokens.iterator(); it2.hasNext(); ) {
                NMToken temp = (NMToken) it2.next();
                myNmTokens.add(MyNMToken.newInstance(
                        temp.getNodeId(),
                        temp.getToken()
                ));
            }
        }

        public List<NMToken> getNMToken() {
            List<NMToken> nmTokens = new ArrayList<NMToken>();
            for (Iterator it2 = this.myNmTokens.iterator(); it2.hasNext(); ) {
                NMToken temp = ((MyNMToken) it2.next()).tansBack();
                nmTokens.add(temp);
            }
            return nmTokens;
        }
    }


    private class MyUpdateNodes implements  Serializable{
        private List<MyNodeReport> myNodeReportList ;
        public MyUpdateNodes(List<NodeReport> updatedNodes){
           this.myNodeReportList = new ArrayList<MyNodeReport>();
            for(Iterator it2 = updatedNodes.iterator(); it2.hasNext();) {
                NodeReport temp = (NodeReport)it2.next();
                myNodeReportList.add(MyNodeReport.newInstance(
                        temp.getNodeId(),
                        temp.getNodeState(),
                        temp.getHttpAddress(),
                        temp.getRackName(),
                        temp.getUsed(),
                        temp.getCapability(),
                        temp.getNumContainers(),
                        temp.getHealthReport(),
                        temp.getLastHealthReportTime(),
                        temp.getNodeLabels()
                ));
            }
        }
        public List<NodeReport> getUpdateNodes(){
            List<NodeReport> updatedNodes = new ArrayList<NodeReport>();
            for(Iterator it2 = this.myNodeReportList.iterator(); it2.hasNext();) {
                NodeReport temp = ((MyNodeReport)it2.next()).tansBack();
                updatedNodes.add(temp);
            }

            return updatedNodes;
        }

    }
    private class MyAllocatedContainers implements  Serializable{
        private List<MyContainer> MyContainerList;
        public MyAllocatedContainers(List<Container> allocatedContainers){
            this.MyContainerList = new ArrayList<MyContainer>();
            for(Iterator it2 = allocatedContainers.iterator(); it2.hasNext();) {
                Container temp = (Container)it2.next();
                MyContainerList.add(MyContainer.newInstance(
                        temp.getId(),
                        temp.getNodeId(),
                        temp.getNodeHttpAddress(),
                        temp.getResource(),
                        temp.getPriority(),
                        temp.getContainerToken()
                ));
            }

        }
        public List<Container> getAllocatedContainers() {
            List<Container> allocatedContainers = new ArrayList<Container>();
            for (Iterator it2 = this.MyContainerList.iterator(); it2.hasNext(); ) {
                Container temp = ((MyContainer) it2.next()).tansBack();
                allocatedContainers.add(temp);
            }
            return allocatedContainers;
        }
    }
    private class MyCompletedContainers implements  Serializable{
        private List<MyContainerStatus> myContainerStatusList;
        public MyCompletedContainers(List<ContainerStatus> completedContainers){
            this.myContainerStatusList = new ArrayList<MyContainerStatus>();
            for(Iterator it2 = completedContainers.iterator(); it2.hasNext();) {
                ContainerStatus temp = (ContainerStatus)it2.next();
                myContainerStatusList.add(
                        MyContainerStatus.newInstance(
                                temp.getContainerId(),
                                temp.getState(),
                                temp.getDiagnostics(),
                                temp.getExitStatus()));

            }
        }
        public List<ContainerStatus> getMyContainerStatusList(){
            List<ContainerStatus> completedContainers = new ArrayList<ContainerStatus>();
            for(Iterator it2 = myContainerStatusList.iterator(); it2.hasNext();) {
                ContainerStatus temp = ((MyContainerStatus)it2.next()).transBack();
                completedContainers.add(temp);
            }
            return completedContainers;
        }
    }



}
