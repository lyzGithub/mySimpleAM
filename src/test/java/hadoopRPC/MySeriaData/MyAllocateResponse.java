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
        return this.myAllocatedContainers.getAllocatedContainers();
    }
    public  void setAllocatedContainers(List<Container> containers){
        this.myAllocatedContainers = new MyAllocatedContainers(containers);
    }


    public  Resource getAvailableResources(){
        return this.availResources.tansBack();
    }


    public  void setAvailableResources(Resource limit){
        this.availResources = MyResource.newInstance(limit.getMemory(),limit.getVirtualCores());
    }


    public  List<ContainerStatus> getCompletedContainersStatuses(){
        return this.myCompletedContainers.getMyContainerStatusList();
    }


    public  void setCompletedContainersStatuses(List<ContainerStatus> containers){
        this.myCompletedContainers = new MyCompletedContainers(containers);
    }


    public   List<NodeReport> getUpdatedNodes(){
        return this.myUpdateNodes.getUpdateNodes();
    }

    public  void setUpdatedNodes(final List<NodeReport> updatedNodes){
        this.myUpdateNodes = new MyUpdateNodes(updatedNodes);
    }


    public  int getNumClusterNodes(){
        return this.numClusterNodes;
    }

    public  void setNumClusterNodes(int numNodes){
        this.numClusterNodes = numNodes;
    }


    public  PreemptionMessage getPreemptionMessage(){
        return myPreemptionMessage.tansBack();
    }


    public  void setPreemptionMessage(PreemptionMessage request){
        this.myPreemptionMessage = MyPreemptionMessage.newInstance(
                request.getStrictContract(),
                request.getContract());
    }


    public  List<NMToken> getNMTokens(){
        return this.myNMTokenList.getNMToken();
    }


    public  void setNMTokens(List<NMToken> nmTokens){
        this.myNMTokenList = new MyNMTokenList(nmTokens);
    }


    public  List<ContainerResourceIncrease> getIncreasedContainers(){

    }


    public  void setIncreasedContainers(
            List<ContainerResourceIncrease> increasedContainers){

    }

    public  List<ContainerResourceDecrease> getDecreasedContainers(){

    }


    public  void setDecreasedContainers(
            List<ContainerResourceDecrease> decreasedContainers){

    }


    public  Token getAMRMToken(){
        return this.myAmRmToken.tansBack();
    }

    public  void setAMRMToken(Token amRMToken){
        this.myAmRmToken = MyToken.newInstance(
                amRMToken.getIdentifier().array(),
                amRMToken.getKind(),
                amRMToken.getPassword().array(),
                amRMToken.getService());
    }


    private class MyNMTokenList {
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


    private class MyUpdateNodes{
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
    private class MyAllocatedContainers{
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
    private class MyCompletedContainers {
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
