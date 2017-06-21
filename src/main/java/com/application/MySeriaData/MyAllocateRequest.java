package com.application.MySeriaData;

/**
 * Created by ubuntu2 on 6/20/17.
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerResourceIncreaseRequest;
import org.apache.hadoop.yarn.api.records.ResourceBlacklistRequest;
import org.apache.hadoop.yarn.api.records.ResourceRequest;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by ubuntu2 on 6/20/17.
 */
public class MyAllocateRequest implements Serializable {
    private static final Log LOG = LogFactory.getLog(MyAllocateRequest.class);


   // List<ContainerResourceIncreaseRequest> increaseRequests; //no need to realise

    private MyBlackListRequest myBlackListRequest;
    private MyReleaseList myReleaseList;
    private MyAskList myAskList;
    private int responseID;
    private float appProgress;


    public class MyBlackListRequest implements Serializable{
        List<String> AddBR ;
        List<String> RemovalBR ;
        public MyBlackListRequest(ResourceBlacklistRequest rBr){
            this.AddBR  = rBr.getBlacklistAdditions();
            this.RemovalBR = rBr.getBlacklistRemovals();
        }
        public List<String> getAddBR(){
            return this.AddBR;
        }
        public List<String> getRemovalBR(){
            return this.RemovalBR;
        }
    }

    public class MyReleaseList implements Serializable{
        List<MyContainerID> myReleaseList;
        public MyReleaseList(List<ContainerId> cTBR ){
            myReleaseList = new ArrayList<MyContainerID>();
            for(Iterator it2 = cTBR.iterator(); it2.hasNext();) {
                myReleaseList.add(new MyContainerID((ContainerId)it2.next()));
            }
        }
        public List<MyContainerID> getMyReleaseList(){
            return this.myReleaseList;
        }
    }



    private class MyAskList implements Serializable{
        List<MyResourceRequest> myAskLit;
        public MyAskList(List<ResourceRequest> resourceAsk){
            myAskLit = new ArrayList<MyResourceRequest>();
            for(Iterator it2 = resourceAsk.iterator(); it2.hasNext();) {
                myAskLit.add(new MyResourceRequest((ResourceRequest)it2.next()));
            }
        }
        public List<MyResourceRequest> getMyAskList(){
            return this.myAskLit;
        }
    }

    public static MyAllocateRequest newInstance(int responseID, float appProgress,
                                                List<ResourceRequest> resourceAsk,
                                                List<ContainerId> containersToBeReleased,
                                                ResourceBlacklistRequest resourceBlacklistRequest) {
        return newInstance(responseID, appProgress, resourceAsk,
                containersToBeReleased, resourceBlacklistRequest, null);
    }


    public static MyAllocateRequest newInstance(int responseID, float appProgress,
                                                List<ResourceRequest> resourceAsk,
                                                List<ContainerId> containersToBeReleased,
                                                ResourceBlacklistRequest resourceBlacklistRequest,
                                                List<ContainerResourceIncreaseRequest> increaseRequests) {
        MyAllocateRequest allocateRequest = new MyAllocateRequest();
        allocateRequest.setResponseId(responseID);
        allocateRequest.setProgress(appProgress);
        allocateRequest.setAskList(resourceAsk);
        allocateRequest.setReleaseList(containersToBeReleased);
        allocateRequest.setResourceBlacklistRequest(resourceBlacklistRequest);
        //allocateRequest.setIncreaseRequests(increaseRequests);
        return allocateRequest;
    }

    public void setResponseId(int responseID){
        this.responseID = responseID;
    }
    public void setProgress(float appProgress){
        this.appProgress = appProgress;
    }
    public void setAskList(List<ResourceRequest> resourceAsk){
        if(resourceAsk == null) {
            this.myAskList = null;
        }
        else {
            this.myAskList = new MyAskList(resourceAsk);
        }
    }
    public void setReleaseList(List<ContainerId> containersToBeReleased){
        if(containersToBeReleased == null){
            this.myReleaseList = null;
        }
        else {
            this.myReleaseList = new MyReleaseList(containersToBeReleased);
        }
    }
    public void setResourceBlacklistRequest(ResourceBlacklistRequest resourceBlacklistRequest){
        if(resourceBlacklistRequest == null) {
            this.myBlackListRequest = null;
        }
        else{

            this.myBlackListRequest = new MyBlackListRequest(resourceBlacklistRequest);
        }

    }

    /*public void setIncreaseRequests(List<ContainerResourceIncreaseRequest> increaseRequests){
        this.increaseRequests = increaseRequests;
    }*/
    public  int getResponseId( ){
        return this.responseID ;
    }
    public float getProgress(){
        return this.appProgress ;
    }
    public List<ResourceRequest> getAskList( ){

        if(this.myAskList == null)
            return null;
        List<MyResourceRequest> rt1 =  this.myAskList.getMyAskList();
        List<ResourceRequest> askList = new ArrayList<ResourceRequest>();
        for(Iterator it2 = rt1.iterator(); it2.hasNext();) {
            askList.add(((MyResourceRequest)it2.next()).transBack());
        }
        return askList;
    }

    public List<ContainerId> getReleaseList(){
        if(this.myReleaseList == null)
            return null;
        List<MyContainerID>  myContainerIDS = this.myReleaseList.getMyReleaseList();
        List<ContainerId> releaseList = new ArrayList<ContainerId>();
        for(Iterator it2 = myContainerIDS.iterator(); it2.hasNext();) {
            releaseList.add(((MyContainerID)it2.next()).tansBack());
        }
        return releaseList;
    }
    public ResourceBlacklistRequest getResourceBlacklistRequest( ){
        if(this.myBlackListRequest == null)
            return null;
        return ResourceBlacklistRequest.newInstance(this.myBlackListRequest.getAddBR(),this.myBlackListRequest.getRemovalBR());

    }

}
