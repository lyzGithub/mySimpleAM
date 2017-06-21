package com.application.MySeriaData;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;

import java.io.Serializable;

/**
 * Created by ubuntu2 on 6/20/17.
 */
public class MyContainerID implements Serializable {
    private long containerID;
    private MyApplicationAttemptID appAttID;
    private class MyApplicationAttemptID implements Serializable{
        private long ClusterTimeStamp;
        private int appAttId;
        private int appId;
        public MyApplicationAttemptID(ApplicationAttemptId appAttId){
            this.ClusterTimeStamp = appAttId.getApplicationId().getClusterTimestamp();
            this.appId = appAttId.getApplicationId().getId();
            this.appAttId = appAttId.getAttemptId();
        }
        public int getAppAttId(){
            return this.appAttId;
        }
        public long getClusterTimeStamp(){
            return this.ClusterTimeStamp;
        }
        public int getAppId(){
            return this.appId;
        }
    }
    public MyContainerID(ContainerId cID){
        this.containerID = cID.getContainerId();
        this.appAttID = new MyApplicationAttemptID(cID.getApplicationAttemptId());
    }

    public ContainerId tansBack(){
        return ContainerId.newContainerId(ApplicationAttemptId.newInstance(
                ApplicationId.newInstance(this.appAttID.ClusterTimeStamp,this.appAttID.appId), this.appAttID.appAttId),
                this.containerID);
    }
}