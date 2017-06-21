package com.application.MySeriaData;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;

import java.io.Serializable;

/**
 * Created by ubuntu2 on 6/20/17.
 */
public class MyContainerStatus implements Serializable {
    //ContainerState enum

    private MyContainerID myContainerID;
    private String containerState;
    private String diagnostics;
    private int exitStatus;
    public static MyContainerStatus newInstance(ContainerId containerId,
                                              ContainerState containerState, String diagnostics, int exitStatus) {
        MyContainerStatus myContainerStatus = new MyContainerStatus();
        myContainerStatus.setState(containerState);
        myContainerStatus.setContainerId(containerId);
        myContainerStatus.setDiagnostics(diagnostics);
        myContainerStatus.setExitStatus(exitStatus);
        return myContainerStatus;
    }
    public void setState(ContainerState containerState){
        if(containerState == null) {
            this.containerState = null;
        }
        else {
            this.containerState = containerState.toString();
        }
    }
    public void setContainerId(ContainerId containerId){
        if(containerId == null) {
            this.myContainerID = null;
        }
        else{
            this.myContainerID = new MyContainerID(containerId);
        }
    }
    public void setDiagnostics(String diagnostics){
        this.diagnostics = (diagnostics == null)?null:diagnostics;
    }
    public void setExitStatus(int exitStatus){
        this.exitStatus = exitStatus;
    }
    public ContainerStatus transBack(){
        return ContainerStatus.newInstance(
                this.myContainerID.tansBack(),
                ContainerState.valueOf( this.containerState),
                this.diagnostics,
                this.exitStatus);
    }
}
