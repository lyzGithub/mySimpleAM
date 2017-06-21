package com.application.MySeriaData;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerResourceDecrease;
import org.apache.hadoop.yarn.api.records.Resource;

import java.io.Serializable;

/**
 * Created by ubuntu2 on 6/20/17.
 */
public class MyContainerResourceDecrease implements Serializable{

    public static MyContainerResourceDecrease newInstance(
            ContainerId existingContainerId, Resource targetCapability) {
        MyContainerResourceDecrease context = new MyContainerResourceDecrease();
        context.setContainerId(existingContainerId);
        context.setCapability(targetCapability);
        return context;
    }
    private MyContainerID myContainerID;
    private MyResource targetCapability;

    public  ContainerId getContainerId(){
        return this.myContainerID.tansBack();
    }


    public  void setContainerId(ContainerId containerId){
        this.myContainerID = new MyContainerID(containerId);
    }


    public  Resource getCapability(){
        return this.targetCapability.tansBack();
    }


    public  void setCapability(Resource capability){
        this.targetCapability = MyResource.newInstance(capability.getMemory(),capability.getVirtualCores());
    }

    public ContainerResourceDecrease tansBack(){
        ContainerResourceDecrease temp = ContainerResourceDecrease.newInstance(
                this.myContainerID.tansBack(),
                this.targetCapability.tansBack());
        return temp;
    }
}
