package com.application.MySeriaData;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerResourceIncrease;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.Token;

import java.io.Serializable;

/**
 * Created by ubuntu2 on 6/20/17.
 */
public class MyContainerResourceIncrease implements Serializable {
    @InterfaceAudience.Public
    public static MyContainerResourceIncrease newInstance(
            ContainerId existingContainerId, Resource targetCapability, Token token) {
        MyContainerResourceIncrease context = new MyContainerResourceIncrease();
        context.setContainerId(existingContainerId);
        context.setCapability(targetCapability);
        context.setContainerToken(token);
        return context;
    }
    private MyContainerID myContainerID;
    private MyResource myResource;
    private MyToken myToken;
    public  ContainerId getContainerId(){
        return this.myContainerID.tansBack();
    }

    public  void setContainerId(ContainerId containerId){
        this.myContainerID = new MyContainerID(containerId);
    }

    public  Resource getCapability(){
        return this.myResource.tansBack();
    }

    public  void setCapability(Resource capability){
        this.myResource = MyResource.newInstance(capability.getMemory(),capability.getVirtualCores());
    }

    public  Token getContainerToken(){
        return this.myToken.tansBack();
    }

    public  void setContainerToken(Token token){
        this.myToken = MyToken.newInstance(
                token.getIdentifier().array(),
                token.getKind(),
                token.getPassword().array(),
                token.getService());
    }

    public ContainerResourceIncrease tansBack(){
        ContainerResourceIncrease temp = ContainerResourceIncrease.newInstance(
                this.getContainerId(),
                this.getCapability(),
                this.getContainerToken()
        );
        return temp;
    }
}
