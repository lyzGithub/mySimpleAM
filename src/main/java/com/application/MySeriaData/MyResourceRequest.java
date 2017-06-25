package com.application.MySeriaData;

import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;

import java.io.Serializable;

/**
 * Created by ubuntu2 on 6/20/17.
 */

public class MyResourceRequest implements Serializable {
    private int priority;
    private String resource_name;
    private int num_containers;
    private boolean relax_locality;
    private MyCapability capability;

    public MyResourceRequest(ResourceRequest request){
        this.capability = new MyCapability(request.getCapability().getMemory(),
                request.getCapability().getVirtualCores());

        this.resource_name = request.getResourceName();
        this.relax_locality = request.getRelaxLocality();
        this.priority = request.getPriority().getPriority();
        this.num_containers = request.getNumContainers();
    }
    public int getPriority(){
        return this.priority;
    }
    public String getResourceName(){
        return this.resource_name;
    }
    public int getNumContainer(){
        return this.num_containers;
    }
    public boolean getRelaxLocality(){
        return this.relax_locality;
    }
    public int getCapabilityMem(){
        return this.capability.getMem();
    }
    public int getCapabilityVc(){
        return this.capability.getVc();
    }
    public ResourceRequest transBack(){
        ResourceRequest rrt =
                ResourceRequest.newInstance(Priority.newInstance(this.priority),this.resource_name,
                        Resource.newInstance(this.capability.getMem(),this.capability.getVc()),this.num_containers);
        return rrt;
    }
}