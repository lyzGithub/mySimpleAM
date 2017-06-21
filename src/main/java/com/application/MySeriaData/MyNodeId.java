package com.application.MySeriaData;

import org.apache.hadoop.yarn.api.records.NodeId;

import java.io.Serializable;

/**
 * Created by ubuntu2 on 6/20/17.
 */
public class MyNodeId implements Serializable {

    private String host;
    private int port;
    public static MyNodeId newInstance(String host, int port){
        MyNodeId nid = new MyNodeId();
        nid.setHost(host);
        nid.setPort(port);
        //nodeId.build();
        return nid;
    }

    public void setHost(String host){
        this.host = host;
    }
    public void setPort(int port){
        this.port = port;
    }

    public NodeId tansBack(){
        return NodeId.newInstance(this.host,this.port);
    }
}
