package com.application.MySeriaData;

import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Token;

import java.io.Serializable;

/**
 * Created by ubuntu2 on 6/20/17.
 */
public class MyNMToken implements Serializable {
    public static MyNMToken newInstance(NodeId nodeId, Token token) {
        MyNMToken nmToken = new MyNMToken();
        nmToken.setNodeId(nodeId);
        nmToken.setToken(token);
        return nmToken;
    }
    private MyNodeId myNodeId;
    private MyToken myToken;

    public  NodeId getNodeId(){
        return this.myNodeId.tansBack();
    }
    public  void setNodeId(NodeId nodeId){
        this.myNodeId = MyNodeId.newInstance(nodeId.getHost(),nodeId.getPort());
    }
    public  Token getToken(){
       return this.myToken.tansBack();
    }

    public  void setToken(Token token){
        this.myToken = MyToken.newInstance(
                token.getIdentifier().array(),
                token.getKind(),
                token.getPassword().array(),
                token.getService());
    }
    public NMToken tansBack(){
        return NMToken.newInstance(this.myNodeId.tansBack(),this.myToken.tansBack());
    }


}
