package com.application.MySeriaData;

import org.apache.hadoop.yarn.api.records.PreemptionContract;
import org.apache.hadoop.yarn.api.records.PreemptionMessage;
import org.apache.hadoop.yarn.api.records.StrictPreemptionContract;

import java.io.Serializable;

/**
 * Created by ubuntu2 on 6/20/17.
 */
public class MyPreemptionMessage implements Serializable {
    public static MyPreemptionMessage newInstance(StrictPreemptionContract set,
                                                PreemptionContract contract) {
        MyPreemptionMessage message = new MyPreemptionMessage();
        message.setStrictContract(set);
        message.setContract(contract);
        return message;
    }
    private MyStrictPreemptionContract myStrictPreemptionContract;
    private MyPreemptionContract myPreemptionContract;

    public  StrictPreemptionContract getStrictContract(){
        return this.myStrictPreemptionContract.transBack();
    }

    public  void setStrictContract(StrictPreemptionContract set){
        this.myStrictPreemptionContract = MyStrictPreemptionContract.newInstance(
                set.getContainers()
        );
    }


    public  PreemptionContract getContract(){
            return this.myPreemptionContract.transBack();
    }

    public  void setContract(PreemptionContract contract){
        this.myPreemptionContract = MyPreemptionContract.newInstance(
                contract.getResourceRequest(),
                contract.getContainers());
    }

    public PreemptionMessage tansBack(){
        PreemptionMessage temp = PreemptionMessage.newInstance(
                this.getStrictContract(),
                this.myPreemptionContract.transBack());
        return temp;
    }
}
