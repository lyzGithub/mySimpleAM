package hadoopRPC.MySeriaData;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.PreemptionContainer;
import org.apache.hadoop.yarn.util.Records;

import java.io.Serializable;

/**
 * Created by ubuntu2 on 6/20/17.
 */
public class MyPreemptionContainer implements Serializable {

    public static MyPreemptionContainer newInstance(ContainerId id) {
        MyPreemptionContainer container = new MyPreemptionContainer();
        container.setId(id);
        return container;
    }
    private MyContainerID myContainerID;

    public  ContainerId getId(){
        return this.myContainerID.tansBack();
    }

    public  void setId(ContainerId id){
        this.myContainerID = new MyContainerID(id);
    }

    public PreemptionContainer transBack(){
        return PreemptionContainer.newInstance(this.myContainerID.tansBack());
    }
}
