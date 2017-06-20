package hadoopRPC.MySeriaData;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.api.records.PreemptionResourceRequest;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.util.Records;

import java.io.Serializable;

/**
 * Created by ubuntu2 on 6/20/17.
 */
public class MyPreemptionResourceRequest implements Serializable {

    public static MyPreemptionResourceRequest newInstance(ResourceRequest req) {
        MyPreemptionResourceRequest request = new MyPreemptionResourceRequest();
        request.setResourceRequest(req);
        return request;
    }
    private MyResourceRequest myResourceRequest;

    public  ResourceRequest getResourceRequest(){
        return this.myResourceRequest.transBack();
    }

    public  void setResourceRequest(ResourceRequest req){
        this.myResourceRequest = new MyResourceRequest(req);
    }
    public PreemptionResourceRequest transBack(){
        PreemptionResourceRequest temp = PreemptionResourceRequest.newInstance(this.myResourceRequest.transBack());
        return temp;
    }
}
