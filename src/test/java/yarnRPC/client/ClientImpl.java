package yarnRPC.client;

import com.application.api.impl.AMRMClientImpl;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;

/**
 * Created by ubuntu2 on 6/20/17.
 */
public class ClientImpl extends Client {
    public ClientImpl() {
        super(ClientImpl.class.getName());
    }
    public  AllocateResponse getAllocateRequset(AllocateRequest allRt){
        return AllocateResponse.newInstance(0, null ,
                null,null,null,null,
                0,null,null);
    }
}
