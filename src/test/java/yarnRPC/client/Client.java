package yarnRPC.client;

import com.application.api.NMTokenCache;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;

/**
 * Created by ubuntu2 on 6/20/17.
 */
public abstract  class Client extends AbstractService {
    private NMTokenCache nmTokenCache;

    @InterfaceAudience.Private
    protected Client(String name) {
        super(name);
        nmTokenCache = NMTokenCache.getSingleton();
    }
    public abstract AllocateResponse getAllocateRequset(AllocateRequest allRt);
}
