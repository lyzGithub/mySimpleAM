package com.application.yarnAm;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import com.application.MySeriaData.ByteTrans;
import com.application.MySeriaData.MyAllocateRequest;
import com.application.MySeriaData.MyAllocateResponse;

import java.net.InetSocketAddress;

/**
 * Created by ubuntu2 on 6/11/17.
 */
public class AmAllocateClient {
    private static final Log LOG = LogFactory
            .getLog(AmAllocateClient.class);
    private String disYarnLabel;
    private AmAllocateService service;
    private Configuration myConf;
    private remoteAPPMaster rp1;
    private final String  yarnAddressLabel = "yarn.resourcemanager.hostname";
    private final String yraLabel = "yarn.resourcemanager.address";
    private final String yrsaLabel = "yarn.resourcemanager.scheduler.address";
    private final String localHost = "0.0.0.0";
    private int amRPCServerHost = 9999;
    public AmAllocateClient(Configuration conf, String disYarnLabel, int amRPCServerHost) throws Exception{
        LOG.info("in AmAllocateClient, new a AmAllocateClient!");
        this.amRPCServerHost = amRPCServerHost;
        this.disYarnLabel = disYarnLabel;
        this.myConf = new YarnConfiguration(conf);
        myConf.set(yraLabel,myConf.get(yarnAddressLabel)+":8032");
        myConf.set(yrsaLabel,myConf.get(yarnAddressLabel)+":8030");
        rp1 = new remoteAPPMaster(myConf,this.amRPCServerHost);
    }

    public AllocateResponse allocateMyRequest(AllocateRequest allocateRequest) throws Exception {
        LOG.info("in AmAllocateClient, start a am service!");

        InetSocketAddress addr = new InetSocketAddress(localHost, amRPCServerHost);
        RPC.getProtocolVersion(AmAllocateService.class);
        service = (AmAllocateService) RPC.getProxy(AmAllocateService.class,
                RPC.getProtocolVersion(AmAllocateService.class), addr, new Configuration());

        LOG.info("in AmAllocateClient, allocateMyRequest to trans request to myRequest!");

        MyAllocateRequest myRequest = MyAllocateRequest.newInstance(
                allocateRequest.getResponseId(),
                allocateRequest.getProgress(),
                allocateRequest.getAskList(),
                allocateRequest.getReleaseList(),
                allocateRequest.getResourceBlacklistRequest());

        LOG.info("in AmAllocateClient, allocateMyRequest to tans myRequest to bytes!");

        byte[] myByy = ByteTrans.ObjectToBytes((Object)myRequest);
        LOG.info("in AmAllocateClient, allocateMyRequest to tans myRequest bytes to RPC server!");

        byte[] byy = service.tansBytes(new BytesWritable(myByy)).getBytes();

        LOG.info("in AmAllocateClient, allocateMyRequest to tans myResponse to response!");

        MyAllocateResponse myRs = (MyAllocateResponse)ByteTrans.bytesToObject(byy);
        LOG.info("in AmAllocateClient, allocateMyRequest to tans myResponse to response!");

        AllocateResponse allRs = AllocateResponse.newInstance(
                myRs.getResponseId(),
                myRs.getCompletedContainersStatuses(),
                myRs.getAllocatedContainers(),
                myRs.getUpdatedNodes(),
                myRs.getAvailableResources(),
                myRs.getAMCommand(),
                myRs.getNumClusterNodes(),
                myRs.getPreemptionMessage(),
                myRs.getNMTokens()
        );
        LOG.info("in AmAllocateClient, return response!");
        return allRs;
    }
    public void UnregisterAM(){
        rp1.setUnRegister();
    }
}
