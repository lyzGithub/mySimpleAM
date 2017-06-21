package com.application.yarnAm;

import com.application.MySeriaData.ByteTrans;
import com.application.MySeriaData.MyAllocateRequest;
import com.application.MySeriaData.MyAllocateResponse;
import com.application.api.NMClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by ubuntu2 on 6/11/17.
 */
public class AmAllocateClient {
    private static final Log LOG = LogFactory
            .getLog(AmAllocateClient.class);
    public static void main(String[] args) throws Exception {
        LOG.info("hello i am in hellotest!");
        InetSocketAddress addr = new InetSocketAddress("127.0.0.1", remoteAPPMaster.IPC_PORT);
        RPC.getProtocolVersion(AmAllocateService.class);
        AmAllocateService service = (AmAllocateService) RPC.getProxy(AmAllocateService.class,
                RPC.getProtocolVersion(AmAllocateService.class), addr, new Configuration());



        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(0);

        // Resource requirements for worker containers
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(128);
        capability.setVirtualCores(1);

        remoteAPPMaster rp1 = new remoteAPPMaster("disYarn1",0);
        ResourceRequest rq = ResourceRequest.newInstance(priority,null,capability,1);
        List<ResourceRequest> myAsk =
                new ArrayList<ResourceRequest>();
        myAsk.add(rq);
        AllocateRequest request1 = AllocateRequest.newInstance(1,0,myAsk,null,null,null);
        AllocateRequest request2 = AllocateRequest.newInstance(2,0,myAsk,null,null,null);
        MyAllocateRequest myRequest = MyAllocateRequest.newInstance(1,0,myAsk,null,null,null);

        LOG.info("get allocate in test hello!");
        byte[] myByy = ByteTrans.ObjectToBytes((Object)myRequest);
        byte[] byy = service.tansBytes(new BytesWritable(myByy)).getBytes();
        MyAllocateResponse myRs = (MyAllocateResponse)ByteTrans.bytesToObject(byy);

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

        Configuration conf = new YarnConfiguration();
        conf.set("yarn.resourcemanager.address","0.0.0.0:8032");
        conf.set("yarn.resourcemanager.scheduler.address","0.0.0.0:8030");
        NMClient nmClient = NMClient.createNMClient();
        nmClient.init(conf);
        nmClient.start();
        nmClient.setNMTokenCache(allRs.getNMTokens());
        LOG.info("wo will start a container in the node!!!");
        int i = 0;
        for (Container container : allRs.getAllocatedContainers()) {
            // Launch container by create ContainerLaunchContext
            i ++;
            ContainerLaunchContext ctx =
                    Records.newRecord(ContainerLaunchContext.class);
            ctx.setCommands(
                    Collections.singletonList(
                            "hello" +
                                    " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
                                    " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
                    ));
            System.out.println("Launching container " + container.getId());
            nmClient.startContainer(container, ctx);
        }
        LOG.info(" start "+ i +" containers success in the node!!!");

        LOG.info("response allocate in test hello!");

        LOG.info("finish in hello test main!");
    }
}
