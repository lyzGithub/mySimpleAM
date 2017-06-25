package com.application.yarnAm;

import com.application.api.NMClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.util.Records;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by ubuntu2 on 6/25/17.
 */
public class testYarnAm {
    private static final Log LOG = LogFactory
            .getLog(testYarnAm.class);
    public static void main(String []args) throws Exception{
        Configuration myConf = new YarnConfiguration();
        LOG.info("in testYarnAm, set the yarn address !!! ");
        myConf.set("yarn.resourcemanager.hostname","114.212.81.167");
        myConf.set("yarn.resourcemanager.address","114.212.81.167:8032");
        myConf.set("yarn.resourcemanager.scheduler.address","114.212.81.167:8030");
        AmAllocateClient amCLient = new AmAllocateClient(myConf,"",51899);
        // Priority for worker containers - priorities are intra-application
        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(0);

        // Resource requirements for worker containers
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(128);
        capability.setVirtualCores(1);

        List<ResourceRequest> askList = null;
        List<ContainerId> releaseList = null;
        AllocateRequest allocateRequest = null;
        List<String> blacklistToAdd = new ArrayList<String>();
        List<String> blacklistToRemove = new ArrayList<String>();
        askList = new ArrayList<ResourceRequest>();
        askList.add(ResourceRequest.newInstance(priority,
                    "*", capability, 1,
                    true, null));
        releaseList = new ArrayList<ContainerId>();
        ResourceBlacklistRequest blacklistRequest =
                (blacklistToAdd != null) || (blacklistToRemove != null) ?
                        ResourceBlacklistRequest.newInstance(blacklistToAdd,
                                blacklistToRemove) : null;
        allocateRequest =
                AllocateRequest.newInstance(1, (float)0.1,
                        askList, releaseList, blacklistRequest);
        AllocateResponse allocateResponse = amCLient.allocateMyRequest(allocateRequest);

        NMClient nmClient = NMClient.createNMClient();
        nmClient.init(myConf);
        nmClient.start();
        nmClient.setNMTokenCache(allocateResponse.getNMTokens());
        for (Container container : allocateResponse.getAllocatedContainers()) {
            // Launch container by create ContainerLaunchContext
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
        for (ContainerStatus status : allocateResponse.getCompletedContainersStatuses()) {

            System.out.println("Completed container " + status.getContainerId());
        }


    }

}
