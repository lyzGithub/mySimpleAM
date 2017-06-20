package com.application.myApp;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.util.Records;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by ubuntu2 on 6/11/17.
 */
public class hellotest {
    private static final Log LOG = LogFactory
            .getLog(hellotest.class);
    public static void main(String[] args) throws Exception {
        LOG.info("hello i am in hellotest!");

        /*mySimpleAM am = new mySimpleAM();
        Thread amTread = new Thread(am);
        amTread.start();*/
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
        List<AllocateRequest> listAllRt = new ArrayList<AllocateRequest>();
        listAllRt.add(request1);
        listAllRt.add(request2);
        LOG.info("get allocate in test hello!");
        AllocateResponse rp = rp1.GetAlloRequest(request1);
        LOG.info("response allocate in test hello!");
        LOG.info(rp.toString());
        LOG.info("finish in hello test main!");
    }
}
