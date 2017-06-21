package hadoopRPC.Client;

/**
 * Created by ubuntu2 on 6/19/17.
 */

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import com.application.yarnAm.remoteAPPMaster;
import hadoopRPC.MySeriaData.MyAllocateRequest;
import hadoopRPC.MySeriaData.ByteTrans;
import hadoopRPC.Server.CaculateServer;
import hadoopRPC.Server.CaculateService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @Date May 7, 2015
 *
 * @Author dengjie
 *
 * @Note RPC Client Main
 */
public class RPCClient extends
        AbstractService {

    private static final Logger LOGGER = LoggerFactory.getLogger(RPCClient.class);

    /**
     * Construct the service.
     *
     * @param name service name
     */
    public RPCClient(String name) {
        super(name);
    }

    public static void main(String[] args) {
        InetSocketAddress addr = new InetSocketAddress("127.0.0.1", CaculateServer.IPC_PORT);
        try {
            RPC.getProtocolVersion(CaculateService.class);
            CaculateService service = (CaculateService) RPC.getProxy(CaculateService.class,
                    RPC.getProtocolVersion(CaculateService.class), addr, new Configuration());
            int add = service.add(new IntWritable(2), new IntWritable(3)).get();
            int sub = service.sub(new IntWritable(5), new IntWritable(2)).get();
            LOGGER.info("2+3=" + add);
            LOGGER.info("5-2=" + sub);

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
            MyAllocateRequest  myRequest = MyAllocateRequest.newInstance(1,0,myAsk,null,null,null);
            byte[] byy = ByteTrans.ObjectToBytes((Object)myRequest);
            service.tansBytes(new BytesWritable(byy));
        } catch (Exception ex) {
            ex.printStackTrace();
            LOGGER.error("Client has error,info is " + ex.getMessage());
        }
    }

}