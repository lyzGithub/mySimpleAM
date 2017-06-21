package hadoopRPC.Server;

/**
 * Created by ubuntu2 on 6/19/17.
 */
import java.io.IOException;

import hadoopRPC.MySeriaData.ByteTrans;
import hadoopRPC.MySeriaData.ConfigureAPI;
import hadoopRPC.MySeriaData.MyAllocateRequest;
import hadoopRPC.MySeriaData.MyAllocateResponse;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.records.AMCommand;


/**
 * @Date May 7, 2015
 *
 * @Author dengjie
 *
 * @Note Implements CaculateService class
 */
public class CaculateServiceImpl implements CaculateService {
    private static final Log LOG = LogFactory.getLog(CaculateServiceImpl.class);
    public ProtocolSignature getProtocolSignature(String arg0, long arg1, int arg2) throws IOException {
        return this.getProtocolSignature(arg0, arg1, arg2);
    }

    /**
     * Check the corresponding version
     */
    public long getProtocolVersion(String arg0, long arg1) throws IOException {
        return ConfigureAPI.VersionID.RPC_VERSION;
    }

    /**
     * Add nums
     */
    public IntWritable add(IntWritable arg1, IntWritable arg2) {
        return new IntWritable(arg1.get() + arg2.get());
    }

    /**
     * Sub nums
     */
    public IntWritable sub(IntWritable arg1, IntWritable arg2) {
        return new IntWritable(arg1.get() - arg2.get());
    }

    public BytesWritable tansBytes(BytesWritable args){
        LOG.info("in tansBytes! tans back to MyAllocateRequest ");
        MyAllocateRequest myAllocateRequest = (MyAllocateRequest) ByteTrans.bytesToObject(args.getBytes());
        if(myAllocateRequest == null)
            LOG.error("in tansBytes! null myAllocateRequest pointer! ");

        LOG.info("in tansBytes! tans  to allocateRequest ");
        AllocateRequest allocateRequest = AllocateRequest.newInstance(
                myAllocateRequest.getResponseId(),
                myAllocateRequest.getProgress(),
                myAllocateRequest.getAskList(),
                myAllocateRequest.getReleaseList(),
                myAllocateRequest.getResourceBlacklistRequest());
        MyAllocateResponse myRs = MyAllocateResponse.newInstance(1,null,null,
                null,null, AMCommand.AM_RESYNC,0,null,null);
        return new BytesWritable(ByteTrans.ObjectToBytes(myRs));
    }


}
