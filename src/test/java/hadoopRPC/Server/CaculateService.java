package hadoopRPC.Server;

/**
 * Created by ubuntu2 on 6/19/17.
 */
import hadoopRPC.MySeriaData.ConfigureAPI;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.ipc.ProtocolInfo;
import org.apache.hadoop.ipc.VersionedProtocol;


/**
 * @Date May 7, 2015
 *
 * @Author dengjie
 *
 * @Note Data calculate service interface
 */
@ProtocolInfo(protocolName = "", protocolVersion = ConfigureAPI.VersionID.RPC_VERSION)
public interface CaculateService extends VersionedProtocol {

    // defined add function
    public IntWritable add(IntWritable arg1, IntWritable arg2);

    // defined sub function
    public IntWritable sub(IntWritable arg1, IntWritable arg2);

    public IntWritable tansBytes(BytesWritable args);

}