package hadoopRPC.MySeriaData;

import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.util.Records;

/**
 * Created by ubuntu2 on 6/20/17.
 */
public class MyNMToken {
    public static NMToken newInstance(NodeId nodeId, Token token) {
        NMToken nmToken = Records.newRecord(NMToken.class);
        nmToken.setNodeId(nodeId);
        nmToken.setToken(token);
        return nmToken;
    }

}
