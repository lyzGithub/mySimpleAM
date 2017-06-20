package hadoopRPC.MySeriaData;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.Records;

import java.io.Serializable;

/**
 * Created by ubuntu2 on 6/20/17.
 */
public class MyResource implements Serializable {
    public static Resource newInstance(int memory, int vCores) {
        Resource resource = Records.newRecord(Resource.class);
        resource.setMemory(memory);
        resource.setVirtualCores(vCores);
        return resource;
    }
}
