package hadoopRPC.MySeriaData;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.Records;

import java.io.Serializable;

/**
 * Created by ubuntu2 on 6/20/17.
 */
public class MyResource implements Serializable {

    private int mem;
    private int vCore;
    public static MyResource newInstance(int memory, int vCores) {
        MyResource resource = new MyResource();
        resource.setMemory(memory);
        resource.setVirtualCores(vCores);
        return resource;
    }
    public void setMemory(int mem){
        this.mem =mem;
    }
    public void setVirtualCores(int vCores ){
        this.vCore = vCores;
    }
    public int getMem(){
        return this.mem;
    }
    public int getvCore(){
        return this.vCore;
    }
    public Resource tansBack(){
        return Resource.newInstance(this.mem,this.vCore);
    }
}
