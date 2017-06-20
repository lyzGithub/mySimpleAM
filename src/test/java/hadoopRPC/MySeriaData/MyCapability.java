package hadoopRPC.MySeriaData;

import java.io.Serializable;

/**
 * Created by ubuntu2 on 6/20/17.
 */
public class MyCapability implements Serializable {
    private int memory;
    private int virtual_cores;
    public MyCapability(int mem, int vc){
        this.memory = mem;
        this.virtual_cores = vc;
    }
    public int  getMem(){
        return this.memory;
    }
    public int getVc(){
        return this.virtual_cores;
    }
}