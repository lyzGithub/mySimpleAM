package hadoopRPC.MySeriaData;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.util.Records;

/**
 * Created by ubuntu2 on 6/19/17.
 */
public class ByteTrans {
    //byte to object
    public static Object bytesToObject(byte[] bytes) {
        Object obj = null;
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream (bytes);
            ObjectInputStream ois = new ObjectInputStream (bis);
            obj = ois.readObject();
            ois.close();
            bis.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        } catch (ClassNotFoundException ex) {
            ex.printStackTrace();
        }
        return obj;
    }
    //object to byte
    public static byte[] ObjectToBytes(java.lang.Object obj) {
        byte[] bytes = null;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(obj);
            oos.flush();
            bytes = bos.toByteArray ();
            oos.close();
            bos.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return bytes;
    }
    public static void main(String[] args){
        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(0);

        // Resource requirements for worker containers
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(128);
        capability.setVirtualCores(1);

        ResourceRequest rq1 = ResourceRequest.newInstance(priority,null,capability,1);
        ResourceRequest rq2 = ResourceRequest.newInstance(priority,null,capability,1);

        List<ResourceRequest> myAsk =
                new ArrayList<ResourceRequest>();
        myAsk.add(rq1);
        myAsk.add(rq2);
        AllocateRequest request1 = AllocateRequest.newInstance(1,0,myAsk,
                null,null,null);
        String temp = request1.toString();
        System.out.println(temp);
    }
}
