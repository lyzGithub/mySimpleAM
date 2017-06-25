package com.application.yarnAm;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.ipc.ProtocolInfo;
import org.apache.hadoop.ipc.VersionedProtocol;
import com.application.MySeriaData.ConfigureAPI;

/**
 * Created by ubuntu2 on 6/21/17.
 */
@ProtocolInfo(protocolName = "", protocolVersion = ConfigureAPI.VersionID.RPC_VERSION)
public interface AmAllocateService extends VersionedProtocol {
    // defined add function
    public IntWritable add(IntWritable arg1, IntWritable arg2);

    // defined sub function
    public IntWritable sub(IntWritable arg1, IntWritable arg2);

    public BytesWritable tansBytes(BytesWritable args) throws Exception;


}

