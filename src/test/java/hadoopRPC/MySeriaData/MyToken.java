package hadoopRPC.MySeriaData;

import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.util.Records;

import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * Created by ubuntu2 on 6/20/17.
 */
public class MyToken implements Serializable {
    public static MyToken newInstance(byte[] identifier, String kind, byte[] password,
                                    String service) {
        MyToken token = new MyToken();
        token.setIdentifier(ByteBuffer.wrap(identifier));
        token.setKind(kind);
        token.setPassword(ByteBuffer.wrap(password));
        token.setService(service);
        return token;
    }
    private ByteBuffer identifier;
    private String kind;
    private ByteBuffer password;
    private String service;
    public  ByteBuffer getIdentifier(){
        return this.identifier;
    }
    public  ByteBuffer getPassword(){
        return this.password;
    }
    public  String getKind(){
        return this.kind;
    }
    public  String getService(){
        return this.service;
    }
    public  void setIdentifier(ByteBuffer identifier){
        this.identifier = identifier;
    }
    public  void setPassword(ByteBuffer password){
        this.password = password;
    }
    public  void setKind(String kind){
        this.kind = kind;
    }
    public  void setService(String service){
        this.service = service;
    }
    public Token tansBack(){
        return Token.newInstance(
                this.identifier.array(),
                this.kind,
                this.password.array(),
                this.service
                );
    }
}
