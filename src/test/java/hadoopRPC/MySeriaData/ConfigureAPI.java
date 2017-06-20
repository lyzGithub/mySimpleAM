package hadoopRPC.MySeriaData;

/**
 * Created by ubuntu2 on 6/19/17.
 */
public class ConfigureAPI {

    public interface VersionID {
        public static final long RPC_VERSION = 7788L;
    }

    public interface ServerAddress {
        public static final int NIO_PORT = 8888;
        public static final String NIO_IP = "127.0.0.1";
    }

}
