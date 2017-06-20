package yarnRPC.server;

import org.apache.hadoop.service.AbstractService;

/**
 * Created by ubuntu2 on 6/20/17.
 */
public class ServerService extends AbstractService implements
        ServerProtocol {

    /**
     * Construct the service.
     *
     * @param name service name
     */
    public ServerService(String name) {
        super(name);
    }
}
