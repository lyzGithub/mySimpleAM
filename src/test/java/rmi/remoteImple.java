package rmi;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

/**
 * Created by ubuntu2 on 6/17/17.
 */


public class remoteImple extends UnicastRemoteObject implements remoteInterface {
    /**
     * 因为UnicastRemoteObject的构造方法抛出了RemoteException异常，因此这里默认的构造方法必须写，必须声明抛出RemoteException异常
     *
     * @throws RemoteException
     */
    private static final Log LOG = LogFactory
            .getLog(remoteImple.class);
    public remoteImple() throws RemoteException {
    }

    /**
     * 简单的返回“Hello World！"字样
     *
     * @return 返回“Hello World！"字样
     * @throws RemoteException
     */
    public String helloWorld() throws RemoteException {
        LOG.info("I am in hello world implement!");
        return "Hello World!";
    }

    /**
     * 一个简单的业务方法，根据传入的人名返回相应的问候语
     *
     * @param someBodyName 人名
     * @return 返回相应的问候语
     * @throws RemoteException
     */
    public String sayHelloToSomeBody(String someBodyName) throws RemoteException {
        LOG.info("I am in sayHelloToSomeBody implement!");
        return "你好，" + someBodyName + "!";
    }
}