package thread;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.DigestInputStream;

/**
 * 求文件的信息摘要码（MD5）
 *
 * @author leizhimin 2008-9-11 22:53:39
 */

/**
 * 求文件的信息摘要码（MD5）
 *
 * @author leizhimin 2008-9-11 22:53:39
 */
public class InstanceCallbackDigest implements Runnable {
    private File inputFile;         //目标文件
    //每个线程绑定一个回调对象
    private InstanceCallbackDigestUserInterface instanceCallback;

    /**
     * 构件时一次注入回调对象
     *
     * @param instanceCallback
     * @param inputFile
     */
    public InstanceCallbackDigest(InstanceCallbackDigestUserInterface instanceCallback, File inputFile) {
        this.instanceCallback = instanceCallback;
        this.inputFile = inputFile;
    }

    public void run() {
        try {
            FileInputStream in = new FileInputStream(inputFile);
            MessageDigest sha = MessageDigest.getInstance("MD5");
            DigestInputStream din = new DigestInputStream(in, sha);
            int b;
            while ((b = din.read()) != -1) ;
            din.close();
            byte[] digest = sha.digest();   //摘要码
            //完成后，回调主线程静态方法，将文件名-摘要码结果传递给住线程
            instanceCallback.receiveDigest(digest);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}