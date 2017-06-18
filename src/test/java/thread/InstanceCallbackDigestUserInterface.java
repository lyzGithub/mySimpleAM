package thread;

import java.io.File;

/**
 * 静态非同步回调
 *
 * @author leizhimin 2008-9-11 23:00:12
 */

/**
 * 静态非同步回调
 *
 * @author leizhimin 2008-9-11 23:00:12
 */
public class InstanceCallbackDigestUserInterface {
    private File inputFile;     //回调与每个文件绑定
    private byte digest[];      //文件的消息摘要码

    public InstanceCallbackDigestUserInterface(File inputFile) {
        this.inputFile = inputFile;
    }

    /**
     * 计算某个文件的消息摘要码
     */
    public void calculateDigest() {
        InstanceCallbackDigest callback = new InstanceCallbackDigest(this, inputFile);
        Thread t = new Thread(callback);
        t.start();
    }

    /**
     * 接收消息摘要码
     *
     * @param digest
     */
    public void receiveDigest(byte[] digest) {
        this.digest = digest;
        //将消息摘要码输出到控制台实际上执行的是this.toString()方法
        System.out.println(this);
    }

    /**
     * 显示结果
     *
     * @return 结果
     */
    public String toString() {
        String result = inputFile.getName() + ": ";
        if (digest != null) {
            for (byte b : digest) {
                result += b + " ";
            }
        } else {
            result += "digest 不可用！";
        }
        return result;
    }

    public static void main(String[] args) {
        String arr[] = {"C:\\xcopy.txt", "C:\\x.txt", "C:\\xb.txt", "C:\\bf2.txt"};
        args = arr;
        for (int i = 0; i < args.length; i++) {
            File f = new File(args[i]);
            InstanceCallbackDigestUserInterface cb = new InstanceCallbackDigestUserInterface(f);
            cb.calculateDigest();
        }
    }
}