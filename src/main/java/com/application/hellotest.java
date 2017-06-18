package com.application;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Created by ubuntu2 on 6/11/17.
 */
public class hellotest {
    private static final Log LOG = LogFactory
            .getLog(hellotest.class);
    public static void main(String[] args) throws Exception {
        LOG.info("hello i am in hellotest!");
        System.out.println("hello");
        mySimpleAM am = new mySimpleAM();
        Thread amTread = new Thread(am);
        amTread.start();

        System.out.println("finish!");
    }
}
