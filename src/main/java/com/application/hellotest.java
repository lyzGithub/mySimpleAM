package com.application;

/**
 * Created by ubuntu2 on 6/11/17.
 */
public class hellotest {
    public static void main(String[] args) throws Exception {

        System.out.println("hello");
        mySimpleAM am = new mySimpleAM();
        Thread amTread = new Thread(am);
        amTread.start();
    }
}
