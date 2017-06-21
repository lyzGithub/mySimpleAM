package com.application.myApp;

/**
 * Created by ubuntu2 on 6/21/17.
 */
public class testAm {
    public static void main(String[] args){
        MySimpleAM mySimpleAM = new MySimpleAM();
        Thread t1 = new Thread(mySimpleAM);
        t1.start();
    }
}
