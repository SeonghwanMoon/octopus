package com.skplanet.data.uploader;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.log4j.Logger;

/**
 * Created by sofangel on 14. 10. 31.
 */
public class Uploader {

    private Logger logger = Logger.getLogger(getClass());

    private ArrayList<Thread> threads = new ArrayList<Thread>();

    public static void main(String[] args) {
        Uploader up = new Uploader();
        Thread tr = null;

        long start = System.currentTimeMillis();

        InitialSequence is = new InitialSequence();
        is.loadParameter();
        is.loadDriver();

        try {
            is.loadFile();
        } catch (IOException e) {
            up.logger.error(e);
        }

        for (int i = 0; i < is.nNumOfThread; i++) {
            tr = new Thread(new UploadThread(is));
            tr.start();
            up.threads.add(tr);
        }

        for (Thread temp : up.threads) {
            try {
                temp.join();
            } catch (InterruptedException e) {
                up.logger.error(e.getMessage());
            }
        }

        try {
            is.fs.close();
            up.logger.info("Total Elapsed time: " + (System.currentTimeMillis() - start));
        } catch (IOException e) {
            up.logger.error(e.getMessage());
        }
    }

}
