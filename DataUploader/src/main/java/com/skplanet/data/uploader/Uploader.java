package com.skplanet.data.uploader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

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
        int totalWriteLine[] = new int [2];

        long start = System.currentTimeMillis();

        InitialSequence is = new InitialSequence();
        is.loadParameter();
        is.loadDriver();

        try {
            is.loadFile();
        } catch (IOException e) {
            up.logger.error(e);
        }

        try {
            ExecutorService executor = Executors.newFixedThreadPool(is.nNumOfThread);

            List<Future<int[]>> list = new ArrayList<Future<int[]>>();

            Callable<int[]> callable = new UploadThread(is);

            for (int i = 0; i < is.nNumOfThread; i++) {
                Future<int[]> future = executor.submit(callable);
                list.add(future);
            }

            for (Future<int[]> temp : list) {
                totalWriteLine[0] += temp.get()[0];
                totalWriteLine[1] += temp.get()[1];
            }

        } catch (Exception e) {
            System.exit(1);
        }







        try {
            is.fs.close();
            up.logger.info("Total Elapsed time: " + (System.currentTimeMillis() - start));
            up.logger.info("적재건수:"+totalWriteLine[0]+ "스킵건수:"+ totalWriteLine[1]);
        } catch (IOException e) {
            up.logger.error(e.getMessage());
            System.exit(1);
        }
    }

}
