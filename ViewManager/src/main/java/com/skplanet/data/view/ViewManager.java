package com.skplanet.data.view;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.skplanet.data.uploader.InitialSequence;
import com.skplanet.data.uploader.UploadThread;
import com.skplanet.data.uploader.Uploader;
import org.apache.log4j.Logger;

/**
 * Created by sofangel on 14. 11. 24.
 */
public class ViewManager {
    private static Logger logger = Logger.getLogger(ViewManager.class);

    public static void main(String[] args) {

        Uploader up = new Uploader();
        ArrayList<Thread> threads = new ArrayList<Thread>();
        long start = System.currentTimeMillis();
        int totalWriteLine[] = new int[2];
        InitialSequence is = new InitialSequence();
        int nExitValue = 0 ;
        List<Future<int[]>> list = new ArrayList<Future<int[]>>();
        ExecutorService executor = null;

        is.loadParameter();
        is.loadDriver();
        ViewOp vo = new ViewOp(is);

        try {
            vo.createView();
            is.loadFile();
            executor = Executors.newFixedThreadPool(is.NumOfThread);
            Callable<int[]> callable = new UploadThread(is);
            for (int i = 0; i < is.NumOfThread; i++) {
                Future<int[]> future = executor.submit(callable);
                list.add(future);
            }
            for (Future<int[]> temp : list) {
                totalWriteLine[0] += temp.get()[0];
                totalWriteLine[1] += temp.get()[1];
            }
            vo.deleteView();
        } catch (Exception e) {
            executor.shutdown();
            nExitValue = 1;
            vo.rollBackView();
        }
        finally {
            try {
                if(is.bHdfs)
                    is.fs.close();
                logger.info("Total Elapsed time: " + (System.currentTimeMillis() - start));
            } catch (Exception e) {
                e.printStackTrace();
                logger.error(e.getMessage());
            }
            if(nExitValue==0) {
                logger.info("적재건수:" + totalWriteLine[0] + ", 스킵건수:" + totalWriteLine[1]);
                System.out.println("적재건수:" + totalWriteLine[0] + ", 스킵건수:" + totalWriteLine[1]);
            }
            System.exit(nExitValue);
        }


    }
}
