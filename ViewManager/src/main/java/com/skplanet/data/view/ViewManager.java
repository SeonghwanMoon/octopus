package com.skplanet.data.view;

import java.io.IOException;
import java.util.ArrayList;

import com.skplanet.data.uploader.InitialSequence;
import com.skplanet.data.uploader.UploadThread;
import com.skplanet.data.uploader.Uploader;
import org.apache.log4j.Logger;

/**
 * Created by sofangel on 14. 11. 24.
 */
public class ViewManager {
    private static Logger logger = Logger.getLogger(ViewManager.class);

    public static int main(String[] args) {

        int ret = 0;
        Uploader up = new Uploader();
        ArrayList<Thread> threads = new ArrayList<Thread>();
        long start = System.currentTimeMillis();

        InitialSequence is = new InitialSequence();

        is.loadParameter();
        is.loadDriver();
        ViewOp vo = new ViewOp(is);
        vo.createView();
        try {
            is.loadFile();
            for (int i = 0; i < is.nNumOfThread; i++) {
                Thread t = new Thread(new UploadThread(is));
                t.start();
                threads.add(t);
            }
            for (Thread temp : threads) {
                try {
                    temp.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    logger.error(e.getMessage());
                }
            }
        } catch (Exception e) {
            vo.rollBackView();
            System.exit(1);
        }
        vo.deleteView();
        try {
            is.fs.close();
            logger.info("Total Elapsed time: " + (System.currentTimeMillis() - start));
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
        return 0;
    }
}
