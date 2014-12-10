package com.skplanet.data.uploader;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentLinkedDeque;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 * Created by sofangel on 14. 10. 31.
 */
public class InitialSequence {
    //    String sServerAddress = null;
    public int nNumOfThread = 0;
    String sLocation = null;
    public boolean bHdfs = false;
    String sHdfsConf = null;
    String sHdfsUrl = null;
    String sDriver = null;
    Properties prop = new Properties();
    File[] sFile = null;
    ConcurrentLinkedDeque clq = new ConcurrentLinkedDeque<Object>();
    public String sConString = null;
    String sQuery1 = null;
    String sQuery2 = null;
    Type[] sType = null;
    String sDelimiter = null;
    public String sTable = null;
    public String uTable = null;
    public String[] sTableArray = new String[2];
    public FileSystem fs = null;
    public Configuration conf = null;
    public boolean bStopByError = false;

    enum Type {
        INTEGER,
        UNSIGNED_INT,
        BIGINT,
        UNSIGNED_LONG,
        TINYINT,
        UNSIGNED_TINYINT,
        SMALLINT,
        UNSIGNED_SMALLINT,
        FLOAT,
        UNSIGNED_FLOAT,
        DOUBLE,
        UNSIGNED_DOUBLE,
        DECIMAL,
        BOOLEAN,
        TIME,
        DATE,
        TIMESTAMP,
        UNSIGNED_TIME,
        UNSIGNED_DATE,
        UNSIGNED_TIMESTAMP,
        VARCHAR,
        CHAR,
        BINARY,
        VARBINARY
    }

    ;

    private static final Logger logger = Logger.getLogger(InitialSequence.class);

    public void loadParameter() {

        InputStream is = this.getClass().getClassLoader().getResourceAsStream("bdb.properties");
        StringTokenizer stk = null;
        String temp = null;
        int cnt = 0;
        try {
            logger.info("load properties");
            prop.load(is);
            nNumOfThread = Integer.parseInt(prop.getProperty("NumOfThread"));
            sLocation = prop.getProperty("Location");
            sHdfsConf = prop.getProperty("HdfsConf");
            sDriver = prop.getProperty("Driver");
            bHdfs = Integer.parseInt(prop.getProperty("StorageType")) == 1 ? true : false;
            sHdfsUrl = prop.getProperty("HdfsUrl");
            sConString = prop.getProperty("ConString");
            sQuery1 = prop.getProperty("Query1");
            sQuery2 = prop.getProperty("Query2");
            sTable = prop.getProperty("Table");
            sTableArray[0] = prop.getProperty("Table") + "1";
            sTableArray[1] = prop.getProperty("Table") + "2";
            stk = new StringTokenizer(prop.getProperty("Type"), ",");
            sType = new Type[stk.countTokens()];
            bStopByError = Boolean.parseBoolean(prop.getProperty("ExitByError"));

            while (stk.hasMoreTokens()) {
                temp = stk.nextToken();
                sType[cnt] = Type.valueOf(temp);
                cnt++;
            }
            sDelimiter = prop.getProperty("Delimiter");
            logger.info("load properties end");
        } catch (IOException e) {
            logger.error(e.getMessage());
            logger.error("Sequence: No prop files. Check the file 'bdb.properties'");
            System.exit(1);
        }
    }

    public void loadDriver() {
        try {
            Class.forName(sDriver);
        } catch (ClassNotFoundException e) {
            logger.error(e.getMessage());
            logger.error("Sequence: No suitable Driver class. Check Driver");
            System.exit(1);
        }
    }

    public void loadFile() throws IOException{
        if (bHdfs)
            loadHdfsFile();
        else
            loadLocalFile();
    }

    public void loadLocalFile() {
        try {
            File file = new File(sLocation);
            if (file.exists()) {
                sFile = file.listFiles();
                for (File temp : sFile) {
                    clq.add(temp);
                }
            }
            System.out.println("Queue Size : " + clq.size());

        } catch (Exception e) {
            logger.error(e.getMessage());
            logger.error("Sequence: File loading.  Check location of file");
            System.exit(1);
        }
    }

    public void loadHdfsFile() throws IOException {
        try {
            Configuration conf = new Configuration();
            conf.addResource(new Path(sHdfsConf + "/hdfs-site.xml"));
            conf.addResource(new Path(sHdfsConf + "/core-site.xml"));
            conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

            this.conf = conf;
            this.fs = FileSystem.get(conf);
            Path path = new Path(sLocation);
            FileStatus[] fsts = null;

            if (fs.exists(path)) {
                fsts = fs.listStatus(path);
                for (FileStatus temp : fsts) {
                    if (!temp.isDir()) {
                        clq.add(temp);
                    }
                }
            }
            System.out.println("Queue Size : " + clq.size());
        } catch (IOException e) {
            logger.error(e.getMessage());
            logger.error("Sequence: File loading.  Check location of HDFS file");
            throw new IOException(e);
        }
    }
}
