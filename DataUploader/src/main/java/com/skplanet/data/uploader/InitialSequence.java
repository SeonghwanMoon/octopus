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
    public int NumOfThread = 0;
    String Location = null;
    public boolean bHdfs = false;
    String HdfsConf = null;
    String HdfsUrl = null;
    String Driver = null;
    Properties prop = new Properties();
    File[] fileArray = null;
    ConcurrentLinkedDeque clq = new ConcurrentLinkedDeque<Object>();
    public String ConString = null;
    String query1 = null;
    String query2 = null;
    Type[] typeArray = null;
    String sDelimiter = null;
    public String viewTable = null;
    public String realTable = null;
    public String[] TableArray = new String[2];
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
            NumOfThread = Integer.parseInt(prop.getProperty("NumOfThread"));
            Location = prop.getProperty("Location");
            HdfsConf = prop.getProperty("HdfsConf");
            Driver = prop.getProperty("Driver");
            bHdfs = Integer.parseInt(prop.getProperty("StorageType")) == 1 ? true : false;
            HdfsUrl = prop.getProperty("HdfsUrl");
            ConString = prop.getProperty("ConString");
            query1 = prop.getProperty("Query1");
            query2 = prop.getProperty("Query2");
            viewTable = prop.getProperty("Table");
            realTable = prop.getProperty("Table");
            TableArray[0] = prop.getProperty("Table") + "1";
            TableArray[1] = prop.getProperty("Table") + "2";
            stk = new StringTokenizer(prop.getProperty("Type"), ",");
            typeArray = new Type[stk.countTokens()];
            bStopByError = Boolean.parseBoolean(prop.getProperty("ExitByError"));

            while (stk.hasMoreTokens()) {
                temp = stk.nextToken();
                typeArray[cnt] = Type.valueOf(temp);
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
            Class.forName(Driver);
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
            File file = new File(Location);
            if (file.exists()) {
                fileArray= file.listFiles();
                for (File temp : fileArray) {
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
            conf.addResource(new Path(HdfsConf + "/hdfs-site.xml"));
            conf.addResource(new Path(HdfsConf + "/core-site.xml"));
            conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

            this.conf = conf;
            this.fs = FileSystem.get(conf);
            Path path = new Path(Location);
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
