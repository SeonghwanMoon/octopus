package com.skplanet.data.uploader;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.StringTokenizer;
import java.util.concurrent.Callable;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

/**
 * Created by sofangel on 14. 10. 31.
 */
public class UploadThread implements Callable<int[]> {
    private Logger logger = Logger.getLogger(getClass());

    private InitialSequence is = null;

    public UploadThread(InitialSequence is) {
        super();
        this.is = is;
        return;
    }

    private Connection getConnection() {

        Connection conn = null;
        try {
            conn = DriverManager.getConnection(is.sConString);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
        return conn;
    }

    private PreparedStatement codeGen(PreparedStatement pstmt, InitialSequence.Type[] type,
            String str) throws Exception {
        try {
            StringTokenizer stk = new StringTokenizer(str, is.sDelimiter);
            int cnt = 1;
            String strtkn = null;
            for (InitialSequence.Type temp : type) {
                strtkn = stk.nextToken();
                switch (temp) {
                case INTEGER:
                    pstmt.setInt(cnt, Integer.valueOf(strtkn));
                    break;
                case UNSIGNED_INT:
                    pstmt.setInt(cnt, Integer.valueOf(strtkn));
                    break;
                case BIGINT:
                    pstmt.setLong(cnt, Long.valueOf(strtkn));
                    break;
                case UNSIGNED_LONG:
                    pstmt.setLong(cnt, Long.valueOf(strtkn));
                    break;
                case TINYINT:
                    pstmt.setByte(cnt, Byte.valueOf(strtkn));
                    break;
                case UNSIGNED_TINYINT:
                    pstmt.setByte(cnt, Byte.valueOf(strtkn));
                    break;
                case SMALLINT:
                    pstmt.setShort(cnt, Short.valueOf(strtkn));
                    break;
                case UNSIGNED_SMALLINT:
                    pstmt.setShort(cnt, Short.valueOf(strtkn));
                    break;
                case FLOAT:
                    pstmt.setFloat(cnt, Float.valueOf(strtkn));
                    break;
                case UNSIGNED_FLOAT:
                    pstmt.setFloat(cnt, Float.valueOf(strtkn));
                    break;
                case DOUBLE:
                    pstmt.setDouble(cnt, Double.valueOf(strtkn));
                    break;
                case UNSIGNED_DOUBLE:
                    pstmt.setDouble(cnt, Double.valueOf(strtkn));
                    break;
                case DECIMAL:
                    pstmt.setBigDecimal(cnt, new BigDecimal(strtkn));
                    break;
                case BOOLEAN:
                    pstmt.setBoolean(cnt, Boolean.valueOf(strtkn));
                    break;
                case TIME:
                    pstmt.setTime(cnt, Time.valueOf(strtkn));
                    break;
                case DATE:
                    pstmt.setDate(cnt, Date.valueOf(strtkn));
                    break;
                case TIMESTAMP:
                    pstmt.setTimestamp(cnt, Timestamp.valueOf(strtkn));
                    break;
                case UNSIGNED_TIME:
                    pstmt.setTime(cnt, Time.valueOf(strtkn));
                    break;
                case UNSIGNED_DATE:
                    pstmt.setDate(cnt, Date.valueOf(strtkn));
                    break;
                case UNSIGNED_TIMESTAMP:
                    pstmt.setTimestamp(cnt, Timestamp.valueOf(strtkn));
                    break;
                case VARCHAR:
                    pstmt.setString(cnt, strtkn);
                    break;
                case CHAR:
                    pstmt.setString(cnt, strtkn);
                    break;
                case BINARY:
                    pstmt.setBytes(cnt, strtkn.getBytes());
                    break;
                case VARBINARY:
                    pstmt.setBytes(cnt, strtkn.getBytes());
                    break;
                default:
                    logger.error("Type Error, Check the bdb.properties or Table column type");
                    break;
                }
                cnt++;
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            throw new Exception(e);
        }
        return pstmt;
    }

    private int[] UploadFromLocal() throws Exception {
        Connection conn = null;
        PreparedStatement pstmt = null;
        int[] readLine = new int[2];
        logger.info("UploadFromLocal");
        try {
            conn = getConnection();
            conn.setAutoCommit(false);

            File file = null;
            pstmt = conn.prepareStatement(is.sQuery1 + " " + is.uTable + " " + is.sQuery2);

            while ((file = (File) is.clq.poll()) != null) {
                file = (File) is.clq.poll();

                FileInputStream f = new FileInputStream(file);
                FileChannel fc = f.getChannel();
                MappedByteBuffer mb = fc.map(FileChannel.MapMode.READ_ONLY, 0l, fc.size());

                logger.info("File channel sizes : " + fc.size());
                long start = System.currentTimeMillis();

                byte[] buffer = new byte[(int) fc.size()];
                mb.get(buffer);
                f.close();
                BufferedReader
                        in =
                        new BufferedReader(new InputStreamReader(new ByteArrayInputStream(buffer)));

                for (String line = in.readLine(); line != null; line = in.readLine()) {
                    try {
                        pstmt = codeGen(pstmt, is.sType, line);
                        pstmt.executeUpdate();
                        if (readLine[0] % 10000 == 9999)
                            conn.commit();
                        readLine[0]++;
                    } catch (Exception e) {
                        logger.error(e.getMessage());
                    }
                }
                System.out.println("rows:" + readLine[0]);

                f.close();
                in.close();
                fc.close();

                return readLine;
            }
        } catch (Exception e) {
            readLine[0] = -1;
            e.printStackTrace();
            logger.error(e.getMessage());
            throw new Exception(e);
        } finally {
            try {
                if (conn != null)
                    conn.commit();
                if (pstmt != null)
                    pstmt.close();
                conn.close();
            } catch (SQLException e) {
                logger.error(e.getMessage());
            }
            return readLine;
        }
    }

    private int[] UploadFromHdfs() throws Exception {
        Connection conn = null;
        PreparedStatement pstmt = null;
        FSDataInputStream fis = null;
        BufferedReader br = null;
        FileStatus fss = null;
        logger.info("UploadFromHdfs");
        int readLine[] = new int[2];
        try {
            conn = getConnection();
            conn.setAutoCommit(false);
            pstmt = conn.prepareStatement(is.sQuery1 + " " + is.uTable + " " + is.sQuery2);

            while ((fss = (FileStatus) is.clq.poll()) != null) {

                is.fs = FileSystem.get(is.conf);
                fis = is.fs.open(fss.getPath());

                br = new BufferedReader(new InputStreamReader(fis));
                long start = System.currentTimeMillis();

                for (String line = br.readLine(); line != null; line = br.readLine()) {
                    try {
                        pstmt = codeGen(pstmt, is.sType, line);
                        pstmt.executeUpdate();
                        if (readLine[0] % 10000 == 9999)
                            conn.commit();
                        readLine[0]++;
                    } catch (Exception e) {
//                        pstmt =
//                                conn.prepareStatement(
//                                        is.sQuery1 + " " + is.uTable + " " + is.sQuery2);
                        if (is.bStopByError) {
                            throw new Exception(e);
                        } else {
                            readLine[1]++;
                            continue;
                        }
                    }
                }
                conn.commit();
                logger.info("rows:" + readLine[0]);

                if (br != null)
                    br.close();
            }
            if (pstmt != null)
                pstmt.close();
            conn.close();
            return readLine;
        } catch (Exception e) {
            try {
                if (br != null)
                    br.close();
                if (conn != null)
                    conn.commit();
                if (pstmt != null)
                    pstmt.close();
                conn.close();
            } catch (Exception e2) {
                e2.printStackTrace();
                logger.error(e2);
            }
            e.printStackTrace();
            logger.error(e.getMessage());
            throw new Exception(e);
        }
        //        finally {
        //            try {
        //                if (br != null)
        //                    br.close();
        //                if (conn != null)
        //                    conn.commit();
        //                if (pstmt != null)
        //                    pstmt.close();
        //                conn.close();
        //            } catch (IOException e) {
        //                logger.error(e.getMessage());
        //            } catch (SQLException e) {
        //                logger.error(e.getMessage());
        //            }
        //            return readLine;
        //        }
    }

    @Override public int[] call() throws Exception {
        int ret[] = null;

        if (is.bHdfs)
            ret = UploadFromHdfs();
        else
            ret = UploadFromLocal();

        return ret;
    }
}
