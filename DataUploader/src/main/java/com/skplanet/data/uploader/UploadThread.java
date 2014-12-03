package com.skplanet.data.uploader;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
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

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

/**
 * Created by sofangel on 14. 10. 31.
 */
public class UploadThread implements Runnable {
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
                                      String str) {
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
        } catch (SQLException e) {
            logger.error(e.getMessage());
        }
        return pstmt;
    }

    private void UploadFromLocal() {
        Connection conn = null;
        PreparedStatement pstmt = null;
        logger.info("UploadFromLocal");
        try {
            conn = getConnection();
            conn.setAutoCommit(false);

            File file = null;
            pstmt = conn.prepareStatement(is.sQuery1 + " " + is.uTable + " " + is.sQuery2);
            file = (File) is.clq.poll();

            FileInputStream f = new FileInputStream(file);
            FileChannel fc = f.getChannel();
            MappedByteBuffer mb = fc.map(FileChannel.MapMode.READ_ONLY, 0l, fc.size());

            logger.info("File channel sizes : " + fc.size());
            long start = System.currentTimeMillis();

            byte[] buffer = new byte[(int) fc.size()];
            mb.get(buffer);
            f.close();
            BufferedReader in = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(buffer)));

            int i = 0;
            for (String line = in.readLine(); line != null; line = in.readLine(), i++) {
                try {
                    pstmt = codeGen(pstmt, is.sType, line);
                    pstmt.executeUpdate();
                    if (i % 10000 == 9999)
                        conn.commit();
                } catch (SQLException e) {
                    logger.error(e.getMessage());
                }
            }
            System.out.println("rows:" + i);

            f.close();
            in.close();
            fc.close();
            System.out.println(System.currentTimeMillis() - start);

        } catch (SQLException e) {
            logger.error(e.getMessage());
        } catch (IOException e) {
            logger.error(e.getMessage());
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
        }
    }

    private void UploadFromHdfs() {
        Connection conn = null;
        PreparedStatement pstmt = null;
        FSDataInputStream fis = null;
        BufferedReader br = null;
//        FileSystem fs = null;
        FileStatus fss = null;
        logger.info("UploadFromHdfs");
        try {
            System.out.println(this);
            conn = getConnection();
            conn.setAutoCommit(false);
//            logger.info(is.sQuery1+" "+is.uTable+" "+is.sQuery2);
            pstmt = conn.prepareStatement(is.sQuery1 + " " + is.uTable + " " + is.sQuery2);
            while ((fss = (FileStatus) is.clq.poll()) != null) {
                int i = 0;
                is.fs = FileSystem.get(is.conf);
                fis = is.fs.open(fss.getPath());

                br = new BufferedReader(new InputStreamReader(fis));
                long start = System.currentTimeMillis();

                for (String line = br.readLine(); line != null; line = br.readLine(), i++) {

                    try {
                        pstmt = codeGen(pstmt, is.sType, line);
                        pstmt.executeUpdate();
                        if (i % 10000 == 9999)
                            conn.commit();
                    } catch (SQLException e) {
                        logger.error(e.getMessage());
                        continue;
                    }
                }
                conn.commit();
                logger.info("rows:" + i);

                if (br != null)
                    br.close();
                logger.info(System.currentTimeMillis() - start);
            }
        } catch (SQLException e) {
            logger.error(e.getMessage());
        } catch (IOException e) {
            logger.error(e.getMessage());
        } finally {
            try {
                if (br != null)
                    br.close();
                if (conn != null)
                    conn.commit();
                if (pstmt != null)
                    pstmt.close();
                conn.close();
            } catch (IOException e) {
                logger.error(e.getMessage());
            } catch (SQLException e) {
                logger.error(e.getMessage());
            }
        }
    }

    @Override
    public void run() {
        if (is.bHdfs) {
            UploadFromHdfs();
        } else
            UploadFromLocal();
    }
}
