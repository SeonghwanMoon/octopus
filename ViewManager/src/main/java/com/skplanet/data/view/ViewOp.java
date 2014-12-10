package com.skplanet.data.view;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.skplanet.data.uploader.InitialSequence;
import org.apache.log4j.Logger;

/**
 * Created by sofangel on 14. 11. 24.
 */
public class ViewOp {

    private Logger logger = Logger.getLogger(getClass());

    private String eTable = null;
    private String vTable = null;

    InitialSequence is = null;

    ViewOp(InitialSequence is) {
        this.is = is;
    }

    private Connection getConnection() {

        Connection conn = null;
        try {
            conn = DriverManager.getConnection(this.is.sConString);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e);
        }
        return conn;
    }

    public void rollBackView() {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        try {
            conn = getConnection();
            pstmt = conn.prepareStatement("select * from  " + vTable + " limit 1");
            rs = pstmt.executeQuery();

            pstmt = conn.prepareStatement("drop table " + vTable);
            pstmt.executeUpdate();

        } catch (SQLException e) {
            logger.error(e.getMessage());
        } finally {
            try {
                if (pstmt != null)
                    pstmt.close();
                if (conn != null)
                    conn.close();
            } catch (SQLException e) {
                logger.error(e.getMessage());
            }
        }
    }

    public void createView() {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        try {
            conn = getConnection();
            File file = null;
            if (is.sTable.equals("istore.userinfo")) {
                pstmt = conn.prepareStatement("select * from " + is.sTableArray[0] + " limit 1");
                try {
                    rs = pstmt.executeQuery();
                    rs.next();
                } catch (Exception e) {
                    logger.error(e.getMessage());
                }
                if (rs != null) {
                    pstmt =
                            conn.prepareStatement("create table " + is.sTableArray[1]
                                    + " (userinfo varchar (100) not null,name varchar(200) , type integer not null, ci varchar(88) not null, last_auth_dt varchar(14) constraint pk primary key (userinfo, type, ci)) SALT_BUCKETS=15,IMMUTABLE_ROWS=FALSE ");
                    pstmt.executeUpdate();
                    eTable = is.sTableArray[0];
                    vTable = is.sTableArray[1];
                    is.uTable = vTable;

                } else {
                    pstmt =
                            conn.prepareStatement("create table " + is.sTableArray[0]
                                    + " (userinfo varchar (100) not null,name varchar(200) , type integer not null, ci varchar(88) not null, last_auth_dt varchar(14) constraint pk primary key (userinfo, type, ci)) SALT_BUCKETS=15,IMMUTABLE_ROWS=FALSE ");
                    pstmt.executeUpdate();
                    eTable = is.sTableArray[1];
                    vTable = is.sTableArray[0];
                    is.uTable = vTable;
                }
            } else {
                pstmt = conn.prepareStatement("select * from " + is.sTableArray[0] + " limit 1");
                logger.info(pstmt);
                try {
                    rs = pstmt.executeQuery();
                    rs.next();
                } catch (Exception e) {
                    logger.error(e.getMessage());
                }
                if (rs != null) {
                    pstmt =
                            conn.prepareStatement("create table " + is.sTableArray[1]
                                    + "  (mbrid varchar(100) not null, ci varchar(88) not null, mdn varchar(100), name varchar(100), last_auth_dt varchar(14) constraint pk primary key (mbrid,ci)) SALT_BUCKETS=15,IMMUTABLE_ROWS=FALSE");
                    pstmt.executeUpdate();
                    eTable = is.sTableArray[0];
                    vTable = is.sTableArray[1];
                    is.uTable = vTable;
                } else {
                    pstmt =
                            conn.prepareStatement("create table " + is.sTableArray[0]
                                    + "  (mbrid varchar(100) not null, ci varchar(88) not null, mdn varchar(100), name varchar(100), last_auth_dt varchar(14) constraint pk primary key (mbrid,ci)) SALT_BUCKETS=15,IMMUTABLE_ROWS=FALSE");
                    pstmt.executeUpdate();
                    eTable = is.sTableArray[1];
                    vTable = is.sTableArray[0];
                    is.uTable = vTable;
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        } finally {
            try {
                if (pstmt != null)
                    pstmt.close();
                if (conn != null)
                    conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public void deleteView() {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        int ret = 0;
        try {
            conn = getConnection();
            File file = null;

            pstmt = conn.prepareStatement("drop view " + is.sTable);
            try {
                ret = pstmt.executeUpdate();
                logger.info("View :" + eTable + " deleted");
            } catch (SQLException e) {
                logger.error(e.getMessage());
            }

            pstmt = conn.prepareStatement(
                    "create view " + is.sTable + " as select * from " + vTable);
            try {
                pstmt.executeUpdate();
                logger.info("Table :" + vTable + " created ");
            } catch (SQLException e) {
                logger.error(e.getMessage());
            }

            pstmt = conn.prepareStatement("drop table " + eTable);
            try {
                ret = pstmt.executeUpdate();
                logger.info("Table :" + eTable + " deleted");
            } catch (SQLException e) {
                logger.error(e.getMessage());
            }
        } catch (SQLException e) {
            logger.error(e.getMessage());
        } finally {
            try {
                if (pstmt != null)
                    pstmt.close();
                if (conn != null)
                    conn.close();
            } catch (SQLException e) {
                logger.error(e.getMessage());
            }
        }
    }
}
