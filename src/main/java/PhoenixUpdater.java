    import java.io.BufferedReader;
    import java.io.File;
    import java.io.IOException;
    import java.io.InputStream;
    import java.sql.Connection;
    import java.sql.DriverManager;
    import java.sql.PreparedStatement;
    import java.sql.SQLException;
    import java.sql.Statement;
    import java.sql.Time;
    import java.util.Calendar;
    import java.util.Properties;
    import java.util.Random;

    /**
     * Created by sofangel on 14. 4. 17.
     */
    public class PhoenixUpdater {
        public int numOfThread = 0;
        public String serverAddress;
        public String table;
        public String location;
        public String file;
        private static int rows = 0;
        private static int NumOfMid = 0;
        private static int NumOfCi = 0;
        private static Random rd = new Random();

        Properties prop = new Properties();

        static {
            try {
                Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }

        public PhoenixUpdater() {
            InputStream is = this.getClass().getClassLoader()
                    .getResourceAsStream("bdb.properties");
            try {
                System.out.println("load properties");
                prop.load(is);
                serverAddress = prop.getProperty("server");
                table = prop.getProperty("table");
                numOfThread = Integer.parseInt(prop.getProperty("thread"));
                location = prop.getProperty("location");
                file = prop.getProperty("file");
                NumOfCi = Integer.parseInt(prop.getProperty("NumOfCi"));
                NumOfMid = Integer.parseInt(prop.getProperty("NumOfMid"));
                rows = Integer.parseInt(prop.getProperty("rows"));

                System.out.println("load properties end");
            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("No prop files");
                System.exit(0);
            }

        }

        public Connection getConnection() {
            // String getDBConnectionString =
            // "jdbc:phoenix:211.234.235.55:2181";
            String getDBConnectionString = "jdbc:phoenix:" + serverAddress;
            System.out.println(getDBConnectionString);
            try {
                Connection conn = DriverManager
                        .getConnection(getDBConnectionString);
                return conn;
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return null;
        }

        public class OCBUpdater implements Runnable {

            int fileNum;
            String file = null;
            File f = null;

            public OCBUpdater() {

                super();
            }

            public OCBUpdater(int num) {
                super();
                fileNum = num;
            }

            public OCBUpdater(String fileName) {
                super();
                if (fileName != null)
                    file = fileName;
                f = new File(file);
                System.out.println(file + ":" + f.exists());
                if (f.exists() != true)
                    throw new ExceptionInInitializerError();
            }

            private void genVisitHistoryQuery(String table, Connection conn, PreparedStatement pstmt) {
                try {
                    pstmt = conn.prepareStatement("upsert into visithistory values (?,?,?,?)");
                    pstmt.setString(1, genMid());
                    pstmt.setString(2, genCI());
                    pstmt.setTime(3, genTime());
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

            private PreparedStatement genUsrInfo2CIQuery(String table, Connection conn, PreparedStatement pstmt) {
                int type = rd.nextInt(3);
                try {
                    pstmt = conn.prepareStatement("upsert into userinfo values (?,?,?,?)");
                    pstmt.setString(1, type==0?genMdn():genMid());
                    pstmt.setString(2, genName());
                    pstmt.setInt(3, type);
                    pstmt.setString(4, genCI());

                    return pstmt;
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return null;
            }

            private PreparedStatement genSyrupInfo(String table, Connection conn, PreparedStatement pstmt) {
                int type = rd.nextInt(3);
                try {
                    pstmt = conn.prepareStatement("upsert into syrupinfo values (?,?)");

                    pstmt.setString(1, genMid());
                    pstmt.setString(2, genCI());

                    return pstmt;
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                return null;
            }
            private String genName() {
                byte[] bt = new byte[30];
                String temp = null;
                rd.nextBytes(bt);
                return bt.toString();
            }

            private String genMid() {
                return "mid" + Integer.toString(rd.nextInt(NumOfMid));
            }

            private String genMdn() {

                return "010" + Integer.toString((1+rd.nextInt(9))*10000000 + (1+rd.nextInt(9))*1000000 + (1+rd.nextInt(9)) *100000 + (1+rd.nextInt(9))*10000
                        +(1+rd.nextInt(9))*1000+ (1+rd.nextInt(9))*100 + (1+rd.nextInt(9))*10 + 1+rd.nextInt(9));
            }

            private String genCI() {
                return "CI" + Integer.toString(rd.nextInt(NumOfCi))
                        + "aaa";
            }

            private Time genTime() {
                Calendar cl = Calendar.getInstance();
                int year = 2014;
                int month = 4 + rd.nextInt(6);
                int date = 1 + rd.nextInt(29);
                int hour = rd.nextInt(24);
                int min = rd.nextInt(60);
                int sec = rd.nextInt(60);
                cl.set(year, month, date, hour, min, sec);
                return new Time(cl.getTimeInMillis());

            }

            public void run() {
                Connection conn = null;
                Statement stmt = null;
                int i = 0;
                System.out.println("Connected:" + this);
                String sql = null;
                BufferedReader reader = null;
                PreparedStatement pstmt = null;
                System.out.println("Total : " + rows);
                String temp[] = new String[2];

                try {
                    conn = getConnection();
                    //                PreparedStatement pstmt = null;
                    //                pstmt = conn.prepareStatement("upsert into ssa values (?,?,?)");
                    conn.setAutoCommit(false);

                    do {
    //                    pstmt = genUsrInfo2CIQuery(table, conn, pstmt);
                        pstmt = genSyrupInfo(table, conn, pstmt);
                        //                    pstmt.setString(1,genMid());
                        //                    pstmt.setString(2,genCI());
                        //                    pstmt.setTime(3,genTime());
                        i += pstmt.executeUpdate();
                        if (i % 20000 == 19999) {
                            conn.commit();
    //                        System.out.println("committed: "+ i);
                        }
                    }
                    while (i < rows);

                    //                if (file == null) {
                    //                    File f = new File(location + fileNum + file + String.valueOf(fileNum));
                    //                }
                    //
                    //                reader = new BufferedReader(new FileReader(f));
                    //                // int total = 0;
                    //
                    //
                    //                while ((temp = reader.readLine()) != null) {
                    ////                    System.out.println(temp);
                    //                    sql = "upsert into " + table + " values ( " + temp + " )";
                    ////                    System.out.println(sql);
                    //                    stmt.executeUpdate(sql);
                    //                    i++;
                    //                    if (i % 50000 == 0) {
                    //                        conn.commit();
                    //                        System.out.println("rows: " + i);
                    //                    }
                    //                }
                } catch (SQLException e1) {
                    System.out.println("ERR : FILE : " + file);
                    System.out.println("ERR : line : " + temp);
                    System.out.println("ERR : SQL : " + sql);
                    e1.printStackTrace();
                } finally {
                    try {
                        if (conn != null)
                            conn.commit();
                        if (pstmt != null)
                            pstmt.close();
                    } catch (SQLException e) {
                        System.out.println("ERR : FILE : " + file);
                        System.out.println("ERR : line : " + temp);
                        System.out.println("ERR : SQL : " + sql);
                        e.printStackTrace();
                    }
                    System.out.println("cnt: " + i);

                }

            }

        }

        public static void main(String[] args) {
            System.out.println("start");

            long start = System.currentTimeMillis();

            PhoenixUpdater pu = new PhoenixUpdater();
            // System.out.println(getValue());lo
            // pu.loadProp();
            //        for (int i = 0 ; i < pu.threadNum; i ++)
            for (int i = 0; i < pu.numOfThread; i++) {
                Thread t = new Thread(pu.new OCBUpdater(i + 1));
                t.start();
            }

            //        new Thread(pu.new OCBUpdater("/data01/temp/ocb1.csv")).start();
            //        new Thread(pu.new OCBUpdater("/data02/temp/ocb2.csv")).start();
            //        new Thread(pu.new OCBUpdater("/data03/temp/ocb3.csv")).start();
            //        new Thread(pu.new OCBUpdater("/data04/temp/ocb4.csv")).start();
            //        new Thread(pu.new OCBUpdater("/data05/temp/ocb5.csv")).start();
            //        new Thread(pu.new OCBUpdater("/data06/temp/ocb6.csv")).start();
            //        new Thread(pu.new OCBUpdater("/data07/temp/ocb7.csv")).start();
            //        new Thread(pu.new OCBUpdater("/data08/temp/ocb8.csv")).start();
            //        new Thread(pu.new OCBUpdater("/data09/temp/ocb9.csv")).start();
            //        new Thread(pu.new OCBUpdater("/data10/temp/ocb10.csv")).start();
            //        new Thread(pu.new OCBUpdater("/data11/temp/ocb11.csv")).start();
            //        new Thread(pu.new OCBUpdater("/data12/temp/ocb12.csv")).start();
            //        new Thread(pu.new OCBUpdater("/data01/temp/ocb13.csv")).start();
            //        new Thread(pu.new OCBUpdater("/data02/temp/ocb14.csv")).start();
            //        new Thread(pu.new OCBUpdater("/data03/temp/ocb15.csv")).start();
            //        new Thread(pu.new OCBUpdater("/data04/temp/ocb16.csv")).start();
            //        new Thread(pu.new OCBUpdater("/data05/temp/ocb17.csv")).start();
            //        new Thread(pu.new OCBUpdater("/data06/temp/ocb18.csv")).start();
            //        new Thread(pu.new OCBUpdater("/data07/temp/ocb19.csv")).start();
            //        new Thread(pu.new OCBUpdater("/data08/temp/ocb20.csv")).start();
            //        new Thread(pu.new OCBUpdater()).start();
            //        new Thread(pu.new OCBUpdater()).start();

            //        new Thread(pu.new OCBUpdater("/data2/tral")).start();
            //        new Thread(pu.new OCBUpdater("/data2/tram")).start();
            //        new Thread(pu.new OCBUpdater("/data2/tran")).start();
            //        new Thread(pu.new OCBUpdater("/data2/trao")).start();

            //        new Thread(pu.new OCBUpdater("/home/hdfs/temp/xaa")).start();
            //        new Thread(pu.new OCBUpdater("/home/hdfs/temp/xab")).start();
            //        new Thread(pu.new OCBUpdater("/home/hdfs/temp/xac")).start();
            //        new Thread(pu.new OCBUpdater("/home/hdfs/temp/xad")).start();
            //        new Thread(pu.new OCBUpdater("/home/hdfs/temp/xae")).start();
            //        new Thread(pu.new OCBUpdater("/home/hdfs/temp/xaf")).start();
            //        new Thread(pu.new OCBUpdater("/home/hdfs/temp/xag")).start();
            //        new Thread(pu.new OCBUpdater("/home/hdfs/temp/xah")).start();
            //        new Thread(pu.new OCBUpdater("/home/hdfs/temp/xai")).start();
            //        new Thread(pu.new OCBUpdater("/home/hdfs/temp/xaj")).start();

            //        new Thread(pu.new OCBUpdater("/app/temp/xaa"), "test1").start();
            //        new Thread(pu.new OCBUpdater("/app/temp/xab"), "test2").start();
            //        new Thread(pu.new OCBUpdater("/app/temp/xac"), "test3").start();
            //        new Thread(pu.new OCBUpdater("/app/temp/xad"), "test4").start();
            //        new Thread(pu.new OCBUpdater("/app/temp/xae"), "test5").start();
            //        new Thread(pu.new OCBUpdater("/app/temp/xaf"), "test6").start();
            //        new Thread(pu.new OCBUpdater("/app/temp/xag"), "test7").start();
            //        new Thread(pu.new OCBUpdater("/app/temp/xah"), "test8").start();
            //        new Thread(pu.new OCBUpdater("/app/temp/xai"), "test9").start();
            //        new Thread(pu.new OCBUpdater("/app/temp/xaj"), "test10").start();
            //
            //
            //        new Thread(pu.new OCBUpdater("/app/temp/yaa"), "test11").start();
            //        new Thread(pu.new OCBUpdater("/app/temp/yab"), "test12").start();
            //        new Thread(pu.new OCBUpdater("/app/temp/yac"), "test13").start();
            //        new Thread(pu.new OCBUpdater("/app/temp/yad"), "test14").start();
            //        new Thread(pu.new OCBUpdater("/app/temp/yae"), "test15").start();
            //        new Thread(pu.new OCBUpdater("/app/temp/yaf"), "test16").start();
            //        new Thread(pu.new OCBUpdater("/app/temp/yag"), "test17").start();
            //        new Thread(pu.new OCBUpdater("/app/temp/yah"), "test18").start();
            //        new Thread(pu.new OCBUpdater("/app/temp/yai"), "test19").start();
            //        new Thread(pu.new OCBUpdater("/app/temp/yaj"), "test20").start();
            //        new Thread(pu.new OCBUpdater("/app/temp/yak"), "test21").start();

            //        System.out.println(System.currentTimeMillis() - start);

            // pu.updateOCBData();

        }

    }
