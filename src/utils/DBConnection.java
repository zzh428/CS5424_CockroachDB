package utils;

import java.sql.*;
import java.util.Random;

import org.postgresql.ds.PGSimpleDataSource;

public class DBConnection {
    private static final int MAX_RETRY_COUNT = 3;
    private static final String RETRY_SQL_STATE = "40001";
    private static final PGSimpleDataSource ds = new PGSimpleDataSource();
    private static final Random rand = new Random();

    public static void init(String[] serverNames) {
        ds.setServerNames(serverNames);
        ds.setUser("root");
        ds.setPassword(null);
        ds.setDatabaseName("wholesale");
        ds.setApplicationName("Wholesale");
    }

    public static Connection getConnection() {
        try {
            Connection c = ds.getConnection();
            return c;
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            return null;
        }
    }

    public static boolean commitTransaction(Connection c) {
        try {
            c.commit();
            return true;
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            return false;
        }
    }

    public static boolean rollbackTransaction(Connection c) {
        try {
            c.rollback();
            return true;
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            return false;
        }
    }


    /**
     * Run SQL code in a way that automatically handles the
     * transaction retry logic so we don't have to duplicate it in
     * various places.
     *
     * @param sqlCode a String containing the SQL code you want to
     * execute.  Can have placeholders, e.g., "INSERT INTO accounts
     * (id, balance) VALUES (?, ?)".
     *
     * @param args String Varargs to fill in the SQL code's
     * placeholders.
     * @return Integer Number of rows updated, or -1 if an error is thrown.
     */
    public static Integer runSQL(Connection connection, String sqlCode, String... args) {

        // This block is only used to emit class and method names in
        // the program output.  It is not necessary in production
        // code.
        StackTraceElement[] stacktrace = Thread.currentThread().getStackTrace();
        StackTraceElement elem = stacktrace[2];
        String callerClass = elem.getClassName();
        String callerMethod = elem.getMethodName();

        int rv = 0;

        try {

            // We're managing the commit lifecycle ourselves so we can
            // automatically issue transaction retries.
            connection.setAutoCommit(false);
            int retryCount = 0;

            while (retryCount <= MAX_RETRY_COUNT) {

                if (retryCount == MAX_RETRY_COUNT) {
                    String err = String.format("hit max of %s retries, aborting", MAX_RETRY_COUNT);
                    throw new RuntimeException(err);
                }

                try (PreparedStatement pstmt = connection.prepareStatement(sqlCode)) {

                    // Loop over the args and insert them into the
                    // prepared statement based on their types.  In
                    // this simple example we classify the argument
                    // types as "integers" and "everything else"
                    // (a.k.a. strings).
                    for (int i=0; i<args.length; i++) {
                        int place = i + 1;
                        String arg = args[i];

                        try {
                            int val = Integer.parseInt(arg);
                            pstmt.setInt(place, val);
                        } catch (NumberFormatException e) {
                            pstmt.setString(place, arg);
                        }
                    }

                    if (pstmt.execute()) {
                        // We know that `pstmt.getResultSet()` will
                        // not return `null` if `pstmt.execute()` was
                        // true
                        ResultSet rs = pstmt.getResultSet();
                        ResultSetMetaData rsmeta = rs.getMetaData();
                        int colCount = rsmeta.getColumnCount();

                        // This printed output is for debugging and/or demonstration
                        // purposes only.  It would not be necessary in production code.
                        System.out.printf("\n%s.%s:\n    '%s'\n", callerClass, callerMethod, pstmt);

                        while (rs.next()) {
                            for (int i=1; i <= colCount; i++) {
                                String name = rsmeta.getColumnName(i);
                                String type = rsmeta.getColumnTypeName(i);

                                // In this "bank account" example we know we are only handling
                                // integer values (technically 64-bit INT8s, the CockroachDB
                                // default).  This code could be made into a switch statement
                                // to handle the various SQL types needed by the application.
                                if (type == "int8") {
                                    int val = rs.getInt(name);

                                    // This printed output is for debugging and/or demonstration
                                    // purposes only.  It would not be necessary in production code.
                                    System.out.printf("    %-8s => %10s\n", name, val);
                                }
                            }
                        }
                    } else {
                        int updateCount = pstmt.getUpdateCount();
                        rv += updateCount;

                        // This printed output is for debugging and/or demonstration
                        // purposes only.  It would not be necessary in production code.
                        System.out.printf("\n%s.%s:\n    '%s'\n", callerClass, callerMethod, pstmt);
                    }

                    break;

                } catch (SQLException e) {

                    if (RETRY_SQL_STATE.equals(e.getSQLState())) {
                        // Since this is a transaction retry error, we
                        // roll back the transaction and sleep a
                        // little before trying again.  Each time
                        // through the loop we sleep for a little
                        // longer than the last time
                        // (A.K.A. exponential backoff).
                        System.out.printf("retryable exception occurred:\n    sql state = [%s]\n    message = [%s]\n    retry counter = %s\n", e.getSQLState(), e.getMessage(), retryCount);
                        connection.rollback();
                        retryCount++;
                        int sleepMillis = (int)(Math.pow(2, retryCount) * 100) + rand.nextInt(100);
                        System.out.printf("Hit 40001 transaction retry error, sleeping %s milliseconds\n", sleepMillis);
                        try {
                            Thread.sleep(sleepMillis);
                        } catch (InterruptedException ignored) {
                            // Necessary to allow the Thread.sleep()
                            // above so the retry loop can continue.
                        }

                        rv = -1;
                    } else {
                        rv = -1;
                        throw e;
                    }
                }
            }
        } catch (SQLException e) {
            System.out.printf("BasicExampleDAO.runSQL ERROR: { state => %s, cause => %s, message => %s }\n",
                    e.getSQLState(), e.getCause(), e.getMessage());
            rv = -1;
        }

        return rv;
    }

    public static ResultSet executeQuery(Connection conn, String sql) {
        try {
            return conn.createStatement().executeQuery(sql);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            return null;
        }
    }
}
