package com.personal.oyl.sample.stream.sink;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;

/**
 * @author OuYang Liang
 * @since 2019-09-05
 */
public final class DBPersister {

    private static final String sql = "insert into `user_statistics`(cust_id, `minute`, num_of_orders, total_order_amt ) values(?,?,?,?);";
    private static final String sql2 = "insert into `minute_statistics`(`minute`, num_of_orders, total_order_amt , num_of_ordered_customer) values(?,?,?,?);";

    private volatile static Connection conn = null;

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private void open() throws SQLException {
        if (null == conn) {
            synchronized (DBPersister.class) {
                if (null == conn) {
                    conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/streaming?characterEncoding=UTF-8", "root", "password");
                }
            }
        }
    }

    public void close() throws SQLException {
        if (null != conn) {
            conn.close();
        }
    }

    public void save(UserStatistics userStatistics) throws SQLException {
        this.open();
        conn.setAutoCommit(false);
        PreparedStatement p = null;

        try {
            p = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);

            p.setInt(1, userStatistics.getCustId());
            p.setString(2, userStatistics.getMinute());
            p.setLong(3, userStatistics.getNumOfOrders());
            p.setBigDecimal(4, userStatistics.getOrderAmt());

            p.executeUpdate();

            conn.commit();
        } finally {
            if (null != p) {
                p.close();
            }
        }
    }

    public void save(Statistics statistics) throws SQLException {
        this.open();
        conn.setAutoCommit(false);
        PreparedStatement p = null;

        try {
            p = conn.prepareStatement(sql2, Statement.RETURN_GENERATED_KEYS);

            p.setString(1, statistics.getMinute());
            p.setLong(2, statistics.getNumOfOrders());
            p.setBigDecimal(3, statistics.getOrderAmt());
            p.setLong(4, statistics.getNumOfOrderedCustomers());

            p.executeUpdate();

            conn.commit();
        } finally {
            if (null != p) {
                p.close();
            }
        }
    }
}
