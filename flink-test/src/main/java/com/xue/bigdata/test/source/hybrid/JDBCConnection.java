package com.xue.bigdata.test.source.hybrid;

import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

public class JDBCConnection implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(JDBCConnection.class);

    public static JDBCConnection openJdbcConnection(JDBCConfig config) {
        JDBCConnection jdbc = new JDBCConnection(config);
        try {
            jdbc.connect();
        } catch (Exception e) {
            LOG.error("Failed to open JDBC connection", e);
            throw new RuntimeException(e);
        }
        return jdbc;
    }

    private JDBCConfig jdbcConfig;

    private Connection conn;

    public JDBCConnection(JDBCConfig config) {
        this.jdbcConfig = config;
    }

    public JDBCConnection connect() throws Exception {
        Class.forName(jdbcConfig.getDriver());
        conn = DriverManager.getConnection(jdbcConfig.getUrl(), jdbcConfig.getUsername(), jdbcConfig.getPassword());
        return this;
    }

    public List<JSONObject> findAll(String sql) throws Exception {
        Statement statement = conn.createStatement();
        ResultSet rs = statement.executeQuery(sql);
        List<JSONObject> records = new ArrayList<>();
        // 获取元数据信息
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        // 获取数据
        while (rs.next()) {
            JSONObject jsonObject = new JSONObject();
            for (int i = 1; i <= columnCount; i++) {
                jsonObject.put(metaData.getColumnName(i), rs.getObject(i));
            }
            records.add(jsonObject);
        }
        // 关闭资源
        if (!Objects.isNull(statement)) {
            statement.close();
        }
        if (!Objects.isNull(rs)) {
            rs.close();
        }
        return records;
    }

    @Override
    public void close() throws Exception {
        if (Objects.isNull(conn)) {
            return;
        }
        conn.close();
    }
}
