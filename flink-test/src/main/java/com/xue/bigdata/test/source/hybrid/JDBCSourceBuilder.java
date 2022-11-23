package com.xue.bigdata.test.source.hybrid;

import java.util.List;

public class JDBCSourceBuilder {
    private final JDBCConfig config = new JDBCConfig();

    public JDBCSourceBuilder port(int port) {
        config.setPort(port);
        return this;
    }

    public JDBCSourceBuilder hostname(String hostname) {
        config.setHostname(hostname);
        return this;
    }

    public JDBCSourceBuilder username(String username) {
        config.setUsername(username);
        return this;
    }

    public JDBCSourceBuilder password(String password) {
        config.setPassword(password);
        return this;
    }

    public JDBCSourceBuilder driver(String driver) {
        config.setDriver(driver);
        return this;
    }

    public JDBCSourceBuilder url(String url) {
        config.setUrl(url);
        return this;
    }

    public JDBCSourceBuilder fetchSize(int fetchSize) {
        config.setFetchSize(fetchSize);
        return this;
    }

    public JDBCSourceBuilder sqlList(List<TableSelect> sqlList) {
        config.setSqlList(sqlList);
        return this;
    }

    public JDBCSource build() {
        return new JDBCSource(config);
    }
}
