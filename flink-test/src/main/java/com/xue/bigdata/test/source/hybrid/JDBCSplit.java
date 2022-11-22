package com.xue.bigdata.test.source.hybrid;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.flink.api.connector.source.SourceSplit;

import java.util.Objects;

public class JDBCSplit implements SourceSplit {

    protected final String splitId;

    private final String sql;

    public JDBCSplit(String sql) {
        this.sql = sql;
        this.splitId = DigestUtils.md5Hex(sql);
    }

    public String getSql() {
        return sql;
    }

    @Override
    public String splitId() {
        return splitId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JDBCSplit that = (JDBCSplit) o;
        return Objects.equals(splitId, that.splitId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(splitId);
    }
}
