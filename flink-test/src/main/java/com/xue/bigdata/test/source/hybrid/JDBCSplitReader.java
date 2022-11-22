package com.xue.bigdata.test.source.hybrid;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

public class JDBCSplitReader implements SplitReader<JSONObject, JDBCSplit> {

    private JDBCConfig jdbcConfig;

    private final Queue<JDBCSplit> splits;

    public JDBCSplitReader(JDBCConfig config) {
        this.jdbcConfig = config;
        this.splits = new LinkedList<>();
    }

    @Override
    public RecordsWithSplitIds<JSONObject> fetch() throws IOException {
        // todo
        System.out.println(splits);
        return null;
    }

    @Override
    public void handleSplitsChanges(SplitsChange<JDBCSplit> splitsChange) {
        if (!(splitsChange instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The SplitChange type of %s is not supported.",
                            splitsChange.getClass()));
        }
        splits.addAll(splitsChange.splits());
    }

    @Override
    public void wakeUp() {
        // do nothing
    }

    @Override
    public void close() throws Exception {

    }
}
