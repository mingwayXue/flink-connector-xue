package com.xue.bigdata.test.source.hybrid;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

public class JDBCSplitReader implements SplitReader<JSONObject, JDBCSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(JDBCSplitReader.class);

    private JDBCConfig jdbcConfig;

    private final Queue<JDBCSplit> splits;

    @Nullable
    private String currentSplitId;

    public JDBCSplitReader(JDBCConfig config) {
        this.jdbcConfig = config;
        this.splits = new LinkedList<>();
    }

    @Override
    public RecordsWithSplitIds<JSONObject> fetch() throws IOException {

        JDBCSplit nextSplit = splits.poll();
        if (Objects.isNull(nextSplit)) {
            return finishedSplit();
        }
        currentSplitId = nextSplit.splitId;
        Iterator<JSONObject> dataIt;
        try {
            dataIt = pollSplitRecords(nextSplit);
        } catch (Exception e){
            e.printStackTrace();
            LOG.error("JDBCSplitReader-fetch error:{}", e.getMessage());
            throw new IOException(e);
        }

        return dataIt == null ? finishedSplit() : JDBCRecords.forRecords(currentSplitId, dataIt);
    }

    /**
     * 获取数据
     * @param nextSplit
     * @return
     */
    private Iterator<JSONObject> pollSplitRecords(JDBCSplit nextSplit) {
        // todo 可优化成一个实例变量
        try (JDBCConnection jdbc = JDBCConnection.openJdbcConnection(jdbcConfig)) {
            List<JSONObject> list = jdbc.findAll(nextSplit.getSql());
            return list.iterator();
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("JDBCSplitReader-pollSplitRecords error:{}", e.getMessage());
        }
        return null;
    }

    private JDBCRecords finishedSplit() {
        final JDBCRecords finishedRecords = JDBCRecords.forFinishedSplit(currentSplitId);
        currentSplitId = null;
        return finishedRecords;
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
        this.currentSplitId = null;
    }
}
