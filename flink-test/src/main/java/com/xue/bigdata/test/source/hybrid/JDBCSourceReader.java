package com.xue.bigdata.test.source.hybrid;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;

import java.util.Map;

public class JDBCSourceReader extends SingleThreadMultiplexSourceReaderBase<JSONObject, String, JDBCSplit, JDBCSplitState> {

    private final JDBCConfig jdbcConfig;

    public JDBCSourceReader(SourceReaderContext context, JDBCConfig config) {
        super(
                () -> new JDBCSplitReader(config),
                (element, output, splitState) -> {
                    output.collect(element.toJSONString());
                },
                context.getConfiguration(),
                context);
        this.jdbcConfig = config;
    }

    @Override
    public void start() {
        if (getNumberOfCurrentlyAssignedSplits() == 0) {
            context.sendSplitRequest();
        }
    }

    @Override
    protected void onSplitFinished(Map<String, JDBCSplitState> finishedSplitIds) {
        context.sendSplitRequest();
    }

    @Override
    protected JDBCSplitState initializedState(JDBCSplit split) {
        return null;
    }

    @Override
    protected JDBCSplit toSplitType(String splitId, JDBCSplitState splitState) {
        return splitState.split;
    }
}
