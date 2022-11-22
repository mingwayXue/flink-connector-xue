package com.xue.bigdata.test.source.hybrid;


import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.SimpleVersionedSerializer;

public class JDBCSource implements Source<String, JDBCSplit, PendingSplitsCheckpoint>, ResultTypeQueryable<String> {

    private static final long serialVersionUID = 1L;

    private final JDBCConfig config;

    public JDBCSource(JDBCConfig config) {
        this.config = config;
    }

    public static JDBCSourceBuilder builder() {
        return new JDBCSourceBuilder();
    }

    /**
     * 有界流
     * @return
     */
    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<String, JDBCSplit> createReader(SourceReaderContext readerContext) throws Exception {
        return new JDBCSourceReader(readerContext, config);
    }

    @Override
    public SplitEnumerator<JDBCSplit, PendingSplitsCheckpoint> createEnumerator(SplitEnumeratorContext<JDBCSplit> enumContext) throws Exception {
        // todo 可在此校验配置是否生效

        return new JDBCSourceEnumerator(enumContext, config);
    }

    @Override
    public SplitEnumerator<JDBCSplit, PendingSplitsCheckpoint> restoreEnumerator(SplitEnumeratorContext<JDBCSplit> enumContext, PendingSplitsCheckpoint checkpoint) throws Exception {
        return null;
    }

    /**
     * JDBCSplit 序列化器（split）
     * @return
     */
    @Override
    public SimpleVersionedSerializer<JDBCSplit> getSplitSerializer() {
        return new JDBCSourceSplitSerializer();
    }

    /**
     * PendingSplitsCheckpoint 序列化器（checkpoint）
     * @return
     */
    @Override
    public SimpleVersionedSerializer<PendingSplitsCheckpoint> getEnumeratorCheckpointSerializer() {
        return null;
    }

    /**
     * 返回值类型
     * @return
     */
    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }
}
