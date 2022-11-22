package com.xue.bigdata.test.source.hybrid;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class JDBCSourceSplitSerializer implements SimpleVersionedSerializer<JDBCSplit> {


    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(JDBCSplit split) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out);
        view.writeUTF(split.splitId());
        view.writeUTF(split.getSql());
        return out.toByteArray();
    }

    @Override
    public JDBCSplit deserialize(int version, byte[] serialized) throws IOException {
        ByteArrayInputStream in = new ByteArrayInputStream(serialized);
        DataInputViewStreamWrapper view = new DataInputViewStreamWrapper(in);
        String splitId = view.readUTF();
        String sql = view.readUTF();
        return new JDBCSplit(sql);
    }
}
