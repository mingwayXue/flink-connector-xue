package com.xue.bigdata.test.source.hybrid;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

public class JDBCRecords implements RecordsWithSplitIds<JSONObject> {

    @Nullable private String splitId;
    @Nullable private Iterator<JSONObject> recordsForSplit;
    private final Set<String> finishedSnapshotSplits;

    public JDBCRecords(
            @Nullable String splitId,
            @Nullable Iterator recordsForSplit,
            Set<String> finishedSnapshotSplits) {
        this.splitId = splitId;
        this.recordsForSplit = recordsForSplit;
        this.finishedSnapshotSplits = finishedSnapshotSplits;
    }

    @Nullable
    @Override
    public String nextSplit() {
        final String nextSplit = this.splitId;
        this.splitId = null;
        if (Objects.isNull(nextSplit)) {
            this.recordsForSplit = null;
        }
        return nextSplit;
    }

    @Nullable
    @Override
    public JSONObject nextRecordFromSplit() {
        final Iterator<JSONObject> recordsForSplit = this.recordsForSplit;
        if (recordsForSplit != null) {
            if (recordsForSplit.hasNext()) {
                return recordsForSplit.next();
            } else {
                return null;
            }
        } else {
            throw new IllegalStateException();
        }
    }

    @Override
    public Set<String> finishedSplits() {
        return finishedSnapshotSplits;
    }

    public static JDBCRecords forRecords(
            final String splitId, final Iterator<JSONObject> recordsForSplit) {
        return new JDBCRecords(splitId, recordsForSplit, Collections.emptySet());
    }

    public static JDBCRecords forFinishedSplit(final String splitId) {
        return new JDBCRecords(null, null, Collections.singleton(splitId));
    }
}
