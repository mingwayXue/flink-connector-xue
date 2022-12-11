package com.xue.bigdata.test.source.hybrid;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.shaded.guava30.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class JDBCSourceEnumerator implements SplitEnumerator<JDBCSplit, PendingSplitsCheckpoint> {

    private static final Logger LOG = LoggerFactory.getLogger(JDBCSourceEnumerator.class);

    private final SplitEnumeratorContext<JDBCSplit> context;

    private final JDBCConfig config;

    private final TreeSet<Integer> readersAwaitingSplit;

    private ExecutorService executor;

    private List<JDBCSplit> remainingSplits;

    private boolean isCompleteSplit;

    private final Object lock = new Object();

    public JDBCSourceEnumerator(SplitEnumeratorContext<JDBCSplit> enumContext, JDBCConfig config) {
        this.context = enumContext;
        this.config = config;
        this.readersAwaitingSplit = new TreeSet<>();
        this.remainingSplits = new CopyOnWriteArrayList<>();
        isCompleteSplit = false;
    }

    /**
     * 启动，需要初始化的工作
     */
    @Override
    public void start() {
        // 开始异步分片
        startAsynchronouslySplit();
    }

    private void startAsynchronouslySplit() {
        if (!config.getSqlList().isEmpty()) {
            if (executor == null) {
                ThreadFactory threadFactory =
                        new ThreadFactoryBuilder().setNameFormat("JDBCSourceEnumerator-startAsynchronouslySplit").build();
                this.executor = Executors.newSingleThreadExecutor(threadFactory);
            }
            executor.submit(this::splitChunksForSqlList);
        }
    }

    private void splitChunksForSqlList() {
        try {
            for (TableSelect ts : config.getSqlList()) {
                // todo 待优化，可根据 fetchSize 分片查询
                synchronized (lock) {
                    remainingSplits.add(new JDBCSplit(ts.getDb(), ts.getTable(), ts.getSelectSql()));
                    lock.notify();
                }
            }
            isCompleteSplit = true;
        } catch (Exception e) {
            LOG.error("JDBCSourceEnumerator-splitChunksForSqlList error:{}", e.getMessage());
            e.printStackTrace();
            synchronized (lock) {
                lock.notify();
            }
        }
    }

    private Optional<JDBCSplit> getNextSplit() {
        synchronized (lock) {
            if (!remainingSplits.isEmpty()) {
                Iterator<JDBCSplit> iterator = remainingSplits.iterator();
                JDBCSplit split = iterator.next();
                remainingSplits.remove(split);
                return Optional.of(split);
            } else if (!isCompleteSplit) {
                // 等待异步完成 split
                try {
                    lock.wait();
                } catch (InterruptedException e) {
                    LOG.error("JDBCSourceEnumerator-getNextSplit error:{}", e.getMessage());
                    e.printStackTrace();
                }
                return getNextSplit();
            } else {
                // 关闭资源
                LOG.info("JDBCSourceEnumerator-getNextSplit End Of Split.");
                closeExecutorService();
                return Optional.empty();
            }
        }
    }

    /**
     * 处理来自 reader 的分片请求
     * @param subtaskId
     * @param requesterHostname
     */
    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        if (!context.registeredReaders().containsKey(subtaskId)) {
            // reader failed between sending the request and now. skip this request.
            return;
        }

        readersAwaitingSplit.add(subtaskId);
        assignSplits();
    }

    private void assignSplits() {
        final Iterator<Integer> awaitingReader = readersAwaitingSplit.iterator();

        while (awaitingReader.hasNext()) {
            int nextAwaiting = awaitingReader.next();
            // 如果不在已注册的 reader 中，则需要剔除
            if (!context.registeredReaders().containsKey(nextAwaiting)) {
                awaitingReader.remove();
                continue;
            }

            Optional<JDBCSplit> nextSplit = getNextSplit();
            if (nextSplit.isPresent()) {
                context.assignSplit(nextSplit.get(), nextAwaiting);
                awaitingReader.remove();
            } else {
                // 后续都没有 split
                context.signalNoMoreSplits(nextAwaiting);
            }
        }
    }

    /**
     * 当 reader 处理 split 失败时的响应
     * @param splits
     * @param subtaskId
     */
    @Override
    public void addSplitsBack(List<JDBCSplit> splits, int subtaskId) {
        remainingSplits.addAll(splits);
    }

    /**
     * 添加新的 reader
     * @param subtaskId
     */
    @Override
    public void addReader(int subtaskId) {
        // do nothing
    }

    /**
     * 保存当前状态
     * @param checkpointId
     * @return
     * @throws Exception
     */
    @Override
    public PendingSplitsCheckpoint snapshotState(long checkpointId) throws Exception {
        // do nothing
        return null;
    }

    /**
     * 关闭资源
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        closeExecutorService();
    }

    private void closeExecutorService() {
        if (executor != null) {
            executor.shutdown();
        }
    }
}
