package com.xue.bigdata.test.source;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import java.util.Iterator;
import java.util.Properties;

/**
 * @author: mingway
 * @date: 2021/10/10 10:27 上午
 */
public class CustomHttpSourceDemo extends RichSourceFunction<JSONArray> implements CheckpointedFunction {

    private boolean isRunning = true;

    private HttpPost httpPost;

    private CloseableHttpClient httpClient;

    // 配置信息
    private final Properties properties;

    private transient ListState<JSONObject> paramStates;

    private JSONObject param;

    public CustomHttpSourceDemo(Properties properties) {
        this.properties = properties;
    }

    /**
     * 连接准备
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        httpClient = HttpClientBuilder.create().build();
        httpPost = new HttpPost(properties.get("HTTP_URL").toString());
    }

    /**
     * 定期快照
     * @param context
     * @throws Exception
     */
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // 清空历史值
        paramStates.clear();
        // 更新最新的状态值
        paramStates.add(param);
    }

    /**
     * 初始化状态
     * @param context
     * @throws Exception
     */
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // 定义状态描述器
        ListStateDescriptor<JSONObject> stateDescriptor = new ListStateDescriptor<JSONObject>("http-param-state", JSONObject.class);
        // 获取数据
        paramStates = context.getOperatorStateStore().getListState(stateDescriptor);
    }

    /**
     * 主要运行方法
     * @param context
     * @throws Exception
     */
    @Override
    public void run(SourceContext<JSONArray> context) throws Exception {
        // 状态相关
        Iterator<JSONObject> iterator = paramStates.get().iterator();
        while (iterator.hasNext()) {
            param = iterator.next();
        }

        int page_size = 2;
        int page_no = 0;
        while (isRunning) {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("page_no", page_no);
            jsonObject.put("page_size", page_size);

            StringEntity entity = new StringEntity(jsonObject.toJSONString());
            entity.setContentEncoding("UTF-8");
            entity.setContentType("application/json");
            httpPost.setEntity(entity);
            HttpResponse response = httpClient.execute(httpPost);

            if (param != null) {
                System.out.println("当前state信息>>>>"+ param.toJSONString());
            }
            param = jsonObject;
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                JSONObject result = JSONObject.parseObject(EntityUtils.toString(response.getEntity()));
                context.collect(result.getJSONArray("list"));
            }

            // 模拟
            Thread.sleep(5000);

            page_no++;
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
