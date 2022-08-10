# flink-connector-xue

# 备忘

## flink idea 本地调试状态
```java
// 创建 configuration 并指定参数"execution.savepoint.path" 即可
Configuration configuration = new Configuration();
configuration.setString("execution.savepoint.path", "file:///Users/flink/checkpoints/ce2e1969c5088bf27daf35d4907659fd/chk-5");

StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
```


# TODO
1. [字节埋点数据实时动态处理引擎](https://mp.weixin.qq.com/s?__biz=MzkxNjA1MzM5OQ==&mid=2247488435&idx=1&sn=5d89a0d24603c08af4be342462409230&chksm=c1549f4bf623165d977426d13a0bdbe821ec8738744d2274613a7ad92dec0256d090aea4b815&scene=21#wechat_redirect)
2. 