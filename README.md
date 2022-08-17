# flink-connector-xue

# 备忘

## flink idea 本地调试状态
```java
// 创建 configuration 并指定参数"execution.savepoint.path" 即可
Configuration configuration = new Configuration();
configuration.setString("execution.savepoint.path", "file:///Users/flink/checkpoints/ce2e1969c5088bf27daf35d4907659fd/chk-5");

StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
```

## Flink SQL JOIN

### 1、Regular Join

#### left join
首先是 left join，以上面的 show_log（左表） left join click_log（右表） 为例：
1. 首先如果 join xxx on 中的条件是等式则代表 join 是在相同 key 下进行的，join 的 key 即 show_log.log_id，click_log.log_id，相同 key 的数据会被发送到一个并发中进行处理。如果 join xxx on 中的条件是不等式，则两个流的 source 算子向 join 算子下发数据是按照 global 的 partition 策略进行下发的，并且 join 算子并发会被设置为 1，所有的数据会被发送到这一个并发中处理。
2. 相同 key 下，当 show_log 来一条数据，如果 click_log 有数据：则 show_log 与 click_log 中的所有数据进行遍历关联一遍输出[+（show_log，click_log）]数据，并且把 show_log 保存到左表的状态中（以供后续 join 使用）。
3. 相同 key 下，当 show_log 来一条数据，如果 click_log 中没有数据：则 show_log 不会等待，直接输出[+（show_log，null）]数据，并且把 show_log 保存到左表的状态中（以供后续 join 使用）。
4. 相同 key 下，当 click_log 来一条数据，如果 show_log 有数据：则 click_log 对 show_log 中所有的数据进行遍历关联一遍。在输出数据前，会判断，如果被关联的这条 show_log 之前没有关联到过 click_log（即往下发过[+（show_log，null）]），则先发一条[-（show_log，null）]，后发一条[+（show_log，click_log）]，代表把之前的那条没有关联到 click_log 数据的 show_log 中间结果给撤回，把当前关联到的最新结果进行下发，并把 click_log 保存到右表的状态中（以供后续左表进行关联）。这也就解释了为什么输出流是一个 retract 流。
5. 相同 key 下，当 click_log 来一条数据，如果 show_log 没有数据：把 click_log 保存到右表的状态中（以供后续左表进行关联）。

#### inner join
以上面的 show_log（左表） inner join click_log（右表） 为例：
1. 首先如果 join xxx on 中的条件是等式则代表 join 是在相同 key 下进行的，join 的 key 即 show_log.log_id，click_log.log_id，相同 key 的数据会被发送到一个并发中进行处理。如果 join xxx on 中的条件是不等式，则两个流的 source 算子向 join 算子下发数据是按照 global 的 partition 策略进行下发的，并且 join 算子并发会被设置为 1，所有的数据会被发送到这一个并发中处理。
2. 相同 key 下，当 show_log 来一条数据，如果 click_log 有数据：则 show_log 与 click_log 中的所有数据进行遍历关联一遍输出[+（show_log，click_log）]数据，并且把 show_log 保存到左表的状态中（以供后续 join 使用）。
3. 相同 key 下，当 show_log 来一条数据，如果 click_log 中没有数据：则 show_log 不会输出数据，会把 show_log 保存到左表的状态中（以供后续 join 使用）。
4. 相同 key 下，当 click_log 来一条数据，如果 show_log 有数据：则 click_log 与 show_log 中的所有数据进行遍历关联一遍输出[+（show_log，click_log）]数据，并且把 click_log 保存到右表的状态中（以供后续 join 使用）。
5. 相同 key 下，当 click_log 来一条数据，如果 show_log 没有数据：则 click_log 不会输出数据，会把 click_log 保存到右表的状态中（以供后续 join 使用）。

#### right join
right join 和 left join 一样，只不过顺序反了，这里不再赘述。

#### full join
以上面的 show_log（左表） full join click_log（右表） 为例：
1. 首先如果 join xxx on 中的条件是等式则代表 join 是在相同 key 下进行的，join 的 key 即 show_log.log_id，click_log.log_id，相同 key 的数据会被发送到一个并发中进行处理。如果 join xxx on 中的条件是不等式，则两个流的 source 算子向 join 算子下发数据是按照 global 的 partition 策略进行下发的，并且 join 算子并发会被设置为 1，所有的数据会被发送到这一个并发中处理。
2. 相同 key 下，当 show_log 来一条数据，如果 click_log 有数据：则 show_log 对 click_log 中所有的数据进行遍历关联一遍。在输出数据前，会判断，如果被关联的这条 click_log 之前没有关联到过 show_log（即往下发过[+（null，click_log）]），则先发一条[-（null，click_log）]，后发一条[+（show_log，click_log）]，代表把之前的那条没有关联到 show_log 数据的 click_log 中间结果给撤回，把当前关联到的最新结果进行下发，并把 show_log 保存到左表的状态中（以供后续 join 使用）
3. 相同 key 下，当 show_log 来一条数据，如果 click_log 中没有数据：则 show_log 不会等待，直接输出[+（show_log，null）]数据，并且把 show_log 保存到左表的状态中（以供后续 join 使用）。
4. 相同 key 下，当 click_log 来一条数据，如果 show_log 有数据：则 click_log 对 show_log 中所有的数据进行遍历关联一遍。在输出数据前，会判断，如果被关联的这条 show_log 之前没有关联到过 click_log（即往下发过[+（show_log，null）]），则先发一条[-（show_log，null）]，后发一条[+（show_log，click_log）]，代表把之前的那条没有关联到 click_log 数据的 show_log 中间结果给撤回，把当前关联到的最新结果进行下发，并把 click_log 保存到右表的状态中（以供后续 join 使用）
5. 相同 key 下，当 click_log 来一条数据，如果 show_log 中没有数据：则 click_log 不会等待，直接输出[+（null，click_log）]数据，并且把 click_log 保存到右表的状态中（以供后续 join 使用）。

> **总结：**
> 1. inner join 会互相等，直到有数据才下发
> 2. left join，right join，full join 不会互相等，只要来了数据，会尝试关联，能关联到则下发的字段是全的，关联不到则另一边的字段为 null。后续数据来了之后，发现之前下发过为没有关联到的数据时，就会做回撤，把关联到的结果进行下发

### 2、Interval Join
以上面案例的 show_log（左表） interval join click_log（右表） 为例（不管是 inner interval join，left interval join，right interval join 还是 full interval join，都会按照下面的流程执行）：

1. **第一步**，首先如果 join xxx on 中的条件是等式则代表 join 是在相同 key 下进行的（上述案例中 join 的 key 即 show_log.log_id，click_log.log_id），相同 key 的数据会被发送到一个并发中进行处理。如果 join xxx on 中的条件是不等式，则两个流的 source 算子向 join 算子下发数据是按照 global 的 partition 策略进行下发的，并且 join 算子并发会被设置为 1，所有的数据会被发送到这一个并发中处理。
2. **第二步**，相同 key 下，一条 show_log 的数据先到达，首先会计算出下面要使用的最重要的三类时间戳：
    - 根据 show_log 的时间戳（l_time）计算出能关联到的右流的时间区间下限（r_lower）、上限（r_upper）
    - 根据 show_log 目前的 watermark 计算出目前右流的数据能够过期做过期处理的时间的最小值（r_expire）
    - 获取左流的 l_watermark，右流的 r_watermark，这两个时间戳在事件语义的任务中都是 watermark
3. **第三步**，遍历所有同 key 下的 click_log 来做 join
    - 对于遍历的每一条 click_log，走如下步骤
    - 经过判断，如果 on 中的条件为 true，则和 click_log 关联，输出[+（show_log，click_log）]数据；如果 on 中的条件为 false，则啥也不干
    - 接着判断当前这条 click_log 的数据时间（r_time）是否小于右流的数据过期时间的最小值（r_expire）（即判断这条 click_log 是否永远不会再被 show_log join 到了）。如果小于，并且当前 click_log 这一侧是 outer join，则不用等直接输出[+（null，click_log）]），从状态删除这条 click_log；如果 click_log 这一侧不是 outer join，则直接从状态里删除这条 click_log。
4. **第四步**，判断右流的时间戳（r_watermark）是否小于能关联到的右流的时间区间上限（r_upper）：
    - 如果是，则说明这条 show_log 还有可能被 click_log join 到，则 show_log 放到 state 中，并注册后面用于状态清除的 timer。
    - 如果否，则说明关联不到了，则输出[+（show_log，null）]
5. **第五步**，timer 触发时
    - timer 触发时，根据当前 l_watermark，r_watermark 以及 state 中存储的 show_log，click_log 的 l_time，r_time 判断是否再也不会被对方 join 到，如果是，则根据是否为 outer join 对应输出[+（show_log，null）]，[+（null，click_log）]，并从状态中删除对应的 show_log，click_log。

上面只是左流 show_log 数据到达时的执行流程（即 ProcessElement1），当右流 click_log 到达时也是完全类似的执行流程（即 ProcessElement2）。

# TODO
1. [字节埋点数据实时动态处理引擎](https://mp.weixin.qq.com/s?__biz=MzkxNjA1MzM5OQ==&mid=2247488435&idx=1&sn=5d89a0d24603c08af4be342462409230&chksm=c1549f4bf623165d977426d13a0bdbe821ec8738744d2274613a7ad92dec0256d090aea4b815&scene=21#wechat_redirect)
2. 