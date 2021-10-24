## flink-tutorial
flink 学习仓库，用于深入掌握 flink 使用，包括：
1. flink yarn 集群搭建：业界最常用的模式
2. flink 流批一体开发
3. flink 实战性能优化
4. flink 与各种数据源进行连接
5. flink 源码解读

## flink 集群搭建

## Flink 基石
Flink 有四大基石：时间语义、窗口管理、状态管理、容错机制

1. [Flink时间处理](./docs/Flink时间处理.md)：详细介绍 Flink 处理时间的方式，Flink 强大的时间处理方式，能够更正确的处理数据。
2. [Flink窗口机制](./docs/Flink窗口机制.md)：详细介绍 Flink 丰富的窗口类型和如何利用这些窗口解决实际业务问题。
3. [Flink状态管理](./docs/Flink状态管理.md)：详细介绍 Flink 状态管理的原理、类型、横向扩展机制、使用方式和实际案例。
4. [Flink容错机制](./docs/Flink容错机制.md)：详细介绍 Flink 容错机制，如何保证在分布式环境下能够出现故障自动恢复同时保证数据准确。

## Flink Table & SQL
[Flink_Table&SQL](./docs/Flink_Table&SQL.md)：详细介绍 Flink Table&SQL 的架构演变、原理和实际使用方式。
## Flink 生产环境案例
1. [DSWordCnt](./src/main/java/bigdata/clebeg/cn/quickstart/BSWordCnt.java): 流批一体的 word cnt，根据数据源自动切换模式
```shell
# 默认情况下，flink 是通过 stream 模式跑
flink run -t yarn-per-job -c bigdata.clebeg.cn.quickstart.BSWordCnt -yjm 512 -ytm 512 --detached original-flink-tutorial-1.0-SNAPSHOT.jar --output hdfs://bigdatacluster/flink-tutorial/bswordcnt/output_02
# execution.runtime-mode=BATCH 添加参数就会编程 batch 模式
flink run -t yarn-per-job -Dexecution.runtime-mode=BATCH -c bigdata.clebeg.cn.quickstart.BSWordCnt -yjm 512 -ytm 512 --detached original-flink-tutorial-1.0-SNAPSHOT.jar --output hdfs://bigdatacluster/flink-tutorial/bswordcnt/output_03
# yarn kill 某个应用
yarn application -kill application_1633745373273_0004
```
2. [WatermakerDemo](./src/main/java/bigdata/clebeg/cn/quickstart/abouttime/WatermakerDemo.java): 模拟通过水印等待窗口延迟触发、延迟数据测流输出
3. [FlinkUvDemo](./src/main/java/bigdata/clebeg/cn/quickstart/state/FlinkUvDemo.java): 真正生产级别可用 Flink海量UV计算问题     
   UV 计算在 Flink 里面是一个难题，一般生产环境会面临两个挑战，挑战1：热点问题，挑战2：大状态问题，此实现在大部分场景上可以解决上述两个问题。
4. [BatchRedisSink](./src/main/java/bigdata/clebeg/cn/quickstart/sink/BatchRedisSink.java): Flink 解决快速输入数据到 redis  
   输出到 Redis 考虑比较全面的话：1. 需要批量输出 2. 需要定时输出 3. 需要使用连接池，如果性能继续优化，还可以异步，后面可以给出优化的功能。


## 参考资料
1. flink 如何做压测：https://www.cxyzjd.com/article/weixin_43291055/102692456
2. 快手实时链路压测解密：https://xie.infoq.cn/article/3622e21cc51a9c6a6c1ce3c69
3. Flink 状态管理：https://www.cnblogs.com/felixzh/p/13167665.html
4. Flink 如何快速写入数据进入 Redis：https://tech.ipalfish.com/blog/2021/06/24/flink-bulk-insert-redis/
5. Flink join 时态表：https://blog.csdn.net/wangpei1949/article/details/103541939
6. 深入理解 Flink 容错机制：http://www.whitewood.me/2019/07/28/%E6%B7%B1%E5%85%A5%E7%90%86%E8%A7%A3-Flink-%E5%AE%B9%E9%94%99%E6%9C%BA%E5%88%B6/
7. Flink 窗口触发机制：https://www.hnbian.cn/posts/a459ab50.html