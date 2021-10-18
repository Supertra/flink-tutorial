## flink-tutorial
flink 学习仓库，用于深入掌握 flink 使用，包括：
1. flink yarn 集群搭建：业界最常用的模式
2. flink 流批一体开发
3. flink 实战性能优化
4. flink 与各种数据源进行连接
5. flink 源码解读

## flink 集群搭建

## flink 流批一体开发
### 入门案例
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
3. [FlinkUvDemo](./src/main/java/bigdata/clebeg/cn/quickstart/state/FlinkUvDemo.java): Flink解决UV计算问题
uv计算在flink里面是一个难题，一般生产环境会面临两个挑战，挑战1：热点问题，挑战2：大状态问题，此实现在大部分场景上可以解决上述两个问题。

## 参考资料
1. flink 如何做压测：https://www.cxyzjd.com/article/weixin_43291055/102692456