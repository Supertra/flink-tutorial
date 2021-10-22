# Flink 窗口机制
## 一、Flink Window 定义
在使用 Flink 处理实时需求的过程中，常常需要计算最近某段时间数据的某个统计量，比如：每个1分钟计算输出1分钟内的访问人数，每个5分钟输出最近10分钟的访问次数等。
Flink 为了满足各种窗口计算需求，实现了丰富的窗口定义方式，具体有两种类型的窗口基于时间的窗口（Time Base Window）和基于数量的窗口（Count-based Window）。    
生成环境中最常用的窗口是基于时间的窗口，Flink 中基于时间的窗口主要有三种类型：
1. 滑动窗口(Sliding Window)：当滑动的时间长度和窗口的时间长度不一致时，称为滑动窗口，比如：每5分钟统计过去10分钟的PV，窗口就是10分钟，滑动5分钟。
2. 滚动窗口(Tumbling Window)：滚动窗口其实就是滑动时间长度和窗口时间长度一致的窗口类型，是特殊的滑动窗口。
3. 会话窗口(Session Window)：会话窗口指的是定一个时间间隔，超过这个时间间隔数据没有到来，就结束当前窗口。

生成环境中主要记住滑动窗口：sliding Window，因为滚动窗口 Tumbling Window 只是一个滑动的时间等于滚动时间的特殊滑动窗口。会话窗口几乎没有场景使用。

## 二、Flink Time Based Window 生命周期
在 Flink 中，基于时间的窗口都是左闭右开的。默认情况下，flink 是从当天 00:00 分开始按窗口指定时长划分窗口，比如用户指定的是5分钟一个窗口，那么
默认 [00:00, 00:05) 为第一个窗口，但是不是每个时间段都产生一个窗口，如果某个窗口没有数据，就不会有这个窗口产生，窗口的产生是数据驱动的，即当 WindowAssigner 
分配窗口的第一个数据时候，才会产生当前窗口，不存在没有数据的窗口。

一个窗口包含如下状态：
1. Window 内容
+ 分配到窗口的数据
+ 增量聚合的结果（如果 window operator 接收了 ReduceFunction 或 AggregateFunction 作为参数）
2. Window 对象
+ WindowAssigner 返回 >= 0 个 Window 对象
+ Window operator 根据返回的 Window 对象聚合元素
+ 每个 Window 对象包含 WindowBegin 和 WindowEnd 时间戳，来区分与其他窗口
3. Window 触发器
+ 一个触发器可以注册定时事件，到达定时时间可执行相应的回调函数，例如：清空当前窗口状态
4. Window 自定义状态
+ 触发器可以使用自定义、per-window或者per-key状态。这个状态完全被触发器控制，而不是被 window operator 控制

当窗口结束时间到了之后，window operator 会删除当前窗口。窗口结束时间是由 window 对象的 WindowEnd 时间戳决定的，无论是使用 process time 还是 event time，
窗口结束时间的类型可以调用 WindowAssigner.isEventTime() 判断。

## 三、Flink 数据窗口划分方式
想要了解数据是如何划分到哪个窗口的，必须先了解 WindowAssigner，下面先看一下 WindowAssigner 的代码：
```java
/**
 * A {@code WindowAssigner} assigns zero or more {@link Window Windows} to an element.
 *
 * <p>In a window operation, elements are grouped by their key (if available) and by the windows to
 * which it was assigned. The set of elements with the same key and window is called a pane.
 * When a {@link Trigger} decides that a certain pane should fire the
 * {@link org.apache.flink.streaming.api.functions.windowing.WindowFunction} is applied
 * to produce output elements for that pane.
 *
 * @param <T> The type of elements that this WindowAssigner can assign windows to.
 * @param <W> The type of {@code Window} that this assigner assigns.
 */
@PublicEvolving
public abstract class WindowAssigner<T, W extends Window> implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * Returns a {@code Collection} of windows that should be assigned to the element.
     *
     * @param element The element to which windows should be assigned.
     * @param timestamp The timestamp of the element.
     * @param context The {@link WindowAssignerContext} in which the assigner operates.
     */
    public abstract Collection<W> assignWindows(T element, long timestamp, WindowAssignerContext context);

    /**
     * Returns the default trigger associated with this {@code WindowAssigner}.
     */
    public abstract Trigger<T, W> getDefaultTrigger(StreamExecutionEnvironment env);

    /**
     * Returns a {@link TypeSerializer} for serializing windows that are assigned by
     * this {@code WindowAssigner}.
     */
    public abstract TypeSerializer<W> getWindowSerializer(ExecutionConfig executionConfig);

    /**
     * Returns {@code true} if elements are assigned to windows based on event time,
     * {@code false} otherwise.
     */
    public abstract boolean isEventTime();

    /**
     * A context provided to the {@link WindowAssigner} that allows it to query the
     * current processing time.
     *
     * <p>This is provided to the assigner by its containing
     * {@link org.apache.flink.streaming.runtime.operators.windowing.WindowOperator},
     * which, in turn, gets it from the containing
     * {@link org.apache.flink.streaming.runtime.tasks.StreamTask}.
     */
    public abstract static class WindowAssignerContext {

        /**
         * Returns the current processing time.
         */
        public abstract long getCurrentProcessingTime();

    }
}
```
WindowAssigner 存在两个泛型参数：
+ T：表示每条数据的类型
+ W：表示窗口的类型

这个抽象类主要有四个方法，简单说一下每个方法的作用：
+ assignWindows：将带有时间戳 timestamp 的数据元素 element 分配给一个或多个窗口，并返回窗口集合
+ getDefaultTrigger：返回 WindowAssigner 默认的 trigger
+ getWindowSerializer：返回一个类型序列化器用来序列化窗口
+ isEventTime：是否是 event time

所有窗口类型都继承 WindowAssigner，下面通过实际工作中最常用的 SlidingEventTimeWindows 底层源码来讲解到底滑动窗口是如何划分窗口。首先看一下源码：
```java
@Override
public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
   if (timestamp > Long.MIN_VALUE) {
      // size 是窗口大小 = 5，slide 是滑动步长 = 2
      List<TimeWindow> windows = new ArrayList<>((int) (size / slide));
      // 获取最后一个窗口的起始时间 = 6
      long lastStart = TimeWindow.getWindowStartWithOffset(timestamp, offset, slide);
      // 假设滑动时长为 2 分钟，窗口是 5 分钟，当前是第 7 分钟这条数据，offset 是 0
      // 那么 7 这条数据属于：[4, 9), [6, 11)
      for (long start = lastStart;
         start > timestamp - size;
         start -= slide) {
         windows.add(new TimeWindow(start, start + size));
      }
      return windows;
   } else {
      throw new RuntimeException("Record has Long.MIN_VALUE timestamp (= no timestamp marker). " +
            "Is the time characteristic set to 'ProcessingTime', or did you forget to call " +
            "'DataStream.assignTimestampsAndWatermarks(...)'?");
   }
}

// 其中 getWindowStartWithOffset 实现如下
/**
 * Method to get the window start for a timestamp.
 * 此方法是获取当前时间戳的窗口起始时间
 * @param timestamp epoch millisecond to get the window start.
 * @param offset The offset which window start would be shifted by.
 * @param windowSize The size of the generated windows.
 * @return window start
 */
public static long getWindowStartWithOffset(long timestamp, long offset, long windowSize) {
        return timestamp - (timestamp - offset + windowSize) % windowSize;
}
```
通过上面的注释可以了解到，Flink 的窗口划分机制就根据每条数据的时间来分配到对应的时间窗口，默认的时间窗口是从 00:00 开始。

## 四、Flink 自定义时间窗口 
实际工作中，需要统计当天的实时访问人数，窗口的类型是左边固定在当天0点，第二天零点，但是要隔一定时间输出一条记录。这种情况下，滑动时间可能是1s，但是窗口大小是动态的，
这就需要用户自定义 window。    
如果需要自定义 window，也同样需要继承 WindowAssigner 类，下面实现一个能够统计当天实时人数的特殊窗口：

