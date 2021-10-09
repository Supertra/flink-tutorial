//中位数是有序列表中间的数。如果列表长度是偶数，中位数则是中间两个数的平均值。
//
// 例如，
//
// [2,3,4] 的中位数是 3
//
// [2,3] 的中位数是 (2 + 3) / 2 = 2.5
//
// 设计一个支持以下两种操作的数据结构：
//
//
// void addNum(int num) - 从数据流中添加一个整数到数据结构中。
// double findMedian() - 返回目前所有元素的中位数。
//
//
// 示例：
//
// addNum(1)
//addNum(2)
//findMedian() -> 1.5
//addNum(3)
//findMedian() -> 2
//
// 进阶:
//
//
// 如果数据流中所有整数都在 0 到 100 范围内，你将如何优化你的算法？
// 如果数据流中 99% 的整数都在 0 到 100 范围内，你将如何优化你的算法？
//
// Related Topics 堆 设计
// 👍 298 👎 0


import java.util.Comparator;
import java.util.PriorityQueue;

//leetcode submit region begin(Prohibit modification and deletion)
class MedianFinder {
    private int totalNum;
    private PriorityQueue<Integer> maxPd;
    private PriorityQueue<Integer> minPd;
    /** initialize your data structure here. */
    public MedianFinder() {
        this.totalNum = 0;
        // 初始化最大堆 保留最小的K个元素 K为需要计算中位数最小的数量
        this.maxPd = new PriorityQueue<>(new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return -o1.compareTo(o2);
            }
        });
        // 初始化最小堆 保留最大的K个元素
        this.minPd = new PriorityQueue<>();
    }

    public void addNum(int num) {
        this.totalNum += 1;
        if (this.maxPd.size() <= this.minPd.size()) {
            if (this.maxPd.size() == 0 || num <= this.minPd.peek()) {
                this.maxPd.add(num);
            } else {
                this.maxPd.add(this.minPd.poll());
                this.minPd.add(num);
            }
        } else {
            if (num > this.maxPd.peek()) {
                this.minPd.add(num);
            } else {
                this.minPd.add(this.maxPd.poll());
                this.maxPd.add(num);
            }
        }
    }

    public double findMedian() {
        if (this.totalNum % 2 == 0) {
            return Double.valueOf(this.maxPd.peek() + this.minPd.peek())/2;
        } else {
            return this.maxPd.peek();
        }
    }

    public static void main(String[] args) {
        MedianFinder obj = new MedianFinder();
        obj.addNum(1);
        System.out.println(obj.findMedian());
        obj.addNum(6);
        System.out.println(obj.findMedian());
        obj.addNum(7);
        System.out.println(obj.findMedian());
        obj.addNum(8);
        System.out.println(obj.findMedian());
        obj.addNum(3);
        obj.addNum(4);
        obj.addNum(2);
        obj.addNum(10);

    }
}