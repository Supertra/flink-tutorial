package bigdata.clebeg.cn.utils;

import org.roaringbitmap.RoaringBitmap;

import java.util.BitSet;
import java.util.Stack;

public class BitMapPerformance {
    static BitSet bitSet = new BitSet();
    static RoaringBitmap rbm = new RoaringBitmap();
    public static Integer bitSetSize(Integer rand) {
        bitSet.set(rand);
        return bitSet.size();
    }

    public static Integer rbmSize(Integer rand) {
        rbm.add(rand);
        rbm.runOptimize();
        return rbm.getSizeInBytes();
    }
    public static int longToSignedInt(long input) {
        return (int) (input & 0xffffffff);
    }

    public static long toUnsignedLong(int x) {
        return ((long) x) & 0xffffffffL;
    }

    public static void main(String[] args) {
//        Random rand = new Random();
//        for (int i = 0; i < Integer.MAX_VALUE; i++) {
//            Integer randInt = Math.abs(rand.nextInt());
//            Long bitSetSize = toUnsignedLong(bitSetSize(longToSignedInt(randInt)))/8;
//            Integer rbmSize = rbmSize(longToSignedInt(randInt));
//            if ((i & (i-1)) == 0 && i > 0) {
//                System.out.printf("%10d: bit_map_size=%10db, total_num=%10d | roaring_bit_map_size=%10db, total_num=%10d\n",
//                        i, bitSetSize, bitSet.cardinality(), rbmSize, rbm.getCardinality());
//            }
//        }
        String nums = "112";
        System.out.println(removeKdigits(nums, 1));

    }

    public static String removeKdigits(String num, int k) {
        // 1432219 k=3
        // 贪心法：需要在 num 中保留 num.length - k
        // 则以此在 num + 1 个数字中选最小的，计算需要剔除的数据，最小的相等则取左边
        if (num.isEmpty() || k == 0 || num.length() <= k) return "0";
        int i = 0, removeNum = 0;
        Stack<Integer> keepStack = new Stack<>();
        while (i < num.length() && removeNum < k && keepStack.size() < num.length() - k) {
            keepStack.push(Integer.valueOf(num.substring(i, i+1)));
            i++;
            int j = i;
            int stop = k - removeNum + i;
            for (; j < stop && j < num.length(); j++) {
                int tmp = Integer.valueOf(num.substring(j, j+1));
                if (tmp < keepStack.peek()) {
                    removeNum++;
                    i = j + 1;
                    keepStack.pop();
                    keepStack.push(tmp);
                }
            }
        }
        String res;
        if (keepStack.size() == num.length() - k) res = "";
        else res = num.substring(i);
        while (!keepStack.empty()) {
            Integer pop = keepStack.pop();
            res = pop + res;
        }
        int j = 0;
        for (; j < res.length(); j++) {
            if (res.charAt(j) != '0') break;
        }
        res = res.substring(j);
        if (res.isEmpty()) return "0";
        return res;
    }
}
