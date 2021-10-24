package com.atguigu.day01;

public class Example4 {
    public static void main(String[] args) {
        System.out.println(fibIter(100)); // 1298777728820984005
        System.out.println(fibIterImproved(100));
    }

    public static Long fib(Integer n) {
        if (n == 1 || n == 2) return (long) n;
        else return fib(n - 1) + fib(n - 2);
    }

    // 缓存的思想
    // 1. 先在缓存中查找数据
    // 2. 找不到的话，将数据从数据库中取出，放入缓存，返回结果
    // 动态规划
    public static Long fibIter(Integer n) {
        if (n == 1 || n == 2) return (long) n;
        // 下标对应的元素是爬下标个楼梯的方法数量
        // 例如，下标是5,那么对应数组中的元素是5个台阶的方法数量
        Long[] array = new Long[n + 1];
        array[1] = 1L;
        array[2] = 2L;
        for (int i = 3; i < n + 1; i++) {
            array[i] = array[i - 1] + array[i - 2];
        }
        return array[n];
    }

    // prev cur
    // cur prev + cur
    // 3 5
    public static Long fibIterImproved(Integer n) {
        if (n == 1|| n == 2) return (long) n;
        long prev = 0;
        long cur = 1;
        for (int i = n; i > 0; i--) {
            long tmp = 0L;
            tmp = prev;
            prev = cur;
            cur = tmp + cur;
        }
        return cur;
    }
}
