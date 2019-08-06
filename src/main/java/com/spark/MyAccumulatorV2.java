package com.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.AccumulatorV2;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by root on 2019/7/21.
 * 自定义累加器
 *
 * 注：有人在网上提出过webui页面上看怎么看spark累加器，本人期初的时候也没找到怎么看，后来才知道，
 * 需要自定义累加器名称后才能看到jsc.sc().register(myAccumulatorV2, "myAccumulatorV2");
 * 后面引号里面的才是累加器名称，这样才能看
 *
 */
public class MyAccumulatorV2 extends AccumulatorV2<String, Set> {

    private Set set=new HashSet<>();
    @Override
    public boolean isZero() {
        return set.isEmpty();
    }
    @Override
    public AccumulatorV2<String, Set> copy() {
        MyAccumulatorV2 myAccumulatorV2=new MyAccumulatorV2();
        synchronized(myAccumulatorV2){
            myAccumulatorV2.set.addAll(set);
        }
        return myAccumulatorV2;
    }
    @Override
    public void reset() {
        set.clear();
    }
    @Override
    public void add(String s) {
        set.add(s);
    }
    @Override
    public void merge(AccumulatorV2<String, Set> accumulatorV2) {
        set.addAll(accumulatorV2.value());
    }
    @Override
    public Set value() {
        return set;
    }
}

class MyAccumulatorV2Main {

    private static JavaSparkContext jsc;

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("MyAccumulatorV2Main");
        jsc = new JavaSparkContext(conf);
        jsc.setLogLevel("debug");

        MyAccumulatorV2 myAccumulatorV2=new MyAccumulatorV2();
        jsc.sc().register(myAccumulatorV2, "myAccumulatorV2");
        JavaRDD rdd = jsc.parallelize(Arrays.asList("A","B","C"), 5).cache();
        rdd.foreach(x ->myAccumulatorV2.add(x.toString())); //将字符串进行add操作
        Set res1 = myAccumulatorV2.value();
        System.out.println(res1);
        MyAccumulatorV2 myAccumulatorV3= (MyAccumulatorV2) myAccumulatorV2.copy();//copy一个对象
        Set res2 = myAccumulatorV3.value();
        System.out.println(res2);
    }
}
