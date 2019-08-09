package com.sparksql.doublemerge;

import org.apache.spark.sql.api.java.UDF2;

import java.util.Random;

/**
 * Created by root on 2019/8/8.
 */
public class RandomPrefixUDF implements UDF2<String, Integer, String> {


    @Override
    public String call(String val, Integer num) throws Exception {
        Random random = new Random();
        int randNum = random.nextInt(num);
        return randNum + "_" + val;
    }
}
