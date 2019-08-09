package com.sparksql.doublemerge;

import org.apache.spark.sql.api.java.UDF1;

/**
 *
 * @author root
 * @date 2019/8/8
 */
public class RemoveRandomPrefixUDF implements UDF1<String, String> {

    @Override
    public String call(String val) throws Exception {
        String[] valSplited = val.split("_");
        return valSplited[1];
    }
}
