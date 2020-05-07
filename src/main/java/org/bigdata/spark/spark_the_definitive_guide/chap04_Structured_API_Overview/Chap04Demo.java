package org.bigdata.spark.spark_the_definitive_guide.chap04_Structured_API_Overview;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @Description:
 * @Author: FanYueXiang
 * @Date: 2020/5/6 11:33 PM
 */
public class Chap04Demo {

    public static void main(String[] args){
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("Chapter4StructuredAPIOverview")
                .getOrCreate();
        Dataset<Row> ds = spark.range(500).toDF("number");
        ds.select(ds.col("number").plus(10)).show();

        Object[] dataObjects = (Object[])spark.range(2).toDF().collect();
        for(Object object: dataObjects) {
            System.out.println(object.toString());
        }

    }

}
