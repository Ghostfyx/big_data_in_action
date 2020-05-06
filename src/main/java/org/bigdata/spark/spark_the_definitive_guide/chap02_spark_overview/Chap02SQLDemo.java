package org.bigdata.spark.spark_the_definitive_guide.chap02_spark_overview;

import org.apache.spark.sql.*;

/**
 * @Description: 第二章 Spark简介
 * @Author: FanYueXiang
 * @Date: 2020/4/29 9:49 PM
 */
public class Chap02SQLDemo {

    public static void main(String[] args){
        SparkSession spark = SparkSession.builder().appName("Chap02Demo").master("local").getOrCreate();
        // Spark Dataset和DataFrame是分布式的Row类型对象集合，可以通过定义Java Bean的形式保证类型安全
        Dataset<Row> myRange = spark.range(1000).toDF("number");
        System.out.println(myRange.collect());

        // 从本地文件系统读取csv文件
        Dataset<Row> flightData2015 = spark.read().option("inferSchema", "true").option("header","true")
                .csv("/Users/yuexiangfan/coding/bigData/big_data_in_action/src/main/java/org/bigdata/spark/learning_spark_fast_data_analysis/data/flight_data/2015-summary.csv");
        flightData2015.show();
        System.out.println("-----------------------------------------------------------");
        // Spark 结构化API创建临时表
        flightData2015.createOrReplaceTempView("flight_data_2015");

        //Spark SQL语句方式执行操作
        String sql = "SELECT DEST_COUNTRY_NAME, sum(count) as SUM_COUNT FROM flight_data_2015 GROUP BY DEST_COUNTRY_NAME";
        Dataset<Row> sqlOperate = spark.sql(sql);
        sqlOperate.show();
        System.out.println("-----------------------------------------------------------");
        // Java使用Dataset类型安全的方式读取数据，
        // 其中一种解决方案是：Pojo类的属性名称必须与输入的结构化数据源列名一致
        // 另一种解决方案是：将数据读取为Dataset<Row>的形式，然后使用map函数转换为JAVA Pojo类型
        Encoder<FlightData> flightDataEncoder = Encoders.bean(FlightData.class);
        Dataset<FlightData> flightDataset2015 = spark.read().option("inferSchema", "true").option("header","true")
                .csv("/Users/yuexiangfan/coding/bigData/big_data_in_action/src/main/java/org/bigdata/spark/learning_spark_fast_data_analysis/data/flight_data/2015-summary.csv")
                .as(flightDataEncoder);
        flightData2015.show();
        Dataset<Row> flightDataDataset = flightDataset2015.groupBy("DEST_COUNTRY_NAME").sum("count");
        flightDataDataset.show();
        System.out.println("-----------------------------------------------------------");
        String maxSql = "SELECT DEST_COUNTRY_NAME, sum(count) as destination_total\n" +
                "                FROM flight_data_2015\n" +
                "                GROUP BY DEST_COUNTRY_NAME\n" +
                "                ORDER BY sum(count) DESC\n" +
                "                LIMIT 5";
        Dataset<Row> maxflightDataCount = spark.sql(maxSql);
        maxflightDataCount.show();
        System.out.println("-----------------------------------------------------------");
        flightDataset2015.groupBy("DEST_COUNTRY_NAME").sum("count")
                .withColumnRenamed("sum(count)", "destination_total")
                .orderBy("destination_total").limit(5);

    }

}
