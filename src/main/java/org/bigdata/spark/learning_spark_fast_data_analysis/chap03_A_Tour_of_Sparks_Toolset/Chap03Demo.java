package org.bigdata.spark.learning_spark_fast_data_analysis.chap03_A_Tour_of_Sparks_Toolset;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

/**
 * @Description: Spark权威指南第三章 Spark工具集介绍示例代码
 * @Author: FanYueXiang
 * @Date: 2020/5/6 6:38 AM
 */
public class Chap03Demo {

    public static void main(String[] args){
        SparkSession spark = SparkSession.builder().appName("Chap03Demo").master("local").getOrCreate();
        Dataset<Row> staticDataFrame = spark.read().option("header", "true").option("inferSchema", "true")
                .csv("/Users/yuexiangfan/coding/bigData/big_data_in_action/src/main/java/org/bigdata/spark/learning_spark_fast_data_analysis/data/retail_data/by_day/*.csv");
        staticDataFrame.createOrReplaceTempView("retail_data");
        StructType  staticSchema = staticDataFrame.schema();
        System.out.println(staticSchema);
        staticDataFrame.selectExpr("CustomerId",
                "(UnitPrice * Quantity) as total_cost",
                "InvoiceDate").groupBy(
                        new Column("CustomerId"))
                .sum("total_cost").sort(new Column("sum(total_cost)").desc()).show(5);
    }
}
