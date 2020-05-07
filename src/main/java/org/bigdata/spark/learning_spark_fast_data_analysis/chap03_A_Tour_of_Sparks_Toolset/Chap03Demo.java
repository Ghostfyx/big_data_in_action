package org.bigdata.spark.learning_spark_fast_data_analysis.chap03_A_Tour_of_Sparks_Toolset;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.StructType;

/**
 * @Description: Spark权威指南第三章 Spark工具集介绍示例代码
 * @Author: FanYueXiang
 * @Date: 2020/5/6 6:38 AM
 */
public class Chap03Demo {

    private static String filePath = "";

    static {
        String os = System.getProperty("os.name").toLowerCase();
        if(os.contains("windows")){
            filePath = "D:/experiment/big_data_in_action/src/main/java/org/bigdata/spark/learning_spark_fast_data_analysis/data/retail_data/by_day/*.csv";
        }else {
            filePath = "/Users/yuexiangfan/coding/bigData/big_data_in_action/src/main/java/org/bigdata/spark/learning_spark_fast_data_analysis/data/retail_data/by_day/*.csv";
        }
    }

    public static void main(String[] args){
        SparkSession spark = SparkSession.builder().appName("Chap03Demo").master("local").getOrCreate();
        Dataset<Row> staticDataFrame = spark.read().option("header", "true").option("inferSchema", "true")
                .csv(filePath);
        staticDataFrame.createOrReplaceTempView("retail_data");
        StructType staticSchema = staticDataFrame.schema();
        System.out.println(staticSchema);
        WindowSpec spec = Window.partitionBy("InvoiceDate").rangeBetween(0, 1);
        staticDataFrame.selectExpr("CustomerId",
                "(UnitPrice * Quantity) as total_cost",
                "InvoiceDate").groupBy(
                        new Column("CustomerId"))
                .sum("total_cost").sort(new Column("sum(total_cost)").desc()).show(5);

        // 模拟流式数据读取;maxFilesPerTigger选项每次读完一个文件后都会被触发
        Dataset<Row> streamingDataFrame = spark.readStream().schema(staticSchema).option("maxFilesPerTrigger", 1).format("csv").option("header","true").load(filePath);
        // 对流式数据与静态数据采取同样的转换操作
        Dataset<Row> purchaseByCustomerPerHour = streamingDataFrame.selectExpr("CustomerId",
                "(UnitPrice * Quantity) as total_cost",
                "InvoiceDate").groupBy(
                new Column("CustomerId"))
                .sum("total_cost").sort(new Column("sum(total_cost)").desc());
        // 将流式数据写入缓存中，每当新的数据到来更新缓存中的统计指标
        purchaseByCustomerPerHour.writeStream()
                .format("memory") // memory代表将表存储内存
                .queryName("customer_purchases") //存入内存的表名
                .outputMode("complete").start(); // complete表示保存表中所有记录

        spark.sql("select * from customer_purchases").show(5);
    }
}
