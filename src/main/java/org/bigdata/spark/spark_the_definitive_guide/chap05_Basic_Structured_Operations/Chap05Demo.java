package org.bigdata.spark.spark_the_definitive_guide.chap05_Basic_Structured_Operations;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * @Description:
 * @Author: FanYueXiang
 * @Date: 2020/5/6 11:56 PM
 */
public class Chap05Demo {

    public static void main(String[] args){
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("Chapter4StructuredAPIOverview")
                .getOrCreate();
        String filePath = "/Users/yuexiangfan/coding/bigData/big_data_in_action/src/main/java/org/bigdata/spark/spark_the_definitive_guide/data/flight_data/json/2015-summary.json";
        Dataset<Row> flightDf = spark.read().json(filePath);
        StructType staticSchema = flightDf.schema();
        // 自定义Schema
        StructType mySchema = new StructType(new StructField[]{
                DataTypes.createStructField("DEST_COUNTRY_NAME", DataTypes.StringType, true),
                DataTypes.createStructField("ORIGIN_COUNTRY_NAME", DataTypes.StringType, true),
                DataTypes.createStructField("count", DataTypes.LongType, false, Metadata.fromJson("{\"hello\":\"world\"}")),

        });
        Dataset<Row> schemaDf = spark.read().schema(mySchema).json(filePath);
        schemaDf.show(5);
    }


}
