package org.bigdata.spark.learning_spark_fast_data_analysis.chap03_A_Tour_of_Sparks_Toolset;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.OneHotEncoderEstimator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.*;

/**
 * @description: Spark 机器学习库 KNN Deamo
 * @author: fanyeuxiang
 * @createDate: 2020-05-06 13:03
 */
public class Chap03KNNDemo {

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
        SparkSession spark = SparkSession.builder().master("local").appName("Chap03KNNDemo").getOrCreate();
        Dataset<Row> staticDataFrame = spark.read().option("header", "true").option("inferSchema", "true").csv(filePath);
        //date_format函数转换日志格式
        Dataset<Row> preppedDataFrame = staticDataFrame.na().fill(0).withColumn("day_of_week", functions.date_format(new Column("InvoiceDate"),"EEEE")).coalesce(5);
        preppedDataFrame.show(5);
        // 将数据分为训练集和测试集
        Dataset<Row> trainingData = preppedDataFrame.where("InvoiceDate < '2011-07-01'");
        Dataset<Row> testData = preppedDataFrame.where("InvoiceDate > '2011-07-01'");
        // Spark ML 中的标签转换器
        StringIndexer stringIndexer = new StringIndexer().setInputCol("day_of_week").setOutputCol("day_of_week_index");
        OneHotEncoderEstimator oneHotEncoderEstimator = new OneHotEncoderEstimator().setInputCols(new String[]{"day_of_week_index"}).setOutputCols(new String[] {"day_of_week_encoded"});
        VectorAssembler vectorAssembler = new VectorAssembler().setInputCols(new String[]{"UnitPrice", "Quantity", "day_of_week_encoded"}).setOutputCol("features");
        Pipeline transformationPipeline = new Pipeline().setStages(new PipelineStage[]{stringIndexer, oneHotEncoderEstimator, vectorAssembler});
        PipelineModel fittedPipeline = transformationPipeline.fit(trainingData);
        Dataset<Row> transformedTraining = fittedPipeline.transform(trainingData);
        transformedTraining.select("features").show(5);
        KMeans kMeans = new KMeans().setK(20).setSeed(1L);
        KMeansModel kmModel = kMeans.fit(transformedTraining);
        Dataset<Row> transformedTest = fittedPipeline.transform(testData);
        Dataset<Row> kmTest = kmModel.transform(transformedTest);
        kmTest.show(5);
    }

}
