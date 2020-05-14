import org.apache.spark.{SparkConf, SparkContext}

object hello {
  def main(args : Array[String]){
    val conf = new SparkConf().setAppName("hwllo").setMaster("local")
    val sc = new SparkContext(conf)
    val data = Array(1, 2, 3, 4, 5)
    sc.parallelize(data, 2)
    Thread.sleep(Long.MaxValue)
  }
}
