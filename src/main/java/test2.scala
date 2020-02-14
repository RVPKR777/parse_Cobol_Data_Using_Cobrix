import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object test2 {
  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("test2")
    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    val df = spark.read.textFile("C:/Users/pavan/Desktop/input.txt").toDF()
    val df1 = spark.read.textFile("C:/Users/pavan/Desktop/input2.txt").toDF()
    val union1 = df.union(df1)
    df.show()
    df1.show()
    //df1.show()
    spark.stop()
  }
}
