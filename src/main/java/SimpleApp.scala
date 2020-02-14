import org.apache.spark.sql.SparkSession

object SimpleApp {
  def main(args: Array[String]) {
    //val logFile = "C:/Users/pavan/Desktop/imp topics.txt" // Should be some file on your system
    val spark = SparkSession.builder().config("spark.master", "local").appName("SimpleApp").getOrCreate()
    val logData = spark.read.textFile("C:/Users/pavan/Desktop/input2.txt").cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    spark.stop()
  }
}