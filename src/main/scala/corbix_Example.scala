import org.apache.spark.sql.SparkSession

object corbix_Example {

  def main(args: Array[String]): Unit = {

    val sparkBuilder = SparkSession.builder().appName("corbix example")
    val spark = sparkBuilder
      .master("local[*]")
      .getOrCreate()

    val A1df = spark
      .read
      .format("cobol")
      .option("copybook", "Copy book path")
      //.option("schema_retention_policy", "collapse_root")    //To check data with schema
      .load("data file path")

    A1df.show(100, truncate = false)

    val op= A1df.rdd.map(_.toString().replace("[","").replace("]", "")).saveAsTextFile("path")

  }
}
