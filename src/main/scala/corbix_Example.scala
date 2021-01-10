import org.apache.spark.sql.SparkSession

object corbix_Example {

  def main(args: Array[String]): Unit = {

    val sparkBuilder = SparkSession.builder().appName("corbix example")
    val spark = sparkBuilder
      .master("local[*]")
      .getOrCreate()

    val df = spark
      .read
      .format("cobol")
      .option("copybook", "C:/Users/pavan/Downloads/copy_book.cpy")
      //.option("schema_retention_policy", "collapse_root")
      .load("C:/Users/pavan/Downloads/source_binary.txt")

    df.show(100, truncate = false)

    val op= df.rdd.map(_.toString().replace("[","").replace("]", "")).saveAsTextFile("C:/Users/pavan/Desktop/sample1")

  }
}
