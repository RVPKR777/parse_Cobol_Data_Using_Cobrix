import org.apache.spark.sql.SparkSession

object moviedataset {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().config("spark.master", "local").appName("moviedatasdet").getOrCreate()
    //val data = spark.read.textFile("C:\\Users\\pavan\\Downloads\\bollywood-movie-dataset\\BollywoodMovieDetail.csv")
    //val group = filerdd.groupByKey()
    val data = spark.sparkContext.parallelize(Seq(("maths", 52), ("english", 75), ("science", 82), ("computer", 65), ("maths", 85)))
    val sorted = data.countByValue()
    sorted.foreach(println)
    spark.stop()
  }
}
