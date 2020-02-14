import org.apache.spark.sql.SparkSession

object pokemon {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().config("spark.master", "local").appName("Pokemon").getOrCreate()
    val filerdd = spark.read.textFile("C:/Users/pavan/Desktop/pokemon.csv").cache()
    val namerdd = filerdd.filter(line => line.contains("water")).count()
    println(s"water pokemons count are: $namerdd")
    spark.stop()
  }
}
