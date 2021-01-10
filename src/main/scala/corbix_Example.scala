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
      .option("copybook", "copybook path")
      .option("schema_retention_policy", "collapse_root")    //To check data with schema
      .load("data file path")

    A1df.show(10, truncate = false)
    val op= A1df.rdd.map(_.toString().replace("[","").replace("]", "")).saveAsTextFile("path")



    /*
    Automating testing
    Code to create a dataframe(with schema) with the expected data file to compare both the dataframes
    */

    /*
    val schemaString = "CXB_CURR_CD_FROM CXB_CURR_CD_TO CXB_SURCHRG_RATE CXB_CURR_XCHG_RATE CXB_EFF_DT CXB_EXP_DT CXB_USER_ID CXB_CRET_TMSTP CXB_LUPD_TMSTP"
    val fields = schemaString.split(" ").map(fieldName => StructField(fieldName,
      StringType, nullable=true))
    val schema = StructType(fields)
    val A2df = spark.read.option("header",
      "false").schema(schema).csv("data file path")
    A2df.show(10, truncate = false)
    A2df.except(A1df)
    */






  }
}
