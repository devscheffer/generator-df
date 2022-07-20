import org.apache.spark.sql.SparkSession

trait SparkSessionTest {

  /** This creates a SparkSession, which will be used to operate on the
    * DataFrames that we create.
    */
  val spark: SparkSession = SparkSession
    .builder()
    .appName("Spark App")
    .config("spark.master", "local")
    .getOrCreate()
}
