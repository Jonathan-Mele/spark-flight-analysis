import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object FlightAnalysis {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("Usage: FlightAnalysis <path_to_csv>")
      sys.exit(1)
    }

    val filePath = args(0)

    val spark = SparkSession.builder()
      .appName("Flight Data Analysis")
      .master("local[*]") 
      .getOrCreate()

    import spark.implicits._

    val flights = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(filePath)

    // total flights to/from each country
    val originCounts = flights.groupBy($"ORIGIN_COUNTRY_NAME")
      .agg(sum($"count").alias("total_origin"))
    val destCounts = flights.groupBy($"DEST_COUNTRY_NAME")
      .agg(sum($"count").alias("total_dest"))

    val totalByCountry = originCounts.join(destCounts,
      originCounts("ORIGIN_COUNTRY_NAME") === destCounts("DEST_COUNTRY_NAME"),
      "outer")
      .withColumn("country",
        coalesce($"ORIGIN_COUNTRY_NAME", $"DEST_COUNTRY_NAME"))
      .withColumn("total_flights",
        coalesce($"total_origin", lit(0)) + coalesce($"total_dest", lit(0)))
      .select("country", "total_flights")
      .orderBy(desc("total_flights"))

    // maximum number of flights to/from any location 
    val maxFlights = totalByCountry.agg(max("total_flights")).first().getLong(0)
    println(s"\nMaximum number of flights (to/from a single country): $maxFlights")

    // Top 5 destination countries
    val top5Destinations = flights.groupBy("DEST_COUNTRY_NAME")
      .agg(sum("count").alias("total_incoming"))
      .orderBy(desc("total_incoming"))
      .limit(5)

    println("\nTop 5 Destination Countries:")
    top5Destinations.show()

    spark.stop()
  }
}
