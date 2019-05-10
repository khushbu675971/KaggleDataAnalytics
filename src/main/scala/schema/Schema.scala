package schema
import org.apache.spark.sql.types._

trait Schema {
  val googlePlayStoreSchema = StructType(List(
      StructField("App", StringType, true),
      StructField("Category", StringType, true),
      StructField("Rating", DoubleType, true),
      StructField("Reviews", IntegerType, true),
      StructField("Size", DoubleType, true),
      StructField("Installs", LongType, true),
      StructField("Type", StringType, true),
      StructField("Price", DoubleType, true),
      StructField("Content Rating", StringType, true),
      StructField("Genres", StringType, true),
      StructField("Last Updated", StringType, true),
      StructField("Current Ver", StringType, true),
      StructField("AndroidVer", DoubleType, true)
    ))

  val userReviewSchema = StructType(List(
      StructField("App", StringType, true),
      StructField("Translated_Review", StringType, true),
      StructField("Sentiment", StringType, true),
      StructField("Sentiment_Polarity", DoubleType, true),
      StructField("Sentiment_Subjectivity", DoubleType, true)
    ))
}