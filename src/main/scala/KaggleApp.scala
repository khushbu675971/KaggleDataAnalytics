import java.util.logging.Logger

import org.apache.spark.sql._
import schema.Schema

object KaggleApp extends Schema with Serializable {
    protected val log = Logger.getLogger(getClass.getName)
      // val googlePlayStoreFilePath = "src/main/resources/googleplaystore.csv"
      // var userReviewFIlePath = "src/main/resources/googleplaystore_user_reviews.csv"
      val bucketCount = 20

    case class HistRow(startPoint: Double, count: Long)

    def runListApplication(spark: SparkSession)= {
      val results1 = spark.sql("SELECT * FROM googlePlayStore WHERE Reviews >= 10000 and Installs >= 50000 and AndroidVer >= 4.0")
      log.info("List all applications where Reviews >= 10.000, Installs >= 50.000, AndroidVer >= Android 4.0")
      results1.show()
      /*
        Saving data to a rdms database for providing it for dashboard
         results1.write
        .format("jdbc")
        .option("url", "jdbc:postgresql:dbserver")
        .option("dbtable", "schema.tablename")
        .option("user", "username")
        .option("password", "password")
        .save()
       */
    }

    def runTop10Application(spark: SparkSession)= {
      val results2 = spark.sql("SELECT App, count(*) As sentiments from userReview where Sentiment = 'Positive' group by App order by sentiments DESC LIMIT 10")
      log.info("Top 10 application with highest sentiments")
      results2.show()
      /*
      Saving data to a rdms database for providing it for dashboard
       results2.write
      .format("jdbc")
      .option("url", "jdbc:postgresql:dbserver")
      .option("dbtable", "schema.tablename")
      .option("user", "username")
      .option("password", "password")
      .save()
     */
    }

    def runCreateHistogram(spark: SparkSession, dataFrame: DataFrame)= {
      val (startValues, counts) = dataFrame.select("Installs").rdd.map(r => r.getLong(0)).histogram(bucketCount)
      import spark.implicits._

      val zippedValues = startValues.zip(counts)
      val rowRDD = spark.sparkContext.parallelize(zippedValues)
      val histDf = rowRDD.map(value => HistRow(value._1,value._2)).toDF

      histDf.createOrReplaceTempView("histogramTable")

      val histResult = spark.sql("select * FROM histogramTable")

      log.info("Histogram for the number of installations")
      histResult.show()
      /*
      Saving data to a rdms database for providing it for dashboard
       histResult.write
      .format("jdbc")
      .option("url", "jdbc:postgresql:dbserver")
      .option("dbtable", "schema.tablename")
      .option("user", "username")
      .option("password", "password")
      .save()
     */

    }

    def runCountDistGenres(spark: SparkSession)= {
      val genresDF = spark.sql("SELECT distinct(genres) FROM googlePlayStore")
      log.info("Count of distinct genres")
      genresDF.show()
      /*
      Saving data to a rdms database for providing it for dashboard
       genresDF.write
      .format("jdbc")
      .option("url", "jdbc:postgresql:dbserver")
      .option("dbtable", "schema.tablename")
      .option("user", "username")
      .option("password", "password")
      .save()
     */
    }

    def runSentimentsRatingJoin(spark: SparkSession)= {
      /*
      This sql query joins the table1 first filtering rows which is satisfying condition more positive sentiments than negative and neutral together with
      then join with table2 on join condition matching appnames and filtering based on rating >=4.2
       */
      val sentiments = spark.sql("SELECT DISTINCT(tmp.App), tmp.total, tmp.negCount, tmp.neutCount, tmp.positiveCount, tt.Rating from (" +
        "SELECT App, count(*) As total, COUNT(IF(Sentiment='Neutral',1,null)) As neutCount, COUNT(IF(Sentiment='Negative',1,null)) As negCount," +
        " COUNT(IF(Sentiment='Positive',1,null)) As positiveCount from userReview group by App having positiveCount > (negCount + neutCount) ) as tmp " +
        "JOIN googlePlayStore tt ON tt.App = tmp.App and tt.Rating >= 4.2"
      )
      log.info("application with Positive sentiments >= (Negative + Neutral) and Rating >= 4.2")
      sentiments.show()
      /*
      Saving data to a rdms database for providing it for dashboard
       sentiments.write
      .format("jdbc")
      .option("url", "jdbc:postgresql:dbserver")
      .option("dbtable", "schema.tablename")
      .option("user", "username")
      .option("password", "password")
      .save()
     */
    }

    def runCorrelation(spark: SparkSession, dataFrame: DataFrame)= {
      /*  Sample Correlation with different columns
          ("Size", "Reviews") = -0.005582867081432516
          ("Size", "Installs") = -0.026140329628850482
          ("Reviews", "AndroidVer") = -0.12727604053419117
          ("Reviews", "Price") = -0.009667264283217675
          ("Installs", "Price") = -0.011689470477901559
      */

      val corr1 = dataFrame.stat.corr("Price", "Reviews")
      log.info("Correlation between Price and Review:" + corr1)

      val corr2 = dataFrame.stat.corr("Installs", "Price")
      log.info("Correlation between Installs and Price:" + corr2)

      val corr3 = dataFrame.stat.corr("Rating", "AndroidVer")
      log.info("Correlation between Rating and AndroidVer:" + corr3)

      /*
     Saving data to a rdms database for providing it for dashboard
      corr1.write
     .format("jdbc")
     .option("url", "jdbc:postgresql:dbserver")
     .option("dbtable", "schema.tablename")
     .option("user", "username")
     .option("password", "password")
     .save()
    */
    }


    def main(args: Array[String]): Unit = {
      val googlePlayStoreFilePath = args(0)
      var userReviewFIlePath = args(1)
      val spark = SparkSession
        .builder()
        .appName("KaggleAnalytics")
        .master("local[8]")
        .getOrCreate()

      // Load the googleplaystore data and create dataframe after cleaning the data for missing values, having value violating data type etc.
      val file1RDD = spark.sparkContext.textFile(googlePlayStoreFilePath)
      val header1 = file1RDD.first()
      val googlePlayStoreRDD = file1RDD.filter(row => row != header1) //Remove Header from file
      val googlePlayStoreRowRDD = googlePlayStoreRDD
        .map(_.split(",(?=([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)")) // remove double quotes for some data eg a,"b",c
        .filter(x => x.length != 12) // filter the data for missing values
        .map(attributes =>
          Row(attributes(0),  attributes(1), attributes(2).toDouble,
            attributes(3) match {
              case s: String => if(s.contains("M")) s.replace("M", "") * 10000 else s.toInt
            },
            attributes(4).replaceAll("[^0-9.]","") match {
              case s: String => if(s.isEmpty) 0.0 else s.toDouble
            },
            attributes(5).replaceAll("\"","").replaceAll(",","").replaceAll("\\+","") match {
              case s: String => if(s.contains("Free")) 0 else s.toLong
            },
            attributes(6),
            attributes(7).substring(1) match{
              case s : String => if(s.isEmpty) 0.0 else s.toDouble
            },
            attributes(8), attributes(9), attributes(10), attributes(11),
            attributes(12).replaceAll("[^0-9.]","") match {
              case s: String => if(s.isEmpty) 0.0 else s.substring(0, 3).toDouble
            }
          ))

      // googlePlayStoreDF
      val googlePlayStoreDF = spark.createDataFrame(googlePlayStoreRowRDD, googlePlayStoreSchema)

      // Register the DataFrame as a SQL temporary view and enable more partitioning to improve JOIN performance
      googlePlayStoreDF.repartition(200).cache().createOrReplaceTempView("googlePlayStore")

      // Load the userreview data and create dataframe after cleaning the data for missing values, having value violating data type etc.
      val file2RDD = spark.sparkContext.textFile(userReviewFIlePath)
      val header2 = file2RDD.first()
      val userReviewRDD = file2RDD.filter(row => row != header2) //Remove Header from file
      val userReviewRowRDD = userReviewRDD
        .map(_.split(",(?=([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)")) //remove double quotes for some data eg a,"b",c
        .filter(x => x.length != 4)  // filter the data for missing values
        .map(attributes =>
          Row(attributes(0), attributes(1), attributes(2),
            attributes(3) match {
              case s: String => if(s.contains("nan")) 0.0 else s.toDouble
            },
            attributes(4) match {
              case s: String => if(s.contains("nan")) 0.0 else s.toDouble
            }
          ))

      val userReviewDF = spark.createDataFrame(userReviewRowRDD, userReviewSchema)

      // Register the DataFrame as a SQL temporary view and enable more partitioning to improve JOIN performance
      userReviewDF.repartition(200).cache().createOrReplaceTempView("userReview")

      //Run the computation by passing created temp views googlePlayStore and userReview
      // 1. List all applications, which have at least 10.000 reviews, at least 50.000 Installations and which are supported on Android 4.0 and higher.
      runListApplication(spark)

      // 2. Find the top 10 applications in regards to the highest number of positive sentiments.
      runTop10Application(spark)

      // 3. Create a histogram (at least the data, visualization is optional) for the number of installations.
      runCreateHistogram(spark, googlePlayStoreDF)

      //4. Count the number of distinct genres
      runCountDistGenres(spark)

      //5. Find all the applications, which have more positive sentiments than negative and neutral together and which have a rating at least 4.2.
      runSentimentsRatingJoin(spark)

      //6. Is there some correlation between some of the attributes (e.g. between Rating and Installs)? Can you back it up with numbers?
      runCorrelation(spark, googlePlayStoreDF)

      spark.close
    }
}
