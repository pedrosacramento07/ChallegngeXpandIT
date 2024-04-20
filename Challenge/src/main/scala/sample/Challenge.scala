package sample
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, collect_list, count,concat_ws, date_format, desc, format_number, max, regexp_replace, split, to_date, when}
import org.apache.spark.sql.functions.explode

import java.io.File


object Challenge {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CSV Import")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("OFF")

    //Read files
    val dfUserReviews = spark.read.option("header", "true").csv("./resources/googleplaystore_user_reviews.csv")
    val dfApps = spark.read.option("header", "true").csv("./resources/googleplaystore.csv")

    //Part 1
    dfUserReviews.createOrReplaceTempView("UsersReviews")
    val df_1 = spark.sql("select App, coalesce (avg(Sentiment_Polarity),0) as Average_Sentiment_Polarity " +
      "from UsersReviews group by App")

    println("Part1 :")
    df_1.show()

    //Part 2
    dfApps.createOrReplaceTempView("Apps")
    val df_2 = spark.sql("select App, Rating from Apps where Rating >= 4 order by Rating desc")

    println("Part2 :")
    df_2.show()

    df_2.coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", "\u00A7")
      .save("best_apps")

    val directory = new File("./best_apps")

    if (directory.exists && directory.isDirectory) {
      val file = directory.listFiles.filter(_.getName.endsWith(".csv")).head
      val newFile = new File(directory, "best_apps.csv")
      file.renameTo(newFile)
    } else {
      println("Specified path is not a directory or does not exist.")
    }

    //Part 3
    dfApps.createOrReplaceTempView("Apps")
    val conversionRate = 0.9
    val df_without_categories = spark.sql(
        "select distinct App,Rating,Reviews,Size,Installs,Type,Price,`Content Rating` as Content_Rating,Genres," +
          "`Last Updated` as Last_Updated,`Current Ver` as Current_Version,`Android Ver` as Minimum_Android_Version " +
          "from Apps as a " +
          "where Reviews = (" +
          "select max(Reviews)" +
          "from Apps as b " +
          "where a.App = b.App) ")
      .withColumn(
        "Size",
        when(col("Size").rlike("\\d"), regexp_replace(col("Size"), "[^\\d.]", "").cast("double"))
          .otherwise(null)
      )
      .withColumn(
        "Price",
        when(col("Price").rlike("\\d"), format_number(regexp_replace(col("Price"), "\\$", "").cast("double") * conversionRate, 2))
          .otherwise(null)
      )
      .withColumn(
        "Genres",
        when(col("Genres").rlike("\\w"), split(col("Genres"), ";"))
          .otherwise(null)
      )
      .withColumn(
        "Last_Updated",
        when(col("Last_Updated").rlike("\\w"), date_format(to_date(col("Last_Updated"), "MMMM d, yyyy"), "yyyy-MM-dd HH:mm:ss"))
          .otherwise(null)
      )

    val df_3_categories = dfApps
      .groupBy("App")
      .agg(concat_ws(",", collect_list("Category")).as("Categories"))
      .withColumn("Categories", split(col("Categories"), ","))
      .distinct()

    val dfJoined = df_without_categories.join(df_3_categories, Seq("App"), "inner")
    dfJoined.createOrReplaceTempView("DfJoined")
    val df_3 = spark.sql("select App, Categories, Rating, Reviews, Size, Installs, Type, Price, Content_Rating, Genres, Last_Updated, Current_Version,Minimum_Android_Version from DfJoined")

    println("Part3 :")
    df_3.show()

    //Part 4
    val df_join_df1_and_df3 = df_3.join(df_1, Seq("App"), "inner")

    println("Part4 :")
    df_join_df1_and_df3.show()

    //Save df_join_df1_and_df3 as a parquet file with gzip compression
    df_join_df1_and_df3.repartition(1).write
      .option("compression", "gzip")
      .parquet("googleplaystore_cleaned.parquet")

    //Part 5
    val df_collapsed_genres = df_join_df1_and_df3.select(col("*"), explode(col("Genres")).as("Genre"))

    val df_4 = df_collapsed_genres.groupBy("Genre")
      .agg(
        count("App").alias("Count"),
        avg("Rating").alias("Average_Rating"),
        avg("Average_Sentiment_Polarity").alias("Average_Sentiment_Polarity")
      )

    println("Part5 :")
    df_4.show()

    // Save df_4 as a parquet file with gzip compression
    df_4.repartition(1).write
      .option("compression", "gzip")
      .parquet("googleplaystore_metrics.parquet")
  }

}
