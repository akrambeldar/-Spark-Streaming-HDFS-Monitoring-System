package com.akram.streaming

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

/**
 * Real-Time Word & Bigram Frequency Counter
 * -----------------------------------------
 * - Reads streaming text files
 * - Cleans and tokenizes text
 * - Computes word counts
 * - Computes adjacent word-pair (bigram) counts
 * - Writes output to Parquet (production style)
 */

object StreamingWordPairs {

  def main(args: Array[String]): Unit = {

    val inputDir       = getArg(args, "--inputDir", "/tmp/stream_in")
    val outputDir      = getArg(args, "--outputDir", "/tmp/stream_out")
    val checkpointDir  = getArg(args, "--checkpointDir", "/tmp/stream_chk")
    val triggerSeconds = getArg(args, "--triggerSecs", "10").toInt

    val spark = SparkSession.builder()
      .appName("StreamingWordPairs")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // ===============================
    // 1. READ STREAMING SOURCE
    // ===============================
    val rawLines = spark.readStream
      .format("text")
      .load(inputDir)
      .select(col("value").alias("line"))

    // ===============================
    // 2. CLEAN & TOKENIZE TEXT
    // ===============================
    val cleaned = rawLines
      .withColumn("clean_line",
        trim(regexp_replace(lower(col("line")), "[^a-z0-9\\s]", ""))
      )

    val words = cleaned
      .withColumn("word", explode(split(col("clean_line"), "\\s+")))
      .filter(length(col("word")) > 0)
      .select("word")

    // ===============================
    // 3. WORD COUNT AGGREGATION
    // ===============================
    val wordCounts = words
      .groupBy("word")
      .count()

    // ===============================
    // 4. BIGRAM (WORD-PAIR) CREATION
    // ===============================
    val tokenArrays = cleaned
      .withColumn("tokens",
        filter(split(col("clean_line"), "\\s+"), x => length(x) > 0)
      )
      .filter(size(col("tokens")) >= 2)

    val bigrams = tokenArrays
      .withColumn("idx", sequence(lit(0), size(col("tokens")) - 2))
      .withColumn("idx", explode(col("idx")))
      .withColumn("w1", col("tokens")(col("idx")))
      .withColumn("w2", col("tokens")(col("idx") + 1))
      .withColumn("pair", concat_ws(" ", col("w1"), col("w2")))
      .select("pair")

    val pairCounts = bigrams
      .groupBy("pair")
      .count()

    // ===============================
    // 5. WRITE STREAMS (PARQUET)
    // ===============================

    val wordQuery = wordCounts.writeStream
      .format("parquet")
      .outputMode("complete")
      .option("checkpointLocation", s"$checkpointDir/words")
      .option("path", s"$outputDir/words")
      .trigger(Trigger.ProcessingTime(s"$triggerSeconds seconds"))
      .start()

    val pairQuery = pairCounts.writeStream
      .format("parquet")
      .outputMode("complete")
      .option("checkpointLocation", s"$checkpointDir/pairs")
      .option("path", s"$outputDir/pairs")
      .trigger(Trigger.ProcessingTime(s"$triggerSeconds seconds"))
      .start()

    wordQuery.awaitTermination()
    pairQuery.awaitTermination()
  }

  private def getArg(args: Array[String], key: String, defaultValue: String): String = {
    val index = args.indexOf(key)
    if (index >= 0 && index < args.length - 1) args(index + 1)
    else defaultValue
  }
}