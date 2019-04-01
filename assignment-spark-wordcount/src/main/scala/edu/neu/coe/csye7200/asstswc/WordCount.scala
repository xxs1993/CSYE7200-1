package edu.neu.coe.csye7200.asstswc

import org.apache.spark.sql.functions.{stddev_samp, stddev_pop}
import org.apache.spark.sql._
/***
  * This is an example of spark word count.
  * Replace the ??? with appropriate implementation and run this file
  * with the argument "input/ScalaWiki.txt"
  * Write down the count of word "Scala" in your submission
  * You can find the implementation from https://spark.apache.org/examples.html
  */

object WordCount extends App {



  override def main(args: Array[String]) = {
    val spark = SparkSession
      .builder()
      .appName("WordCount")
      .master("local[*]")
      .getOrCreate()
    val dataFrame = spark.read.csv("rating.csv")
    processDataFrame(dataFrame).show()
    spark.stop()
  }

  /**
    *
    * process data : get dataFrame needed
    * @param df
    * @return
    */
  def processDataFrame(df:DataFrame):DataFrame={
    implicit val encoder = Encoders.tuple(Encoders.scalaInt,Encoders.scalaDouble)
    val df1 = df.filter(x=>x.getString(0)!="movieId")
       .map(x=>(x.getString(0).toInt,x.getString(1).toDouble))
    val df_groupBy = df1.groupBy("_1")
    val df_std = df_groupBy.agg(stddev_samp("_2")).toDF("movieId","std")
    val df_avg = df_groupBy.avg("_2").toDF("movieId","average").join(df_std,"movieId").sort("movieId")
    return df_avg
  }




}
