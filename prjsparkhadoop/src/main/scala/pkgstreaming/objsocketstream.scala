package pkgstreaming

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext._
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import org.apache.log4j.{Level, Logger}
import java.util.regex.Pattern
import java.util.regex.Matcher
import scala.io.Source
import scala.io.Codec
import java.nio.charset.CodingErrorAction

import pkgutilities.utilities._
import org.json4s.{DefaultFormats, MappingException}
import org.json4s.jackson.JsonMethods._
import org.apache.spark.sql.functions._


object objsocketstream {

  def main(args: Array[String]) {

      // Set the log level to only print errors
      Logger.getLogger("org").setLevel(Level.ERROR)
  
      implicit val codec = Codec("UTF-8")
      codec.onMalformedInput(CodingErrorAction.REPLACE)
      codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

      val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SocketStreaming")
      val sc = new SparkContext(sparkConf)
      val sqlcontext = new SQLContext(sc);      
      val ssc = new StreamingContext(sc, Seconds(30))
      import sqlcontext.implicits._

    	val lines = ssc.socketTextStream("localhost", 9000)

    	lines.foreachRDD { (rdd : RDD[String]) =>  
        
      	  // Get the singleton instance of SparkSession
          val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
          import spark.implicits._
  
          // Convert RDD[String] to RDD[case class] to DataFrame
          val lines = rdd.map(w => Record(w.toString())).toDF()
  
          // Creates a temporary view using the DataFrame
          lines.createOrReplaceTempView("vwsocketstream")
          lines.printSchema()
          lines.collect.foreach(println)
  
          //Word count within RDD    
          lines.write.mode(SaveMode.Append).option("header", "true").csv(op_dir+"/socketstreamop.csv")

      }
   }  	
}
/*
//** Case class for converting RDD to DataFrame */
case class Record(word: String)

/** Lazily instantiated singleton instance of SparkSession */
object sSparkSessionSingleton {

  @transient  private var instance: SparkSession = _

  def getInstance(sparkConf: SparkConf): SparkSession = {
    if (instance == null) {
      instance = SparkSession
        .builder
        .config(sparkConf)
        .getOrCreate()
    }
    instance
  }
}*/