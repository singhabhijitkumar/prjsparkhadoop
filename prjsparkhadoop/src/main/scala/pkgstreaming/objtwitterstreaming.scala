package pkgstreaming

import org.apache.spark.sql.functions.udf
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

import pkgutilities.{SparkSessionSingleton, utilities, Record}
import pkgutilities.utilities._
import pkgutilities.mail._
import org.json4s.{DefaultFormats, MappingException}
import org.json4s.jackson.JsonMethods._
import org.apache.spark.sql.functions._

object objtwitterstreaming {
/*
  def getJsonContent(jsonstring: String): (String, String) = {
    implicit val formats = DefaultFormats
    val parsedJson = org.json4s.jackson.JsonMethods.parse(jsonstring)  
    val value1 = (parsedJson \ "key1").extract[String]
    val level2value1 = (parsedJson \ "key2" \ "level2key1").extract[String]
    (value1, level2value1)
}
  
  val getJsonContentUDF = udf((jsonstring: String) => getJsonContent(jsonstring))
  */
  def main(args: Array[String]) {
      // Set the log level to only print errors
      Logger.getLogger("org").setLevel(Level.ERROR)
  
      implicit val codec = Codec("UTF-8")
      codec.onMalformedInput(CodingErrorAction.REPLACE)
      codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
      
      try {
        pkgutilities.mail.send (
          from = "john.smith@mycompany.com" -> "John Smith",
          to = "singhabhijitkumar2@gmail.com" :: "marketing@mycompany.com" :: Nil,
          subject = "Our 5-year plan",
          message = "Here is the presentation with the stuff we're going to for the next five years."
          //,attachment = new java.io.File("/home/boss/important-presentation.ppt")
        )
        }
       catch {
          case ex: Exception => println("unable to send mail")
          println(ex.printStackTrace)
        }
      
      val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Streaming")
      val sc = new SparkContext(sparkConf)
      val sqlcontext = new SQLContext(sc);      
      import sqlcontext.implicits._
      val ssc = new StreamingContext(sc, Seconds(30))

      /** Configures Twitter service credentials using twiter.txt in the main workspace directory */
      for (line <- Source.fromFile(resouces_dir+"/twitter.txt").getLines) {
        val fields = line.split(" ")
        if (fields.length == 2) {
          System.setProperty("twitter4j.oauth." + fields(0), fields(1))
        }
      }
      
      // Create a DStream from Twitter using our streaming context
      val tweets = TwitterUtils.createStream(ssc, None, Array("#YogiAdityanath", "#SriSri"))
    
      // Convert RDDs of the words DStream to DataFrame and run SQL query
      tweets.foreachRDD { m =>
    println("**************************STARTING********************")
        // Now extract the text of each status update into DStreams using map()
        val statuses = m.map(status => status.getText())

        // Get the singleton instance of SparkSession
        val spark = SparkSessionSingleton.getInstance(m.sparkContext.getConf)
        import spark.implicits._

        // Convert RDD[String] to RDD[case class] to DataFrame
        //val wordsDataFrame_json = m.map(w => Record(w.toString())).toDF()
        //val wordsDataFrame = wordsDataFrame_json.withColumn("parsedJson",  getJsonContentUDF(wordsDataFrame_json("word")))
        val wordsDataFrame = m.map(w => Record(w.toString())).toDF()

        // Creates a temporary view using the DataFrame
        wordsDataFrame.createOrReplaceTempView("words")
        wordsDataFrame.printSchema()
        wordsDataFrame.collect.foreach(println)
        
        // Do word count on table using SQL and print it
        val wordCountsDataFrame =
        spark.sql("""
                select text, count(1) as cnt
                from (select regexp_extract(word, 'text=(.*)source=', 1) as text from words)
                group by 1
                order by cnt desc""")
        println(s"=========  =========")
        wordCountsDataFrame.collect.foreach(println)
        wordCountsDataFrame.printSchema()  
    }        
      
    ssc.checkpoint("C:/checkpoint/objtwitterstreaming")
    ssc.start()
    ssc.awaitTermination()

    }
  
    
}
