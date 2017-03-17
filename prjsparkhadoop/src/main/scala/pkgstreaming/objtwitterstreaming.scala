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

object objtwitterstreaming {

  def getJsonContent(jsonstring: String): (String, String) = {
    implicit val formats = DefaultFormats
    val parsedJson = parse(jsonstring)  
    val value1 = (parsedJson \ "key1").extract[String]
    val level2value1 = (parsedJson \ "key2" \ "level2key1").extract[String]
    (value1, level2value1)
}
  val getJsonContentUDF = udf((jsonstring: String) => getJsonContent(jsonstring))
  
  def main(args: Array[String]) {
      // Set the log level to only print errors
      Logger.getLogger("org").setLevel(Level.ERROR)
  
      implicit val codec = Codec("UTF-8")
      codec.onMalformedInput(CodingErrorAction.REPLACE)
      codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

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
      val tweets = TwitterUtils.createStream(ssc, None)
    
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
                where text like '%#%'
                group by 1
                order by cnt desc""")
        println(s"=========  =========")
        wordCountsDataFrame.collect.foreach(println)
        wordCountsDataFrame.printSchema()  
    }        
      
    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()

    }
    
}

//** Case class for converting RDD to DataFrame */
case class Record(word: String)


/** Lazily instantiated singleton instance of SparkSession */
object SparkSessionSingleton {

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
}