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
//import org.apache.spark.streaming.kafka._
import org.apache.log4j.{Level, Logger}
import java.util.regex.Pattern
import java.util.regex.Matcher
//import kafka.serializer.StringDecoder
import scala.io.Source
import scala.io.Codec
import java.nio.charset.CodingErrorAction

import pkgutilities.utilities._
import org.json4s.{DefaultFormats, MappingException}
import org.json4s.jackson.JsonMethods._
import org.apache.spark.sql.functions._

object objsparkstreaming {

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

      
      //twitterstreaming(ssc)
    }
    
/*    def kafkastreaming(ssc: StreamingContext) {
  
      // Construct a regular expression (regex) to extract fields from raw Apache log lines
      val pattern = apacheLogPattern()
  
      // hostname:port for Kafka brokers, not Zookeeper
      val kafkaParams = Map("metadata.broker.list" -> "localhost:9092")
      // List of topics you want to listen for from Kafka
      val topics = List("testLogs").toSet
      // Create our Kafka stream, which will contain (topic,message) pairs. We tack a 
      // map(_._2) at the end in order to only get the messages, which contain individual
      // lines of data.
      val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        ssc, kafkaParams, topics).map(_._2)
       
      // Extract the request field from each log line
      val requests = lines.map(x => {val matcher:Matcher = pattern.matcher(x); if (matcher.matches()) matcher.group(5)})
      
      // Extract the URL from the request
      val urls = requests.map(x => {val arr = x.toString().split(" "); if (arr.size == 3) arr(1) else "[error]"})
      
      // Reduce by URL over a 5-minute window sliding every second
      val urlCounts = urls.map(x => (x, 1)).reduceByKeyAndWindow(_ + _, _ - _, Seconds(300), Seconds(1))
      
      // Sort and print the results
      val sortedResults = urlCounts.transform(rdd => rdd.sortBy(x => x._2, false))
      sortedResults.print()
      
      // Kick it off
      ssc.checkpoint("C:/checkpoint/")
      ssc.start()
      ssc.awaitTermination()
    }
  */
  
    def twitterstreaming(ssc: StreamingContext) {
    // Kafka setup instructions for Windows: https://dzone.com/articles/running-apache-kafka-on-windows-os      
    /** Listens to a stream of Tweets and keeps track of the most popular
   	    hashtags over a 5 minute window.
   	*/   
    
    /** Configures Twitter service credentials using twiter.txt in the main workspace directory */
      for (line <- Source.fromFile(resouces_dir+"/twitter.txt").getLines) {
        val fields = line.split(" ")
        if (fields.length == 2) {
          System.setProperty("twitter4j.oauth." + fields(0), fields(1))
        }
      }
      
      // Create a DStream from Twitter using our streaming context
      val tweets = TwitterUtils.createStream(ssc, None)

      tweets.foreachRDD(m => {
            println("size is " + m.collect()) })
            
      // Now extract the text of each status update into DStreams using map()
      val statuses = tweets.map(status => status.getText())

      statuses.foreachRDD(m => {
            println("size is " + m.collect()) })
      
      // Blow out each word into a new DStream
      val tweetwords = statuses.flatMap(tweetText => tweetText.split(" "))

      tweetwords.foreachRDD(m => {
            println("size is " + m.collect()) })
            
      // Now eliminate anything that's not a hashtag
      val hashtags = tweetwords.filter(word => word.startsWith("#"))

      hashtags.foreachRDD(m => {
            println("size is " + m.collect()) })
                  
      // Map each hashtag to a key/value pair of (hashtag, 1) so we can count them up by adding up the values
      val hashtagKeyValues = hashtags.map(hashtag => (hashtag, 1))
      
      // Now count them up over a 5 minute window sliding every one second
      val hashtagCounts = hashtagKeyValues.reduceByKeyAndWindow( (x,y) => x + y, (x,y) => x - y, Seconds(10), Seconds(10))
      //  You will often see this written in the following shorthand:
      //val hashtagCounts = hashtagKeyValues.reduceByKeyAndWindow( _ + _, _ -_, Seconds(300), Seconds(1))
      
      // Sort the results by the count values
      val sortedResults = hashtagCounts.transform(rdd => rdd.sortBy(x => x._2, false))
      
      // Print the top 10
      sortedResults.print
      
      // Set a checkpoint directory, and kick it all off
      // I could watch this all day!
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