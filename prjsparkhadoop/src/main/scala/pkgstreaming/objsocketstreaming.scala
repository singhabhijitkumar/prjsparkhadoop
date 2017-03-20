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
import org.apache.spark.streaming.StreamingContext._
import org.apache.log4j.{Level, Logger}
import java.util.regex.Pattern
import java.util.regex.Matcher
import scala.io.Source
import scala.io.Codec
import java.nio.charset.CodingErrorAction

import pkgutilities.{SparkSessionSingleton, utilities, Record}
import pkgutilities.utilities._

import scala.util.Try

object objsocketstreaming {

  def main(args: Array[String]) {

      // Set the log level to only print errors
      Logger.getLogger("org").setLevel(Level.ERROR)
  
      implicit val codec = Codec("UTF-8")
      codec.onMalformedInput(CodingErrorAction.REPLACE)
      codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
      
      val checkpointDirectory = "C:/checkpoint/objsocketstreaming"
      val ip = "127.0.0.1"
      val port = 9000    
      val ssc = StreamingContext.getOrCreate(checkpointDirectory, 
                                             () => createContext(ip, port, checkpointDirectory))
                                             
      
      ssc.start()
      ssc.awaitTermination()
      //ssc.awaitTerminationOrTimeout(60000)
      //ssc.stop()
      }
  
  def createContext(ip: String, port: Int, checkpointDirectory: String)    : StreamingContext = {

      // If you do not see this printed, that means the StreamingContext has been loaded
      // from the new checkpoint
      println("Creating new context")
      val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SocketStreaming")
      // Create the context with a 30 second batch size
      val ssc = new StreamingContext(sparkConf, Seconds(30))
      
      ssc.checkpoint(checkpointDirectory)
    
      val lines = ssc.socketTextStream(ip, port)

    	val filteredlines = lines.filter( y => y.startsWith("REALLY"))
    
    	filteredlines.foreachRDD { (rdd : RDD[String]) =>  
          // Get the singleton instance of SparkSession
          val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
          import spark.implicits._
        
          // Convert RDD[String] to RDD[case class] to DataFrame
          val rddtext = rdd.map(w => Record(w.toString())).toDF()
        
          // Creates a temporary view using the DataFrame
          rddtext.createOrReplaceTempView("vwsocketstream")
          rddtext.printSchema()
          rddtext.collect.foreach(println)
          //Word count within RDD    
          rddtext.write.mode(SaveMode.Append).option("header", "true").csv(op_dir+"/socketstreamop.csv")
      }
    	
    return ssc   	
    
   }
  
  	def cleanseRDD(ipstring: String): String = {
		
    		val opstring = ipstring.toString().indexOfSlice("awesome")
    		if (opstring != 0){
    		  return ipstring }
    		else {
    		  return null} 
    		}
  	
}