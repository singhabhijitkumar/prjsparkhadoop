package pkgstreaming

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import java.util.regex.Pattern
import java.util.regex.Matcher
import java.text.SimpleDateFormat
import java.util.Locale

/*object objstructuredstreaming {
    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .getOrCreate()    
    
    // Create DataFrame representing the stream of input lines from connection to localhost:9999
    val dfsocket = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()
    
    dfsocket.isStreaming    // Returns True for DataFrames that have streaming sources
    
    dfsocket.printSchema 
    
      /* Make a DataFrame to hold the Kafka data - thanks Adam! */
      val kafkaDF = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "56.178.67.102:9902")
        .option("subscribe", "en,ru,zh,pl,de")
        .load()
      
      /* If I don't cast the value to a String it looks like this:
       * Raw kafka value: eyJpc1JvYm90IjpmYWxzZSwiY2hhbm5lbCI6IiNlbi53aWtpcGVkaWEiLCJ0aW1lc3RhbXAiOiIyMDE3LTAzLTA5VDAwOjIyOjQ2LjgxOFoiLCJ1cmwiOiJodHRwczovL2VuLndpa2lwZWRpYS5vcmcvdy9pbmRleC5waHA/ZGlmZj03NjkzNTM0MTAmb2xkaWQ9NzY5MzUzMTY0IiwiaXNVbnBhdHJvbGxlZCI6ZmFsc2UsInBhZ2UiOiJBbWVyaWNhbiBNdXNpYyBBd2FyZCBmb3IgRmF2b3JpdGUgU291bC9SJkIgTWFsZSBBcnRpc3QiLCJ3aWtpcGVkaWEiOiJlbiIsIndpa2lwZWRpYVVSTCI6Imh0dHA6Ly9lbi53aWtpcGVkaWEub3JnIiwiY29tbWVudCI6Ii8qIDIwMDBzICovIiwidXNlclVSTCI6Imh0dHA6Ly9lbi53aWtpcGVkaWEub3JnL3dpa2kvVXNlcjoxMTYuNDUuMTQwLjIxIiwicGFnZVVSTCI6Imh0dHA6Ly9lbi53aWtpcGVkaWEub3JnL3dpa2kvQW1lcmljYW5fTXVzaWNfQXdhcmRfZm9yX0Zhdm9yaXRlX1NvdWwvUiZCX01hbGVfQXJ0aXN0IiwiZGVsdGEiOjIwLCJmbGFnIjoiIiwiaXNOZXdQYWdlIjpmYWxzZSwiaXNBbm9ueW1vdXMiOnRydWUsImdlb2NvZGluZyI6eyJjb3VudHJ5Q29kZTIiOiJLUiIsImNpdHkiOiJTZW91bCIsImxhdGl0dWRlIjozNy41OTg0OTkyOTgwOTU3LCJjb3VudHJ5IjoiUmVwdWJsaWMgb2YgS29yZWEiLCJsb25naXR1ZGUiOjM3LjU5ODQ5OTI5ODA5NTcsInN0YXRlUHJvdmluY2UiOiIxMSIsImNvdW50cnlDb2RlMyI6IktPUiJ9LCJ1c2VyIjoiMTE2LjQ1LjE0MC4yMSIsIm5hbWVzcGFj..."
       * Which is base64 -- remember Kafka is binary﻿
      ﻿﻿ */
      
      val justValue = kafkaDF.select($"value".cast("string").as("json_message"))
      
      /* let's see the goods */
      ﻿    
    // Read all the csv files written atomically in a directory
    val userSchema = new StructType().add("name", "string").add("age", "integer")
    val csvDF = spark
      .readStream
      .option("sep", ";")
      .schema(userSchema)      // Specify schema of the csv files
      .csv("/path/to/directory")    // Equivalent to format("csv").load("/path/to/directory")

    /*val query = lines.writeStream
      .outputMode("complete")
      .format("console")
      .start()
  */
    
      query.awaitTermination()
    
      
      
}


package com.sundogsoftware.sparkstreaming

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.functions._

import java.util.regex.Pattern
import java.util.regex.Matcher
import java.text.SimpleDateFormat
import java.util.Locale

import Utilities._

object StructuredStreaming {
  
   // Case class defining structured data for a line of Apache access log data
   case class LogEntry(ip:String, client:String, user:String, dateTime:String, request:String, status:String, bytes:String, referer:String, agent:String)
   
   val logPattern = apacheLogPattern()
   val datePattern = Pattern.compile("\\[(.*?) .+]")
      
   // Function to convert Apache log times to what Spark/SQL expects
   def parseDateField(field: String): Option[String] = {

      val dateMatcher = datePattern.matcher(field)
      if (dateMatcher.find) {
              val dateString = dateMatcher.group(1)
              val dateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH)
              val date = (dateFormat.parse(dateString))
              val timestamp = new java.sql.Timestamp(date.getTime());
              return Option(timestamp.toString())
          } else {
          None
      }
   }
   
   // Convert a raw line of Apache access log data to a structured LogEntry object (or None if line is corrupt)
   def parseLog(x:Row) : Option[LogEntry] = {
     
     val matcher:Matcher = logPattern.matcher(x.getString(0)); 
     if (matcher.matches()) {
       val timeString = matcher.group(4)
       return Some(LogEntry(
           matcher.group(1),
           matcher.group(2),
           matcher.group(3),
           parseDateField(matcher.group(4)).getOrElse(""),
           matcher.group(5),
           matcher.group(6),
           matcher.group(7),
           matcher.group(8),
           matcher.group(9)
           ))
     } else {
       return None
     }
   }
  
   def main(args: Array[String]) {
      // Use new SparkSession interface in Spark 2.0
      val spark = SparkSession
        .builder
        .appName("StructuredStreaming")
        .master("local[*]")
        .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
        .config("spark.sql.streaming.checkpointLocation", "file:///C:/checkpoint")
        .getOrCreate()
        
      setupLogging()
        
      // Create a stream of text files dumped into the logs directory
      val rawData = spark.readStream.text("logs")
            
      // Must import spark.implicits for conversion to DataSet to work!
      import spark.implicits._
            
      // Convert our raw text into a DataSet of LogEntry rows, then just select the two columns we care about
      val structuredData = rawData.flatMap(parseLog).select("status", "dateTime")
    
      // Group by status code, with a one-hour window.
      val windowed = structuredData.groupBy($"status", window($"dateTime", "1 hour")).count().orderBy("window")
      
      // Start the streaming query, dumping results to the console. Use "complete" output mode because we are aggregating
      // (instead of "append").
      val query = windowed.writeStream.outputMode("complete").format("console").start()
      
      // Keep going until we're stopped.
      query.awaitTermination()
      
      spark.stop()
   }
}
*/