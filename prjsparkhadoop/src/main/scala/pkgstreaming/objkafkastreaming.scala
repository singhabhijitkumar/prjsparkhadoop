package pkgstreaming
// Kafka setup instructions for Windows: https://dzone.com/articles/running-apache-kafka-on-windows-os
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel

import java.util.regex.Pattern
import java.util.regex.Matcher


import org.apache.spark.streaming.kafka._
import kafka.serializer.StringDecoder

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import scala.io.Codec
import java.nio.charset.CodingErrorAction

object DirectKafkaWordCount {
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("SparkKafkaStreaming").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val topicsSet = List("test").toSet
    val kafkaParams = Map[String, String](
        "metadata.broker.list" -> "localhost:9092",
        "zookeeper.connect" -> "localhost:2181",
        "zookeeper.connection.timeout.ms" -> "10000")
        
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
    ssc, kafkaParams, topicsSet)
    
    // Get the messages
    val lines = messages.map(_._2)
    lines.print()

    // Start the computation
    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }
}

