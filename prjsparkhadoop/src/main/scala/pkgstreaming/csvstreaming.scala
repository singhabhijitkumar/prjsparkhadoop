package pkgstreaming

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.regexp_extract
import scala.io.Codec
import java.nio.charset.CodingErrorAction

import pkgutilities.utilities._

object csvstreaming {

    def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
   
    val spark = SparkSession
      .builder
      .appName("StructuredStreaming")    
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .config("spark.sql.streaming.checkpointLocation", "file:///C:/checkpoint/csvstreaming")
      .getOrCreate()

    // Must import spark.implicits for conversion to DataSet to work!
    import spark.implicits._
    
    // Read all the csv files written atomically in a directory
    val userSchema = new StructType()
                        .add("name", "string")
                        .add("age", "float")
                        .add("city", "string")
                        .add("sex", "string")
                        .add("joiningdate", "string")
                        .add("loaddate", "timestamp")
    
    val csvDF = spark
      .readStream
      .option("sep", ";")
      .schema(userSchema)      // Specify schema of the csv files
      .csv("C:/eclipse_4.5_workspace/prjsparkhadoop/src/main/resources/ip_files/dstream/*")    
      //.text("logs") for unstructured files
       
    //csvDF.createOrReplaceTempView("TB_CSVDF")    
    //val nonaggquery = spark.sql("SELECT * FROM TB_CSVDF WHERE age>30")
    val nonaggquery = csvDF.select("name", "age", "city", "sex", "loaddate")
                           .withColumn("date_time", $"loaddate".cast("timestamp"))
                           .where("length(name)>=7")

    //csvDF.isStreaming) // Returns True for DataFrames that have streaming sources    
    //csvDF.printSchema 
        
    //write stream
    //val appendstream = nonaggquery.writeStream.outputMode("append").format("console").start()
    val appendstream  = nonaggquery.writeStream.outputMode("append").format("parquet").start(op_dir+"stream.parquet")
        
    appendstream.awaitTermination()
        
    /*val aggquery = nonaggquery
        .withWatermark("loaddate", "10 minutes")
        .groupBy($"city", window($"loaddate", "5 minutes", "2 minutes"))
        //.avg("age")
        .agg(avg("age"), count("*"))
        .orderBy("city")
        
		//.groupByKey(x=>(x.species, x.jedi, x.haircolor)).agg(mean($"weight").as[Double], count($"species").as[Long])        
    val completesteam = aggquery.writeStream
                                .outputMode("complete")
                                .format("console")
                                .option("truncate", "false")
                                .start()
    
    // Keep going until we're stopped.
    completesteam.awaitTermination()    
    */
    
    spark.stop()

  }
}