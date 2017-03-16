package pkgfileutils

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import scala.io.Codec
import java.nio.charset.CodingErrorAction
import pkgutilities.utilities._

object json {

  def main(args: Array[String]) {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
        .builder
        .appName("SparkJSON")
        .master("local[*]")
        .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
        .getOrCreate()
    
    readJSON(spark)
    
    writeJSON(spark)    
    
    explodejson(spark)
    
    spark.stop()    
  }



  def readJSON(spark:SparkSession) {
    //read JSONL      
    val df_json = spark.read.json(ip_dir+"/sample.json")
    df_json.printSchema()
    df_json.createOrReplaceTempView("tb_sample")
    df_json.show()
    
    //read nested JSON
    val df_nested_json = spark.read.json(ip_dir+"/nested.json")
    df_nested_json.printSchema()
    df_nested_json.createOrReplaceTempView("tb_nested")
    df_nested_json.show()    
  }
  
  def writeJSON(spark:SparkSession ) {
    //write JSONL      
    val df_op_json =  spark.sql("SELECT * FROM tb_sample WHERE IsCaptain = FALSE")
    df_op_json.show()
    df_op_json.printSchema()
    df_op_json.write.mode(SaveMode.Append).json(op_dir+"/op.json")
 
    val df_op_nested_json =  spark.sql("""SELECT name, address.city, address.state
    FROM tb_nested""")
    df_op_nested_json.show()
    df_op_nested_json.printSchema()  
    df_op_nested_json.collect.foreach(println)
  }

  def explodejson (spark:SparkSession) {
  
    //explode JSONL      
    val df_json = spark.read.json(ip_dir+"/explode.json")
    df_json.printSchema()
    df_json.createOrReplaceTempView("tb_explode_json")
    df_json.show()
    
    val df_op_explode_json =  spark.sql("""select id, 
                                                  name, 
                                                  price, 
                                                  details.otherdetails, 
                                                  tag_list, 
                                                  hash_list
                                           from tb_explode_json
                                           lateral view explode(details.tags) exploded_tags as tag_list
                                           lateral view outer explode(details.hash) exploded_hash as hash_list""")
    df_op_explode_json.show()
    df_op_explode_json.printSchema()  
    df_op_explode_json.collect.foreach(println)

  }
  
  
}
