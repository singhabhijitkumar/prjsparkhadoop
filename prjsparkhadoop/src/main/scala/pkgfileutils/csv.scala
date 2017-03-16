package pkgfileutils

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import scala.io.Codec
import java.nio.charset.CodingErrorAction
import pkgutilities.utilities._

object csv {

  def main(args: Array[String]) {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

   if (args.length > 0) {
      System.err.println("Unwanted parameters passed.")
      args.foreach(arg => println(arg))
      println(args.length)
      System.exit(1)
    }
    
    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
        .builder
        .appName("SparkCSV")
        .master("local[*]")
        .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
        .getOrCreate()
    
    readCSV(spark)
    
    writeCSV(spark)    
    
    spark.stop()    
  }



  def readCSV(spark:SparkSession) {
    //read CSV      
    val df_csv = spark.read.option("header", "false").option("inferSchema", "true").csv(ip_dir+"/fakefriends.csv")
    df_csv.printSchema()
    df_csv.createOrReplaceTempView("fakefriends")
    df_csv.show()
    
    val df_dsv = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", "|").csv(ip_dir+"/pipedelimiteddata.dsv")
    df_dsv.printSchema()
    df_dsv.createOrReplaceTempView("tb_pipedelimited")
    df_dsv.show()    
  }
  
  def writeCSV(spark:SparkSession ) {
    //write CSV and tab DSV      
    val df_op_csv =  spark.sql("SELECT * FROM fakefriends WHERE _c1 = 'Hugh'")
    df_op_csv.show()
    df_op_csv.printSchema()
    df_op_csv.write.mode(SaveMode.Append).option("header", "true").csv(op_dir+"/op.csv")
    df_op_csv.write.mode(SaveMode.Append).option("header", "true").option("delimiter", "\t").csv(op_dir+"/op.tsv")
    
    //Write Pipe DSV
    val df_op_dsv =  spark.sql("SELECT * FROM tb_pipedelimited WHERE ADDRESS is not null")
    df_op_dsv.show()
    df_op_dsv.printSchema()    
  }
  
}
