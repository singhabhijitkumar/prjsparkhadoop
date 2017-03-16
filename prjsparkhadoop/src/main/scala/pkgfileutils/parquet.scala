package pkgfileutils

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import scala.io.Codec
import java.nio.charset.CodingErrorAction
import pkgutilities.utilities._

object parquet {

    def main(args: Array[String]) {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
           
    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
        .builder
        .appName("SparkParquet")
        .master("local[*]")
        .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
        .getOrCreate()

        // read csv
        csv.readCSV(spark)
        
        val df_op_parquet =  spark.sql("SELECT * FROM fakefriends")
        println("strating")
        df_op_parquet.show()
        df_op_parquet.printSchema()
        df_op_parquet.write.mode(SaveMode.Append).parquet(op_dir+"data.parquet")
        
        spark.stop()
    
      }

}