package pkgsqlnosql

import org.apache.spark.sql.SQLContext
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import scala.io.Codec
import java.nio.charset.CodingErrorAction
import pkgutilities.utilities._

object MySQLHIVECassandra {

  def main(args: Array[String]) {
        // Set the log level to only print errors
        Logger.getLogger("org").setLevel(Level.ERROR)

    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)        
        
        // Use new SparkSession interface in Spark 2.0
        val spark = SparkSession
            .builder
            .appName("SparkSQL")
            .master("local[*]")
            .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
            .config("spark.cassandra.connection.host", "127.0.0.1")//cassandra db server
            .config("spark.cassandra.connection.port", "9042")//cassandra db port            
            .config("spark.cassandra.username", "cassandra") //Optional            
            .config("spark.cassandra.password", "cassandra") //Optional
            .getOrCreate()
            
        sparksql(spark)    
        jdbc(spark)
        hive(spark)
        cassandra(spark)
        
        spark.stop()
  }
  
    def sparksql(spark:SparkSession){
        //directly query parquet file
        val sqlDF = spark.sql(s"SELECT * FROM parquet.`${op_dir}/data.parquet`")
        sqlDF.show()
        
        //load parquet data to datafram
        val df_parquet = spark.read.parquet(op_dir+"/data.parquet")
        df_parquet.printSchema()
        df_parquet.createOrReplaceTempView("tb_parquet")
        df_parquet.show()
            
        val df_sql_analytics = spark.sql("""SELECT _c0,_c1,_c2,_c3,
          LEAD(_c0,1,0) over ( order by _c2) as lead,
          SUM(_c2) over ( partition by _c1) as sum
          FROM tb_parquet
         """)
    
        df_sql_analytics.collect.foreach(println) 
     
        val p1 = "('Jean-Luc','Hugh','Deanna','Brunt','Nerys')"
        val p2 = "(55)"
    
        val v_sql = s"""SELECT _c1, _c2, SUM(_c3) AS SUM
                       FROM tb_parquet 
                       WHERE _c1 IN $p1 AND _c2 IN $p2
                       GROUP BY _c1,_c2"""
        println(v_sql)
        val df_sql_dynamic = spark.sql(v_sql)
        df_sql_dynamic.show()
        df_sql_dynamic.collect.foreach(println)
  }
  
  def jdbc(spark:SparkSession){
    val df_mysql = spark.read
                   .format("jdbc")
									 .option("url", "jdbc:mysql://ip-172-31-13-154:3306/sqoopex")
									 .option("driver", "com.mysql.jdbc.Driver")
									 .option("dbtable", "employee")
									 .option("user", "sqoopuser")
									 .option("password", "NHkkP876rp")
									 .load()
									 .where("name=michael")//need to test the where condition
									 
    df_mysql.show()
    
    df_mysql.write.format("jdbc")
					 .option("url", "jdbc:mysql://ip-172-31-13-154:3306/sqoopex")
					 .option("driver", "com.mysql.jdbc.Driver")
					 .option("dbtable", "employee")
           .mode(SaveMode.Append) 
					 .option("user", "sqoopuser")
					 .option("password", "NHkkP876rp")
    }

  def hive(spark:SparkSession ) {
          //CREATE TABLE AND LOAD FILE IN ONE STATEMENT
          spark.sql(s"""CREATE EXTERNAL TABLE IF NOT EXISTS defaultdb.stg_fakefriends
          (field1 string, field2 string, field3 string)
          COMMENT 'to load data from fakefriends.csv'
          ROW FORMAT DELIMITED
          FIELDS TERMINATED BY ','
          STORED AS TEXTFILE
          location $ip_dir'/fakefriends.csv'""")
          
          spark.sql("""CREATE TABLE IF NOT EXISTS defaultdb.tb_fakefriends (field1 string, field2 string, field3 string)
          ROW FORMAT DELIMITED
          FIELDS TERMINATED BY ','
          STORED AS ORC""")

          //LOAD FILE INTO EXISTING TABLE
          spark.sql(s"LOAD DATA LOCAL INPATH $ip_dir'/fakefriends.csv' INTO TABLE stg_fakefriends")

          spark.sql("INSERT OVERWRITE TABLE defaultdb.tb_fakefriends SELECT * FROM defaultdb.stg_fakefriends")
      
          val result = spark.sql("SELECT * FROM tb_fakefriends")
          
          result.show()
  }

  def cassandra(spark:SparkSession ) {
    val dfCassandra = spark.read.option("header", "false").option("inferSchema", "true").csv(ip_dir+"/fakefriends.csv")

    // Write it into Cassandra
    dfCassandra.write
      .mode(SaveMode.Append) 
      .format("org.apache.spark.sql.cassandra")      
      .option("table","fakefriends")
      .option("keyspace","projectspark")
      .save()
      
    // Read it back from Cassandra into a new Dataframe
    val dfreadCassandra = spark.read
        .format("org.apache.spark.sql.cassandra")
        .option("table","tablename")
        .option("keyspace","keyspacename")
        .load()
  }
 
}