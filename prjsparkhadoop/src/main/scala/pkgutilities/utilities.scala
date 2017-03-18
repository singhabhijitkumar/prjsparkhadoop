package pkgutilities

import org.apache.log4j.Level
import java.util.regex.Pattern
import java.util.regex.Matcher
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

object utilities {
  
  //Directory where the data files for the examples exist.
	val ip_dir = "C:/Users/Admih/GIT/prjsparkhadoop/src/main/resources/ip_files/"
	val op_dir = "C:/Users/Admih/GIT/prjsparkhadoop/src/main/resources/op_files/"  
	val resouces_dir = "C:/Users/Admih/GIT/prjsparkhadoop/src/main/resources/"
	
  /** Retrieves a regex Pattern for parsing Apache access logs. */
  def apacheLogPattern():Pattern = {
    val ddd = "\\d{1,3}"                      
    val ip = s"($ddd\\.$ddd\\.$ddd\\.$ddd)?"  
    val client = "(\\S+)"                     
    val user = "(\\S+)"
    val dateTime = "(\\[.+?\\])"              
    val request = "\"(.*?)\""                 
    val status = "(\\d{3})"
    val bytes = "(\\S+)"                     
    val referer = "\"(.*?)\""
    val agent = "\"(.*?)\""
    val regex = s"$ip $client $user $dateTime $request $status $bytes $referer $agent"
    Pattern.compile(regex)    
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