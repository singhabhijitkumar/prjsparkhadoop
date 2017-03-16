package pkgutilities

import org.apache.log4j.Level
import java.util.regex.Pattern
import java.util.regex.Matcher

object utilities {
  
  //Directory where the data files for the examples exist.
	val ip_dir = "C:/eclipse_4.5_workspace/prjsparkhadoop/src/main/resources/ip_files/"
	val op_dir = "C:/eclipse_4.5_workspace/prjsparkhadoop/src/main/resources/op_files/"  
	val resouces_dir = "C:/eclipse_4.5_workspace/prjsparkhadoop/src/main/resources/"
	
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
