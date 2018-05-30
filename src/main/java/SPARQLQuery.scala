/*
* This defines the query object*/


import java.io.File
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object SPARQLQuery {

  //var q:Query = null
  //var queryList = ArrayBuffer[Query]() // stores all queries in a single arraybuffer
  var queryList = ArrayBuffer[QueryClass]()

  def createQuery(): ArrayBuffer[QueryClass] ={
    val directoryPath = Configuration.queryPath
    val dir = new File(directoryPath)
    if (dir.exists && dir.isDirectory) {
      val fileList = dir.listFiles()
      var i = 0
      for (i <- fileList){
        //val queryLines = sparkContext.textFile(fileList(i).toString)
        // Please note that the query file should contain no comments or additional characters
        val queryString:String = Source.fromFile(i).mkString
        /* Apache jena query factory class approach
        val temp = QueryFactory.create(queryLines)
        val temp = QueryFactory.read(fileList(i).toString)
        queryList += temp
        val patterns = temp.getQueryPattern
        println(temp)*/
        val q = new QueryClass(queryString)
        queryList += q

        //i += 1
      }
    }
  return queryList
  }
}

// verify and remove from sparkexecutor
