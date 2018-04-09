/*
* This defines the query object*/

import java.io.File

import org.apache.jena.query.{Query, QueryFactory}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object SPARQLQuery extends QueryFactory{

  //var q:Query = null
  var queryList = ArrayBuffer[Query]() // stores all queries in a single arraybuffer
  def createQuery(): ArrayBuffer[Query] ={
    val directoryPath = Configuration.queryPath
    val dir = new File(directoryPath)
    if (dir.exists && dir.isDirectory) {
      val fileList = dir.listFiles()
      var i = 0
      while(i< fileList.length){
        //val queryLines = sparkContext.textFile(fileList(i).toString)
        val queryLines = Source.fromFile(fileList(i)).mkString
        val temp = QueryFactory.create(queryLines)
        queryList += temp
        //val patterns = temp.getQueryPattern
        println(temp)
        i += 1
      }
    }
  return queryList
  }
}

// verify and remove from sparkexecutor
