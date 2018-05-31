/*
* This defines the query object*/


import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object SPARQLQuery {

  //var q:Query = null
  //var queryList = ArrayBuffer[Query]() // stores all queries in a single arraybuffer
  var queryList = ArrayBuffer[QueryClass]()
  var q : QueryClass = null

  def createQuery(): QueryClass ={
    //val directoryPath = Configuration.queryPath
    //val dir = new File(directoryPath)
    val filepath = Configuration.queryPath
    if (filepath.endsWith(".txt")) {
      val queryString:String = Source.fromFile(filepath).mkString
        q = new QueryClass(queryString)
      }
    return q
    }
}

// verify and remove from sparkexecutor
