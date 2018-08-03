/*
* This defines the query object*/


import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object SPARQLQuery {

  // stores all queries in a single arraybuffer
  var queryList = ArrayBuffer[QueryClass]()
  var q : QueryClass = null

  def createQuery(): QueryClass ={
    val filepath = Configuration.queryPath
    if (filepath.endsWith(".txt")) {
      val queryString:String = Source.fromFile(filepath).mkString
        q = new QueryClass(queryString)
      }
    return q
    }
}

