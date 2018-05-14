

// Please note that the query file should contain no comments or additional characters
class QueryClass(var queryString: String) {

  var prefixes = "PREFIX.*".r.findAllIn(queryString).mkString("\n").replaceAll("PREFIX ","").split("\n")//.split("\n") //extracted prefixes into a Array String
  queryString = this.queryString.replaceAll("PREFIX.*","") // removing the prefixes from the queryString

  for (prefix <- prefixes){
    val prefixPartsList = prefix.split(" ")
    val s = "(?<=" + prefixPartsList(0) + ")(\\w+)"
    val matchedInsertions = s.r.findAllIn(queryString).mkString(" ").split(" ")
    if(prefixPartsList.size > 1){
      for (matched <- matchedInsertions){
        val replacement = prefixPartsList(1).replace(">",matched+">")
        queryString= queryString.replaceAll(prefixPartsList(0)+ matched, replacement)
      }
    }
  }

  this.queryString = this.queryString.replaceAll("^[^_]*\\{","")
  this.queryString = this.queryString.replaceAll("}", "")
  this.queryString = this.queryString.replaceAll(" \\.","")
  val patterns = this.queryString.split("\n")//.foreach(f => f.split("<"))
  val patternPredicates = this.createPatternPredicates()
  val numberOfPatterns = patternPredicates.size
  //val x = 4

  def getQueryPattern(): Array[String] ={
    return this.patterns
  }
  def getPrefixes(): Array[String] ={
    return this.prefixes
  }

  def getPatternPredicates(): Array[(Int, String, String, String)]={
    return this.patternPredicates
  }

  private def createPatternPredicates(): Array[(Int, String, String, String)] ={ //  mutable.Map[String, Int]
    //var predicates = Set[String]()//ArrayBuffer[String]()
    //var predicateMap = mutable.Map[String, Int]() //mutable.Map[Int, String]()
    var predicateArrayString :String= ""
    val r = 0 to (this.patterns.size -1)
    for( i <- r){
      val splitpattern = this.patterns(i).split(" ")
      if(splitpattern.size > 2){
        //predicateMap(splitpattern(1)) =  i
        // order is pred, subject, object
        predicateArrayString = predicateArrayString + i +" " +splitpattern(1) + " " + splitpattern(0) + " " +splitpattern(2) +"\n"
      }
    }
    //return predicateMap
    var predicateArray = predicateArrayString.split("\n")
    var predicateArray2 = predicateArray.map(_.split(" "))
    val tuple = predicateArray2.map(x=>(x(0).toInt,x(1), x(2), x(3)))
    /*
    for( pattern <- this.patterns){
      val splitpattern = pattern.split(" ")
      if(splitpattern.size > 1){
        predicates = predicates+ splitpattern(1)

      } */
    return tuple



    //return predicates
    //return Tuple2(r,predicates.toSeq.sorted)
  }
}

