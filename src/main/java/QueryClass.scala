import org.slf4j.LoggerFactory
// Please note that the query file should contain no comments or additional characters
class QueryClass(var queryString: String) {
  val log = LoggerFactory.getLogger(getClass)
  var prefixes = "PREFIX.*".r.findAllIn(queryString).mkString("\n").replaceAll("PREFIX ","").split("\n") //extracted prefixes into a Array String
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
  this.queryString = this.queryString.replaceAll("\\r","")
  val patterns = this.queryString.split("\n")//.foreach(f => f.split("<"))
  val patternPredicates: Array[(Int, String, String, String)] = this.createPatternPredicates()
  val numberOfPatterns = patternPredicates.size

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

    var predicateArrayString :String= ""
    val r = 0 to (this.patterns.size -1)
    for( i <- r){
      val splitpattern = this.patterns(i).split(" ")
      if(splitpattern.size > 2){
        predicateArrayString = predicateArrayString + i +" " +splitpattern(1) + " " + splitpattern(0) + " " +splitpattern(2) +"\n"
      }
    }

    var predicateArray = predicateArrayString.split("\n")
    var predicateArray2 = predicateArray.map(_.split(" "))
    val tuple = predicateArray2.map(x=>(x(0).toInt,x(1), x(2), x(3))) //predicate, subject, object
    return tuple
  }
}

