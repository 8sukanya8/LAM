
// Please note that the query file should contain no comments or additional characters
class QueryClass(var queryString: String) {

  var prefixes = "PREFIX.*".r.findAllIn(queryString).mkString("\n").replaceAll("PREFIX ","").split("\n")//.split("\n") //extracted prefixes into a Array String
  //prefixes = prefixes.replaceAll("PREFIX ","").split("\n")
  queryString = this.queryString.replaceAll("PREFIX.*","") // removing the prefixes from the queryString
  for (prefix <- prefixes){
    val prefixPartsList = prefix.split(" ")
    if(prefixPartsList.size > 1){
      this.queryString= this.queryString.replaceAll(prefixPartsList(0), prefixPartsList(1) )
    }
  }
  //queryString.matches("^(?!#).+$")
 // "(^#).*".r
  this.queryString = this.queryString.replaceAll("^[^_]*\\{","")
  this.queryString = this.queryString.replaceAll("}", "")
  this.queryString = this.queryString.replaceAll(" \\.","")
  val patterns = this.queryString.split("\n")

  def getQueryPattern(): Array[String] ={
    return this.patterns
  }
  def getPrefixes(): Array[String] ={
    return this.prefixes
  }
}

