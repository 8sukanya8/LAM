

/*This Object is exerts the mail control over the flow of the program
*
*
* */

object main {
  def main(args: Array[String]): Unit = {

    val argumentParser = new ArgumentParser()
    argumentParser.parseInput(args)
    SparkExecutor.ConfigureSpark()
    SparkExecutor.createGraph() // graph is accessible through sparkexecutor
    val queryList = SPARQLQuery.createQuery()
    val x = 1
    for(query <- queryList){
      val result = SparkExecutor.bgp(query)
    }


    // create a separate query class finish regex for query.scala
    // design the executor interface
    //
  }
}
