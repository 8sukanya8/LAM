

/*This Object is exerts the mail control over the flow of the program
*
*
* */

object main { //extends App
  def main(args: Array[String]): Unit = {

    val argumentParser = new ArgumentParser()
    argumentParser.parseInput(args)
    SparkExecutor.ConfigureSpark()
    SparkExecutor.createGraph() // graph is accessible through sparkexecutor
    val queryList = SPARQLQuery.createQuery()
    for(query <- queryList){
      println("\n Executing: \n "+ query.queryString)
      val result = SparkExecutor.bgp(query)
    }

  }
}
// problem in validation

// create vertex class and add the property of a match table
// https://stackoverflow.com/questions/34188566/how-to-attach-properties-to-vertices-in-a-graphx-and-retrieve-the-neighbourhood
// https://stackoverflow.com/questions/34542627/i-need-to-do-join-joinvertices-or-add-a-field-in-tuple-in-graph-by-spark-graphx/34556928#34556928