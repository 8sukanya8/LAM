import org.slf4j.LoggerFactory

/*This Object is exerts the mail control over the flow of the program
*
*
* */

object main { //extends App
  val log = LoggerFactory.getLogger(getClass)
  def main(args: Array[String]): Unit = {
    val argumentParser = new ArgumentParser()
    argumentParser.parseInput(args)
    SparkExecutor.ConfigureSpark()
    val grapht1 = System.nanoTime
    SparkExecutor.createGraph() // graph is accessible through sparkexecutor
    val graphLoadingDuration = (System.nanoTime - grapht1) / 1e9d
    log.info("\n\nGraph loaded in "+ graphLoadingDuration + " seconds\n")
    log.info("\n\nNumber of triples " + SparkExecutor.tripleCount() + "\n")

    val queryList = SPARQLQuery.createQuery()
    for(query <- queryList){
      log.info("\n\n Executing: \n "+ query.queryString + "\n")
      val querystart = System.nanoTime
      SparkExecutor.bgp(query)
      val queryDuration = (System.nanoTime - querystart) / 1e9d
      log.info("\n\nQuery executed in "+ queryDuration + " seconds\n")
    }

  }
}
// problem in validation

// create vertex class and add the property of a match table
// https://stackoverflow.com/questions/34188566/how-to-attach-properties-to-vertices-in-a-graphx-and-retrieve-the-neighbourhood
// https://stackoverflow.com/questions/34542627/i-need-to-do-join-joinvertices-or-add-a-field-in-tuple-in-graph-by-spark-graphx/34556928#34556928