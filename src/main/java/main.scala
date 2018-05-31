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
    val graphLoadingStartTime = System.nanoTime
    SparkExecutor.createGraph() // graph is accessible through sparkexecutor
    log.info("\n\n Parallelism: "+ Configuration.parallelism + "\n")
    val graphLoadingDuration = (System.nanoTime - graphLoadingStartTime) / 1e9d
    log.info("\n\nGraph loaded in "+ graphLoadingDuration + " seconds\n")
    log.info("\n\nNumber of triples " + SparkExecutor.tripleCount() + "\n")

    val query = SPARQLQuery.createQuery()
    log.info("\n\n Executing: \n "+ query.queryString + "\n")
    val queryExecutionStartTime = System.nanoTime
    SparkExecutor.bgp(query)
    val queryExecutionDuration = (System.nanoTime - queryExecutionStartTime) / 1e9d
    log.info("\n\nQuery executed in "+ queryExecutionDuration + " seconds\n")
  }
}
