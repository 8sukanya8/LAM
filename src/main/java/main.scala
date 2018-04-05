/*This Object is exerts the mail control over the flow of the program
*
*
* */

object main {
  def main(args: Array[String]): Unit = {

    val argumentParser = new ArgumentParser()
    argumentParser.parseInput(args)
    //Configuration.graphPath.foreach( i => println(i))
    //println("\n\n")
    //Configuration.queryPath.foreach( i => println(i))
    SparkExecutor.ConfigureSpark()
    val RDFGraph = SparkExecutor.createGraph()
    SparkExecutor.loadQuery()
    // create a separate query class
    // design the executor interface
    //
  }
}
