import net.sansa_stack.rdf.spark.graph.LoadGraph
import net.sansa_stack.rdf.spark.io.rdf._
import org.apache.jena.riot.Lang
import org.apache.spark.{SparkConf, SparkContext}

object SparkExecutor {

  private var sparkContext:SparkContext = null


  def ConfigureSpark(): Unit ={
    val conf = new SparkConf().setAppName("LAM").setMaster("local")
    sparkContext = new SparkContext(conf)
  }

  def createGraph(): Unit ={
    val filename = Configuration.graphPath(0)
    print("Loading graph from file"+ filename)
    //val lines = sparkContext.textFile(filename).map(_.split(" "))
    //val linesContents = lines.collect()
    //println(linesContents)

    val lang = Lang.TTL
    val triples = sparkContext.rdf(lang)(filename) //NTripleReader.load(sparkContext, URI.create(filename)) //
    val triplesContents = triples.collect()
    triples.take(5).foreach(println(_))

    val graph = LoadGraph(triples)
    val nodesCollect = graph.vertices.collect()

    val edgescollect = graph.edges.collect()
    val x = 4

    // is lines automatically parallelised?
    // how to parallelize ?
    // how to parse ttl file? solved using library sansa
    // how to load it in GraphX? solved using library sansa
    // how to load multiple files and make one graph? solved using library sansa and rdf3rdf application


  }

  // set all configuration parameters
  // https://spark.apache.org/docs/2.2.0/configuration.html
}
