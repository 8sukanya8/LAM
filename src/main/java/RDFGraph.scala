
import net.sansa_stack.rdf.spark.graph.LoadGraph
import net.sansa_stack.rdf.spark.io._
import org.apache.jena.riot.Lang
import org.apache.spark.sql.SparkSession

object RDFGraph {

  def createGraph(ss: SparkSession): Unit ={
    val filename = Configuration.graphPath(0)
    print("Loading graph from file"+ filename)
    val lang = Lang.TTL
    val triples = ss.rdf(lang)(filename)
    //val triples = NTripleReader.load(ss, URI.create(filename))

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

}
