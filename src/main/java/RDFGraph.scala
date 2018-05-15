
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import scala.io.Source

object RDFGraph {
  val log = LoggerFactory.getLogger(getClass)
  def createGraph(ss: SparkSession): Graph[Any, String] ={
    val filename = Configuration.graphPath
    log.info("\n\nLoading graph from file\n" + filename + "\n")
    /* sansa rdf usage
    val lang = Lang.TTL
    val triples = ss.rdf(lang)(filename)
    val triples = NTripleReader.load(ss, URI.create(filename))
    val triplesContents = triples.collect()
    triples.take(5).foreach(println(_))
    val graph = LoadGraph(triples)
    val nodesCollect = graph.vertices.collect()
    val edgescollect = graph.edges.collect()
    */

    // reading n-triple file into a string
    val fileContents = Source.fromFile(filename).getLines.mkString(sep = "\n")
    // processing fileContents to remove unwanted characters '<','>' and '.'

    //val editedFileContents = fileContents.replaceAll("[<>.]", "")

    // creating lines rdf from editedFileContents
    /* large files might be a problem for in memory processing and editing
    * In that case simplt edit the file before feeding to spark.
    * In this case n3 format does not exculsively matter because we would not be feeding this to a libray
    * https://stackoverflow.com/questions/38021228/parallelize-by-new-line
    */
    val lines = ss.sparkContext.parallelize(fileContents.split("\n"))
    //val linesContents = lines.collect()
    // mapping lines into triples
    val filteredLines = lines.map(_.split(" "))
    //val filteredLinesContents = filteredLines.collect()
    val triple = filteredLines.map(x=>(x(0),x(1),x(2)))
    //val tripleContents = triple.collect()

    // creating subjects and objects
    val subjects: RDD[String] = triple.map(triple => triple._1)
    //val subjectsContents = subjects.collect()
    val objects: RDD[String] = triple.map(triple => triple._3)
    //val objectsContents = objects.collect()

    // identify distinct nodes
    val distinctNodes: RDD[String] = ss.sparkContext.union(subjects, objects).distinct
    //val distinctNodesContents = distinctNodes.collect()

    // each distinct node must be zipped with a unique id for uniform identification
    val zippedNodes: RDD[(String, Long)] = distinctNodes.zipWithUniqueId
    //val zippedNodesContents = zippedNodes.collect()
    //val mapTableList = new mutable.HashMap[Int,(Any,Any)]() // Int for Table number, Any for Table mapping, Any for MatchTable consisting of Matchsets

    // create graph using graphX
    val edges = filteredLines.map( x=> (x(0),(x(2),x(1)))).join(zippedNodes).
      map( x=> (x._2._1._1,(x._2._1._2,x._2._2))).join(zippedNodes).
      map( x=> new Edge(x._2._1._2,x._2._2,x._2._1._1)) // source, destination, edge attr

    //class VertexProperty()
    //case class Attribute( val value:   String ) extends VertexProperty
    //case class MatchSetList( val ml:  Any ) extends VertexProperty

    val nodes: RDD[(VertexId, Any)] = zippedNodes.map(node => {
      val id = node._2
      val attribute = node._1
      (id, attribute)
    })


    /*val nodes: RDD[(VertexId, Any)] = zippedNodes.map(node => {
      val id = node._2
      val attribute = node._1
      (id, attribute)
    })*/
    //val edgesContents = edges.collect()
    //val nodesContents = nodes.collect()
    Graph(nodes, edges)
    //val graphNodesContents = graph.vertices.collect()
    //val graphEdgesContents = graph.edges.collect()
    //val x = 4

  }

}
