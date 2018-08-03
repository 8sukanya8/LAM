
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

object RDFGraph {
  val log = LoggerFactory.getLogger(getClass)
  def createGraph(ss: SparkSession): Graph[Any, String] ={
    val RDFfilename = Configuration.graphPath
    log.info("\n\nLoading graph from file\n" + RDFfilename + "\n")
    // reading n-triple file into a string
    val lines = ss.sparkContext.textFile(RDFfilename)
    // mapping lines into triples
    val filteredLines = lines.map(_.split(" "))
    val triples: RDD[(String,String,String)] = filteredLines.map(x=>(x(0),x(1),x(2))).distinct()

    // creating subjects and objects
    val subjects: RDD[String] = triples.map(triple => triple._1)
    //val subjectsContents = subjects.collect()
    val objects: RDD[String] = triples.map(triple => triple._3)
    //val objectsContents = objects.collect()

    // identify distinct nodes
    val distinctNodes: RDD[String] = ss.sparkContext.union(subjects, objects).distinct

    // each distinct node must be zipped with a unique id for uniform identification
    val zippedNodes: RDD[(String, Long)] = distinctNodes.zipWithUniqueId

    // create graph using graphX
    val edges = triples.map( triple => (triple._1,(triple._3,triple._2))).join(zippedNodes). // join as subjects
      map( tuple => (tuple._2._1._1,(tuple._2._1._2,tuple._2._2))).join(zippedNodes). // join as objects
      map( tuple => new Edge(tuple._2._1._2,tuple._2._2,tuple._2._1._1)) // source, destination, edge attr
    val nodes: RDD[(VertexId, Any)] = zippedNodes.map(node => {
      val id = node._2
      val attribute = node._1
      (id, attribute)
    })

    Graph(nodes, edges).persist(StorageLevel.MEMORY_ONLY)//.partitionBy(PartitionStrategy.EdgePartition1D)
  }

}
