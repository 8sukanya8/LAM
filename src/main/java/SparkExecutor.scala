/*
* Spark Executor is the doorway to accessing spark specific distributed processing
* */

import org.apache.spark.graphx.Graph
import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.SparkSession
//import net.sansa_stack.rdf.spark.model.{JenaSparkRDDOps, TripleRDD}


object SparkExecutor {

  private var spark:SparkSession = null
  var graph: Graph[Any, String] = null
  //var spark:net.sansa_stack.rdf.spark.io.
  def ConfigureSpark(): Unit ={
    //val conf = new SparkConf().setAppName("LAM").setMaster("local")
    //sparkContext = new SparkContext(conf)

    spark = SparkSession.builder
      .master("local[*]")
      .config("spark.driver.cores", 1)
      .appName("LAM")
      .getOrCreate()

  }

  def createGraph(): Unit ={
    //Graph[Any, String]
    graph = RDFGraph.createGraph(spark)
    //return graph
  }

  def bgp(query: QueryClass): Unit ={
    val verticesContents = graph.vertices.collect()
    val edgesContents = graph.edges.collect()
    val tripletscontents = graph.triplets.collect()
    /*graph.triplets.flatMap(triplet => {
      //val result = scala.collection.mutable.ArrayBuffer.empty[(Long, IMessage)]
      val subjectNode = triplet.srcAttr;
      val objectNode = triplet.dstAttr;
      val edge: EdgeAttribute = triplet.attr;
    }*/
    //val ms = new MatchSet(1, ("sub", "predicate", "obj"))
    val pattern = query.getQueryPattern




    // return type Graph[Any, String]


  }


  // set all configuration parameters
  // https://spark.apache.org/docs/2.2.0/configuration.html
}
