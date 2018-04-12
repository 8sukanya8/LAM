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
    val patternPredicates = spark.sparkContext.broadcast(query.getPatternPredicates())
    val patternPredicatesContents = patternPredicates.value
    val matchSetList = graph.triplets.filter(triplet => patternPredicates.value.contains(triplet.attr) )
    /*val matchSetList = graph.triplets.map(triplet => {
      val sub = triplet.srcAttr
      val pred = triplet.attr
      val obj = triplet.dstAttr
      for (pattern <- patterns){
        val splitPattern = pattern.split(" ")
        if(splitPattern.size > 1){
          val patternPredicate = splitPattern(1)
          if(patternPredicate == pred){
            println(sub, obj, patternPredicate, pred)
          }
        }

      }

    }) */
    val matchSetListContents = matchSetList.collect()

    //val ms = new MatchSet(1, ("sub", "predicate", "obj"))

    val x = 4

  }


  // set all configuration parameters
  // https://spark.apache.org/docs/2.2.0/configuration.html
}
