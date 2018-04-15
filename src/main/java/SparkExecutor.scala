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
    val patternPredicatesRDD = spark.sparkContext.parallelize(query.getPatternPredicates()) // get patterns from query
    val patternPredicatesRDDContents = patternPredicatesRDD.collect()
    val patternPredicates = spark.sparkContext.broadcast(patternPredicatesRDD.collect()) // broadcast the patterns
    val patternPredicatesContents = patternPredicates.value

    val matchSetListInitial = graph.triplets.filter (triplet => patternPredicates.value.map(x => x._2).contains(triplet.attr))
    val matchSetListInitialContents = matchSetListInitial.collect()
    val matchSetList = matchSetListInitial.cartesian(patternPredicatesRDD).filter( tuple => tuple._1.attr == tuple._2._2).map( tuple => (tuple._2._1, tuple._1))
    val matchSetListContents = matchSetList.collect()


    //val patternPredicates = spark.sparkContext.broadcast(query.getPatternPredicates())
    //val patternPredicatesContents = patternPredicates.value
    //val matchSetList = graph.triplets.filter (triplet => patternPredicates.value.map(x => x._2).contains(triplet.attr))

    //val m = matchSetList.map( x => (x, matchSetList.map(t => t.attr).union(patternPredicatesRDD.map(x=> x._2)))) // spark context missing
    //val mCon = m.collect()

    //val matchSetList3 = graph.triplets.filter(triplet => patternPredicates.value.keys.toSet.contains(triplet.attr)).
    //  map(triplet => (patternPredicates.value(triplet.attr), triplet))

    //val matchSetList3Contents = matchSetList3.collect()
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


    //val ms = new MatchSet(1, ("sub", "predicate", "obj"))

    val x = 4

  }


  // set all configuration parameters
  // https://spark.apache.org/docs/2.2.0/configuration.html
}
