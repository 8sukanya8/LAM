/*
* Spark Executor is the doorway to accessing spark specific distributed processing
* */

import org.apache.spark.graphx.{EdgeTriplet, Graph}
import org.apache.spark.rdd.RDD
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
    val matchSetList1 = graph.triplets.filter (triplet => patternPredicates.value.map(x => x._2).contains(triplet.attr))
    val matchSetList1Contents = matchSetList1.collect()
    // tuple1 = graph and tuple 2 = patterns
    val matchSetList2 = matchSetList1.cartesian(patternPredicatesRDD).filter( tuple => tuple._1.attr == tuple._2._2).map( tuple => (tuple._2._1, tuple._2._3, tuple._2._4,  tuple._1))
    val matchSetList3 = matchSetList2.map(tuple => (tuple._1, ((tuple._2, tuple._4.srcAttr), (tuple._3, tuple._4.dstAttr)), tuple._4))
    //val matchSetList3Contents = matchSetList3.collect()
    val matchSetList4 = validateMatchSetList(matchSetList3)
    val matchSetList2Contents = matchSetList4.collect()

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

  def validateMatchSetList(MatchSetList : RDD[(Int, ((String, Any), (String, Any)), EdgeTriplet[Any, String])]): RDD[(Int, ((String, Any), (String, Any)), EdgeTriplet[Any, String])] ={
    // filters only those matchsets which have no violation in mappings.
    // Violation in mappings is caused when either/both nodes in a query pattern are actual values and not variables.
    // we need to check if these values are matches by the graph edge triplet nodes.
    val res1 = MatchSetList.filter(tuple => !((!tuple._2._1._1.startsWith("?") && (tuple._2._1._1 != tuple._3.srcAttr)) ||
      (!tuple._2._2._1.startsWith("?") && (tuple._2._2._1 != tuple._3.dstAttr)))) // nodes not starting with '?' are actual values
    //val res1Contents = res1.collect()
    return res1
  }



  // set all configuration parameters
  // https://spark.apache.org/docs/2.2.0/configuration.html
}
