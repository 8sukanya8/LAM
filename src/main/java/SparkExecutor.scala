/*
* Spark Executor is the doorway to accessing spark specific distributed processing
* */

import org.apache.spark.graphx._
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
    spark.sparkContext.setLogLevel("ERROR") // preventing verbose messages from being printed on the console
  }

  def createGraph(): Unit ={
    //Graph[Any, String]
    graph = RDFGraph.createGraph(spark)
    //return graph
  }

  def bgp(query: QueryClass): Unit ={
    /*
    * This function is responsible for running the bgp matching on the graph.
    * Query patterns are broadcasted and through a series of transformations on a graph Matchtables are created per Vertex
    * */

    // View the graph. Remove the collect statements once testing is completed.
    val verticesContents = graph.vertices.collect()
    val edgesContents = graph.edges.collect()
    val tripletscontents = graph.triplets.collect()

    // get patterns from query and broadcast the patterns
    val patternPredicatesRDD = spark.sparkContext.parallelize(query.getPatternPredicates())
    val patternPredicates = spark.sparkContext.broadcast(patternPredicatesRDD.collect())

    // matchSetList creation via a series of transformations
    // matchSetList1 aims to reduce the number of edge triplets by checking if the edge attr is contained by any of the query patterns. This is necessary as we will be doing a cartesian join later on. Smaller the number of edges to be joined, smaller the cost of joining.
    val matchSetList1 = graph.triplets.filter (triplet => patternPredicates.value.map(x => x._2).contains(triplet.attr))
    val matchSetList1Contents = matchSetList1.collect()

    // tuple1 = graph and tuple 2 = patterns
    // matchSetList2 forms new tuples by joining triples from matchsetlist1 with patternPredicatesRDD and mapping them to a structure (pattern ID, pattern subject, pattern object, triplet)
    val matchSetList2 = matchSetList1.cartesian(patternPredicatesRDD).filter( tuple => tuple._1.attr == tuple._2._2).map( tuple => (tuple._2._1, tuple._2._3, tuple._2._4,  tuple._1))
    val matchSetList2Contents = matchSetList2.collect()

    // matchSetList3 creates the mappings for each match set. Map structure is (pattern ID, (pattern Subject, triplet Src attribute), (pattern Object, triplet dest attribute), triplet)
    val matchSetList3 = matchSetList2.map(tuple => (tuple._1, ((tuple._2, tuple._4.srcAttr), (tuple._3, tuple._4.dstAttr)), tuple._4))
    val matchSetList3Contents = matchSetList3.collect()

    // flatMatchTable is created by filtering (function validateMatchSetList) matchSetList3 to remove those tuples in which the pattern nodes are not variables and do not match the triplet nodes
    // example: Given pattern "?X <http://swat.cse.lehigh.edu/onto/univ-bench.owl#teacherOf> <http://www.Department0.University0.edu/Course0> "
    // Allow pattern node "?X" to be mapped to srcAttr "<http://www.Department0.University0.edu/FullProfessor0>"
    // Do not allow pattern node "<http://www.Department0.University0.edu/Course0>" to be mapped to "<http://www.Department0.University0.edu/Course12>"
    // This is because Course0 is an actual value and not a variable
    val flatMatchTable = validateMatchSetList(matchSetList3)
    val flatMatchTableContents = flatMatchTable.collect()

    // vertexMatchSetList is created to give a vertex centric view of the Match sets. Map structure is ((triplet.srcId, triplet Src attribute), (pattern ID, (pattern Subject, triplet Src attribute), (pattern Object, triplet dest attribute), triplet))
    val vertexMatchSetList = flatMatchTable.map(tuple => ((tuple._3.srcId,tuple._3.srcAttr), (tuple._1, tuple._2, tuple._3))).groupByKey()
    val vertexMatchSetListContents = vertexMatchSetList.collect()

    //Adding indices for tables. Do we need  this to persist for next step?
    val vertexMatchTableList = vertexMatchSetList.map( vertex=> (vertex._1, vertex._2.zipWithIndex))
    val vertexMatchTableListContents = vertexMatchTableList.collect()

    // create a new graph for next superstep
    val newVertices : RDD[(VertexId, Any)] = vertexMatchSetList.map( vertex => {
      val id = vertex._1._1
      val attribute = (vertex._1,vertex._2)//vertex._1._2 .asInstanceOf[List[Any]]
      (id, attribute)
    } )
    val newVerticesContents = newVertices.collect()
    val newEdges = flatMatchTable.map(tuple => new Edge(tuple._3.srcId,tuple._3.dstId,tuple._3.attr))
    val EdgesFromCurrentSSContents = newEdges.collect()
    val newgraph = Graph (newVertices, newEdges)
    val newgraphVerticesContents = newgraph.vertices.collect()
    val newgraphEdgesContents = newgraph.edges.collect()

    //val joinNeighbours = graph.vertices.join(graph.triplets.map(x => (x.dstId, x.dstAttr)))
    //val aggregated = graph.aggregateMessages[Any](triplet => {triplet.sendToDst(triplet.srcAttr)},(a, b) => (a,b))

    //send the vertex property along the outgoing edge and aggregate messages at destination vertices
    // destinationMessages contains the combined messages received by all vertices
    val destinationMessages = newgraph.aggregateMessages[Tuple2[Any,Seq[Any]]](triplet => //Tuple2[Any, Seq[Any]] compact buffer is a subclass of sequence
    {triplet.sendToDst(triplet.srcAttr.asInstanceOf[Tuple2[Any,Seq[Any]]])},(a, b) => {
      (a._1, (a._2.++:(b._2)))
    })
    val destinationMessagesContents = destinationMessages.collect()

    // neighbours collect null values as the attributes of the vertices
    //val neighbors = graph.collectNeighbors(edgeDirection = EdgeDirection.Out)
    //val z = vertexMatchSetListContents(0)._2 //vertexMatchTableListContents(0)._2
    //val tuplez = z.toArray
    //val s1 = vertexMatchTableList.filter(vertex => vertex._1 == (2141,"<http://www.Department0.University0.edu/GraduateStudent131>")).map(vertex => (vertex._1, (vertex._2.map(tuples => (tuples._1,tuplez, tuples._2)))))
    //val s = vertexMatchTableList.filter(vertex => vertex._1 == (2141,"<http://www.Department0.University0.edu/GraduateStudent131>")).map(vertex => (vertex._1, (vertex._2++(z))))
    //val sContents = s.collect()
    //val s1Contents = s1.collect()

    //val s1 = vertexMatchTableList.lookup(161).++( z)
    val x = 4


  }

  //description of each pregel function
  //https://spark.apache.org/docs/2.0.2/api/java/org/apache/spark/graphx/Pregel.html
  //https://www.cakesolutions.net/teamblogs/graphx-pregel-api-an-example
  def sendMsg1(triplet: EdgeTriplet[Any, String], MatchTableList: RDD[((Int,String), ((String, Any), (String, Any)), EdgeTriplet[Any, String])]) : RDD[((Int,String), ((String, Any), (String, Any)), EdgeTriplet[Any, String])] = {
    val sourceVertexId = triplet.srcId
    val sourceVertexAttr = triplet.srcAttr
    // point of optimisation, right now sending the entire matchtable list. highly inefficient
    val message = MatchTableList.filter(tuple => (tuple._1._2 ==sourceVertexAttr && tuple._1._1 == sourceVertexId))
    return message
  }



  def mergeMsg1(msg1: RDD[((Int,String), ((String, Any), (String, Any)), EdgeTriplet[Any, String])], msg2: RDD[((Int,String), ((String, Any), (String, Any)), EdgeTriplet[Any, String])]): RDD[((Int,String), ((String, Any), (String, Any)), EdgeTriplet[Any, String])] = {
    return msg1.union(msg2)
  }

  def vprog1(vertexId: VertexId, value: String, message: RDD[((Int,String), ((String, Any), (String, Any)), EdgeTriplet[Any, String])]): Unit = {
    val vertexMatchTableList = message
  }

  def validateMatchSetList(MatchSetList : RDD[(Int, ((String, Any), (String, Any)), EdgeTriplet[Any, String])]): RDD[(Int, ((String, Any), (String, Any)), EdgeTriplet[Any, String])] ={
    // filters only those matchsets which have no violation in mappings.
    // Violation in mappings is caused when either/both nodes in a query pattern are actual values and not variables.
    // we need to check if these values are matches by the graph edge triplet nodes.
    val res1 = MatchSetList.filter(tuple => !((!tuple._2._1._1.startsWith("?") &&
      (tuple._2._1._1 != tuple._3.srcAttr)) ||
      (!tuple._2._2._1.startsWith("?") &&
        (tuple._2._2._1 != tuple._3.dstAttr)))) // nodes not starting with '?' are actual values
    //val res1Contents = res1.collect()
    return res1
  }





  // set all configuration parameters
  // https://spark.apache.org/docs/2.2.0/configuration.html
  // nested rdd operations
  //https://stackoverflow.com/questions/30089646/spark-use-reducebykey-on-nested-structure
  //https://stackoverflow.com/questions/27535060/can-reducebykey-be-used-to-change-type-and-combine-values-scala-spark
  //https://stackoverflow.com/questions/45937736/spark-scala-creating-nested-structure-using-reducebykey-using-rdd-only

}
