/*
* Spark Executor is the doorway to accessing spark specific distributed processing
* */

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.annotation.tailrec
import scala.collection.immutable.HashMap
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
    //spark.sparkContext.setLogLevel("ERROR") // preventing verbose messages from being printed on the console
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
    val matchSetList4 = validateMatchSetList(matchSetList3)
    val matchSetList4Contents = matchSetList4.collect()

    //matchSetList4 is further processed to convert the mapping to a hashmap which is more suitable for checking the structure conservation criteria later on
    val flatMatchTable = matchSetList4.map(tuple => (tuple._1, convertMappingToHashmap(tuple._2),tuple._3))
    val flatMatchTableContents = flatMatchTable.collect()

    // vertexMatchSetList is created to give a vertex centric view of the Match sets. Map structure is ((triplet.srcId, triplet Src attribute), (pattern ID, (pattern Subject, triplet Src attribute), (pattern Object, triplet dest attribute), triplet))
    val vertexMatchSetList = flatMatchTable.map(tuple => ((tuple._3.srcId,tuple._3.srcAttr), (tuple._1, tuple._2, tuple._3))).groupByKey()
    val vertexMatchSetListContents = vertexMatchSetList.collect()

    //Adding indices for tables. Do we need  this to persist for next step?
    val vertexMatchTableList = vertexMatchSetList.map( vertex=> (vertex._1, Seq(vertex._2.zipWithIndex)))
    val vertexMatchTableListContents = vertexMatchTableList.collect()

    // create a new graph for next superstep
    val newVertices : RDD[(VertexId, Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])])] = vertexMatchSetList.map( vertex => { //Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]
      val id = vertex._1._1
      val attribute = vertex._2.asInstanceOf[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]]// vertex._1, //.asInstanceOf[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])
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
    val destinationMessages = newgraph.aggregateMessages[Seq[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]]](triplet => //Tuple2[Any, Seq[Any]] compact buffer is a subclass of sequence //Tuple2[Any,
    {triplet.sendToDst(Seq(triplet.srcAttr.asInstanceOf[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]]))}, (a, b) => {
       a.++(b) // _2 //a.++(b)
    })
    val destinationMessagesContents = destinationMessages.collect()

    val destinationMessagesWithDestAttr = destinationMessages.join(newgraph.vertices).map(x => (x._1, (x._2._1, Seq(x._2._2))))//.map( x => (x._1, Seq(x._2._1, x._2._2.asInstanceOf[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]]))) //(x._2._1.++ (Seq(x._2._2.asInstanceOf[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]])))))
    val destinationMessagesWithDestAttrContents = destinationMessagesWithDestAttr.collect()

    val y = destinationMessagesWithDestAttr.map(x => (x._1, x._2))

    //val z = List((1, Seq(Seq(Tuple2(1,2),Tuple2(3,1)), Seq(Tuple2(3,4)))), (2, Seq(Seq(Tuple2(4,5),Tuple2(8,6)), Seq(Tuple2(3,1)))))
    //val zRDD = spark.sparkContext.parallelize(z)
    //zRDD.map( x => (x._1, x._2.flatten)).collect()

    val ycontents = y.collect()
    val x1 = destinationMessagesWithDestAttrContents(0)
    //val flatx1 = x1._2.flatten
    val x1permuted = permute(x1._2)

    //val x2 = destinationMessagesContents(0)
    //val x2permuted = permute(x2._2)

    val nextIterationVertices = destinationMessagesWithDestAttr.mapValues( x => permute(x))
    val nextIterationVerticesContents = nextIterationVertices.collect()

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

  def permute (matchTableListCombined : (Seq[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]],Seq[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]])) : Seq[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]] = {
    if (matchTableListCombined._1.isEmpty){
      matchTableListCombined._2
    }
    else{

      val flatMatchSetList = matchTableListCombined._1.flatten

      def append(checkList: Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])], Acc: Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]): Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])] = {
        // call tail first
        @tailrec
        def appendWithAccumulator(checkList: Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])], Acc: Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]): Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])] = {
          checkList match {
            case Nil => Acc
            case h :: t => if (!Acc.map(_._1).contains(h._1) && // structure conservation criteria 1: pattern id Slot should be empty
              verifyMapping(Acc.map(_._2),h._2)){ // structure conservation criteria 2: Common variable mappings should have same vertex attribute
              appendWithAccumulator(t, Acc.++(Seq(h)))
            }
            else {
              appendWithAccumulator(t, Acc)
            }
          }
        }

        def verifyMapping(mtSeq: Seq [HashMap[String, String]], ms: HashMap[String, String]): Boolean ={
          /*
          * This function checks if the common mapping variables between matchset ms and matchTableList mtSeq correspond with each other */
          for(key <- ms.keys){
            //println("key "+ key)
            for(mt <- mtSeq){
              if(mt.contains(key)){
                //println(" mt("+key+") " + mt(key) + " ms("+ key +") " + ms(key))
                if (!mt(key).equals( ms(key))) return false
              }
            }
          }
          true
        }
        appendWithAccumulator(checkList, Acc)
      }

      def loop(matchTableList : Seq[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]]) : Seq[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]] ={
        @tailrec
        def loopWithAccumulator(matchTableList : Seq[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]], Acc: Seq[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]]): Seq[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]] ={
          matchTableList match {
            case Nil => Acc
            case h::t => loopWithAccumulator(t, Acc.++(Seq(append(flatMatchSetList, h)))) // structure conservation criteria 2: Add only if such a match_table doesn't already exist
          }
        }
        loopWithAccumulator(matchTableList, Seq.empty[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]])
      }

      if (!matchTableListCombined._2.isEmpty) loop(matchTableListCombined._2)
      else loop(Seq(flatMatchSetList))
    }

      //val flatMatchSetList = matchTableList.flatten // y or checklist


  }


  def convertMappingToHashmap (mapping: ((String,Any),(String,Any))): HashMap[String, String] ={
    HashMap(mapping._1._1 -> mapping._1._2.toString, mapping._2._1 -> mapping._2._2.toString)

  }
    //var newMatchTableList

    /*val x = Seq((0,'a'),(1,'b'),(2,'c'),(3,'d'),(4,'e'),(5,'f'))
    val y = Seq((6,'g'), (7,'h'), (0,'a'))

    def sum(checkList: Seq[(Int,Char)], list: Seq[(Int,Char)]): Seq[(Int,Char)] = {
      @tailrec
      def sumWithAccumulator(checkList1: Seq[(Int,Char)], acc: Seq[(Int,Char)]): Seq[(Int,Char)] = {
        checkList1 match {
          case Nil => acc
          case h :: t => if(!acc.map(_._1).contains(h._1)){
            sumWithAccumulator(t, acc.++(Seq(h)))
          }
          else{
            sumWithAccumulator(t, acc)
          }
        }
      }
      sumWithAccumulator(checkList, list)
    }

    sum(y,x) */

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
    MatchSetList.filter(tuple => !((!tuple._2._1._1.startsWith("?") &&
      (tuple._2._1._1 != tuple._3.srcAttr)) ||
      (!tuple._2._2._1.startsWith("?") &&
        (tuple._2._2._1 != tuple._3.dstAttr)))) // nodes not starting with '?' are actual values
    //val res1Contents = res1.collect()

  }







  // set all configuration parameters
  // https://spark.apache.org/docs/2.2.0/configuration.html
  // nested rdd operations
  //https://stackoverflow.com/questions/30089646/spark-use-reducebykey-on-nested-structure
  //https://stackoverflow.com/questions/27535060/can-reducebykey-be-used-to-change-type-and-combine-values-scala-spark
  //https://stackoverflow.com/questions/45937736/spark-scala-creating-nested-structure-using-reducebykey-using-rdd-only

}
