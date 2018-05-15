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

  def tripleCount(): Long ={
    graph.triplets.count()
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

    val matchSetList = matchListTransformations(patternPredicates, patternPredicatesRDD)
    //matchSetList4 is further processed to convert the mapping to a hashmap which is more suitable for checking the structure conservation criteria later on
    val flatMatchTable = matchSetList.map(tuple => (tuple._1, convertMappingToHashmap(tuple._2),tuple._3))
    //val flatMatchTableContents = flatMatchTable.collect()

    // vertexMatchSetList is created to give a vertex centric view of the Match sets. Map structure is ((triplet.srcId, triplet Src attribute), (pattern ID, (pattern Subject, triplet Src attribute), (pattern Object, triplet dest attribute), triplet))
    val vertexMatchSetList = flatMatchTable.map(tuple => ((tuple._3.srcId,tuple._3.srcAttr), (tuple._1, tuple._2, tuple._3))).groupByKey()
    //val vertexMatchSetListContents = vertexMatchSetList.collect()

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
    //val newgraphEdgesContents = newgraph.edges.collect()

    //send the vertex property along the outgoing edge and aggregate messages at destination vertices
    // destinationMessages contains the combined messages received by all vertices
    val destinationMessages = newgraph.aggregateMessages[Seq[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]]](triplet => //Tuple2[Any, Seq[Any]] compact buffer is a subclass of sequence //Tuple2[Any,
    {triplet.sendToDst(Seq(triplet.srcAttr.asInstanceOf[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]]))}, (a, b) => {
       a.++(b) // _2 //a.++(b)
    })
    val destinationMessagesContents = destinationMessages.collect()

    val destinationMessagesWithDestAttr = destinationMessages.join(newgraph.vertices).map(x => (x._1, (x._2._1, Seq(x._2._2))))//.map( x => (x._1, Seq(x._2._1, x._2._2.asInstanceOf[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]]))) //(x._2._1.++ (Seq(x._2._2.asInstanceOf[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]])))))
    val destinationMessagesWithDestAttrContents = destinationMessagesWithDestAttr.collect()

    val x1 = destinationMessagesWithDestAttrContents(0)
    val x1permuted = permute(x1._2)
    val nextIterationVertices = destinationMessagesWithDestAttr.mapValues( x => permute(x))
    val nextIterationVerticesContents = nextIterationVertices.collect()
    val refinedNextIterationVertices = nextIterationVertices.filter(_._2.size > 0) // filter vertices which have received some inputs from neighbours
    val refinedNextIterationVerticesContents = refinedNextIterationVertices.collect()
    if(refinedNextIterationVertices.isEmpty()){// || (nextIterationVertices.map(_._2.size).reduce((x,y)=> (x+y))==0)){ //time consuming
      print("No matches found!")
    } // develop a better way of checking null value of newvertices
    else
      superStep(refinedNextIterationVertices, newgraph, query.numberOfPatterns)
    //val x = 4
  }



  def superStep (newVertices: RDD[(VertexId, Seq[Seq[(Int, HashMap[String,String], EdgeTriplet[Any,Any])]])], oldGraph: Graph[Seq[(Int, HashMap[String,String], EdgeTriplet[Any,Any])],String], numberOfPatterns: Int): Unit ={

    def stepLoop (newVertices: RDD[(VertexId, Seq[Seq[(Int, HashMap[String,String], EdgeTriplet[Any,Any])]])]): Seq[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]] ={ //oldGraph: Graph[Seq[(Int, HashMap[String,String], EdgeTriplet[Any,Any])],String])
      def terminationCondition(): Boolean ={
        return true
      }

      if (terminationCondition()){
        val joinAsSubjects = newVertices.join(graph.triplets.map(triplet => (triplet.srcId,(triplet.attr,triplet.dstId))))
        val joinAsSubjectsContents = joinAsSubjects.collect()
        val joinAsObjects =  newVertices.join(joinAsSubjects.map(triplet => (triplet._2._2._2,(triplet._1, triplet._2._2._2, triplet._2._2._1))))
        val joinAsObjectsContents = joinAsObjects.collect()
        val newEdges = joinAsObjects.map(edge => new Edge(edge._2._2._1, edge._2._2._2, edge._2._2._3))
        if(!newEdges.isEmpty()){
          //val newEdgesContents = newEdges.collect()
          // edge filtration not implemented
          val newgraph = Graph(newVertices, newEdges)
          val newgraphverCon = newgraph.vertices.collect()
          val newgraphedgesCon = newgraph.edges.collect()
          val destinationMessages = newgraph.aggregateMessages[Seq[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]]](
            triplet => { //Tuple2[Any, Seq[Any]] compact buffer is a subclass of sequence //Tuple2[Any,
              if(!triplet.srcAttr.contains(null)){
                triplet.sendToDst(triplet.srcAttr)
              }
            },
            (a, b) => a.++(b)
          )
          //val destinationMessagesContents = destinationMessages.collect()
          val destinationMessagesWithDestAttr = destinationMessages.join(newgraph.vertices).map(x => (x._1, (x._2._1, x._2._2)))//.map( x => (x._1, Seq(x._2._1, x._2._2.asInstanceOf[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]]))) //(x._2._1.++ (Seq(x._2._2.asInstanceOf[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]])))))
          val destinationMessagesWithDestAttrContents = destinationMessagesWithDestAttr.collect()
          val x = destinationMessagesWithDestAttrContents(1)
          val xpermute = permute(x._2)
          val nextIterationVertices = destinationMessagesWithDestAttr.mapValues( x => permute(x))
          val nextIterationVerticesContents = nextIterationVertices.collect()
          val refinedNextIterationVertices = nextIterationVertices.filter(_._2.size > 0) // filter vertices which have received some inputs from neighbours
          val refinedNextIterationVerticesContents = refinedNextIterationVertices.collect()
          stepLoop(refinedNextIterationVertices)
        }
        else{
          printMappings(newVertices, numberOfPatterns)
          return Nil
        }
         //newgraph)
      }
      else{
          printMappings(newVertices, numberOfPatterns)
          return Nil
      }

    }
    stepLoop(newVertices)

  }


  def permute (matchTableListCombined : (Seq[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]],Seq[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]])) : Seq[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]] = {

    if (matchTableListCombined._1.isEmpty){
      if(!matchTableListCombined._2.isEmpty)
        matchTableListCombined._2
      else // throw error
        return null
    }
    else{

      def generateFlatMatchSetList(): Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])] = {
        if ( (matchTableListCombined._2 == null) || (matchTableListCombined._2.contains(null))){
          matchTableListCombined._1.flatten.distinct
        }
        else{
          matchTableListCombined._1.flatten.union(matchTableListCombined._2.flatten).distinct // adding the portion of the matchtables too
        }

      }

      val flatMatchSetList = generateFlatMatchSetList()

      def generateMatchTableList (): Seq[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]] ={
        if ((matchTableListCombined._2 == null) || (matchTableListCombined._2.contains(null))){
          flatMatchSetList.map(matchset => Seq(matchset))
        }
        else{
          matchTableListCombined._2
        }

      }
      val matchTableList = generateMatchTableList()

      def append(checkList: Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])], Acc: Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])], SuperAcc: Seq[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]] ): Seq[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]] = {
        // call tail first
        @tailrec
        def appendWithAccumulator(checkList: Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])], Acc: Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])], SuperAcc: Seq[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]] ): Seq[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]] = {
          checkList match {
            case Nil => SuperAcc
            case h :: t => if (!Acc.map(_._1).contains(h._1) && // structure conservation criteria 1: pattern id Slot should be empty
              verifyMapping(Acc.map(_._2),h._2) && !(SuperAcc.contains(Seq(Acc.++(Seq(h)))))){ // structure conservation criteria 2: Common variable mappings should have same vertex attribute
              appendWithAccumulator(t, Acc, SuperAcc.++(Seq(Acc.++(Seq(h)))))
            }
            else {
              appendWithAccumulator(t, Acc, SuperAcc) //do we want to keep match tables that have not been permuted for the future? If yes then superAcc must append Acc
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
        appendWithAccumulator(checkList, Acc, SuperAcc)
      }

      def loop(matchTableList : Seq[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]]) : Seq[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]] ={
        @tailrec
        def loopWithAccumulator(matchTableList : Seq[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]], Acc: Seq[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]]): Seq[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]] ={
          matchTableList match {
            case Nil => Acc
            case h::t => {
              val mt = append(flatMatchSetList, h, Seq.empty[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]])
              if(AccDoesNotContainMatchTable(Acc, mt) && (mt.size > 0))
                loopWithAccumulator(t, Acc.++(mt))
              else{
                //if(Acc.isEmpty){
                //  loopWithAccumulator(t,Seq.empty[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]])
                //}
                //else
                loopWithAccumulator(t, Acc)
              }

            } // structure conservation criteria 2: Add only if such a match_table doesn't already exist
          }
        }
        loopWithAccumulator(matchTableList, Seq.empty[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]])
      }
      def AccDoesNotContainMatchTable(Acc: Seq[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]], mt: Seq[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]]): Boolean = {
        val flatMt = mt.flatten
        var bool = true
        for(existingMatchTable <- Acc){
          val flatExistingMatchTable = existingMatchTable
          var tempBool = true
          for(matchset <- flatMt){
            def flatExistingMatchTableContainsflatMt(): Boolean ={
              if(flatExistingMatchTable.contains(matchset)) // Since, flatExistingMatchTable does not contain matchset, therefore Acc cannot
                false //
              else
                true
            }
            tempBool = tempBool && flatExistingMatchTableContainsflatMt()
          }
          bool = bool && tempBool
        }

        bool
      }

      loop(matchTableList)
      //if ( (matchTableListCombined._2 == null) || (matchTableListCombined._2.contains(null))) flatMatchSetList.map(matchset => Seq(matchset))//matchTableListCombined._1
      //else loop(matchTableListCombined._2) //loop(flatMatchSetList.map(matchset => Seq(matchset)))
        //flatMatchSetList.map(matchset => Seq(matchset))

    }
      //val flatMatchSetList = matchTableList.flatten // y or checklist
  }


  def matchListTransformations(patternPredicates: org.apache.spark.broadcast.Broadcast[Array[(Int, String, String, String)]], patternPredicatesRDD : RDD[(Int, String, String, String)]): RDD[(Int, ((String, Any), (String, Any)), org.apache.spark.graphx.EdgeTriplet[Any,String])]={
    // matchSetList creation via a series of transformations
    // matchSetList1 aims to reduce the number of edge triplets by checking if the edge attr is contained by any of the query patterns. This is necessary as we will be doing a cartesian join later on. Smaller the number of edges to be joined, smaller the cost of joining.
    val matchSetList1 = graph.triplets.filter (triplet => patternPredicates.value.map(x => x._2).contains(triplet.attr))
    //val matchSetList1Contents = matchSetList1.collect()
    // tuple1 = graph and tuple 2 = patterns
    // matchSetList2 forms new tuples by joining triples from matchsetlist1 with patternPredicatesRDD and mapping them to a structure (pattern ID, pattern subject, pattern object, triplet)
    val matchSetList2 = matchSetList1.cartesian(patternPredicatesRDD).filter( tuple => tuple._1.attr == tuple._2._2).map( tuple => (tuple._2._1, tuple._2._3, tuple._2._4,  tuple._1))
    //val matchSetList2Contents = matchSetList2.collect()
    // matchSetList3 creates the mappings for each match set. Map structure is (pattern ID, (pattern Subject, triplet Src attribute), (pattern Object, triplet dest attribute), triplet)
    val matchSetList3 = matchSetList2.map(tuple => (tuple._1, ((tuple._2, tuple._4.srcAttr), (tuple._3, tuple._4.dstAttr)), tuple._4))
    //val matchSetList3Contents = matchSetList3.collect()
    // flatMatchTable is created by filtering (function validateMatchSetList) matchSetList3 to remove those tuples in which the pattern nodes are not variables and do not match the triplet nodes
    // example: Given pattern "?X <http://swat.cse.lehigh.edu/onto/univ-bench.owl#teacherOf> <http://www.Department0.University0.edu/Course0> "
    // Allow pattern node "?X" to be mapped to srcAttr "<http://www.Department0.University0.edu/FullProfessor0>"
    // Do not allow pattern node "<http://www.Department0.University0.edu/Course0>" to be mapped to "<http://www.Department0.University0.edu/Course12>"
    // This is because Course0 is an actual value and not a variable
    val matchSetList4 = validateMatchSetList(matchSetList3)
    //val matchSetList4Contents = matchSetList4.collect()
    // remove duplicate edges
    val matchSetList5 = matchSetList4.distinct()
    //val matchSetList5Contents = matchSetList5.collect()
    matchSetList5

  }

  def convertMappingToHashmap (mapping: ((String,Any),(String,Any))): HashMap[String, String] ={
    HashMap(mapping._1._1 -> mapping._1._2.toString, mapping._2._1 -> mapping._2._2.toString)
  }

  //description of each pregel function
  //https://spark.apache.org/docs/2.0.2/api/java/org/apache/spark/graphx/Pregel.html
  //https://www.cakesolutions.net/teamblogs/graphx-pregel-api-an-example

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

  def printMappings(matchTableList : RDD[(VertexId, Seq[Seq[(Int, HashMap[String,String], EdgeTriplet[Any,Any])]])], numberOfPatterns: Int): Unit ={
    println("Printing answers")
    val matchTableflattened = matchTableList.flatMap( x => x._2)
    for(matchTable <- matchTableflattened){
      if(matchTable.size == numberOfPatterns){
        print("Completed query")
      }
      else
        print("Incomplete query")
      for(matchset <- matchTable){
        for(key <- matchset._2.keys){
          if(key.startsWith("?"))
            println(key + " -> " + matchset._2(key))
        }
        //println(matchset._2.toString())
      }
    }
  }
  // set all configuration parameters
  // https://spark.apache.org/docs/2.2.0/configuration.html
}
