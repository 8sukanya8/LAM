/*
* Spark Executor is the doorway to accessing spark specific distributed processing
* */

import java.io.{BufferedWriter, File, FileWriter, OutputStreamWriter}

import org.apache.spark.{SparkConf, graphx}
import org.apache.spark.graphx.{VertexId, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.collection.immutable.HashMap


object SparkExecutor {
  val log = LoggerFactory.getLogger(getClass)
  private var spark:SparkSession = null
  var graph: Graph[Any, String] = null
  var identifiedTerminalVertices : RDD[VertexId] = null

  def ConfigureSpark(): Unit ={
    val conf =
      new SparkConf().setAppName("LAM")
      .set("spark.driver.cores", Configuration.parallelism.toString)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    if(Configuration.master.equals("local[*]")){
      conf.setMaster("local[*]")
    }
    spark = SparkSession.builder
      .config(conf)
      .getOrCreate()

  }

  def createGraph(): Unit ={
    graph = RDFGraph.createGraph(spark)
  }

  /*def graphProfiler(): Unit ={
    RDFGraph.graphProfiler(graph)
  }*/

  def tripleCount(): Long ={
    graph.triplets.count()
  }

  def bgp(query: QueryClass): Unit ={
    /** Executes the bgp matching on the graph.
    * Query patterns are broadcasted and through a series of transformations on a graph Matchtables are created per Vertex
    * */

    Configuration.numberOfQueryPatterns = query.numberOfPatterns
    // get patterns from query and broadcast the patterns
    val patternPredicatesAsRDD = spark.sparkContext.parallelize(query.getPatternPredicates())
    val patternPredicatesAsBroadcast = spark.sparkContext.broadcast(patternPredicatesAsRDD.collect())

    val flatMatchTable = createFlatMatchTable(patternPredicatesAsBroadcast, patternPredicatesAsRDD)


    // vertexMatchSetList is created to give a vertex centric view of the Match sets. Map structure is ((triplet.srcId, triplet Src attribute), (pattern ID, (pattern Subject, triplet Src attribute), (pattern Object, triplet dest attribute), triplet))
    val vertexMatchSetList =
      flatMatchTable.map(tuple => ((tuple._3.srcId,tuple._3.srcAttr),
      Seq(Tuple3(tuple._1, tuple._2, tuple._3)))).groupByKey()
    //val vertexMatchSetListContents = vertexMatchSetList.collect()

    // create a new graph for next superstep
    val newVertices : RDD[(VertexId, Seq[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]])] = vertexMatchSetList.map( vertex => { //Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]
      val id = vertex._1._1
      val attribute = vertex._2.asInstanceOf[Seq[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]]]// vertex._1, //.asInstanceOf[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])
      (id, attribute)
    } )

    val newEdges = flatMatchTable.map(tuple => new Edge(tuple._3.srcId,tuple._3.dstId,tuple._3.attr))
    val newgraph = Graph (newVertices, newEdges)
    val originalGraphNewVertices = flatMatchTable.map(tuple => (tuple._3.srcId,tuple._3.srcAttr)).distinct()
    graph = Graph(originalGraphNewVertices, newEdges)
    superStep(newgraph)
  }

  def superStep( newgraph: Graph[Seq[Seq[(Int, HashMap[String,String], EdgeTriplet[Any,Any])]],String]): Unit = { //terminalVerticesInGraphContents : Array[graphx.VertexId]
    Configuration.superstepStart = System.nanoTime
    val NeighbourAndSelfMatchTableList = exchangeMessages(newgraph)
    val nextIterationVertices = getPermutedVertices(NeighbourAndSelfMatchTableList)

    nextIterationVertices.cache()
    if(nextIterationVertices.isEmpty()){
      log.info("\n\nSuperstep from self permute: "+ Configuration.superStepCount +" Duration: "+ (Configuration.superstepEnd - Configuration.superstepStart)/ 1e9d + "\n")
      printMappings(newgraph.vertices, Configuration.numberOfQueryPatterns)
    }
    else{
      if (terminationConditionNotMet()) {
        val nextIterationEdges = getNextIterationEdges(nextIterationVertices)
        nextIterationEdges.cache()
        if (nextIterationEdges.isEmpty()) {
          Configuration.superstepEnd = System.nanoTime
          log.info("\n\nSuperstep : "+ Configuration.superStepCount +" Duration: "+ (Configuration.superstepEnd - Configuration.superstepStart)/1e9d + "\n")
          vertexSelfPermute(nextIterationVertices)
        }
        else {
          val nextIterationGraph = Graph(nextIterationVertices, nextIterationEdges)
          Configuration.superstepEnd = System.nanoTime
          log.info("\n\nSuperstep : "+ Configuration.superStepCount +" Duration: "+ (Configuration.superstepEnd - Configuration.superstepStart)/1e9d + "\n")
          superStep(nextIterationGraph) //terminalVerticesInGraphContents
        }
      }
      else {
        log.info("\n\nSuperstep from self permute: "+ Configuration.superStepCount +" Duration: "+ (Configuration.superstepEnd - Configuration.superstepStart)/ 1e9d + "\n")
        printMappings(nextIterationVertices, Configuration.numberOfQueryPatterns)
      }
    }
  }

  def vertexSelfPermute(vertices: RDD[(VertexId, Seq[Seq[(Int, HashMap[String,String], EdgeTriplet[Any,Any])]])]): Unit ={
    Configuration.superstepStart = System.nanoTime
    val newVertices = getPermutedVertices(vertices.mapValues(x => (x,x)))
    if(newVertices.isEmpty()){
      printMappings(vertices, Configuration.numberOfQueryPatterns)
    }
    else{
      if (!terminationConditionNotMet()){
        log.info("\n\nSuperstep from self permute: "+ Configuration.superStepCount +" Duration: "+ (Configuration.superstepEnd - Configuration.superstepStart)/ 1e9d + "\n")
        printMappings(newVertices, Configuration.numberOfQueryPatterns)
      }
      else {
        Configuration.superstepEnd = System.nanoTime
        log.info("\n\nSuperstep from self permute: "+ Configuration.superStepCount +" Duration: "+ (Configuration.superstepEnd - Configuration.superstepStart)/ 1e9d + "\n")
        vertexSelfPermute(newVertices)
      }
    }
  }

  def getNextIterationEdges(newVertices: RDD[(VertexId, Seq[Seq[(Int, HashMap[String,String], EdgeTriplet[Any,Any])]])]): RDD[Edge[String]] ={
    val joinAsSubjects = newVertices.join(graph.triplets.map(triplet => (triplet.srcId,(triplet.attr,triplet.dstId))))
    val joinAsObjects =  newVertices.join(joinAsSubjects.map(triplet => (triplet._2._2._2,(triplet._1, triplet._2._2._2, triplet._2._2._1))))
    val newEdges = joinAsObjects.map(edge => new Edge(edge._2._2._1, edge._2._2._2, edge._2._2._3))
    newEdges
  }

  def exchangeMessages(newgraph: Graph[Seq[Seq[(Int, HashMap[String,String], EdgeTriplet[Any,Any])]],String]): RDD[(VertexId, (Seq[Seq[(Int, HashMap[String,String], EdgeTriplet[Any,Any])]], Seq[Seq[(Int, HashMap[String,String], EdgeTriplet[Any,Any])]]))] ={
    val destinationMessages = newgraph.aggregateMessages[Seq[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]]](triplet => { //Tuple2[Any, Seq[Any]] compact buffer is a subclass of sequence //Tuple2[Any,
      if(!triplet.srcAttr.contains(null)){
        triplet.sendToDst(triplet.srcAttr)
      }
    }, (a, b) => a.++(b))
    val destinationMessagesWithDestAttr =
      destinationMessages.
        join(newgraph.vertices).map(x => (x._1, (x._2._1, x._2._2)))//.map( x => (x._1, Seq(x._2._1, x._2._2.asInstanceOf[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]]))) //(x._2._1.++ (Seq(x._2._2.asInstanceOf[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]])))))
    destinationMessagesWithDestAttr
  }

  def getPermutedVertices(MatchTableListTuple: RDD[(VertexId, (Seq[Seq[(Int, HashMap[String,String], EdgeTriplet[Any,Any])]], Seq[Seq[(Int, HashMap[String,String], EdgeTriplet[Any,Any])]]))]): RDD[(VertexId, Seq[Seq[(Int, HashMap[String,String], EdgeTriplet[Any,Any])]])] ={
    val nextIterationVertices = MatchTableListTuple.mapValues( x => permute(x))
    val refinedNextIterationVertices = nextIterationVertices.filter(_._2.size > 0) // filter vertices which have received some inputs from neighbours
    refinedNextIterationVertices
  }

  def generateFlatMatchSetList(selfMatchTableList: Seq[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]], neighbourMatchTableList:Seq[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]]): Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])] = {
    if ( (selfMatchTableList == null) || (selfMatchTableList.contains(null))){
      neighbourMatchTableList.flatten.distinct
    }
    else{
      neighbourMatchTableList.flatten.union(selfMatchTableList.flatten).distinct // adding the portion of the matchtables too
    }
  }


  def generateMatchTableList (selfMatchTableList: Seq[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]], flatMatchSetList:Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]): Seq[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]] ={
    if ((selfMatchTableList == null) || (selfMatchTableList.contains(null))){
      flatMatchSetList.map(matchset => Seq(matchset))
    }
    else{
      selfMatchTableList
    }
  }

  def IsNotDuplicated(combination: Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]): Boolean ={
    val patternIdList = combination.map(x => x._1)
    if( patternIdList.size == patternIdList.toSet.size)
      return true
    else
      return false
  }

  def generateCombinations(flatMatchSetList: Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]): Seq[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]] ={
    def loop(count: Int, acc: Seq[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]]): Seq[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]] ={
        if(count==0){
          acc
        }
        else{
          val combi̦naț̦̦̦ions = flatMatchSetList.combinations(count)//Seq(flatMatchSetList.combinations(count).toList).flatten.flatten
          val validCombinations = combi̦naț̦̦̦ions.filter(combo => IsNotDuplicated(combo))
          loop(count-1, acc.++:(combi̦naț̦̦̦ions))
        }
    }
    loop(Configuration.maxAllowedCombinations, Seq.empty[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]])
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

  def appendMatchSetToMatchTable(checkList: Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])], Acc: Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])], SuperAcc: Seq[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]] ): Seq[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]] = {
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
        for(mt <- mtSeq){
          if(mt.contains(key)){
            if (!mt(key).equals( ms(key))) return false
          }
        }
      }
      true
    }
    appendWithAccumulator(checkList, Acc, SuperAcc)
  }

  def getMatchTablesNotContainedInAcc(existingMatchTables: Seq[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]], prospectiveMatchTables: Seq[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]]):  Seq[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]]= {
    def loopProspectiveMatchTables (prospectiveMatchTables: Seq[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]], Acc: Seq[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]]): Seq[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]] ={
      prospectiveMatchTables.toList match{
        case Nil => Acc
        case h :: t => {
          if (ExistingMatchTablesContainsProspectiveMatchTable(h, existingMatchTables))
            loopProspectiveMatchTables(t, Acc)
          else
            loopProspectiveMatchTables(t, Acc.++(Seq(h)))

        }
      }
    }

    def ExistingMatchTablesContainsProspectiveMatchTable(prospectiveMatchTable: Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])], existingMatchTables: Seq[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]]): Boolean = {
      existingMatchTables.toList match {
        case Nil => return false
        case h :: t => {
          if ( prospectiveMatchTable.map(mt => if (h.contains(mt)) true else false).reduce(_ && _) ) return true
          else
            ExistingMatchTablesContainsProspectiveMatchTable(prospectiveMatchTable, t)
        }
      }
    }

    loopProspectiveMatchTables(prospectiveMatchTables, Seq.empty[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]])
  }

  def permute (matchTableListCombined : (Seq[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]],Seq[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]])) : Seq[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]] = {
    val neighbourMatchTableList = matchTableListCombined._1
    val selfMatchTableList = matchTableListCombined._2
    if (neighbourMatchTableList.isEmpty && !selfMatchTableList.isEmpty){
      selfMatchTableList
    }
    else{
      val flatMatchSetList = generateFlatMatchSetList(selfMatchTableList, neighbourMatchTableList)
      val matchTableList = generateMatchTableList(selfMatchTableList, flatMatchSetList)
      if(selfMatchTableList == null && (flatMatchSetList.size <2)){
        return matchTableList
      }
      val newmt = appendMatchTablesToMatchTableList(flatMatchSetList,matchTableList)
      newmt
    }
  }

  def appendMatchTablesToMatchTableList(flatMatchSetList: Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])], matchTableList : Seq[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]]) : Seq[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]] ={
    @tailrec
    def loopWithAccumulator(matchTableList : Seq[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]], Acc: Seq[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]]): Seq[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]] ={
      matchTableList.toList match {
        case Nil => Acc
        case h::t => {
          val newMatchTables = appendMatchSetToMatchTable(flatMatchSetList,
            h,
            Seq.empty[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]])
          val refinedMatchTables = getMatchTablesNotContainedInAcc(Acc,newMatchTables)
          if(refinedMatchTables.size > 0)
            loopWithAccumulator(t, Acc.++(refinedMatchTables)) // .distinct
          else{
            loopWithAccumulator(t, Acc)
          }
        } // structure conservation criteria 2: Add only if such a match_table doesn't already exist
      }
    }
    loopWithAccumulator(matchTableList, Seq.empty[Seq[(Int, HashMap[String, String], EdgeTriplet[Any, Any])]])
  }


  def createFlatMatchTable(patternPredicates: org.apache.spark.broadcast.Broadcast[Array[(Int, String, String, String)]], patternPredicatesRDD : RDD[(Int, String, String, String)]): RDD[(Int, HashMap[String, String], EdgeTriplet[Any,String])]={
    // matchSetList creation via a series of transformations
    // matchSetList1 aims to reduce the number of edge triplets by checking if the edge attr is contained by any of the query patterns. This is necessary as we will be doing a cartesian join later on. Smaller the number of edges to be joined, smaller the cost of joining.
    // matchSetList2 forms new tuples by joining triples from matchsetlist1 with patternPredicatesRDD and mapping them to a structure (pattern ID, pattern subject, pattern object, triplet)
    val matchSetList2 = graph.triplets.cartesian(patternPredicatesRDD).filter( tuple => tuple._1.attr == tuple._2._2).map( tuple => (tuple._2._1, tuple._2._3, tuple._2._4,  tuple._1))
    //val matchSetList2Contents = matchSetList1.collect()
    // matchSetList3 creates the mappings for each match set. Map structure is (pattern ID, (pattern Subject, triplet Src attribute), (pattern Object, triplet dest attribute), triplet)
    val matchSetList3 = matchSetList2.map(tuple => (tuple._1, ((tuple._2, tuple._4.srcAttr), (tuple._3, tuple._4.dstAttr)), tuple._4))
    // flatMatchTable is created by filtering (function validateMatchSetList) matchSetList3 to remove those tuples in which the pattern nodes are not variables and do not match the triplet nodes
    // example: Given pattern "?X <http://swat.cse.lehigh.edu/onto/univ-bench.owl#teacherOf> <http://www.Department0.University0.edu/Course0> "
    // Allow pattern node "?X" to be mapped to srcAttr "<http://www.Department0.University0.edu/FullProfessor0>"
    // Do not allow pattern node "<http://www.Department0.University0.edu/Course0>" to be mapped to "<http://www.Department0.University0.edu/Course12>"
    // This is because Course0 is an actual value and not a variable
    val matchSetList4 = validateMatchSetList(matchSetList3)
    // remove duplicate edges and convert the mapping to a hashmap which is more suitable for checking the structure conservation criteria later on
    val matchSetList5 = matchSetList4.distinct().map(tuple => (tuple._1, convertMappingToHashmap(tuple._2),tuple._3))
    matchSetList5
  }

  def convertMappingToHashmap (mapping: ((String,Any),(String,Any))): HashMap[String, String] ={
    HashMap(mapping._1._1 -> mapping._1._2.toString, mapping._2._1 -> mapping._2._2.toString)
  }

  def validateMatchSetList(MatchSetList : RDD[(Int, ((String, Any), (String, Any)), EdgeTriplet[Any, String])]): RDD[(Int, ((String, Any), (String, Any)), EdgeTriplet[Any, String])] ={
    // filters only those matchsets which have no violation in mappings.
    // Violation in mappings is caused when either/both nodes in a query pattern are actual values and not variables.
    // we need to check if these values are matches by the graph edge triplet nodes.
    MatchSetList.filter(tuple => !((!tuple._2._1._1.startsWith("?") &&
      (tuple._2._1._1 != tuple._3.srcAttr)) ||
      (!tuple._2._2._1.startsWith("?") &&
        (tuple._2._2._1 != tuple._3.dstAttr)))) // nodes not starting with '?' are actual values

  }

  def printMappings(matchTableList : RDD[(VertexId, Seq[Seq[(Int, HashMap[String,String],
    EdgeTriplet[Any,Any])]])], numberOfPatterns: Int): Unit ={
    println("Printing answers")
    val hdfs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val outputPath = new org.apache.hadoop.fs.Path(Configuration.outputPath)
    val overwrite = true
    val bw = new BufferedWriter(new OutputStreamWriter(hdfs.create(outputPath, overwrite)))
    val matchTableflattened = matchTableList.flatMap( x => x._2).filter(x => !x.isEmpty)//.distinct()
    val matchTableflattenedcontents = matchTableflattened.collect()
    var resultString: String = ""

    for(matchTable <- matchTableflattenedcontents){
      if(matchTable.size == numberOfPatterns)
        resultString = resultString + "\nComplete query"
      else
        resultString = resultString + "\nIncomplete query"
      var answerMap = scala.collection.mutable.Map[String, String]()
      var keys:String = ""
      var values:String = ""
      for(matchset <- matchTable){
        for(key <- matchset._2.keys){
          if(key.startsWith("?"))
            if(!answerMap.contains(key)){
              answerMap(key) =  matchset._2(key)
            }
        }
      }
        for(key  <- answerMap.keys){
          keys = keys + key +"\t"
            values = values + answerMap(key) + "\t"
        }
      resultString = resultString + "\n"+ keys + "\n" + values//map.toString()
    }
    Configuration.result = resultString
    bw.write(resultString)
    bw.close()
    log.info(resultString)
  }


  def terminationConditionNotMet(): Boolean ={

    Configuration.superStepCount += 1
    log.info("\n\nSuperstep count: "+ Configuration.superStepCount + "\n")
    if(Configuration.superStepCount >= Configuration.numberOfQueryPatterns){
      return false
    }
    return true
  }


}


