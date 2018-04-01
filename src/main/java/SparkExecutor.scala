import java.io.File

import org.apache.jena.query.QueryFactory
import org.apache.spark.sql.SparkSession

import scala.io.Source
//import org.apache.spark.sql.SparkSession
//import net.sansa_stack.rdf.spark.model.{JenaSparkRDDOps, TripleRDD}


object SparkExecutor {

  private var spark:SparkSession = null
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
    val filename = Configuration.graphPath(0)
    //print("Loading graph from file"+ filename)

    val graph = RDFGraph.createGraph(spark)

    //val lines = sparkContext.textFile(filename).map(_.split(" "))
    //val linesContents = lines.collect()
    //println(linesContents)

    //val lang = Lang.TTL
    //val triples = spark.rdf(lang)(filename) //NTripleReader.load(sparkContext, URI.create(filename)) //
    //val triplesContents = triples.collect()
    //triples.take(5).foreach(println(_))

    //val graph = LoadGraph(triples)
    //val nodesCollect = graph.vertices.collect()
    //val edgescollect = graph.edges.collect()
    //val x = 4

    // is lines automatically parallelised?
    // how to parallelize ?
    // how to parse ttl file? solved using library sansa
    // how to load it in GraphX? solved using library sansa
    // how to load multiple files and make one graph? solved using library sansa and rdf3rdf application

  }
  def loadQuery(): Unit ={
    val directoryPath = Configuration.queryPath
    val dir = new File(directoryPath)
    if (dir.exists && dir.isDirectory) {
      val fileList = dir.listFiles()
      var i = 0
      while(i< fileList.length){
        //val queryLines = sparkContext.textFile(fileList(i).toString)
        val queryLines = Source.fromFile(fileList(i)).mkString
        val q = QueryFactory.create(queryLines)
        val patterns = q.getQueryPattern
        println(q)
        i += 1
      }


    }

  }

  // set all configuration parameters
  // https://spark.apache.org/docs/2.2.0/configuration.html
}
