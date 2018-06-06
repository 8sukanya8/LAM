

object Configuration  {
  //val graphPath = trailArg[String]()
  //val graphPathList = trailArg[List[Int]]()
  //var graphPath = ArrayBuffer[String]()
  var graphPath: String = ""
  //var graphPath: String = "/user/sukanya/University0_0.nt"
  var queryPath: String = ""
  //var queryPath: String = "/user/sukanya/Queries"
  var outputPath: String = ""//"/user/sukanya/out"
  //var outputPath: String = "/Users/sukanyanath/Desktop/output.txt"
  var superStepCount: Int = 0
  var numberOfQueryPatterns: Int = 0
  val maxAllowedCombinations = 2
  var parallelism = 1
  var master:String = null

  //https://www.safaribooksonline.com/library/view/scala-cookbook/9781449340292/ch11s09.html

}
