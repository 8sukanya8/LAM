

/*This class parses the arguments and assigns values to config parameters
*
*  @author Sukanya Nath
*
*/
class ArgumentParser() {

  def parseInput(args: Array[String]): Unit ={
    var i = 0
    //if(args.length < )
    while (i< args.length){
      // adding the list of files for graphs
      // change this to only directory
      if ((args(i).equals("-graphPath"))|| (args(i).equals("-g"))) {
        i += 1
        Configuration.graphPath = args(i)
      }

      // adding the queries directory
      if ((args(i).equals("-queryPath"))|| (args(i).equals("-q"))){
        i += 1
        Configuration.queryPath = args(i)
      }

      if ((args(i).equals("-outputPath"))|| (args(i).equals("-o"))){
        i += 1
        Configuration.outputPath = args(i)
      }
      if ((args(i).equals("-parallelism"))|| (args(i).equals("-p"))){
        i += 1
        Configuration.parallelism = args(i).toInt
      }
      if ((args(i).equals("-master"))|| (args(i).equals("-m"))){
        i += 1
        if(args(i).equals("local")){
          Configuration.master = "local[*]"
        }
      }
      i += 1
    }

  }

}
