

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
        /*if(i <= args.length) {
          var n :Int = args(i).toInt + i// number of graph files
          var j = i+1
          while ((j <= n) && (j<= args.length)) { // add all the graph files to Configuration.graphPath
            Configuration.graphPath += args(j)
            j += 1
          }
          i += n // crossing the entire list of graph files
        }*/
      }

      // adding the queries directory
      if ((args(i).equals("-queryPath"))|| (args(i).equals("-q"))){
        i += 1
        Configuration.queryPath = args(i)
        /*if(i <= args.length) {
          var n :Int = args(i).toInt + i// number of query files
          var j = i+1
          while ((j <= n) && (j<= args.length)) { // add all the graph files to Configuration.graphPath
            Configuration.queryPath += args(j)
            j += 1
          }
          i += n // crossing the entire list of graph files
        }*/
      }

      i += 1
    }

  }

}
