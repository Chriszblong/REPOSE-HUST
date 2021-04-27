package common

import java.io.FileWriter

case class QueryInfo(var passNode:Double=0.0,
                     var cost:Double=0.0,
                     var queryTime:Double=0.0,
                     var caltrajLength:Double=0.0,
                     var allTrieNodeNum:Double=0.0){
  def outputFile(filePath:String)={
    val out=new FileWriter(filePath,true)

//    out.write("passNode")
//    out.write(",")
//    out.write("cost")
//    out.write(",")
//    out.write("queryTime")
//    out.write(",")
//    out.write("caltrajLength")
//    out.write(",")
//    out.write("allTrieNodeNum")
//    out.write(",")
//    out.write("\n")
    out.write(passNode.toString)
    out.write(",")
    out.write(cost.toString)
    out.write(",")
    out.write(queryTime.toString)
    out.write(",")
    out.write(caltrajLength.toString)
    out.write(",")
    out.write(allTrieNodeNum.toString)
    out.write(",")
    out.write("\n")
    out.close()
  }
}
