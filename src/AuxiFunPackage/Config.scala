package AuxiFunPackage

case class Config(gama:Double,var knn:Int,partitionNum:Int,repeatNum:Int=1,pivotNum:Int=0) {
  val isOutputQueryInfo=true;
  var dataset="SF"
  var measure="Hausdorff"    //Hausdorff  Frechet DTW
  private var datasetFilePath="/home/wlg/DataSet/"
  var rawDatasetFilePath = datasetFilePath+"rawData_SF.txt"
 // var targetDatasetFilePath="/home/wlg/DataSet/rawData_DITA.txt"
  // val targetDatasetFilePath = datasetFilePath+"/query.txt"
  var queryDatasetFilePath =  datasetFilePath+"queryData_SF.txt"

  var knnResultFIlePath =  "/home/wlg/IdeaProjects/TrajecorySearch/Result/"+measure+"/"+dataset+"/knn_result.txt"
  var queryResultFilePath= "/home/wlg/IdeaProjects/TrajecorySearch/Result/"+measure+"/"+dataset+"/result.txt"
  var isGlobalTrajRange=false
  var globalTrajRange=new Array[Double](4) //minLong,maxLong,minLati,maxLati

  var optimalTree=0
  //全局变量
  var indexTime=0.0
  var NodeNum=0
}

