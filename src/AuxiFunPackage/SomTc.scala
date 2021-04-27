package AuxiFunPackage

import scala.collection.mutable.ArrayBuffer

case class SomTc(){
  var finalClusters:Array[Cluster]=null
  def iteratorCluster(refTrajAndTrajIds:Array[(Vector[Int],Array[Int])], clusterNum:Int, cellNumOneRaw:Int): Unit ={
    val clusters=refTrajAndTrajIds.map(x=>{Cluster(x._1,x._2)})
    var newClusters:Array[Cluster]=clusters
    val rollUpOperator=RollUp(cellNumOneRaw)
    terminalFlag = false
    if(clusters.length<=clusterNum){
      terminalFlag=true
    }
    while (!isTerminal()) {
      rollUpOperator.rollUp(clusters)
      newClusters=updateClusters(clusters,clusterNum)
      if(rollUpOperator.isCanRollUp()){
        rollUpOperator.update(cellNumOneRaw/2)
      }
      else {
        terminalFlag = true
      }
    }
    finalClusters=newClusters

  }
  def getCluster()={
    finalClusters
  }
  private def updateClusters(clusters:Array[Cluster],clusterNum:Int):Array[Cluster]={
    val representMapClusters=collection.mutable.Map[Vector[Int],Cluster]()
    var curCluster:Cluster=null
    var haveVisit =0
    for(i<- clusters.indices if !isTerminal()){
      val curCluster=clusters(i)
      if(representMapClusters.contains(curCluster.represent)) {
        representMapClusters(curCluster.represent).mergeCluster(curCluster)
      }
      else{
        val newCluster=Cluster(curCluster.represent,Array[Int]())
        newCluster.mergeCluster(curCluster)
        representMapClusters+=(newCluster.represent->newCluster)
      }
      haveVisit=i
      if(representMapClusters.size+clusters.length-i-1<=clusterNum){
        terminalFlag=true
      }
    }

    var res=representMapClusters.values.toArray
    res++=clusters.zipWithIndex.filter(_._2>haveVisit).map(_._1)
    assert(res.length>=clusterNum,"res.length<clusterNum in updateClusters")
    res
  }

  private def isTerminal(): Boolean ={
     terminalFlag
  }
  private var terminalFlag=false
}

case class Cluster(var represent:Vector[Int],var data:Array[Int]){
  var size:Int =represent.size
  val mergeClusters=new ArrayBuffer[Cluster](0)
  def mergeCluster(other:Cluster)={
    mergeClusters+=other
    size+=other.size
  }
}

case class RollUp(var cellNumOneRaw:Int){
  def rollUp(clusters:Array[Cluster]): Unit ={
    clusters.foreach(x=>{
      x.represent=x.represent.map(x=>{
        getNewGridVal(x)
      })
    })
  }

  def isCanRollUp()={
    cellNumOneRaw>=2
  }

  def getNewGridVal(value:Int): Int ={
     (value/(cellNumOneRaw*2))*cellNumOneRaw/2+(value%cellNumOneRaw)/2
  }

  def update(newGridLen:Int): Unit ={
    cellNumOneRaw=newGridLen
  }
}
