package IndexPackage
import scala.collection.mutable.ArrayBuffer
import TrajectoryPackage._
import AuxiFunPackage._
import common._
abstract class TrieIndex  {
    def queryBatch(queryTrajMat:TrajectoryMatric,knn:Int): Unit ={
      val querySize=queryTrajMat.trajectory.size
      var count=0
      println("******************************")
      queryTrajMat.trajectory.foreach((queryTraj)=>{
        if(count%(querySize/30)==0)
          print("*")
        val singleQueryRes=querySingle(queryTraj,knn)
        queryRes +=singleQueryRes._1
        allCollisionPoint+=singleQueryRes._2
        allCollisionBuckets+=singleQueryRes._3

        count+=1
      })
      println("")
    }
    def getRecall(actualRes:Result,compKnn:Int)={
      var allKnn=0;
      val querySize=actualRes.result.size
      for ( i <- 0 until querySize ){
        var singleQueryKnn=0
        for(j <- 0 until compKnn){
          for(k <- 0 until compKnn){
            if(actualRes.result(i)(j).b==queryRes(i)(k).b)
              singleQueryKnn+=1
          }
        }
        if (singleQueryKnn !=100)
          println("singleQueryKnn is"+singleQueryKnn)
        allKnn+=singleQueryKnn
      }
      println("all knn is"+allKnn)
      allKnn/(querySize.toDouble)/compKnn
    }
    def getAllCollisionPoint=allCollisionPoint
    def getAllCollisionBuckets=allCollisionBuckets
    def getBatchQueryRes=queryRes
    def querySingle(queryTraj:Trajectory,knn:Int):(ArrayBuffer[MutableMyPair[Double,Int]],Int,Int)
    def constructIndex(rawTrajMat: TrajectoryMatric,gama:Double,PivotNum:Int=0):Unit
    var rawTrajMat:TrajectoryMatric=null
    val queryRes =new ArrayBuffer[ArrayBuffer[MutableMyPair[Double,Int]]]()
    var allCollisionPoint=0
    var allCollisionBuckets=0
    var grid: Grid = null
    var my_Trie: Trie = null


     var queryInfo=QueryInfo()
}
class HausdorffTrieIndex(myCfg:Config) extends TrieIndex{
  override def querySingle(queryTraj:Trajectory,knn:Int)={
    val timer=new MyTimer
    timer.restart()
    val hausdorffTrie=my_Trie.asInstanceOf[HausdorffTrie]
    val queryRes=hausdorffTrie.search(grid,rawTrajMat,queryTraj,knn)
    if(myCfg.isOutputQueryInfo){
      queryInfo.queryTime=timer.elapsed()
      queryInfo.allTrieNodeNum=hausdorffTrie.nodeNum
      queryInfo.cost=hausdorffTrie.queryMiddleInfo.cost/(rawTrajMat.trajectory.size)
      queryInfo.passNode=hausdorffTrie.queryMiddleInfo.passNode
      queryInfo.caltrajLength=hausdorffTrie.queryMiddleInfo.caltrajLength
      hausdorffTrie.queryMiddleInfo.passNode=0
      hausdorffTrie.queryMiddleInfo.caltrajLength=0
      hausdorffTrie.queryMiddleInfo.cost=0
    }
    queryRes
  }

  override def constructIndex(in: TrajectoryMatric,gama:Double,PivotNum:Int=0): Unit = {
    this.rawTrajMat=in
    grid=new HausdorffGrid(gama)
    grid.ConstructGrid(rawTrajMat)
    grid.MatricProjtoGrid(rawTrajMat)
    if(myCfg.optimalTree==1){
      println("开始优化Trie")
      grid.Convert_Optimal()
      println("结束优化Trie")
    }
    my_Trie=new HausdorffTrie(PivotNum)
    my_Trie.ConstructTrieFromMatric(rawTrajMat,grid)
    my_Trie.TriePivotOptimize(in,grid)
  }
}

class FrechetTrieIndex extends TrieIndex{
  override def querySingle(queryTraj:Trajectory,knn:Int)={
    val frechetTrie=my_Trie.asInstanceOf[FrechetTrie]
    val queryRes=frechetTrie.search(grid,rawTrajMat,queryTraj,knn)
    queryRes
  }

  override def constructIndex(in: TrajectoryMatric,gama:Double,PivotNum:Int=0): Unit = {
    this.rawTrajMat=in
    grid=new FrechetGrid(gama)
    grid.ConstructGrid(rawTrajMat)
    grid.MatricProjtoGrid(rawTrajMat)
    my_Trie=new FrechetTrie(PivotNum)
    my_Trie.ConstructTrieFromMatric(rawTrajMat,grid)
    my_Trie.TriePivotOptimize(in,grid)
  }
}
class DTWTrieIndex extends TrieIndex {
  override def querySingle(queryTraj:Trajectory,knn:Int)={
    val dtwTrie=my_Trie.asInstanceOf[DTWTrie]
    val queryRes=dtwTrie.search(grid,rawTrajMat,queryTraj,knn)
    queryRes
  }

  override def constructIndex(in: TrajectoryMatric,gama:Double,PivotNum:Int=0): Unit = {
    this.rawTrajMat=in
    grid=new DTWGrid(gama)
    grid.ConstructGrid(rawTrajMat)
    grid.MatricProjtoGrid(rawTrajMat)
    my_Trie=new DTWTrie
    my_Trie.ConstructTrieFromMatric(rawTrajMat,grid)
  }

}

class LCSSTrieIndex extends TrieIndex {
  override def querySingle(queryTraj:Trajectory,knn:Int)={
    val dtwTrie=my_Trie.asInstanceOf[LCSSTrie]
    val queryRes=dtwTrie.search(grid,rawTrajMat,queryTraj,knn)
    queryRes
  }

  override def constructIndex(in: TrajectoryMatric,gama:Double,PivotNum:Int=0): Unit = {
    this.rawTrajMat=in
    grid=new LCSSGrid(gama)
    grid.ConstructGrid(rawTrajMat)
    grid.MatricProjtoGrid(rawTrajMat)
    my_Trie=new LCSSTrie
    my_Trie.ConstructTrieFromMatric(rawTrajMat,grid)
  }

}

