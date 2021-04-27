package IndexPackage
import scala.util.Random
import AuxiFunPackage._
import org.apache.spark.Partitioner
import RDD._
import TrajectoryPackage._
import org.apache.hadoop.classification.InterfaceAudience.Public
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer


case class GlobalTrieIndexBySequence(allTablesMap:Array[(Vector[Int],Array[Int])],numParts:Int)  extends Partitioner{
  override def numPartitions: Int = numParts
  override def getPartition(key: Any): Int = { IdMapPartition(key.toString.toInt)}

  def partitionRDD(myCfg:Config)={
    var trajNum=0
    allTablesMap.foreach(x=>{
      trajNum+=x._2.size
    })
    val Num=math.ceil(trajNum/(numParts.toDouble)).toInt
    IdMapPartition=new Array[Int](trajNum)
    IdMapPartition=IdMapPartition.zipWithIndex.map(x=>math.floor(x._2/Num).toInt)
  }
  var IdMapPartition:Array[Int]=null
}

class GlobalTrieIndexBySomTCWithDisSimilar(allTablesMap:Array[(Vector[Int],Array[Int])],numParts:Int)
      extends GlobalTrieIndexBySequence(allTablesMap,numParts){
  override def numPartitions: Int = numParts
  override def getPartition(key: Any): Int = {
    if(IdMapPartition(key.toString.toInt)==(-1)){
      assert(false,"-1 in getPartition")
    }
    IdMapPartition(key.toString.toInt)
  }

  override def partitionRDD(myCfg:Config)={
    myCfg.isGlobalTrajRange=true
    var trajNum=0
    allTablesMap.foreach(trajNum+=_._2.length)
    val somTc=new SomTc
    val clusterNum=getClusterNum(trajNum,myCfg.partitionNum)
    val grid=new HausdorffGrid(myCfg.gama)
    grid.ConstructGrid(myCfg.globalTrajRange)
    val cellOneRaw=grid.getLongCellNum
    val cellOneCol=grid.getLatCellNum
    assert(cellOneRaw==cellOneCol)

    somTc.iteratorCluster(allTablesMap,clusterNum,cellOneRaw)
    val trajIdFromCluster=getTrajIdFromCluster(somTc)

    assert(trajIdFromCluster.toSet.size==trajNum)
    assert(trajIdFromCluster.length ==trajNum)
    assign(trajIdFromCluster)

  }

  protected def getTrajIdFromCluster(somTc:SomTc)={
    val trajId:ArrayBuffer[Int]=new ArrayBuffer[Int](0)
    somTc.getCluster().foreach(cluster=>{
      findTrajIdByDfs(trajId,cluster)
    })
    trajId.toArray
  }

  protected def findTrajIdByDfs(trajId:ArrayBuffer[Int],cluster:Cluster):Unit={
      if(cluster.mergeClusters.size==0){
        trajId++=cluster.data
        return Unit
      }
    cluster.mergeClusters.foreach(c=>{
      findTrajIdByDfs(trajId,c)
    })
  }

  protected def assign(trajId:Array[Int])={
    IdMapPartition=Array.fill[Int](trajId.length)(-1)
    var i=0;
    trajId.zipWithIndex.foreach(x=>{
      IdMapPartition(x._1)=i
      i+=1
      if(i==numPartitions)  i=0
    })
  }
  protected def getClusterNum(trajNum:Int,partitionNum:Int)={
      trajNum/partitionNum
  }
}

class GlobalTrieIndexBySomTCWithSimilar(allTablesMap:Array[(Vector[Int],Array[Int])],numParts:Int)
            extends GlobalTrieIndexBySomTCWithDisSimilar(allTablesMap,numParts){
  override protected def assign(trajId: Array[Int]): Unit = {
    IdMapPartition=Array.fill[Int](trajId.length)(-1)
    val gap=math.ceil(trajId.size/(numPartitions.toDouble))
    trajId.zipWithIndex.foreach(x=>{
      IdMapPartition(x._1)=math.floor(x._2/gap).toInt
    })
  }
}

