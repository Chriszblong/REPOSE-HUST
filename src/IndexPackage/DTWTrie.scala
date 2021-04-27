package IndexPackage

import AuxiFunPackage.{AuxiFunction, MutableMyPair}
import TrajectoryPackage.{Point, Trajectory, TrajectoryMatric}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class DTWTrie extends Trie{
  override def Init(queryTraj: Trajectory,grid: Grid): Unit = {
    val cellNum = grid.cellNum
    val querySize=queryTraj.point.size
    queryPointGridDist=Array.fill[Double](querySize,cellNum)(-1.0)
  }

  override def subInsert_UpdateDistProjToTraj(curNode: TrieNode, trajLabel: Array[Int],
                                              rawTrajMat: TrajectoryMatric): Unit = {
//    val tmpDistProjToTraj=ArrayBuffer[Double]()
//    val tmpDistProjToTraj_WithLabel:ArrayBuffer[(Double,Int)]=ArrayBuffer()
//    trajLabel.foreach(label=>{
//      var dist:Double=0.0
//      util.Try {
//        dist=measure(curNode.trajProjValCoor,rawTrajMat.trajectory(label).point);
//      } match {
//        case util.Success(x)=>x;
//        case  util.Failure(error)=>{
//          dist=measure(curNode.trajProjValCoor,rawTrajMat.trajectory(label).point);
//        }
//      }
//      tmpDistProjToTraj+=dist
//      tmpDistProjToTraj_WithLabel+=(dist->label) //(dist,label)
//      curNode.maxDist=math.max(curNode.maxDist,dist)
//      curNode.minDist=math.min(curNode.minDist,dist)
//    })
//    curNode.distProjToTraj=tmpDistProjToTraj.toArray.sorted
//    curNode.distProjToTraj_WithLabel=tmpDistProjToTraj_WithLabel.toArray.sortBy(_._1)
  }

  override def Get_Current_Knn_Dist(rawTrajMat: TrajectoryMatric,
                                    queryTraj: Trajectory,
                                    knn: Int,
                                    collision_id: Array[Int],
                                    sort_vec:mutable.PriorityQueue[MutableMyPair[Double, Int]]) = {
    val threshold=if(sort_vec.size>=knn) {
      sort_vec.head.a
    }
    else{
      Double.MaxValue
    }
    collision_id.foreach((label)=>{
      queryMiddleInfo.caltrajLength+=rawTrajMat.trajectory(label).point.size
      val dist=measure(rawTrajMat.trajectory(label).point,queryTraj.point,threshold)
      if(sort_vec.size<knn){
        sort_vec+=new MutableMyPair(dist,rawTrajMat.trajectory(label).id)
      }
      else if(sort_vec.head.a>dist){
        sort_vec.dequeue()
        sort_vec+=new MutableMyPair(dist,rawTrajMat.trajectory(label).id)
      }
    })
    sort_vec
  }
  override def measure(t1: Array[Point], t2: Array[Point]): Double = AuxiFunction.Cal_DTW(t1,t2)
  def measure(t1: Array[Point], t2: Array[Point],threshold:Double=Double.MaxValue): Double = AuxiFunction.Cal_DTW(t1,t2,threshold)
  override def getNewWrapNode(node:TrieNode,parentWarpNode:WarpTrieNode,
                              grid: Grid,queryTraj:Trajectory,diffVal:Double):WarpTrieNode={

    val curWarpTrieNode=new WarpTrieNode(node)//新的warp结点
    val querySize=queryTraj.point.size   //查询轨迹长度

    val weightDist=if(parentWarpNode==null){                    //父结点为空：首层
      getWeightAndDist(new ArrayBuffer[Double](querySize),curWarpTrieNode,grid,queryTraj,-1)
    }
    else
      getWeightAndDist(parentWarpNode.col_DistResult,curWarpTrieNode,grid,queryTraj,parentWarpNode.cMax)
    if(parentWarpNode!=null)
      curWarpTrieNode.cMax=math.max(parentWarpNode.cMax,weightDist._1)
    curWarpTrieNode.weight=  weightDist._1
    curWarpTrieNode.distQueryTrajToBuckets=weightDist._2
    curWarpTrieNode
  }

  def getWeightAndDist( last_ColDistResult:ArrayBuffer[Double],
                        curWrapNode:WarpTrieNode,
                        grid: Grid,
                        queryTraj:Trajectory,
                        cMax:Double): (Double,Double) = {
    import math.{max,min,sqrt}
    val curCellLable=curWrapNode.trieNode.cellLabel
    val querySize=queryTraj.point.size
    val dist_vec=new ArrayBuffer[Double]
    val col_DistResult=curWrapNode.col_DistResult
    col_DistResult ++= new Array[Double](querySize)

    /*计算距离*/
    var dist = .0
    var i = 0
    queryTraj.point.foreach(in => {
      dist = queryPointGridDist(i)(curCellLable)
      if (dist == -1.0) {
        dist = grid.minDistPoint(curCellLable, in)
        queryPointGridDist(i)(curCellLable) = dist
      }
      i+=1
      dist_vec += dist
    })

    var res_min=0.0
    /*更新当前列向量*//*更新当前列向量*/
    if (last_ColDistResult.size == 0) { //第一列
      var sumColunmn = 0.0
      var i = 0
      for(i<-0 until querySize) {
        sumColunmn += dist_vec(i)
        col_DistResult(i) = sumColunmn
      }
 //     return (col_DistResult(0),0)
      return (col_DistResult(0),col_DistResult(0))
    }
    else{
      col_DistResult(0) = last_ColDistResult(0) + dist_vec(0)
      res_min = col_DistResult(0)
      for (i <- 1 until querySize){
        col_DistResult(i) = dist_vec(i) + min(min(col_DistResult(i - 1), last_ColDistResult(i)), last_ColDistResult(i - 1))
        res_min=min(col_DistResult(i),res_min)
      }
    }
  //  (res_min,0)
   // (max(res_min,cMax),0)
    (max(res_min,cMax),col_DistResult(querySize-1))
  }


  //桶基于三角不等式滤除
  def getDistLowBound(curWarpNode:WarpTrieNode) ={
  //  curWarpNode.distQueryTrajToBuckets
    -1
  }


  private var queryPointGridDist:Array[Array[Double]]=null
}
