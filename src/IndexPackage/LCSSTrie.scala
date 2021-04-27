package IndexPackage

import AuxiFunPackage.AuxiFunction
import TrajectoryPackage.{Point, Trajectory, TrajectoryMatric}

import scala.collection.mutable.ArrayBuffer
import scala.math.abs

class LCSSTrie extends Trie {
  override def subInsert_UpdateDistProjToTraj(curNode: TrieNode, trajLabel: Array[Int],
                                              rawTrajMat: TrajectoryMatric): Unit = {
  }
  override def measure(t1: Array[Point], t2: Array[Point]): Double = AuxiFunction.Cal_LCSS(t1,t2)

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
    curWarpTrieNode.weight=  math.max(weightDist._1 - diffVal, 0.0)
    curWarpTrieNode.distQueryTrajToBuckets=weightDist._2
    curWarpTrieNode
  }

  def getWeightAndDist( last_ColDistResult:ArrayBuffer[Double],
                        curWrapNode:WarpTrieNode,
                        grid: Grid,
                        queryTraj:Trajectory,
                        cMax:Double): (Double,Double) = {
    import math.{max,min,sqrt}
    val eps=10
    val new_point=grid.cellMatric(curWrapNode.trieNode.cellLabel)
    val curCellLable=curWrapNode.trieNode.cellLabel
    val querySize=queryTraj.point.size
    val dist_vec=new ArrayBuffer[Double]
    val col_DistResult=curWrapNode.col_DistResult
    col_DistResult ++= new Array[Double](querySize)

    var res_min=0.0
    /*更新当前列向量*//*更新当前列向量*/
    if (last_ColDistResult.size == 0) { //第一列
      for(i<-0 until querySize) {
        if(abs(queryTraj.point(i).longitude - new_point.longitude)<eps &&
          abs(queryTraj.point(i).latitude - new_point.latitude)<eps)
          col_DistResult(i)=1
        else if(i!=0)
          col_DistResult(i)=col_DistResult(i-1)
        else
          col_DistResult(i)=0
        res_min=min(res_min,col_DistResult(i))
      }
      return (res_min,0)
    }
    else{
      if(abs(queryTraj.point(0).longitude - new_point.longitude)<eps &&
        abs(queryTraj.point(0).latitude - new_point.latitude)<eps)
        col_DistResult(0) = 1
      else
        col_DistResult(0) = last_ColDistResult(0)
      res_min = col_DistResult(0)
      for(i<-1 until querySize) {
        if(abs(queryTraj.point(i).longitude - new_point.longitude)<eps &&
          abs(queryTraj.point(i).latitude - new_point.latitude)<eps)
          col_DistResult(i)=1+last_ColDistResult(i-1)
        else
          col_DistResult(i)=max(last_ColDistResult(i),col_DistResult(i-1))
        res_min=min(res_min,col_DistResult(i))
      }
    }
    (res_min,0)
  }
  //桶基于三角不等式滤除
  def getDistLowBound(curWarpNode:WarpTrieNode) ={
    -1.0
  }


  private var queryPointGridDist:Array[Array[Double]]=null
}
