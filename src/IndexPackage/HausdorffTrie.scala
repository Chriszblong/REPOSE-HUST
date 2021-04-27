package IndexPackage

import collection.mutable.ArrayBuffer
import TrajectoryPackage._
import AuxiFunPackage._

class HausdorffTrie(pivotSize:Int=0) extends Trie(pivotSize){
  override def measure(t1: Array[Point], t2: Array[Point]): Double = AuxiFunction.Cal_Hausdorff(t1,t2)
  override def getNewWrapNode(node:TrieNode,parentWarpNode:WarpTrieNode,
                              grid: Grid,queryTraj:Trajectory,diffVal:Double):WarpTrieNode={

    val curWarpTrieNode=new WarpTrieNode(node)//新的warp结点
    val querySize=queryTraj.point.size   //查询轨迹长度

    val weightDist=if(parentWarpNode==null){                    //父结点为空：首层
      curWarpTrieNode.col_DistResult ++=Array.fill[Double](querySize)(Double.MaxValue)
      getWeightAndDist(curWarpTrieNode,grid,queryTraj)
    }
    else {
      curWarpTrieNode.col_DistResult ++=parentWarpNode.col_DistResult
      curWarpTrieNode.row_DistResult ++=parentWarpNode.row_DistResult
      getWeightAndDist(curWarpTrieNode,grid,queryTraj)
    }
    curWarpTrieNode.weight=  math.max(weightDist._1 - diffVal, 0.0)
    curWarpTrieNode.distQueryTrajToBuckets=weightDist._2
    curWarpTrieNode
  }

  def getWeightAndDist( curWrapNode:WarpTrieNode,
                        grid: Grid,
                        queryTraj:Trajectory): (Double,Double) =
  {
    import math.{max,min,sqrt}

    val new_point=grid.cellMatric(curWrapNode.trieNode.cellLabel)
    val col_DistResult=curWrapNode.col_DistResult
    val row_DistResult=curWrapNode.row_DistResult
    var dist = .0
    var row_min = Double.MaxValue
    val querySize=queryTraj.point.size

    var i=0;
    queryTraj.point.foreach(in=>{
      dist = sqrt((in.longitude - new_point.longitude) * (in.longitude - new_point.longitude)
        + (in.latitude - new_point.latitude) * (in.latitude - new_point.latitude))
      if (dist < col_DistResult(i)) col_DistResult(i) = dist
      if (dist < row_min) row_min = dist
      i+=1
    })

    row_DistResult+=(row_min)
    var row_dist_max = Double.MinValue
    var col_dist_max = Double.MinValue

    col_DistResult.foreach(in=> col_dist_max=max(in,col_dist_max))
    row_DistResult.foreach(in=>row_dist_max=max(in,row_dist_max))
    (row_dist_max,max(row_dist_max,col_dist_max))
  }
  def getDistLowBound(curWarpNode:WarpTrieNode) ={
    import math.{abs,min,max}

    val curTrieNode=curWarpNode.trieNode
    val query_to_buckets_dist=curWarpNode.distQueryTrajToBuckets
    val minDistProjToTraj=curTrieNode.distProjToTraj(0)
    val maxDistProjToTraj=curTrieNode.distProjToTraj(curTrieNode.distProjToTraj.size-1)

    val dist_low_bound=if(query_to_buckets_dist<=minDistProjToTraj){
      math.abs(query_to_buckets_dist-minDistProjToTraj)
    }else if(query_to_buckets_dist >= maxDistProjToTraj){
      math.abs(query_to_buckets_dist-maxDistProjToTraj)
    }else{
      val range_pos = AuxiFunction.Binary_range_search(curTrieNode.distProjToTraj, query_to_buckets_dist)
      min(abs(query_to_buckets_dist - curTrieNode.distProjToTraj(range_pos)),
        abs(query_to_buckets_dist - curTrieNode.distProjToTraj(range_pos+1)))

      min(abs(query_to_buckets_dist - curTrieNode.distProjToTraj(range_pos)),
        abs(query_to_buckets_dist - curTrieNode.distProjToTraj(range_pos+1)))
    }
    dist_low_bound
  }

  override def Init(queryTraj: Trajectory, grid: Grid): Unit = {
    val pivotTrajSize=pivotTraj.size
    val tmpDistQueryPivot=new Array[Double](pivotTrajSize)
    var i=0
    pivotTraj.foreach(traj=>{
      tmpDistQueryPivot(i)=measure(traj.point,queryTraj.point)
      i+=1
    })
    distQueryPivot=tmpDistQueryPivot
  }

  override def TriePivotOptimize(trajMat:TrajectoryMatric,grid:Grid):Unit= {
    pivotTraj=InitPivotTraj(trajMat)
    val timer = new MyTimer
    timer.restart()
    println("开始构建Pivot索引")
    pivotTraj.zipWithIndex.foreach(x=>{
      val traj=x._1
      val i=x._2
      val col_DistResult =new ArrayBuffer[Double](0)
      val row_DistResult= new ArrayBuffer[Double](0)
      col_DistResult   ++=Array.fill[Double](traj.point.size)(Double.MaxValue)

      TrieRoot.nextLevelNode.foreach(nextNode=>{
        val newCol_DistResult =new ArrayBuffer[Double](0)
        val newRow_DistResult= new ArrayBuffer[Double](0)
        newCol_DistResult++=col_DistResult
        newRow_DistResult++=row_DistResult
        subTriePivotOptimize(newCol_DistResult, newRow_DistResult, nextNode._2, grid, i);
      })
    })
    println("构建Pivot索引完成,耗时"+timer.elapsed()+"ms")
  }

  override def measurePivot(col_DistResult:ArrayBuffer[Double],
                            row_DistResult:ArrayBuffer[Double],
                            queryTraj:Trajectory,
                            grid:Grid,
                            gridLabel:Int):Double=
  {
    import math.{max,min,sqrt}
    var dist = .0
    var row_min = Double.MaxValue

    var i=0;
    queryTraj.point.foreach(in=>{
      dist = grid.minDistPoint(gridLabel,in)
      if (dist < col_DistResult(i)) col_DistResult(i) = dist
      if (dist < row_min) row_min = dist
      i+=1
    })

    row_DistResult+=(row_min)
    var row_dist_max = Double.MinValue
    var col_dist_max = Double.MinValue

    col_DistResult.foreach(in=> col_dist_max=max(in,col_dist_max))
    row_DistResult.foreach(in=>row_dist_max=max(in,row_dist_max))
    max(row_dist_max,col_dist_max)
  }
}
