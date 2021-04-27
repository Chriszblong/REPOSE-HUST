package IndexPackage
import collection.mutable.ArrayBuffer
import TrajectoryPackage._
import AuxiFunPackage._

class FrechetTrie(pivotSize:Int) extends Trie(pivotSize) {
  override def measure(t1: Array[Point], t2: Array[Point]): Double = AuxiFunction.Cal_Frechet(t1,t2)

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
  def getWeightAndDist( last_frechet_vector:ArrayBuffer[Double],
                        curWrapNode:WarpTrieNode,
                       grid: Grid,
                       queryTraj:Trajectory,
                        cMax:Double): (Double,Double) =
  {
    import math.{max,min,sqrt}
    val new_point=grid.cellMatric(curWrapNode.trieNode.cellLabel)
    val new_frechet_vector=curWrapNode.col_DistResult
    val querySize=queryTraj.point.size
    var res_min = .0
    var col_max_dist = Double.MinValue
    val dist_vec=new ArrayBuffer[Double]
    new_frechet_vector ++=new Array[Double](querySize)
    /*计算新的一列点与点距离*/
    queryTraj.point.foreach(in=>{
      dist_vec+=math.sqrt((in.longitude-new_point.longitude)*(in.longitude-new_point.longitude)+
        (in.latitude-new_point.latitude)*(in.latitude-new_point.latitude))
    })
    /*更新Frechet向量*/
    if (last_frechet_vector.size == 0) {		//第一列
      for(i<- 0 until(querySize)){
        new_frechet_vector(i)=if(col_max_dist > dist_vec(i)) col_max_dist
        else {
          col_max_dist= dist_vec(i)
          col_max_dist
        }
      }
      return (new_frechet_vector(0),new_frechet_vector(0))
    }
    else{
      new_frechet_vector(0)=math.max(last_frechet_vector(0),dist_vec(0))
      res_min=new_frechet_vector(0)
      for(i<- 1 until querySize){
        new_frechet_vector(i)=max(dist_vec(i), min(min(new_frechet_vector(i - 1),
                    last_frechet_vector(i)), last_frechet_vector(i - 1)))
        if (res_min > new_frechet_vector(i))
          res_min = new_frechet_vector(i);
      }
    }
    (max(res_min,cMax),new_frechet_vector(querySize-1))
    //((res_min),new_frechet_vector(querySize-1))
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

  override def TriePivotOptimize(trajMat:TrajectoryMatric,grid:Grid):Unit= {
    pivotTraj=InitPivotTraj(trajMat)
    val timer = new MyTimer
    timer.restart()
    println("开始构建Pivot索引")
    pivotTraj.zipWithIndex.foreach(x=>{
      val traj=x._1
      val i=x._2
      TrieRoot.nextLevelNode.foreach(nextNode=>{
        val newCol_DistResult =new ArrayBuffer[Double](0)
        val newRow_DistResult= new ArrayBuffer[Double](0)
        subTriePivotOptimize(newCol_DistResult, newRow_DistResult, nextNode._2, grid, i);
      })
    })
    println("构建Pivot索引完成,耗时"+timer.elapsed()+"ms")
  }

  override def measurePivot(last_frechet_vector:ArrayBuffer[Double],
                            row_DistResult:ArrayBuffer[Double],
                            queryTraj:Trajectory,
                            grid:Grid,
                            gridLabel:Int):Double=
  {
    import math.{max,min,sqrt}

    val new_frechet_vector=new ArrayBuffer[Double](0)
    val querySize=queryTraj.point.size
    var res_min = .0
    var col_max_dist = Double.MinValue
    val dist_vec=new ArrayBuffer[Double]
    new_frechet_vector ++=new Array[Double](querySize)
    /*计算新的一列点与点距离*/
    queryTraj.point.foreach(in=>{
      dist_vec+= grid.minDistPoint(gridLabel,in)
    })
    /*更新Frechet向量*/
    if (last_frechet_vector.size == 0) {		//第一列
      for(i<- 0 until(querySize)){
        new_frechet_vector(i)=if(col_max_dist > dist_vec(i)) col_max_dist
        else {
          col_max_dist= dist_vec(i)
          col_max_dist
        }
      }
      last_frechet_vector ++=new_frechet_vector
      return (new_frechet_vector(0))
    }
    else{
      new_frechet_vector(0)=math.max(last_frechet_vector(0),dist_vec(0))
      res_min=new_frechet_vector(0)
      for(i<- 1 until querySize){
        new_frechet_vector(i)=max(dist_vec(i), min(min(new_frechet_vector(i - 1),
          last_frechet_vector(i)), last_frechet_vector(i - 1)))
        if (res_min > new_frechet_vector(i))
          res_min = new_frechet_vector(i);
      }
    }

    var t=0
    last_frechet_vector.map(x=>{new_frechet_vector(t);t+=1})
    (new_frechet_vector(querySize-1))
  }

}
