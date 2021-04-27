package IndexPackage
import TrajectoryPackage._
import AuxiFunPackage._
import common.QueryInfo

import scala.collection.mutable
abstract class Trie(pivotSize:Int=0) {
    import collection.mutable.ArrayBuffer
    class TrieNode(
                    val cellLabel:Int,
                    val trajProjVal:Array[Int]=Array(),
                    val trajProjValCoor:Array[Point]=Array()
                  ){
      var distProjToTraj=Array[Double]()
      var distProjToTraj_WithLabel:Array[(Double,Int)]=Array()
      var maxDist=Double.MinValue
      var minDist=Double.MaxValue
      var trajLabel:Array[Int]=Array()
      var nextLevelNode=collection.mutable.Map[Int,TrieNode]()
      var hyper_rings=Array.fill[(Double, Double)] (pivotSize)((Double.MaxValue,Double.MinValue))     //pivot optimal

    }
    class WarpTrieNode(
                        val trieNode:TrieNode

                      ){
      var cMax:Double = -1
      val col_DistResult = new ArrayBuffer[Double]()
      val row_DistResult= new ArrayBuffer[Double]()
      var distQueryTrajToBuckets=0.0
      var weight=0.0
    }
  def Init(queryTraj:Trajectory,grid: Grid):Unit={}
  def ConstructTrieFromMatric(rawTrajMat:TrajectoryMatric,grid:Grid)={
    var count=0
    val tablesMapSize=grid.tablesMap.size
    println("开始构建Trie")
    grid.tablesMap.foreach((in)=>{

      Insert(in._1.toArray,in._2,rawTrajMat,grid)
      count+=1
    })
    println("\n构建Trie结束")
  }
  def ConstructDfsTrie(allTablesMap:Array[(Vector[Int],Array[Int])])={
    var count=0
    val tablesMapSize=allTablesMap.size
    println("开始构建DfsTrie")
    allTablesMap.foreach((in)=>{

      InsertDfsTree(in._1.toArray,in._2)
      count+=1
    })
    println("\n构建DfsTrie结束")
  }
  def Insert( projVal:Array[Int],
              trajLabel:Array[Int],
              rawTrajMat:TrajectoryMatric,
              grid: Grid
            ):Unit={
    var curNode=TrieRoot
    var level=0
    val projValSize=projVal.size
    val tmpTrajProjVal=ArrayBuffer[Int]()
    while(level<projValSize){
      val curProjVal=projVal(level)
      tmpTrajProjVal+=curProjVal

      if(curNode.nextLevelNode.contains(curProjVal)==false){ //without corresponding sbuTree
        val tmpTrajProjValCoor= tmpTrajProjVal.map((in)=>{
          val curCell=grid.cellMatric(in);
          new Point(curCell.longitude,curCell.latitude);
        })
        val newTrieNode=new TrieNode(   //尝试下内存管理机制
          curProjVal,
          tmpTrajProjVal.toArray,
          tmpTrajProjValCoor.toArray
          )
        curNode.nextLevelNode+=(curProjVal->newTrieNode)
        curNode=newTrieNode
        nodeNum+=1
      }
      else {
        curNode=curNode.nextLevelNode(curProjVal)
      }
      level+=1
    }
    curNode.trajLabel=trajLabel
    subInsert_UpdateDistProjToTraj(curNode,trajLabel,rawTrajMat)
  }

  def InsertDfsTree( projVal:Array[Int],
              trajLabel:Array[Int]
            ):Unit={
    var curNode=TrieRoot
    var level=0
    val projValSize=projVal.size
    val tmpTrajProjVal=ArrayBuffer[Int]()
    while(level<projValSize){

      val curProjVal=projVal(level)
      tmpTrajProjVal+=curProjVal

      if(curNode.nextLevelNode.contains(curProjVal)==false){ //without corresponding sbuTree

        val newTrieNode=new TrieNode(   //尝试下内存管理机制
          curProjVal,
          tmpTrajProjVal.toArray,
          null
        )
        curNode.nextLevelNode+=(curProjVal->newTrieNode)
        curNode=newTrieNode
        nodeNum+=1
      }
      else {
        curNode=curNode.nextLevelNode(curProjVal)
      }
      level+=1
    }
    curNode.trajLabel=trajLabel
  }


    //更新
  def subInsert_UpdateDistProjToTraj(
                                      curNode:TrieNode,
                                      trajLabel:Array[Int],
                                      rawTrajMat:TrajectoryMatric)={
      val tmpDistProjToTraj=ArrayBuffer[Double]()
      val tmpDistProjToTraj_WithLabel:ArrayBuffer[(Double,Int)]=ArrayBuffer()
      trajLabel.foreach(label=>{
        var dist:Double=0.0
        util.Try {
           dist=measure(curNode.trajProjValCoor,rawTrajMat.trajectory(label).point);
        } match {
          case util.Success(x)=>x;
          case  util.Failure(error)=>{
            dist=measure(curNode.trajProjValCoor,rawTrajMat.trajectory(label).point);
          }
        }
        tmpDistProjToTraj+=dist
        tmpDistProjToTraj_WithLabel+=(dist->label) //(dist,label)
        curNode.maxDist=math.max(curNode.maxDist,dist)
        curNode.minDist=math.min(curNode.minDist,dist)

      })
      curNode.distProjToTraj=tmpDistProjToTraj.toArray.sorted
      curNode.distProjToTraj_WithLabel=tmpDistProjToTraj_WithLabel.toArray.sortBy(_._1)

  }
    import scala.collection.mutable.PriorityQueue

    def search(grid: Grid,
               rawTrajMat:TrajectoryMatric,
               queryTraj:Trajectory,
               knn:Int)={

      Init(queryTraj,grid)
      var collision_point=0
      var collision_buckets=0
      val priorQue=PriorityQueue[WarpTrieNode]()(Ordering.by[WarpTrieNode,Double](-_.weight))
      val diffVal=math.sqrt(2.0)/2.0*grid.gama

       //将根结点的孩子节点插入到队列
      this.TrieRoot.nextLevelNode.foreach((node=>{
        val newWarpNode=getNewWrapNode(node._2,null,grid,queryTraj,diffVal)
        priorQue.enqueue(newWarpNode)
      }))
      var sortKnnResult=mutable.PriorityQueue[MutableMyPair[Double, Int]]()
      var new_knn_dist=Double.MaxValue


      while(priorQue.nonEmpty){
        queryMiddleInfo.passNode+=1;
        val curWarpNode=priorQue.dequeue()
        val curTrieNode=curWarpNode.trieNode

        if(curTrieNode.trajLabel.size!=0){

          val dist_low_bound=getDistLowBound(curWarpNode)
          if ( dist_low_bound < new_knn_dist){
            queryMiddleInfo.cost+=curTrieNode.trajLabel.size
            sortKnnResult  = Get_Current_Knn_Dist(rawTrajMat,queryTraj,knn,curTrieNode.trajLabel,sortKnnResult)

            val sortKnnResultSize=sortKnnResult.size
            new_knn_dist=if(sortKnnResultSize<knn) Double.MaxValue
            else
              sortKnnResult.head.a
            collision_point+=curTrieNode.trajLabel.size
            collision_buckets+=1
          }
          else{
       //     println("剪枝")
          }
        }

        curTrieNode.nextLevelNode.foreach(in=>{
          val newWarpNode=getNewWrapNode(in._2,curWarpNode,grid,queryTraj,diffVal)
          if(new_knn_dist>=newWarpNode.weight ) {
            if(Is_Pivot_Filter(new_knn_dist,in._2,grid.gama))
               priorQue.enqueue(newWarpNode)
//            else
//              println("Pivot Ok")
          }
          else{
      //      println("剪枝")
          }

        })

      }

      (new ArrayBuffer()++=(sortKnnResult.toArray.sortBy(_.a)),collision_point,collision_buckets)
    }

    def Is_Pivot_Filter(range:Double,curNode:TrieNode, gama:Double):Boolean={
      val diff = math.sqrt(2) * gama
      var i=0
      curNode.hyper_rings.foreach(hyper=> {
        if ((distQueryPivot(i) - curNode.hyper_rings(i)._2 - diff > range)
          || (curNode.hyper_rings(i)._1 - distQueryPivot(i) > range))
          return false
      })
      true
    }

    def Get_Current_Knn_Dist(rawTrajMat:TrajectoryMatric,
                             queryTraj:Trajectory,
                             knn:Int,
                             collision_id:Array[Int],
                             sort_vec:mutable.PriorityQueue[MutableMyPair[Double, Int]]
                            )={
      collision_id.foreach((label)=>{
        queryMiddleInfo.caltrajLength+=rawTrajMat.trajectory(label).point.size
        val dist=measure(rawTrajMat.trajectory(label).point,queryTraj.point)
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

    def dfsSearchBuckets()={
      val res=new ArrayBuffer[Int]
      subDfsSearchBuckets(TrieRoot,res)
      res.toArray
    }
    private def subDfsSearchBuckets(curNode:TrieNode,res: ArrayBuffer[Int]):Unit={
      curNode.nextLevelNode.foreach(nextNode=>{
        subDfsSearchBuckets(nextNode._2,res)
      })
      if(curNode.trajLabel.size!=0){
        res ++= curNode.trajLabel
      }
    }

  protected def InitPivotTraj(trajMat:TrajectoryMatric)={
    import scala.util.Random
    val trajSize=trajMat.trajectory.size
    var tmpPivotTraj=List[Trajectory]()
    var repeat_flag_set =Set[Int]()
    for(i<- 0 until pivotSize){
      var flag=true
      while(flag) {
        val pivotTrajId = Random.nextInt(trajSize-1)
         if (repeat_flag_set.contains(pivotTrajId)==false) {
           flag=false
           repeat_flag_set +=pivotTrajId
           tmpPivotTraj=trajMat.trajectory(pivotTrajId)::tmpPivotTraj
         }

      }
    }
    tmpPivotTraj.reverse.toArray
  }

  def TriePivotOptimize(trajMat:TrajectoryMatric,grid:Grid):Unit= {
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


  def subTriePivotOptimize(   col_result:ArrayBuffer[Double],
                              row_result:ArrayBuffer[Double],
                              curNode:TrieNode,
                              grid:Grid,
                              pivotLabel:Int):(Double,Double)=
  {
    import math.{min,max}
    val new_dist =measurePivot(col_result, row_result, pivotTraj(pivotLabel), grid, curNode.cellLabel);//轨迹和局部桶的距离

    if (curNode.trajLabel.size != 0)			//真实桶
    {
      curNode.hyper_rings(pivotLabel) = (new_dist->new_dist)
    }
    else
    {
      curNode.hyper_rings(pivotLabel) = (Double.MaxValue->Double.MinValue)
    }
    curNode.nextLevelNode.foreach(nextNode=>{
      val newCol_DistResult =new ArrayBuffer[Double](0)
      val newRow_DistResult= new ArrayBuffer[Double](0)
      newCol_DistResult++=col_result
      newRow_DistResult++=row_result
      val distMinMax = subTriePivotOptimize(newCol_DistResult, newRow_DistResult, nextNode._2, grid,pivotLabel)
      curNode.hyper_rings(pivotLabel)=(min(distMinMax._1, curNode.hyper_rings(pivotLabel)._1),
                                       max(distMinMax._2, curNode.hyper_rings(pivotLabel)._2)   )
    })
     curNode.hyper_rings(pivotLabel)
  }

      /*点到网格最小距离取代点到点距离*/
    def measurePivot(col_DistResult:ArrayBuffer[Double],
                     row_DistResult:ArrayBuffer[Double],
                     queryTraj:Trajectory,
                     grid:Grid,
                     gridLabel:Int):Double=
    {
      Double.MaxValue
    }



    def getDistLowBound(curWarpNode:WarpTrieNode):Double
    def measure(t1:Array[Point], t2:Array[Point]):Double
    def getNewWrapNode(node:TrieNode,
                       parentWarpNode:WarpTrieNode,
                       grid: Grid,
                       queryTraj:Trajectory,
                       diffVal:Double
                      ):WarpTrieNode

    var nodeNum=0
    val TrieRoot=new TrieNode(-1)
    var pivotTraj=new Array[Trajectory](pivotSize)
    var distQueryPivot:Array[Double]=null //查询轨迹到Pivot的距离
    var queryMiddleInfo=QueryInfo()
}
