package common
import AuxiFunPackage.AuxiFunction
import scala.util.Random
import TrajectoryPackage.{Point, _}
import math.{sqrt,min,max}

class kMeans(trajs:TrajectoryMatric, k:Int,measure:(Array[Point], Array[Point])=>Double) {
  private def Init()={
    val size=trajs.trajectory.size
    val k_MeansCentroid=new Array[(Int)](k)  //中心轨迹标号
    k_MeansCentroid.map(x=>{
      var flag=true
      var next=0
      while(flag) {
        next= Random.nextInt(size)
        if (k_MeansCentroid.exists(_==next)==false)
          flag=false
      }
      next
    })
  }
  def iterate(count:Int)={
    var k_MeansCentroid=Init()
    for(i<- 0 until count){
      println(s"第${i}次:")
      val newCluster=reCluster(k_MeansCentroid)
      k_MeansCentroid=UpdateCentroid(newCluster)
    }
  }

//  def measure(t1: Array[Point], t2: Array[Point]): Double = AuxiFunction.Cal_Frechet(t1,t2)

  private def reCluster(k_MeansCentroid:Array[(Int)])={
    val res=trajs.trajectory.map(x=>{
      var minlabel=0
      var minDist=Double.MaxValue
      k_MeansCentroid.foreach(label=>{
        val dist=measure(trajs.trajectory(label).point,x.point)
        if(dist<minDist) {
          minDist=dist
          minlabel=label
        }
      })
      (x.id,minlabel,minDist)
    })
    res.groupBy(_._2).map(x=>(x._1,x._2.map(y=>(y._1,y._3))) ) //中心号,(轨迹标号,轨迹到中心的距离)
  }

  private def UpdateCentroid(clusters:Map[Int,Array[(Int,Double)]] )={
      var cluster_i=0
      val newClusters =clusters.map(clus=>{
        val clusterTraj=clus._2
        var minErr=Double.MaxValue
        var newCentroid=0
        for(i<- 0 until clusterTraj.size){
          var err=0.0
          for(j<- 0 until clusterTraj.size){
            err+=measure(trajs.trajectory(clusterTraj(i)._1).point,
                      trajs.trajectory(clusterTraj(j)._1).point)
          }
          if(err<minErr){
            minErr=err
            newCentroid=i
          }
        }
        println(s"第${cluster_i}个类的平均距离为${minErr/clusterTraj.size}")
        cluster_i+=1
        (newCentroid,minErr/clusterTraj.size)
      }).toArray
    println(s"所有类平均误差为${newClusters.reduceLeft((a,b)=>(0,a._2+b._2))._2/k}")
    newClusters.map(_._1)
  }
}


