package AuxiFunPackage
import IndexPackage._
import TrajectoryPackage._

import math.{max, min, pow, sqrt}
import scala.collection.mutable.ArrayBuffer
object AuxiFunction {
  /*给定一个数据，和一个搜索值find_val，找到find_val所处的buff序号，
 *	buff[i]<=find_val<buff[i+1]
 *	return i
 *   find_val>=buff[end]  return end-1
 *   find_val<buff[0]   return -1
 备注：buff是从小到大排序，且buff[0]<=find_val
 */
  def Binary_range_search(buff:Array[Double], find_val:Double):Int={
    val size=buff.size
    if (find_val >= buff(size-1))
      return size - 2;
    var left = 0
    var right = size - 1;
    var mid:Int=0
    var res:Int=0

    import scala.util.control.Breaks._
    var breakFlag=false
    while ((right - left) > 1&&breakFlag==false)
    {
      mid = (right + left) / 2;
      if (buff(mid) < find_val)
        left = mid;
      else if (buff(mid) > find_val)
        right = mid;
      else
        breakFlag=true;
    }
    if ((right - left) > 1)
      res = mid;//找到了相同点
    else
      res = left;
    res;
  }
  def Cal_Frechet(t1:Array[Point], t2:Array[Point])={

    val n = t1.size
    val m = t2.size
    val distMatrix=Array.fill[Double](n,m)(-1)
    var i = 0
    while ( i < n) {
      var j = 0
      while (j < m) {
        val dist = math.sqrt(math.pow(t1(i).longitude - t2(j).longitude, 2) + math.pow(t1(i).latitude - t2(j).latitude, 2))
        if ((i == 0) && j == 0) distMatrix(i)(j) = dist
        else if (i == 0) distMatrix(i)(j) = math.max(distMatrix(i)(j - 1), dist)
        else if (j == 0) distMatrix(i)(j) = math.max(distMatrix(i - 1)(j), dist)
        else distMatrix(i)(j) = max(min(min(distMatrix(i - 1)(j), distMatrix(i)(j - 1)), distMatrix(i - 1)(j - 1)), dist)
        j += 1
      }
      i += 1
    }
    distMatrix(n - 1)(m - 1)
  }
  def Cal_Hausdorff(t1:Array[Point], t2:Array[Point])={
    val t1_s = t1.size
    val t2_s = t2.size
    val distMatrix=Array.fill[Double](t1_s+1,t2_s+1)(-1)

    for (i <-0 until t1_s)
    {
      distMatrix(i)(t2_s) = Int.MaxValue;
      for ( j <- 0 until t2_s)
      {
        if (i == 0)
          distMatrix(t1_s)(j) = Int.MaxValue;

        distMatrix(i)(j) = sqrt(pow((t1(i).longitude - t2(j).longitude), 2) + pow((t1(i).latitude - t2(j).latitude), 2));
        if (distMatrix(i)(j) < distMatrix(i)(t2_s))
          distMatrix(i)(t2_s) = distMatrix(i)(j);

        if (distMatrix(i)(j) < distMatrix(t1_s)(j))
          distMatrix(t1_s)(j) =distMatrix(i)(j);
      }
    }
    var max_dis = distMatrix(0)(t2_s);
    for (i <- 0 until t1_s)
      max_dis=max(max_dis,distMatrix(i)(t2_s))
    for (j <- 0 until t2_s)
      max_dis=max(max_dis,distMatrix(t1_s)(j))

    max_dis;
  }
  def Cal_DTW(t1:Array[Point], t2:Array[Point],threshold:Double=Double.MaxValue):Double={
    val n = t1.size
    val m = t2.size
    val distMatrix=Array.fill[Double](n+1,m+1)(-1)
    distMatrix(0)(0) = 0;
    for (i <- 1 to m ) {
      distMatrix(0)(i) =  Int.MaxValue
   }
    for (i <- 1 to n ) {
      distMatrix(i)(0) =  Int.MaxValue;
    }
    for (i <- 1 to n ) {
      var myMinDist = Double.MaxValue;
    for (j <- 1 to m) {
      var tmp1=0
      var tmp2=0
      if (distMatrix(i - 1)(j - 1) < distMatrix(i - 1)(j)) {
        tmp1 = i - 1; tmp2 = j - 1;
      }
      else {
        tmp1 = i - 1; tmp2 = j;
      }
      if (distMatrix(tmp1)(tmp2) > distMatrix(i)(j - 1)) {
        tmp1 = i; tmp2 = j - 1;
      }
      if(distMatrix(tmp1)(tmp2)<threshold){
        distMatrix(i)(j) = sqrt(pow((t1(i - 1).longitude - t2(j - 1).longitude), 2)
          + pow((t1(i - 1).latitude - t2(j - 1).latitude), 2)) + +distMatrix(tmp1)(tmp2);
      }
      else{
        distMatrix(i)(j)=Double.MaxValue;
      }
      myMinDist = min(myMinDist, distMatrix(i)(j))
    }
      if (myMinDist >= threshold)
      {
        return Double.MaxValue
      }
  }
    distMatrix(n)(m);
  }

  def Cal_LCSS(t1:Array[Point], t2:Array[Point],eps:Double=10,del:Double=100000):Double= {
    import math.{abs, max}

    val n = t1.size
    val m = t2.size
    val distMatrix = Array.fill[Double](n + 1, m + 1)(-1)

    distMatrix(0)(0) = 0;
    for (i <- 1 to m) {
      distMatrix(0)(i) = 0
    }
    for (i <- 1 to n) {
      distMatrix(i)(0) = 0
    }

    for (i <- 1 to n) {
      for (j <- 1 to m) {
        if (abs(t1(i - 1).longitude - t2(j - 1).longitude) < eps &&
          abs(t1(i - 1).latitude - t2(j - 1).latitude) < eps &&
          abs(i - j) <= del)
          distMatrix(i)(j) = 1 + distMatrix(i - 1)(j - 1)
        else
          distMatrix(i)(j) = max(distMatrix(i - 1)(j), distMatrix(i)(j - 1))
      }
    }
    distMatrix(n)(m);
  }


  


  def getTrieIndex(myCfg:Config)={
    val myIndex=myCfg.measure match {
      case "Frechet" =>new FrechetTrieIndex
      case "Hausdorff"=> new HausdorffTrieIndex(myCfg)
      case "DTW"=>new DTWTrieIndex
      case "LCSS"=>new LCSSTrieIndex
      //case "DTW"=>  new DTWTrieIndex
      case _ =>{println("索引类型不支持");new DTWTrieIndex}
    }
    myIndex
  }
  def convertTrieIndex(myCfg:Config,index:TrieIndex)={
    val myIndex=myCfg.measure match {
      case "Frechet" => index.asInstanceOf[FrechetTrieIndex]
      case "Hausdorff"=>index.asInstanceOf[HausdorffTrieIndex]
      case "DTW"=>index.asInstanceOf[DTWTrieIndex]
      case "LCSS"=>index.asInstanceOf[LCSSTrieIndex]
      case _ =>{println("索引类型不支持");index.asInstanceOf[FrechetTrieIndex]}
    }
    myIndex
  }

  def getRecall(actualRes:Result,queryRes:Array[Array[MutableMyPair[Double,Int]]],compKnn:Int)={
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
      if(queryRes(i)(compKnn-1).a-actualRes.result(i)(compKnn-1).a<0.0001)
        singleQueryKnn=compKnn
      if (singleQueryKnn !=compKnn)
        println("singleQueryKnn is"+singleQueryKnn)
      allKnn+=singleQueryKnn
    }
    println("all knn is"+allKnn)
    allKnn/(querySize.toDouble)/compKnn
  }

  def getRecall(myCfg: Config, trajMat:Array[Trajectory],queryMat:Array[Trajectory],queryRes:Array[Array[MutableMyPair[Double,Int]]]):Double={
    import collection.mutable.ArrayBuffer
    val tmpActualRes=new ArrayBuffer[ArrayBuffer[MutableMyPair[Double,Int]]]()
    var i=0
    queryMat.foreach(qtraj=>{
      println(s"正在计算第${i}个查询点")
      val tmp=new ArrayBuffer[MutableMyPair[Double,Int]]()
      var j=0
      trajMat.foreach(traj=>{
          val dist=myCfg.measure match {
          case "Frechet" => Cal_Frechet(qtraj.point,traj.point)
          case "Hausdorff"=>Cal_Hausdorff(qtraj.point,traj.point)
          case "DTW"=>Cal_DTW(qtraj.point,traj.point)
          case "LCSS"=>Cal_LCSS(qtraj.point,traj.point)
          case _ => println("error in getRecall");Double.MaxValue
        }
        j+=1
        tmp+=(new MutableMyPair(dist,traj.id))
      })
      tmpActualRes+=tmp
      i+=1
    })
    val actualRes=tmpActualRes.map(x=>x.sortBy(_.a).take(myCfg.knn).toArray).toArray
    getRecall(Result(actualRes),queryRes,myCfg.knn)

  }

}
