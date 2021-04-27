package AuxiFunPackage
import TrajectoryPackage._
import IndexPackage._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class HausdorffGrid(val gama:Double) extends Grid {
  override def trajProjtoGrid(traj:Trajectory) = {
    import collection.mutable.ArrayBuffer
    val tmpRes= mutable.Set[Int]()
    val x_len=longiCellBoundry.size-1

    traj.point.foreach((in)=>{
      val posX=AuxiFunction.Binary_range_search(this.longiCellBoundry,in.longitude)
      val posY=AuxiFunction.Binary_range_search(this.latiCellBoundry,in.latitude)

      util.Try {
        if (!(  longiCellBoundry(posX) <= in.longitude &&
          ( !(posX + 1<longiCellBoundry.length) ||  longiCellBoundry(posX + 1) > in.longitude)
          && latiCellBoundry(posY) <= in.latitude &&
          ( !(posY + 1<latiCellBoundry.length) ||  latiCellBoundry(posY + 1) > in.latitude)
          )) {
          println("DecideCell error\n")
          while(true)
            assert(false)
        }
      } match {
        case util.Success(x)=>x;
        case  util.Failure(error)=>{
          println(s"posX is ${posX}, posY is ${posY}")
          longiCellBoundry.foreach(x=>println(x))
          latiCellBoundry.foreach(x=>println(x))
          println("posX is "+posX)
          println("longiCellBoundry is "+longiCellBoundry.length)
          println("posY is "+posY)
          println("latiCellBoundry is "+latiCellBoundry.length)
          println("longiCellBoundry(0) is "+longiCellBoundry(0))
          println("longiCellBoundry(longiCellBoundry.length-1) is "+longiCellBoundry(longiCellBoundry.length-1))
          println("latiCellBoundry(0) is "+latiCellBoundry(0))
          println("latiCellBoundry(latiCellBoundry.length-1) is "+latiCellBoundry(latiCellBoundry.length-1))
          println("in.longitude is "+in.longitude)
          println("in.latitude is "+in.latitude)
          println()
          assert(false)
        }
      }
      val pos = posX + posY * x_len
      if(pos>=this.cellNum){
        println("pos<this.cellNum")
        println(s"cellNum is ${cellNum}")
        println(s"pos is ${pos}")
        println(s"posX is ${posX}, posY is ${posY}")
        longiCellBoundry.foreach(x=>println(x))
        latiCellBoundry.foreach(x=>println(x))
        println("posX is "+posX)
        println("longiCellBoundry is "+longiCellBoundry.length)
        println("posY is "+posY)
        println("latiCellBoundry is "+latiCellBoundry.length)
        println("longiCellBoundry(0) is "+longiCellBoundry(0))
        println("longiCellBoundry(longiCellBoundry.length-1) is "+longiCellBoundry(longiCellBoundry.length-1))
        println("latiCellBoundry(0) is "+latiCellBoundry(0))
        println("latiCellBoundry(latiCellBoundry.length-1) is "+latiCellBoundry(latiCellBoundry.length-1))
        println("in.longitude is "+in.longitude)
        println("in.latitude is "+in.latitude)
        println()
        assert(pos<this.cellNum)
      }

      if(tmpRes.contains(pos)==false)
        tmpRes+=pos             //sava cell label
    })
    tmpRes.toArray.sorted
  }
  override def Convert_Optimal()={
    import collection.mutable.ArrayBuffer
    table2= Map[ArrayBuffer[Int],ArrayBuffer[Int]]()
    val table3= tablesMap
    val hashSet=new ArrayBuffer[(ArrayBuffer[Int],ArrayBuffer[Int])]()
    tablesMap.foreach(x=>{
      val tmp1=ArrayBuffer[Int]()
      tmp1 ++=x._1
      val tmp2=ArrayBuffer[Int]()
      tmp2 ++=x._1
      hashSet+=( tmp1 -> tmp2 )
    })
    Divide_Set(hashSet)
    tablesMap=collection.mutable.Map[Vector[Int],Array[Int]]()
    table2.foreach(x=>{
      if (x._1.size != x._2.size)
        println("\"尺度错误,Convert_Optimal error\\n\"")
      x._1.foreach(a=>{
        var flag = false
        x._2.foreach(b=>{
          if(a==b){
            flag = true
          }
        })
        if (!flag)
          println("\"Convert_Optimal error\\n\"")
      })
      if(tablesMap.contains(x._2.toVector)){
        tablesMap(x._2.toVector) = table3(x._1.toVector)
      }
      else{
        tablesMap+=(x._2.toVector -> table3(x._1.toVector))
      }
    })
  }
  def Divide_Set(hashSet:ArrayBuffer[(ArrayBuffer[Int],ArrayBuffer[Int])]):Unit={
    val v1 = ArrayBuffer[(ArrayBuffer[Int],ArrayBuffer[Int])]()
    val v2 = ArrayBuffer[(ArrayBuffer[Int],ArrayBuffer[Int])]()
    if (hashSet.size ==0)
      return Unit
    val frequentItem = Sub_Find_Frequent_Cell(hashSet)
    var frequentItemLabel=0
    hashSet.foreach(x=>{
      if (x._1.count(_==frequentItem)>0){
        x._1.zipWithIndex.foreach(label=>{
          if(label._1==frequentItem)
            frequentItemLabel=label._2
        })
        if(table2.contains(x._2))
          table2(x._2)+=frequentItem
        else{
          table2 +=(x._2 -> ArrayBuffer[Int]())
          table2(x._2)+=frequentItem
        }
        x._1.remove(frequentItemLabel)
        if(x._1.size != 0)
          v1+=(x);
      }
      else{
        v2+=(x);
      }
    })
    Divide_Set(v1)
    Divide_Set(v2)
  }
  def Sub_Find_Frequent_Cell(hashSet:ArrayBuffer[(ArrayBuffer[Int],ArrayBuffer[Int])])={
    var FrequentItem=(-1 -> -1)
    for(i<- 0 until cellNum){
      var count = 0
      hashSet.foreach(x=>{
        if(x._1.count(_==i)>0)
          count+=1
      })
      if (count > FrequentItem._2) {
        FrequentItem=(i->count)
      }
    }
    FrequentItem._1
  }
  var table2=Map[ArrayBuffer[Int],ArrayBuffer[Int]]()
}
class FrechetGrid(val gama:Double) extends Grid {
  override def trajProjtoGrid(traj:Trajectory) ={
    import collection.mutable.ArrayBuffer
    val tmpRes=new ArrayBuffer[Int]()
    val x_len=longiCellBoundry.size-1

    traj.point.foreach((in)=>{
      val posX=AuxiFunction.Binary_range_search(this.longiCellBoundry,in.longitude)
      val posY=AuxiFunction.Binary_range_search(this.latiCellBoundry,in.latitude)
      if (!(longiCellBoundry(posX) <= in.longitude && longiCellBoundry(posX + 1) > in.longitude
          && latiCellBoundry(posY) <= in.latitude && latiCellBoundry(posY + 1) > in.latitude)) {
         println("DecideCell error\n")
        while(true)
        assert(false)
      }
      val pos = posX + posY * x_len
      tmpRes+=pos             //sava cell label
    })
    val res=new ArrayBuffer[Int]()
    var last_pos=tmpRes(0)
    res+=last_pos
    tmpRes.tail.foreach((in)=>{
      if(in!=last_pos){
        res+=in
        last_pos=in
      }

    })
    res.toArray
  }
}
//class DTWGrid(val gama:Double) extends Grid {
//  override def trajProjtoGrid(traj:Trajectory) ={
//    import collection.mutable.ArrayBuffer
//    val tmpRes=new ArrayBuffer[Int]()
//    val x_len=longiCellBoundry.size-1
//
//    traj.point.foreach((in)=>{
//      val posX=AuxiFunction.Binary_range_search(this.longiCellBoundry,in.longitude)
//      val posY=AuxiFunction.Binary_range_search(this.latiCellBoundry,in.latitude)
//      if (!(longiCellBoundry(posX) <= in.longitude && longiCellBoundry(posX + 1) > in.longitude
//        && latiCellBoundry(posY) <= in.latitude && latiCellBoundry(posY + 1) > in.latitude)) {
//        println("DecideCell error\n")
//        while(true)
//          assert(false)
//      }
//      val pos = posX + posY * x_len
//      tmpRes+=pos             //sava cell label
//    })
//      tmpRes.toArray
//  }
//}

class DTWGrid(val gama2:Double) extends FrechetGrid(gama2) {

}

class LCSSGrid(val gama2:Double) extends FrechetGrid(gama2) {

}