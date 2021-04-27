package IndexPackage
import TrajectoryPackage._
import AuxiFunPackage._
import javafx.scene.control.Cell
import scala.collection.mutable.{ArrayBuffer}

abstract class Grid() {
  class Cell{
    def setCoordinate(label:Int)={
      val x_len = longiCellBoundry.size - 1
      val x = label % x_len
      val y = label / x_len
      val res = null
      this.longitude = (longiCellBoundry(x) + longiCellBoundry(x + 1)) / 2
      this.latitude = (latiCellBoundry(y) + latiCellBoundry(y + 1)) / 2
    }
    var longitude=0.0
    var latitude=0.0
  }

  def MatricProjtoGrid(trajMat:TrajectoryMatric)={
    val tmpTablesMap=collection.mutable.Map[Vector[Int],collection.mutable.ArrayBuffer[Int]]()
    var count=0

    trajMat.trajectory.foreach((in)=>{
      val projVal=trajProjtoGrid(in).toVector
      if(tmpTablesMap.contains(projVal))
        tmpTablesMap(projVal)+=count
      else {
        tmpTablesMap+=(projVal->ArrayBuffer[Int]())
        tmpTablesMap(projVal)+=count
      }
      count+=1
    })
    tablesMap=tmpTablesMap map (in=>(in._1,in._2.toArray))

    //update CellMatric
    val tmpCellMatric=new collection.mutable.ArrayBuffer[Cell]()
    println("cellNum is "+cellNum)
    for(i <- 0 until cellNum) {
      tmpCellMatric+=new Cell()
      tmpCellMatric(i).setCoordinate(i)
    }
    this.cellMatric=tmpCellMatric.toArray
  }

  def dfsMatricProjtoGrid(trajMat:TrajectoryMatric)={
    val tmpTablesMap=collection.mutable.Map[Vector[Int],collection.mutable.ArrayBuffer[Int]]()
    trajMat.trajectory.foreach((in)=>{
      val projVal=trajProjtoGrid(in).toVector
      if(tmpTablesMap.contains(projVal))
        tmpTablesMap(projVal)+=in.id
      else {
        tmpTablesMap+=(projVal->ArrayBuffer[Int]())
        tmpTablesMap(projVal)+=in.id
      }
    })
    tablesMap=tmpTablesMap map (in=>(in._1,in._2.toArray))

    //update CellMatric
    val tmpCellMatric=new collection.mutable.ArrayBuffer[Cell]()
    for(i <- 0 until cellNum) {
      tmpCellMatric+=new Cell()
      tmpCellMatric(i).setCoordinate(i)
    }
    this.cellMatric=tmpCellMatric.toArray
  }
  def getLongCellNum={
    this.longiCellBoundry.length-1
  }
  def getLatCellNum={
    this.latiCellBoundry.length-1
  }
  def ConstructGrid(rawTrajectoryMatric: TrajectoryMatric): Unit ={
    var min_lon = rawTrajectoryMatric.Min_longitude
    var min_lat = rawTrajectoryMatric.Min_latitude
    var max_lon = rawTrajectoryMatric.Max_longitude
    var max_lat = rawTrajectoryMatric.Max_latitude
    val newRange=adjustForOrder2(rawTrajectoryMatric);
    min_lon=newRange._1
    max_lon=newRange._2
    min_lat=newRange._3
    max_lat=newRange._4


    val lenhth_lon = math.ceil((max_lon - min_lon-0.0000005) / gama).toInt
    val lenhth_lat = math.ceil((max_lat - min_lat-0.0000005) / gama).toInt

    assert(!(lenhth_lon < 1 || lenhth_lat < 1))

    /*存储网格坐标*/
   // import collection.mutable.Buffer
    val tmplongiCellBoundry=new collection.mutable.ArrayBuffer[Double]()
    val tmplatiCellBoundry=new collection.mutable.ArrayBuffer[Double]()
    for (i <-  0 to lenhth_lon.toInt ){
      tmplongiCellBoundry+=min_lon + i * gama
    }
    for (i <-  0 to lenhth_lat.toInt ){
      tmplatiCellBoundry+=min_lat + i * gama
    }
    assert(tmplatiCellBoundry(tmplatiCellBoundry.size-1) >= max_lat && tmplatiCellBoundry(0) <= min_lat
      && tmplongiCellBoundry(tmplongiCellBoundry.size-1) >= max_lon && tmplongiCellBoundry(0) <= min_lon);
    this.longiCellBoundry=tmplongiCellBoundry.toArray
    this.latiCellBoundry=tmplatiCellBoundry.toArray
    assert(longiCellBoundry.length==latiCellBoundry.length)
    this.cellNum=((tmplongiCellBoundry.size - 1)*(tmplatiCellBoundry.size - 1))
  }

  def ConstructGrid(globalTrajRange: Array[Double]): Unit ={
    var min_lon = globalTrajRange(0)
    var min_lat = globalTrajRange(2)
    var max_lon = globalTrajRange(1)
    var max_lat = globalTrajRange(3)
    val newRange=adjustForOrder2(globalTrajRange);
    min_lon=newRange._1
    max_lon=newRange._2
    min_lat=newRange._3
    max_lat=newRange._4

    val lenhth_lon = math.ceil((max_lon - min_lon-0.0000005) / gama).toInt
    val lenhth_lat = math.ceil((max_lat - min_lat-0.0000005) / gama).toInt


    assert(!(lenhth_lon < 1 || lenhth_lat < 1))

    /*存储网格坐标*/
    // import collection.mutable.Buffer
    val tmplongiCellBoundry=new collection.mutable.ArrayBuffer[Double]()
    val tmplatiCellBoundry=new collection.mutable.ArrayBuffer[Double]()
    for (i <-  0 to lenhth_lon.toInt ){
      tmplongiCellBoundry+=min_lon + i * gama
    }
    for (i <-  0 to lenhth_lat.toInt ){
      tmplatiCellBoundry+=min_lat + i * gama
    }
    assert(tmplatiCellBoundry(tmplatiCellBoundry.size-1) >= max_lat && tmplatiCellBoundry(0) <= min_lat
      && tmplongiCellBoundry(tmplongiCellBoundry.size-1) >= max_lon && tmplongiCellBoundry(0) <= min_lon);
    this.longiCellBoundry=tmplongiCellBoundry.toArray
    this.latiCellBoundry=tmplatiCellBoundry.toArray
    assert(longiCellBoundry.length==latiCellBoundry.length)
    this.cellNum=((tmplongiCellBoundry.size - 1)*(tmplatiCellBoundry.size - 1))
  }


  def minDistPoint(cellLabel:Int,point:Point)={
    val halfGama = gama/(2.0)
    val center_coor = Point(cellMatric(cellLabel).longitude,cellMatric(cellLabel).latitude)
    val leftLow_Point= Point(center_coor.longitude-halfGama,center_coor.latitude - halfGama)
    val rightHigh_Point= Point(center_coor.longitude + halfGama,center_coor.latitude + halfGama)

    var ans = 0.0;
    if (point.longitude < leftLow_Point.longitude) {
      ans += (leftLow_Point.longitude - point.longitude) * (leftLow_Point.longitude - point.longitude);
    }
    else if (point.longitude > rightHigh_Point.longitude) {
      ans += (point.longitude - rightHigh_Point.longitude) * (point.longitude - rightHigh_Point.longitude);
    }

    if (point.latitude < leftLow_Point.latitude) {
      ans += (leftLow_Point.latitude - point.latitude) * (leftLow_Point.latitude - point.latitude);
    }
    else if (point.latitude > rightHigh_Point.latitude) {
      ans += (point.latitude - rightHigh_Point.latitude) * (point.latitude - rightHigh_Point.latitude);
    }
    math.sqrt(ans);
  }

  def adjustForOrder2(rawTrajectoryMatric: TrajectoryMatric ) ={
    val lonRange=increaseWithThres(rawTrajectoryMatric.Max_longitude-rawTrajectoryMatric.Min_longitude)
    val latRange=increaseWithThres(rawTrajectoryMatric.Max_latitude-rawTrajectoryMatric.Min_latitude)
    val maxRange=math.max(latRange,lonRange)
    (rawTrajectoryMatric.Min_longitude,rawTrajectoryMatric.Min_longitude+maxRange, rawTrajectoryMatric.Min_latitude,rawTrajectoryMatric.Min_latitude+maxRange)
  }

  def adjustForOrder2(globalTrajRange: Array[Double] ) ={
    val lonRange=increaseWithThres(globalTrajRange(1)-globalTrajRange(0))
    val latRange=increaseWithThres(globalTrajRange(3)-globalTrajRange(2))
    val maxRange=math.max(latRange,lonRange)
    (globalTrajRange(0),globalTrajRange(0)+maxRange, globalTrajRange(2),globalTrajRange(2)+maxRange)
  }

  def increaseWithThres(initVal:Double): Double ={
      var base=1;
      while(base*gama<initVal){
        base*=2
      }
    base*gama
  }
  def Convert_Optimal():Unit={}
  def trajProjtoGrid(traj:Trajectory):Array[Int]
  var tablesMap= collection.mutable.Map[Vector[Int],Array[Int]]()
  val gama:Double
  var cellNum=0
  var cellMatric:Array[Cell]= Array[Cell]()
  var longiCellBoundry:Array[Double]= Array[Double]()
  var latiCellBoundry:Array[Double]= Array[Double]()
}
