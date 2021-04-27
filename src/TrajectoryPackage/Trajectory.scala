package TrajectoryPackage
case class Point( val longitude:Double=0.0,val latitude:Double=0.0){
}

case class Trajectory (point:Array[Point],id:Int){

}

case class TrajectoryMatric(trajectory:Array[Trajectory]){
  var Max_longitude = Double.MinValue
  var Min_longitude =  Double.MaxValue
  var Max_latitude =  Double.MinValue
  var Min_latitude =  Double.MaxValue

  def updateRange()={
    if(trajectory!=null){
      trajectory.foreach(traj=>
        traj.point.foreach(p=>{
          {
            Max_latitude = math.max(p.latitude,Max_latitude) //记录最大经纬度
            Max_longitude =math.max(p.longitude,Max_longitude)
            Min_latitude = math.min(p.latitude,Min_latitude)
            Min_longitude = math.min(p.longitude,Min_longitude)
          }
        })
      )
    }
  }
  def setRange(array: Array[Double]): Unit ={
    Min_longitude=array(0)
    Max_longitude=array(1)
    Min_latitude=array(2)
    Max_latitude=array(3)

  }
  def getRange()={
    val array=new Array[Double](4)
    array(0)=Min_longitude
    array(1)=Max_longitude
    array(2)=Min_latitude
    array(3)=Max_latitude
    array
  }
  updateRange()
}

