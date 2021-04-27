package AuxiFunPackage
import TrajectoryPackage._
import java.io.{FileInputStream, _}
import java.lang.{Long}
import java.nio.ByteBuffer
import scala.io.Source
import collection.mutable.ArrayBuffer

object FileDeal {
   def getDataFromTxt(line: (String, scala.Long)): Trajectory = {
     val id=line._1.split(";").head.toInt
       val points = line._1.split(";").tail.map(_.split(","))
      .map(x => Point(x(0).toDouble,x(1).toDouble))
    Trajectory(points,id)
  }
  def getDataFromTxtFile(filepath:String):TrajectoryMatric = {
      val input=Source.fromFile(filepath).getLines().toArray
      .zipWithIndex.map(x=>{
        val y=(x._1,x._2.toLong)
        FileDeal.getDataFromTxt(y)} )
     TrajectoryMatric(input)
  }



  def getDataFromBinaryFile(BinaryFilePath:String):TrajectoryMatric={
    val myfile=new FileInputStream(new File(BinaryFilePath))
    val trajNum=readInt(myfile)

    val tmpTrajMat=TrajectoryMatric(null)
    val tmpTrajectory=ArrayBuffer[Trajectory]()

    println("******************************")
    for (i<- 0 until trajNum){
      if(i%(trajNum/30)==0) print("*")

      val tmpPoint=ArrayBuffer[Point]()
      val lineNum=readInt(myfile)
      val ignore=new Array[Byte](20)

      for(j<- 0 until lineNum){
        val tmplongitude=readDouble(myfile)
        val tmplatitude=readDouble(myfile)

        myfile.read(ignore)
        tmpPoint+=new Point(tmplongitude,tmplatitude)
      }

      tmpTrajectory+=Trajectory(tmpPoint.toArray,i)

    }
    println("")

    val myTrajectoryMatric=TrajectoryMatric(tmpTrajectory.toArray)
    myTrajectoryMatric
  }
  def readResultFile(resultFilePath:String) ={
    var myResult=ArrayBuffer[Array[MutableMyPair[Double,Int]]]()
    val infile=Source.fromFile(resultFilePath)
    val allLines=infile.getLines.toList
    val qnKnn=allLines.head.split(" ")
    allLines.tail.foreach((ele)=>{
      var lineList:List[MutableMyPair[Double,Int]]=Nil
      val tmpBuff=ele.split("\t").toArray
      for(i <- 0 until tmpBuff.size by 2){
        lineList=new MutableMyPair(tmpBuff(i+1).toDouble,tmpBuff(i).toInt)::lineList
      }
      myResult+=(lineList.reverse).toArray
    })
    myResult.toArray
  }

  //将数据存储成DITA格式
  def SaveDatatoDITATxt(trajMat:TrajectoryMatric,outputPath:String): Unit ={
    val out=new java.io.PrintWriter(outputPath)

    trajMat.trajectory
      .filter(_.point.length>=6)
      .filter(_.point.length<=1000)
      .zipWithIndex.map(trajs=>{
      out.print(s"${trajs._2};")
      trajs._1.point.foreach(myPoint=>{
        out.print(s"${myPoint.longitude.toString},${myPoint.latitude.toString};")
      })
      out.println("")
    })
    out.close()

  }

  private def readDouble(in:FileInputStream )={
    val tmpArray=new Array[Byte](8)
    //in.read(tmpArray,0,8)
    in.read(tmpArray)
    var _array: Array[Byte] = tmpArray.reverse
    ByteBuffer.wrap(_array).getDouble
  }

  private def readInt(in:FileInputStream )={
    val tmpArray=new Array[Byte](4)
   // in.read(tmpArray,0,4)
    in.read(tmpArray)
      ( 0xff & tmpArray(0) | (0xff00 & (tmpArray(1) << 8)) | (0xff0000 & (tmpArray(2) << 16)) | (0xff000000 & (tmpArray(3) << 24)))
  }

  def printToFile(data:Array[Int],filePath:String,firstLine:String)={
    import java.io._
    val fileIter=new FileWriter(filePath)
    fileIter.write(firstLine+"\n")
    data.zipWithIndex.foreach(in=>{
      fileIter.write(in._2+","+in._1+"\n")
    })
    fileIter.close()
  }
}



