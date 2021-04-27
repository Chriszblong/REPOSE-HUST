import AuxiFunPackage._
import IndexPackage.{DTWTrieIndex, FrechetTrieIndex, TrieIndex}
import TrajectoryPackage._
import Main.dataOutBuff_ToFile
import common._
case class TrajectorySimilarityWithKnnAlgorithms() {

    def test()={
        val dataOutBuff=new collection.mutable.ArrayBuffer[(String,String)]()
        val paramConfig=new Config(gama=0.02,knn=100,partitionNum = 10)

        /*从磁盘读取数据*/
        val rawTrajMat =TrajectoryMatric(
              scala.io.Source
              .fromFile(paramConfig.rawDatasetFilePath)
              .getLines().toArray
              .zipWithIndex.map(a=>FileDeal.getDataFromTxt(a._1,a._2.toLong))
              .take(3000)
        )
        val myKeans=new kMeans(rawTrajMat,5,AuxiFunction.Cal_Hausdorff)
        myKeans.iterate(5)

    }

    def singleSearchKnn()={
        val dataOutBuff=new collection.mutable.ArrayBuffer[(String,String)]()
        val timer=new MyTimer()
        val paramConfig=new Config(gama=0.1,knn=100,partitionNum = 10)
        
        /*从磁盘读取数据*/
        println("开始读取数据")
        timer.restart()
        val (rawTrajMat,queryTrajMat,knnRealResult)=ReadAllDataFromDisk(paramConfig)
        println("读取数据结束,耗时："+timer.elapsed()+"ms")

        //转换数据成DITA格式
//        FileDeal.SaveDatatoDITATxt(rawTrajMat,"/home/wlg/DataSet/rawData_TDrive.txt")
//        FileDeal.SaveDatatoDITATxt(queryTrajMat,"/home/wlg/DataSet/queryData_TDrive.txt")

        dataOutBuff+=("knn"->paramConfig.knn.toString)
        dataOutBuff+=("gama"->paramConfig.gama.toString)
        dataOutBuff+=("数据集大小"->rawTrajMat.trajectory.size.toString)
        dataOutBuff+=("查询轨迹数目"->queryTrajMat.trajectory.size.toString)

        println("开始构建索引")
        timer.restart()
        val index=constructIndex(rawTrajMat,paramConfig)
        dataOutBuff+=("索引时间"->timer.elapsed().toString)
        println("构建索引结束,耗时：",dataOutBuff(dataOutBuff.size-1)._2+"ms")

        println("开始查询")
        timer.restart()
        queryBatch(queryTrajMat,index,paramConfig)
        AuxiFunction.getRecall(knnRealResult,index.getBatchQueryRes.map(_.toArray).toArray,paramConfig.knn)
        dataOutBuff+=("查询结束,耗时："->timer.elapsed().toString)
        println("查询结束,总耗时："+dataOutBuff(dataOutBuff.size-1)._2+"ms"+
          "平均耗时："+dataOutBuff(dataOutBuff.size-1)._2.toDouble/queryTrajMat.trajectory.size+"ms")
        dataOutBuff+=("recall"->index.getRecall(knnRealResult,paramConfig.knn).toString)
        dataOutBuff+=("cost"->(index.getAllCollisionPoint.toDouble/queryTrajMat.trajectory.size).toString)
        dataOutBuff+=("buckets"->(index.getAllCollisionBuckets.toDouble/queryTrajMat.trajectory.size).toString)
        dataOutBuff_ToFile(dataOutBuff,paramConfig.queryResultFilePath)

    }

    private def constructIndex(rawTrajMat:TrajectoryMatric, myCfg:Config):TrieIndex={
        //对应类型的索引
        val myIndex=AuxiFunction.getTrieIndex(myCfg)
        myIndex.constructIndex(rawTrajMat,myCfg.gama)
        myIndex
    }
    private def queryBatch(queryTrajMat:TrajectoryMatric,index:TrieIndex, myCfg:Config): Unit ={
        val myIndex=AuxiFunction.convertTrieIndex(myCfg,index)
        myIndex.queryBatch(queryTrajMat,myCfg.knn)
    }
    private def ReadAllDataFromDisk(myCfg:Config)={
        val myFileDeal=FileDeal;
        println("读取原始数据集")
  //      val rawTrajMat=myFileDeal.getDataFromBinaryFile(myCfg.rawDatasetFilePath)
        val rawTrajMat=myFileDeal.getDataFromTxtFile(myCfg.rawDatasetFilePath)
        println(s"原始数据集的数据量为${rawTrajMat.trajectory.size}")
        println("读取查询数据集")
        val queryTrajMat=myFileDeal.getDataFromTxtFile(myCfg.queryDatasetFilePath)
        println(s"查询数据集的数据量为${queryTrajMat.trajectory.size}")
        val knnRealResult=Result(myFileDeal.readResultFile(myCfg.knnResultFIlePath))
        (rawTrajMat,queryTrajMat,knnRealResult)
    }
}
