package RDD
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd._
import TrajectoryPackage._
import AuxiFunPackage._
import IndexPackage.{GlobalTrieIndexBySequence, _}
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}
import common._
import org.apache.spark.storage.StorageLevel
class TrieRDD(dataRDD: RDD[Trajectory],partitionNum:Int) {
    def getReferenceTrajAndTrajIdFromPartition(myCfg:Config)={
      val gridCodeRDD=dataRDD.mapPartitions(trajs=>{
        val tmpTrajMat=TrajectoryMatric(trajs.toArray)
        tmpTrajMat.setRange(myCfg.globalTrajRange)
        val grid=myCfg.measure match { //only for partition
          case "Frechet" =>new HausdorffGrid(myCfg.gama)
          case "Hausdorff"=> new HausdorffGrid(myCfg.gama)
          case "DTW"=>new HausdorffGrid(myCfg.gama)
          case _ =>{println("索引类型不支持");new HausdorffGrid(myCfg.gama)}
        }
        grid.ConstructGrid(tmpTrajMat)
        grid.dfsMatricProjtoGrid(tmpTrajMat)
        val res=grid.tablesMap.map(x=>(x._1,x._2)).toArray
        Array((res)).toIterator
      })
      val allTablesMap=gridCodeRDD.collect().flatten.groupBy(_._1).toArray.map(x=>{
        (x._1,x._2.map(_._2).flatten)
      })
      allTablesMap
    }

    def buildDistributeIndex(myCfg:Config)={
      this.queryInfos=Array.fill[QueryInfo](myCfg.partitionNum)(QueryInfo())
      val timer=new MyTimer()
      timer.restart()
      println("收集计算信息")
      val allTablesMap=getReferenceTrajAndTrajIdFromPartition(myCfg)
      var trajNum=0
      var avrLen=0.0
      allTablesMap.foreach(x=>{
        trajNum+=x._2.length
        avrLen+=x._1.size
      })
      avrLen=avrLen/(allTablesMap.length.toDouble)
      println(s"结束收集计算信息,总共有${trajNum}条轨迹,${allTablesMap.length}个网格码,平均长度是${avrLen}")

      val globalTrieIndex=new GlobalTrieIndexBySomTCWithDisSimilar(allTablesMap, partitionNum)
      globalTrieIndex.partitionRDD(myCfg)
      println("分区开始")
      val partitionRDD=dataRDD.map(x=>(x.id,x)).partitionBy(globalTrieIndex).map(_._2)
        .mapPartitions(trajs=>{
          val tmpTrajMat=TrajectoryMatric(trajs.toArray)
          if(myCfg.isGlobalTrajRange)
            tmpTrajMat.setRange(myCfg.globalTrajRange)
          Array(tmpTrajMat).toIterator
        })
      println("分区结束")
      println("构建索引开始")
      //每个分区数据建立局部索引
      val packRDD=partitionRDD.mapPartitionsWithIndex((partitionId,dataIter)=>{
          val dataMat=dataIter.toArray.head
          val myIndex=AuxiFunction.getTrieIndex(myCfg)
          myIndex.constructIndex(dataMat,myCfg.gama,myCfg.pivotNum)
         Array(PackPartiton(partitionId,dataMat,myIndex)).toIterator
      })
      packRDD.persist(StorageLevel.MEMORY_AND_DISK_SER)
      packRDD.count()
      /*获得节点总数*/
      val allNodeNum=packRDD.map(x=>{
        println(s"节点数目是${x.trieIndex.my_Trie.nodeNum}")
        x.trieIndex.my_Trie.nodeNum

      }).collect().sum
      myCfg.NodeNum=allNodeNum
      myCfg.indexTime=timer.elapsed()
      println(s"构建索引结束，用时${ myCfg.indexTime}")
      packRDD
    }

    def distributeSearchKnn(sc: SparkContext,queryTraj:Trajectory, packPartitionRDD:RDD[PackPartiton],myCfg:Config)= {
        val shareQueryTraj=sc.broadcast(queryTraj)
        if(!myCfg.isOutputQueryInfo){
          packPartitionRDD.mapPartitions(iter=>{
            val queryResult= iter.toArray.head.trieIndex.querySingle(shareQueryTraj.value,myCfg.knn)
            queryResult._1.toArray.toIterator
          }).collect()
            .sortBy(_.a)
            .take(myCfg.knn)
        }
        else{
          val res=packPartitionRDD.mapPartitions(iter=>{
            val packTrieIndex= iter.toArray.head
            val trieIndex=packTrieIndex.trieIndex
            val queryResult=trieIndex.querySingle(shareQueryTraj.value,myCfg.knn)
            Array((queryResult._1,trieIndex.queryInfo)).toIterator
          }).collect()

          var i=0;
          res.foreach(x=>{
            queryInfos(i).queryTime+=x._2.queryTime
            queryInfos(i).cost+=x._2.cost
            queryInfos(i).allTrieNodeNum+=x._2.allTrieNodeNum
            queryInfos(i).caltrajLength+=x._2.caltrajLength
            queryInfos(i).passNode+=x._2.passNode
            i+=1
          })
          res.flatMap(_._1).sortBy(_.a)
            .take(myCfg.knn).toArray
        }
    }

    def batchDistributeSearchKnn(sc: SparkContext,queryTrajMat:TrajectoryMatric, packPartitionRDD:RDD[PackPartiton],myCfg:Config)={
      var queryId=0
      val res=queryTrajMat.trajectory.map(queryTraj=>{
          println(s"当前是第${queryId}个查询")
          queryId+=1
          distributeSearchKnn(sc,queryTraj,packPartitionRDD,myCfg)
        })
      val querySize=queryTrajMat.trajectory.size
      queryInfos.foreach(x=>{
        x.queryTime=x.queryTime/querySize
        x.cost= x.cost/querySize
        x.allTrieNodeNum=x.allTrieNodeNum/querySize
        x.caltrajLength= x.caltrajLength/querySize
        x.passNode= x.passNode/querySize
      })
      res
    }
    var queryInfos=Array.fill[QueryInfo](partitionNum)(QueryInfo())
}
