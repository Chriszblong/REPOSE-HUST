# REPOSE-HUST
REPOSE: Distributed Top-k Trajectory Similarity Search with Local Reference Point Tries.

The project is run on Spark2.2.0, scala2.10 and hadoop2.6.

Build
In main directory, you can use maven to build project with command "mvn package" and dependencies will be automatically downloaded.

Run
Run it by feeding the package to spark-submit with parameter ${dataSetSymbol} ${rawDataPath} ${queryDataPath} ${resultPathForSave} ${gama} ${partitionNum} ${knn} ${metric}  ${repeatNum} ${pivotNum}  ${minLongiForDataset} ${maxLongi} ${minLati} ${maxLati} ${isOptimizeTrie}

If you use our code, please remember to cite our paper:

REPOSE: Distributed Top-k Trajectory Similarity Search with Local Reference Point Tries.  
Bolong Zheng, Lianggui Weng, Xi Zhao, Kai Zeng, Xiaofang Zhou, Christian S. Jensen.  
IEEE International Conference on Data Engineering (ICDE) 2021, Crete, 708-719.  


