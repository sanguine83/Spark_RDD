# Steps to execute

# From Spark_RDD_Operations Folder
spark_rdd.sh

Note: 
SPARK Commands Used:

Client Mode:
spark-submit --deploy-mode client --class RDD_Operations.Driver /home/suresh/IdeaProjects/Spark_RDD_File_Joins/out/artifacts/Spark_RDD_File_Joins_jar/Spark_RDD_File_Joins.jar 100

Cluster Mode:
spark-submit --deploy-mode cluster --class RDD_Operations.Driver /home/suresh/IdeaProjects/Spark_RDD_File_Joins/out/artifacts/Spark_RDD_File_Joins_jar/Spark_RDD_File_Joins.jar 100
