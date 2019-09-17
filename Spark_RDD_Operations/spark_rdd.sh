#!/bin/bash

# Configuration
MAIN_PATH="$( cd "$(dirname "$0")" ; pwd -P )"
DT=`date +'%m%d%Y'`
CONFIG_PATH=${MAIN_PATH}/config
LOG_PATH=${MAIN_PATH}/logs

# Read configuration details
while read config_item value
do
export "$config_item"="$value"
done < ${CONFIG_PATH}/configuration

# Clean up old logs
rm ${LOG_PATH}/log_${DT}
hdfs dfs -rm -R ${HDFS_OUTPUT_PATH}

# Variables Used
echo "LOG Path : " ${LOG_PATH} >> ${LOG_PATH}/log_${DT}
echo "HDFS Location : " ${HDFS_OUTPUT_PATH} >> ${LOG_PATH}/log_${DT}

echo "###############################################################################" >> ${LOG_PATH}/log_${DT}
echo "Submit SPARK Job" >> ${LOG_PATH}/log_${DT}
echo "spark-submit --deploy-mode cluster --class RDD_Operations.Driver ${JAR_FILE_PATH}/Spark_RDD_File_Joins.jar 100" >> ${LOG_PATH}/log_${DT}
spark-submit --deploy-mode cluster --class RDD_Operations.Driver ${JAR_FILE_PATH}/Spark_RDD_File_Joins.jar 100
echo "###############################################################################" >> ${LOG_PATH}/log_${DT}

# Consolidate and Clean up files after job completion
echo "Consolidate and Clean up files after job completion" >> ${LOG_PATH}/log_${DT}
hdfs dfs -rm -r ${HDFS_OUTPUT_PATH}/_SUCCESS >> ${LOG_PATH}/log_${DT}
echo "state#year#month#day#hour#sales" > ${LOG_PATH}/${OUTPUT_FILE}
hdfs dfs -put ${LOG_PATH}/${OUTPUT_FILE} ${HDFS_OUTPUT_PATH}
hdfs dfs -cat ${HDFS_OUTPUT_PATH}/part* | hdfs dfs -appendToFile - ${HDFS_OUTPUT_PATH}/${OUTPUT_FILE}
hdfs dfs -rm ${HDFS_OUTPUT_PATH}/part* >> ${LOG_PATH}/log_${DT}
rm ${LOG_PATH}/${OUTPUT_FILE}

echo "###############################################################################" >> ${LOG_PATH}/log_${DT}
echo "Output File : " >> ${LOG_PATH}/log_${DT}
hdfs dfs -ls ${HDFS_OUTPUT_PATH}/${OUTPUT_FILE} >> ${LOG_PATH}/log_${DT}
echo "###############################################################################" >> ${LOG_PATH}/log_${DT}
