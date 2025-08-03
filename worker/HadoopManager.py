"""

This module is hadoop manager
- connects to hadoop manager
- write data to hadoop - hdfs
- read data from hadoop - hdfs

"""
import os
from hdfs import InsecureClient 


hdfs_host = os.environ.get("HDFS_NAMENODE_HOST", "localhost")
hdfs_port = os.environ.get("HDFS_NAMENODE_PORT", "9870")

hdfs_url = f"http://{hdfs_host}:{hdfs_port}"

# print("Connecting to HDFS at:", hdfs_url)

# For example, using requests:



from SparkManager import SparkManager

class HadoopManager:
    def __init__(self):
        self.hadoop_client = InsecureClient(hdfs_url,user="root")
        self.spark = SparkManager.create_spark_session()
    
    # writing temporarly saved parquet data to hadoop
    def write_to_hdfs(self):
        try:
            parquet_data = self.spark.read.parquet(path)
            self.hadoop_client.upload('/data','my local file text')
            print("The data is saved to hadoop successfully")
        except Exception as e:
            print(f"Saving data to hadoop has failed: {e}" )
    
    #reading parquet data from hadoop  
    def read_from_hdfs(self):
        try:
            data = self.spark.read.parquet(self.HADOOP_OUTPUT_PATH)
            return data 
        except Exception as e:
            print("reading data from hadoop has failed: {e}")