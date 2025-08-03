"""
-This module reads data from hadoop cluster
-This process it go through the lines and 
-cancluate the average
-and save it ot hadoop again  

""" 

from SparkManager import SparkManager 
from HadoopManager import HadoopManager
import json, pathlib

class Compute:
    def __init__(self,DATA_PATH:str):
        self.DATA_PATH = DATA_PATH
        self.DATA_OUTPUT_PATH = DATA_OUTPUT_PATH
        self.spark = SparkManager.create_spark_session()
    
    def compute_average(self):
        try:
            data_frame = self.spark.read.parquet(self.DATA_PATH)
            if data_frame is not None:
                numeric_cols = [f.name for f in df.Schema.fields 
                                if f.dataType.typeName() in  {"double", "float", "long", "integer"}
                                ]
                aggregate_row = data_frame.agg(*[F.avg(c).alais(c) for c in numeric_cols]).first() 
                means = {c:float(aggregate_row[c] for c in numeric_cols)} 
                json_data = json.dumps(means)
                HadoopManager.write_to_hdfs(means, self.DATA_OUTPUT_PATH) 
                print(f"Data aggreagtion has been successfully done and saved to {self.DATA_OUTPUT_PATH}")
            else:
                print(f"Data aggreagation in the average format has failed ")
        except Exception as e:
            print(f"Data computation has failed.")
        
        