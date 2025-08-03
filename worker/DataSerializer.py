"""
This class is for active data serializer. 
It converts csv data to the parquet.
Save it to hadoop
Read from hadoop
"""

from SparkManager import SparkManager 
from pyspark.sql import DataFrame
from HadoopManager import HadoopManager
import tempfile

data_path= ["Data/half1_BTCUSDT_1s.csv","Data/half2_BTCUSDT_1s.csv"]

class DataSerializer:
    def __init__(self,data_paths: list):
        self.data_paths=data_paths
        self.combined_data = None 
        self.spark = SparkManager.create_spark_session()
        
    #   Reading all csv data anc ombine them
    def read_data_to_hdfs(self)->DataFrame:
        try:
            for i,path in enumerate(self.data_paths): 
                df = self.spark.read.option("header",True).csv(path)
                if i == 0:
                    self.combined_data = df 
                else:
                    self.combined_data = self.combined_data.unionByName(df)
            self.serialize_to_parquet(self.combined_data)
        except Exception as e:
            print(f"There has been an error in reading and combining {self.combined_data}: {e}") 
    
    # Saving the data aas a paquet file and finally returning parquet temporary file 
    def serialize_to_parquet(self, data: str):
        try:
            if self.combined_data is not None:
                with tempfile.TemporaryDirectory() as tempdir:
                    print(f"Writing to temp path: {tempdir}")
                    data.write.mode('overwrite').parquet(tempdir) 
                    parquet_data = self.spark.parquet(tempdir)
                    self.save_as_hdfs(parquet_data)
                    
            else:
                pass 
        except Exception as e:
            print(f"There as an error in serializing the data: {e}")       
                #saving to hadoop is done here

    # Saving to the hdfs
    def save_as_hdfs(self,data):
        if data is not None:
            writer = HadoopManager().write_to_hdfs(data)
        else:
            print("There is no data saved to hadoop")
    
