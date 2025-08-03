"""
This class is si for creating spark connection.
Returns spark connection 
Parameters: Name of the master node 


"""
from pyspark.sql import SparkSession 

class SparkManager:
    def __init__(self,NAME_MASTER_NODE: str, APP_NAME: str):
        self.NAME_MASTER_NODE = 'bigdatamaster'
        self.APP_NAME = 'bigdataproject'
    # Creating spark session with master node names as bigdataproject
    def create_spark_session(self)-> object:
        try:
            spark =(SparkSession.builder
                        .master(self.NAME_MASTER_NODE)
                        .appName(self.APP_NAME)
                        .getOrCreate()
                ) 
            print(f"The session with master node {self.NAME_MASTER_NODE} and app name {self.APP_NAME} is created")
            return spark
        
        except:
            print(f"Creating spark session with master name {self.NAME_MASTER_NODE} and app name {self.APP_NAME} has failed")
            raise RuntimeError 
    # Stopping the session whith master named as "bigdataproject"
    def stop_spark_session(self):
        try:
            current_spark_session = SparkSession.active()
        except Exception as e:
            print(f"There is no active session running with app name {self.APP_NAME}",e)
        current_spark_session.stop()
        print(f"The spark session on {self.NAME_MASTER_NODE} has stopped")
        