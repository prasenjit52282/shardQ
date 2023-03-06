import numpy as np
from helper import DataHandler

class Publishers:
    def __init__(self,is_SQL=False,tablename=None,SQL_handle=None):
        self.is_SQL=is_SQL
        self.sql_handle=SQL_handle
        self._setup(tablename)

    def _setup(self,tablename):
        self.publ=self.get_init_pub_list(tablename)
        
    def get_init_pub_list(self,tablename):
        return DataHandler(columns=['topic'],dtypes=["str"],is_SQL=self.is_SQL,SQL_handle=self.sql_handle,table_name=tablename)

    def add_publisher(self):
        pub_id=self.publ.Insert([np.nan])
        return pub_id

    def reg_publisher_with_topic(self,pub_id,topic_name): #id is same is index
        self.publ.Update(pub_id,"topic",topic_name)

    def is_publisher_reg_with_topic(self,pub_id,topic_name):
        return self.publ.GetAT(pub_id,"topic")==topic_name

    def is_valid_id(self,pub_id):
        return 0<=pub_id<self.publ.Count