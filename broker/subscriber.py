import numpy as np
from helper import DataHandler

class Subscribers:
    def __init__(self,is_SQL=False,tablename=None,SQL_handle=None):
        self.is_SQL=is_SQL
        self.sql_handle=SQL_handle
        self._setup(tablename)

    def _setup(self,tablename):
        self.subl=self.get_init_sub_list(tablename)
        
    def get_init_sub_list(self,tablename):
        return DataHandler(columns=['topic','curr_idx'],dtypes=["str","int"],is_SQL=self.is_SQL,SQL_handle=self.sql_handle,table_name=tablename)

    def add_subscriber(self):
        sub_id=self.subl.Insert([np.nan,np.nan])
        return sub_id

    def reg_subcriber_with_topic(self,sub_id,topic_name,last_topic_idx): #last_topic_idx=curr idx of msg in topic
        self.subl.Update(sub_id,'topic',topic_name)
        self.subl.Update(sub_id,'curr_idx',last_topic_idx)

    def is_subscriber_reg_with_topic(self,sub_id,topic_name):
        return self.subl.GetAT(sub_id,"topic")==topic_name
    
    def topic_consumed_increase_curr_idx(self,sub_id): #only when a valid consume has happened
        self.subl.IncrementBy(sub_id,'curr_idx',1)
        
    def get_curr_idx(self,sub_id):
        return int(self.subl.GetAT(sub_id,'curr_idx'))

    def is_valid_id(self,sub_id):
        return 0<=sub_id<self.subl.Count