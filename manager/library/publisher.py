import numpy as np
from .helper import DataHandler,SQLHandler

class Publishers:
    def __init__(self):
        self.is_SQL=True
        self.sql_handle=SQLHandler(host='localhost',user='root',password='abc',db='dQdb')
        self._setup("publ")

    def _setup(self,tablename):
        self.publ=self.get_init_pub_list(tablename)
        
    def get_init_pub_list(self,tablename):
        return DataHandler(columns=['comb','ridx'],dtypes=["str","int"],is_SQL=self.is_SQL,SQL_handle=self.sql_handle,table_name=tablename)

    def add_publisher(self,combined_id):
        pub_id=self.publ.Insert([combined_id,0])
        return pub_id

    def is_publisher_reg_with_topic_part(self,pub_id,topic_name,part):
        T,P=self.publ.GetAT(pub_id,"comb").split("@")[0].split("x")
        return T==topic_name and P==part

    def get_round_robin(self,pub_id):
        fused_id=self.publ.GetAT(pub_id,"comb")
        ids=fused_id.split("|")
        if len(ids)==1:
            return ids[0]
        else:
            idx=self.publ.GetAT(pub_id,"ridx")
            next_idx=(idx+1)%len(ids)
            self.publ.Update(pub_id,"ridx",next_idx)
            return ids[idx]

    def translate(self,pub_id):
        combined_id=self.get_round_robin(pub_id)
        TxP=combined_id.split("@")[0]
        nhop_pub_id='@'.join(combined_id.split('@')[1:])
        return TxP,nhop_pub_id

    def is_valid_id(self,pub_id):
        return 0<=pub_id<self.publ.Count