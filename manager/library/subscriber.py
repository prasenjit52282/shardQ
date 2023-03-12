import numpy as np
from .helper import DataHandler,SQLHandler

class Subscribers:
    def __init__(self):
        self.is_SQL=True
        self.sql_handle=SQLHandler(host='localhost',user='root',password='abc',db='dQdb')
        self._setup("subl")

    def _setup(self,tablename):
        self.subl=self.get_init_sub_list(tablename)
        
    def get_init_sub_list(self,tablename):
        return DataHandler(columns=['comb','ridx'],dtypes=["str","int"],is_SQL=self.is_SQL,SQL_handle=self.sql_handle,table_name=tablename)

    def add_subscriber(self,combined_id):
        sub_id=self.subl.Insert([combined_id,0])
        return sub_id

    def is_subscriber_reg_with_topic_part(self,sub_id,topic_name,part):
        T,P=self.subl.GetAT(sub_id,"comb").split("@")[0].split("x")
        return T==topic_name and P==part

    def get_round_robin(self,sub_id):
        fused_id=self.subl.GetAT(sub_id,"comb")
        ids=fused_id.split("|")
        if len(ids)==1:
            return ids[0]
        else:
            idx=self.subl.GetAT(sub_id,"ridx")
            next_idx=(idx+1)%len(ids)
            self.subl.Update(sub_id,"ridx",next_idx)
            return ids[idx]

    def translate(self,sub_id):
        combined_id=self.get_round_robin(sub_id)
        TxP=combined_id.split("@")[0]
        nhop_sub_id='@'.join(combined_id.split('@')[1:])
        return TxP,nhop_sub_id

    def translateAll(self,sub_id):
        fused_id=self.subl.GetAT(sub_id,"comb")
        ids=fused_id.split("|")
        l=[]
        for d in ids:
            TxP=d.split("@")[0]
            nhop_sub_id='@'.join(d.split('@')[1:])
            l.append((TxP,nhop_sub_id))
        return l

    def is_valid_id(self,sub_id):
        return 0<=sub_id<self.subl.Count