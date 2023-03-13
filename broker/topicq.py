import shelve
from helper import DataHandler
from threading import Semaphore

class WAL:
    def __init__(self,loc):
        self.loc=loc

    def open(self):
        self.log=shelve.open(self.loc)

    def close(self):
        self.log.close()
    
    def keep(self,topic_name,msg):
        self.open()
        self.log[topic_name]=msg
        self.close()

    def remove(self,topic_name):
        self.open()
        self.log.pop(topic_name)
        self.close()

class TopicQueues:
    def __init__(self,is_SQL=False,tablenames=[],SQL_handle=None):
        self.queues={}
        self.locks={}
        self.logger=WAL("topicLog")

        self.is_SQL=is_SQL
        self.sql_handle=SQL_handle
        if self.is_SQL:
            self._restoreSQLTables(tablenames)
            self._restoreWAL()

    def _restoreWAL(self):
        self.logger.open()
        itemkeys=list(self.logger.log.keys())
        for topic_name in itemkeys:
            msg=self.logger.log.pop(topic_name)
            self.add_msg_for_topic(topic_name,msg,wal=False)
        self.logger.close()

    def _restoreSQLTables(self,tablenames):
        for tbname in tablenames:
            self.add_new_topic(tbname)
        
    def get_empty_topic_log(self,table_name):
        return DataHandler(columns=['msg'],dtypes=['str'],is_SQL=self.is_SQL,SQL_handle=self.sql_handle,table_name=table_name)

    def add_msg_to_log(self,log_df,msg): #index is the order & time
        log_df.Insert([msg])
        
    def add_new_topic(self,topic_name):
        self.queues[topic_name]=self.get_empty_topic_log(topic_name)
        self.locks[topic_name]=Semaphore()
        
    def add_msg_for_topic(self,topic_name,msg,wal=True):
        self.locks[topic_name].acquire()
        if wal:self.logger.keep(topic_name,msg)
        self.add_msg_to_log(self.queues[topic_name],msg)
        if wal:self.logger.remove(topic_name)
        self.locks[topic_name].release()
        
    def topic_qsize(self,topic_name,curr_idx_in_q):
        return self.queues[topic_name].Count-curr_idx_in_q
        #curr_idx_in_q is where (not read the data yet,already 1 less so no +1) the consumer_id currently in queue
    
    def topic_last_idx(self,topic_name):
        idx=self.queues[topic_name].Count
        return idx

    def get_topic_list(self):
        return list(self.queues.keys())

    def is_topic_exist(self,topic_name):
        return topic_name in self.queues

    def get_msg_for_topic_at_idx(self,topic_name,idx):
        return self.queues[topic_name].GetAT(idx,'msg')