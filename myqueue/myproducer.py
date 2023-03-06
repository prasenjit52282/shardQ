from .api import ApiHandler
from .task import TaskManager, Task

class MyProducer:
    def __init__(self,topics,broker):
        self.api=ApiHandler(broker)
        self.tsk_mgr=TaskManager()
        self._setup(topics)
        
    def _setup(self,topics):
        self.topics_to_id={}
        for t in topics:
            self.topics_to_id[t]=self.api.reg_producer(t)
            
    def can_send(self):
        return self.api.can_send()
    
    def send(self,topic,msg):
        Task(self.api.produce,(topic,self.topics_to_id[topic],msg),self.tsk_mgr).start()
        
    def stop(self):
        self.tsk_mgr.wait()



def readProduceLog(file):
    with open(file,'r') as f:
        while True:
            line=f.readline()
            if not line:break
            _,msg,_,topic=line.split()
            yield (topic,msg)