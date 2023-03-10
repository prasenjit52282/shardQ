from .api import ApiHandler
from collections import defaultdict
from .task import TaskManager, Task

class MyProducer:
    def __init__(self,topics,host):
        self.api=ApiHandler(host)
        self.tsk_mgr=TaskManager()
        self._setup(topics)
        
    def _setup(self,topics):
        self.ids=defaultdict(lambda:{})
        for t,p in topics:
            part=None if p=='*' else p
            self.ids[t][p]=self.api.reg_producer(t,part)
            
    def can_send(self):
        return self.api.can_send()
    
    def send(self,topic,part,msg):
        Task(self.api.produce,(self.ids[topic][part],msg),self.tsk_mgr).start()
        
    def stop(self):
        self.tsk_mgr.wait()



def readProduceLog(file):
    with open(file,'r') as f:
        while True:
            line=f.readline()
            if not line:break
            _,msg,part,topic=line.split()
            yield (topic,part,msg)