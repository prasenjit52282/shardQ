import time
from .api import ApiHandler
from collections import defaultdict
from .task import TaskManager, Task

class MyProducer:
    def __init__(self,topics,host,timeout=20):
        self.timeout=timeout
        self.api=ApiHandler(host)
        self.tsk_mgr=TaskManager()
        self._setup(topics)
        
    def _setup(self,topics):
        self.ids=defaultdict(lambda:{})
        for t,p in topics:
            part=None if p=='*' else p
            self.ids[t][p]=self.api.reg_producer(t,part,retry=True)
            
    def can_send(self):
        return self.api.can_send()

    def try_produce_until_timeout(self,producer_id,message):
        t=0
        while t<self.timeout:
            try:
                self.api.produce(producer_id,message)
                return
            except:
                t+=1
                time.sleep(1)

    
    def send(self,topic,part,msg):
        Task(self.try_produce_until_timeout,(self.ids[topic][part],msg),self.tsk_mgr).start()
        
    def stop(self):
        self.tsk_mgr.wait()



def readProduceLog(file):
    with open(file,'r') as f:
        while True:
            line=f.readline()
            if not line:break
            _,msg,part,topic=line.split()
            yield (topic,part,msg)