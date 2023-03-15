import os
from .api import ApiHandler
from iterators import TimeoutIterator

class MyConsumer:
    def __init__(self,topics,host,timeout=20):
        self.timeout=timeout
        self.api=ApiHandler(host)
        self._setup(topics)
        
    def _setup(self,topics):
        self.ids=[]
        for tp in topics:
            t=tp[0];p=None if tp[1]=='*' else tp[1]
            self.ids.append(self.api.reg_consumer(t,p,retry=True))
    
    def get_next(self):
        for m in TimeoutIterator(self.consume(),timeout=self.timeout,sentinel=None):
            if m==None:raise StopIteration("timeout: no more msg to consume")
            yield m
    
    def consume(self):
        while True:
            for consumer_id in self.ids:
                if self.api.can_get_next(consumer_id):
                    res=self.api.consume(consumer_id)
                    if res==None:
                        continue
                    yield res
        
    def stop(self):
        os.kill(os.getpid(), 9)



class writeComsumeLog:
    def __init__(self,file):
        self.f=open(file,"w")
        
    def writeline(self,string):
        self.f.write(string+"\n")
        self.f.flush()
        
    def close(self):
        self.f.close()