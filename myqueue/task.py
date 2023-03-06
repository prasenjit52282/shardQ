import threading

class TaskManager:
    def __init__(self):
        self.pool=set()
        
    def add(self,th):
        self.pool.add(th)
        
    def remove(self,th):
        self.pool.remove(th)
        
    def wait(self):
        while len(self.pool)>0:pass

class Task(threading.Thread):
    def __init__(self,target,args=(),manager=None):
        super(Task,self).__init__()
        self.args=args
        self.target=target
        self.manager=manager
        self.manager.add(self)
        
    def run(self):
        self.target(*self.args)
        self.manager.remove(self)