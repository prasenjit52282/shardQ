import requests

class ApiHandler:
    def __init__(self,url='localhost:5000'):
        self.url='http://'+url

    def setbroker(self,broker='broker0'):
        self.url=f'http://{broker}:5000'
        
    def raiseExceptionOnProhabitedTopic(self,topic):
        if topic=='subl' or topic=='publ':
            raise Exception(f"topic name: {topic} is not allowed")
        
    def raiseExceptionOnFailure(self,res):
        if res.status_code==400 or res.status_code==500:
            raise Exception(self.decodeResponse(res,'message'))
            
    def decodeResponse(self,res,field):
        return res.json()[field]
        
    def can_send(self):
        res=requests.get(self.url+'/topics')
        return True if res.status_code==200 else False
    
    def can_get_next(self,topic,consumer_id):
        return True if self.get_size(topic,consumer_id)>0 else False
        
    def get_topics(self):
        res=requests.get(self.url+'/topics')
        self.raiseExceptionOnFailure(res)
        return self.decodeResponse(res,'message')
            
    def add_topics(self,topic):
        self.raiseExceptionOnProhabitedTopic(topic)
        res=requests.post(self.url+'/topics',json={'topic_name':topic})
        self.raiseExceptionOnFailure(res)
        return self.decodeResponse(res,'message')
            
    def reg_consumer(self,topic):
        res=requests.post(self.url+'/consumer/register',json={'topic':topic})
        self.raiseExceptionOnFailure(res)
        return self.decodeResponse(res,'consumer_id')
        
    def reg_producer(self,topic):
        self.raiseExceptionOnProhabitedTopic(topic)
        res=requests.post(self.url+'/producer/register',json={'topic':topic})
        self.raiseExceptionOnFailure(res)
        return self.decodeResponse(res,'producer_id')
        
    def produce(self,topic,producer_id,message):
        res=requests.post(self.url+'/producer/produce',json={'topic':topic,'producer_id':producer_id,'message':message})
        self.raiseExceptionOnFailure(res)
    
    def consume(self,topic,consumer_id):
        res=requests.get(self.url+'/consumer/consume',params={'topic':topic,'consumer_id':consumer_id})
        self.raiseExceptionOnFailure(res)
        return Message(topic,self.decodeResponse(res,'message'))
        
    def get_size(self,topic,consumer_id):
        res=requests.get(self.url+'/size',params={'topic':topic,'consumer_id':consumer_id})
        self.raiseExceptionOnFailure(res)
        return self.decodeResponse(res,'size')

class Message:
    def __init__(self,topic,msg):
        self.message=msg
        self.topic=topic
    
    def __repr__(self):
        return f'<topic:{self.topic}, msg:{self.message}>'