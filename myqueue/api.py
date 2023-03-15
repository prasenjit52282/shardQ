import time
import requests

class ApiHandler:
    def __init__(self,host='localhost',primary='5000',readonly='5001'):
        self.manager='http://'+host+':'+primary
        self.manager2='http://'+host+':'+readonly
        
    #----------------- Error & Decode ----------------------#
    def raiseExceptionOnProhabited(self,name):
        if (name=='subl') or (name=='publ') or ('@' in name) or ('x' in name) or ('|' in name):
            raise Exception(f"Name: {name} is not allowed")
        
    def raiseExceptionOnFailure(self,res):
        if res.status_code==400 or res.status_code==500:
            raise Exception(self.decodeResponse(res,'message'))
            
    def decodeResponse(self,res,field):
        return res.json()[field]

    #------------------- Querry -------------------------------#  
    def can_send(self):
        return self.can_query_manager() and self.can_query_manager2()  
        
    def can_query_manager(self):
        try:
            res=requests.get(self.manager+'/topics')
            status= True if len(self.decodeResponse(res,"message"))>0 else False
        except:
            status=False
        return status

    def can_query_manager2(self):
        try:
            res=requests.get(self.manager2+'/topics')
            status= True if len(self.decodeResponse(res,"message"))>0 else False
        except:
            status=False
        return status
    
    def can_get_next(self,consumer_id):
        try:
            return True if self.get_size(consumer_id)>0 else False
        except:
            return False
        
    #------------------- Manager -------------------------------#    
    def add_topic(self,topic, part):
        self.raiseExceptionOnProhabited(topic)
        self.raiseExceptionOnProhabited(part)
        res=requests.post(self.manager+'/topics/add',json={'topic':topic,'part':part})
        self.raiseExceptionOnFailure(res)
        return self.decodeResponse(res,'message')
        
    def reg_producer(self,topic,part=None,retry=False):
        data={'topic':topic} if part==None else {'topic':topic,'part':part}
        self.raiseExceptionOnProhabited(topic)
        if part!=None:self.raiseExceptionOnProhabited(part)
        while True:
            try:
                res=requests.post(self.manager+'/producer/register',json=data)
                self.raiseExceptionOnFailure(res)
                break
            except:
                time.sleep(1)
            if not retry: break
        return self.decodeResponse(res,'message')
        
    def produce(self,producer_id,message):
        res=requests.post(self.manager+'/producer/produce',json={'producer_id':producer_id,'message':message})
        self.raiseExceptionOnFailure(res)
        return self.decodeResponse(res,'message')
    
    #------------------------ Manager2 --------------------------#
    def get_topics(self):
        res=requests.get(self.manager2+'/topics')
        self.raiseExceptionOnFailure(res)
        return self.decodeResponse(res,'message')

    def reg_consumer(self,topic, part=None,retry=False):
        data={'topic':topic} if part==None else {'topic':topic,'part':part}
        while True:
            try:
                res=requests.post(self.manager2+'/consumer/register',json=data)
                self.raiseExceptionOnFailure(res)
                break
            except:
                time.sleep(1)
            if not retry: break
        return self.decodeResponse(res,'message')

    def consume(self,consumer_id):
        try:
            res=requests.get(self.manager2+'/consumer/consume',params={'consumer_id':consumer_id})
            self.raiseExceptionOnFailure(res)
            return Message(self.decodeResponse(res,'topic'),self.decodeResponse(res,'message'))
        except:
            return None
        
    def get_size(self,consumer_id):
        res=requests.get(self.manager2+'/size',params={'consumer_id':consumer_id})
        self.raiseExceptionOnFailure(res)
        return self.decodeResponse(res,'message')

    #------------------- Brokers -----------------------------#
    def get_brokers(self):
        res=requests.get(self.manager2+'/brokers')
        self.raiseExceptionOnFailure(res)
        return self.decodeResponse(res,'message')

    def add_broker(self,broker_name):
        self.raiseExceptionOnProhabited(broker_name)
        if 'broker' not in broker_name: raise Exception(f"{broker_name} does not contain broker keyword")
        res=requests.get(self.manager+'/brokers/add', params={'broker_name':broker_name})
        self.raiseExceptionOnFailure(res)
        return self.decodeResponse(res,'message')

    def rm_broker(self,broker_name):
        self.raiseExceptionOnProhabited(broker_name)
        if 'broker' not in broker_name: raise Exception(f"{broker_name} does not contain broker keyword")
        res=requests.get(self.manager+'/brokers/rm', params={'broker_name':broker_name})
        self.raiseExceptionOnFailure(res)
        return self.decodeResponse(res,'message')

    def test_broker(self,broker_name):
        self.raiseExceptionOnProhabited(broker_name)
        if 'broker' not in broker_name: raise Exception(f"{broker_name} does not contain broker keyword")
        res=requests.get(self.manager+'/brokers/test', params={'broker_name':broker_name})
        self.raiseExceptionOnFailure(res)
        return self.decodeResponse(res,'message')

class Message:
    def __init__(self,topic,msg):
        self.message=msg
        self.topic=topic
    
    def __repr__(self):
        return f'<topic:{self.topic}, msg:{self.message}>'