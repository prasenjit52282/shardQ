import requests
from .api import ApiHandler

class Brokers:
    def __init__(self):
        self.api=ApiHandler()
        self.topics={}
    
    @property
    def list(self):
        res=requests.get("http://manager:5000/brokers")
        return res.json()["message"]

    @property
    def curr_topics(self):
        self.refreshTopics()
        return self.topics

    def refreshTopics(self):
        res=requests.get("http://manager:5000/topics")
        self.topics=res.json()["message"]

    def checkifTopicPartExist(self,T,P):
        self.refreshTopics()
        try:
            print(self.topics[T][P])
            return True
        except KeyError:
            return False

    def consumer_registration(self,T,P,subl):
        self.refreshTopics()
        if P is None:
            if T not in self.topics:
                return f"{T} does not exist, thus can not register as consumer", 400
            all_ids=[]
            for part,bkr in self.topics[T].items():
                self.api.setbroker(bkr)
                sob_id=self.api.reg_consumer(f"{T}x{part}")
                all_ids.append(f"{T}x{part}@"+sob_id)
            combined_id='|'.join(all_ids)
            sub_id=subl.add_subscriber(combined_id)
            return sub_id, 200
        else:
            if not self.checkifTopicPartExist(T,P):
                return f"{T}:{P} does not exist, thus can not register as consumer",400
            bkr=self.topics[T][P]
            try:
                self.api.setbroker(bkr)
                sob_id=self.api.reg_consumer(f"{T}x{P}")
                sub_id=subl.add_subscriber(f"{T}x{P}@"+sob_id)
                return sub_id, 200
            except Exception as e:
                return str(e),400


    def consume(self,bkr,TxP,nhop_sub_id):
        T,P=TxP.split("x")
        if (not self.checkifTopicPartExist(T,P)) or bkr!=self.topics[T][P]:
            return f"{T}:{P}@{bkr} does not exist, thus can not consume",400
        try:
            self.api.setbroker(bkr)
            res=self.api.consume(TxP,nhop_sub_id)
            return res.message,200
        except Exception as e:
            return str(e),400

    def get_size(self,bkr,TxP,nhop_sub_id):
        T,P=TxP.split("x")
        if (not self.checkifTopicPartExist(T,P)) or bkr!=self.topics[T][P]:
            return f"{T}:{P}@{bkr} does not exist, invalid size",400
        try:
            self.api.setbroker(bkr)
            res=self.api.get_size(TxP,nhop_sub_id)
            return res,200
        except Exception as e:
            return str(e),400