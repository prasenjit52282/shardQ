import os
import json
import random
from .api import ApiHandler
from collections import defaultdict

class Brokers:
    def __init__(self):
        self.api=ApiHandler()
        self.topics={}
        self.refresh()

    def refresh(self):
        containers=os.popen("sudo docker network inspect mynet | grep Name | tail -n +2 | cut -d ':' -f2 | tr -d ',\n\"' ").read().strip().split()
        self.brokers=[e for e in containers if 'broker' in e]

    @property
    def list(self):
        self.refresh()
        return self.brokers

    def choose(self):
        self.refresh()
        return random.choice(self.brokers)

    def lisTopics(self):
        self.refresh()
        topics=defaultdict(lambda:{})
        for bkr in self.brokers:
            try:
                self.api.setbroker(bkr)
                msg=self.api.get_topics()
                for TxP in msg:
                    T,P=TxP.split("x")
                    topics[T][P]=bkr
            except:pass
        return dict(topics)
    
    def checkifTopicPartExist(self,T,P):
        try:
            print(self.topics[T][P])
            return True
        except KeyError:
            return False

    def add_topic(self,T,P):
        if self.checkifTopicPartExist(T,P):
            return f"{T}:{P} already exist",400
        bkr=self.choose()
        try:
            self.api.setbroker(bkr)
            res=self.api.add_topics(f"{T}x{P}")
            self.topics=self.lisTopics()
            return res, 200
        except Exception as e:
            return str(e),400

    def producer_registration(self,T,P,publ):
        self.add_topic(T,P)
        bkr=self.topics[T][P]
        try:
            self.api.setbroker(bkr)
            prod_id=self.api.reg_producer(f"{T}x{P}")
            pub_id=publ.add_publisher(f"{T}x{P}@"+prod_id)
            return pub_id, 200
        except Exception as e:
            return str(e),400

    def produce(self,bkr,TxP,nhop_pub_id,msg):
        try:
            self.api.setbroker(bkr)
            self.api.produce(TxP,nhop_pub_id,msg)
            return "Success",200
        except Exception as e:
            return str(e),400

    def consumer_registration(self,T,P,subl):
        if not self.checkifTopicPartExist(T,P):
            return f"{T}:{P} does not exist",400
        bkr=self.topics[T][P]
        try:
            self.api.setbroker(bkr)
            sob_id=self.api.reg_consumer(f"{T}x{P}")
            sub_id=subl.add_subscriber(sob_id)
            return sub_id, 200
        except Exception as e:
            return str(e),400


    def consume(self,bkr,TxP,nhop_sub_id):
        try:
            self.api.setbroker(bkr)
            res=self.api.consume(TxP,nhop_sub_id)
            return res,200
        except Exception as e:
            return str(e),400

    def get_size(bkr,TxP,nhop_sub_id):
        try:
            self.api.setbroker(bkr)
            res=self.api.get_size(TxP,nhop_sub_id)
            return res,200
        except Exception as e:
            return str(e),400