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

    @property
    def curr_topics(self):
        self.refreshTopics()
        return self.topics

    def refreshTopics(self):
        self.topics=self.lisTopics()

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
        self.refreshTopics()
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
            self.refreshTopics()
            return res, 200
        except Exception as e:
            return str(e),400

    def producer_registration(self,T,P,publ):
        if P is None:
            if T not in self.topics:
                return f"{T} does not exist thus no partitions are available", 400
            all_ids=[]
            for part,bkr in self.topics[T].items():
                self.api.setbroker(bkr)
                prod_id=self.api.reg_producer(f"{T}x{part}")
                all_ids.append(f"{T}x{part}@"+prod_id)
            combined_id='|'.join(all_ids)
            pub_id=publ.add_publisher(combined_id)
            return pub_id, 200
        else:
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
        T,P=TxP.split("x")
        if (not self.checkifTopicPartExist(T,P)) or bkr!=self.topics[T][P]:
            return f"{T}:{P}@{bkr} does not exist, thus can not produce",400
        try:
            self.api.setbroker(bkr)
            self.api.produce(TxP,nhop_pub_id,msg)
            return TxP+":"+"Success",200
        except Exception as e:
            return str(e),400

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


    def add_broker(self,broker_name,persist):
        if broker_name in self.brokers: 
            return f"{broker_name} already exist", 400
        res=os.popen(f'sudo docker run --name {broker_name} --network mynet --network-alias {broker_name} -e BID={broker_name} -e PERSIST={persist}  -d broker:latest').read()
        if len(res)==0:
            return f"Unable to add {broker_name} - check manager logs", 400
        else:
            self.refresh()
            return f"successfully added {broker_name}", 200


    def remove_broker(self,broker_name):
        if broker_name=='all':
            for b in self.brokers:
                os.system(f'sudo docker stop {b} && sudo docker rm {b}')
            self.refreshTopics()
            return "Removed all brokers",200
        else:
            if broker_name not in self.brokers: 
                return f"{broker_name} does not exist", 400
            res=os.popen(f'sudo docker stop {broker_name} && sudo docker rm {broker_name}').read()
            if len(res)==0:
                return f"Unable to remove {broker_name} - check manager logs",400
            else:
                self.refreshTopics()
                return f"Removed {broker_name}", 200

    def test_broker(self,broker_name):
        if broker_name not in self.brokers: 
            return f"{broker_name} does not exist", 400
        res=os.popen(f'bash /mgr/testbroker.sh {broker_name} 5000').read()
        if len(res)==0:
            return f"Unable to test {broker_name} - check manager logs",400
        else:
            return res, 200