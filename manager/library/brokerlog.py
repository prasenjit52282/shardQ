import os
import json
import random
from collections import defaultdict

class Brokers:
    def __init__(self):
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
            msg=json.loads(os.popen(f"curl -XGET 'http://{bkr}:5000/topics'").read())["message"]
            if type(msg)==str:
                continue
            else:
                for TxP in msg:
                    T,P=TxP.split("x")
                    topics[T][P]=bkr
        return dict(topics)
    
    def checkifTopicPartExist(self,T,P):
        topics=self.lisTopics()
        try:
            print(topics[T][P])
            return True
        except KeyError:
            return False


    def add_topic(self,T,P):
        if self.checkifTopicPartExist(T,P):
            return f"{T}:{P} already exist", 400
        bkr=self.choose()
        res=\
        json.loads(os.popen(f"curl -XPOST http://{bkr}:5000/topics -d "+"\'{\"topic_name\":"+\
                            f"\"{T}x{P}\""+\
                            "}\' -H \'Content-Type: application/json\'").read())
        if res["status"]=="failure":
            return f"{T}:{P} already exist", 400
        else:
            return f"{T}:{P} created successfully", 200