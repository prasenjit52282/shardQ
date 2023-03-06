from publisher import Publishers
from subscriber import Subscribers
from topicq import TopicQueues
from helper import SQLHandler

class Manager:
    def __init__(self,is_SQL=False,broker_id=None):
        self.broker_id=broker_id
        if is_SQL:
            self._setupSQL()
        else:
            self._setupInMEM()

    def _setupSQL(self):
        self.sql_handler=SQLHandler(host='localhost',user='root',password='abc',db='dQdb')
        self.tq=TopicQueues(is_SQL=True,tablenames=self.sql_handler.jobrunner.apply(self.sql_handler.getTopicTables),SQL_handle=self.sql_handler)
        self.pubs=Publishers(is_SQL=True,tablename='publ',SQL_handle=self.sql_handler)
        self.subs=Subscribers(is_SQL=True,tablename='subl',SQL_handle=self.sql_handler)
    
    def _setupInMEM(self):
        self.tq=TopicQueues()
        self.pubs=Publishers()
        self.subs=Subscribers()
        
    def CreateTopic(self,topic_name):
        if self.tq.is_topic_exist(topic_name):
            return dict(status="failure",message=f"Topic:{topic_name} already exist"), 400
        else:
            self.tq.add_new_topic(topic_name)
            return dict(status="success",message=f"Topic:{topic_name} created successfully"), 200
        
    def ListTopics(self):
        topics=self.tq.get_topic_list()
        if len(topics)==0:
            return dict(status="failure",message=f"No Topic found"), 400
        else:
            return dict(status="success",message=topics), 200
        
    def RegisterConsumer(self,topic_name):
        if not self.tq.is_topic_exist(topic_name):
            return dict(status="failure",message=f"Topic:{topic_name} does not exist"), 400
        else:
            sub_id=self.subs.add_subscriber()
            last_topic_idx=self.tq.topic_last_idx(topic_name)
            self.subs.reg_subcriber_with_topic(sub_id,topic_name,last_topic_idx)
            return dict(status="success",consumer_id=self.broker_id+'@'+str(sub_id)), 200

    def RegisterProducer(self,topic_name):
        if not self.tq.is_topic_exist(topic_name):
            self.tq.add_new_topic(topic_name)
        pub_id=self.pubs.add_publisher()
        self.pubs.reg_publisher_with_topic(pub_id,topic_name)
        return dict(status="success",producer_id=self.broker_id+'@'+str(pub_id)), 200

    def validate_id(self,combined_id):
        broker_part,identity_part=combined_id.split('@')
        if broker_part!=self.broker_id:
            raise Exception(f"broker part of the id does not match {broker_part}!={self.broker_id}")
        else:
            return int(identity_part)

    def Enqueue(self,topic_name,combined_pub_id,msg):
        try:
            pub_id=self.validate_id(combined_pub_id)
        except Exception as e:
            return dict(status="failure", message=str(e)), 400

        if not self.pubs.is_valid_id(pub_id):
            return dict(status="failure",message=f"Producer:{pub_id} does not exist"), 400
        if not self.tq.is_topic_exist(topic_name):
            return dict(status="failure",message=f"Topic:{topic_name} does not exist"), 400
        if not self.pubs.is_publisher_reg_with_topic(pub_id,topic_name):
            return dict(status="failure",message=f"Producer:{pub_id} is not registered with Topic:{topic_name}"), 400
        self.tq.add_msg_for_topic(topic_name,msg)
        return dict(status="success"), 200

    def Dequeue(self,topic_name,combined_sub_id):
        try:
            sub_id=self.validate_id(combined_sub_id)
        except Exception as e:
            return dict(status="failure", message=str(e)), 400

        if not self.subs.is_valid_id(sub_id):
            return dict(status="failure",message=f"Consumer:{sub_id} does not exist"), 400
        if not self.tq.is_topic_exist(topic_name):
            return dict(status="failure",message=f"Topic:{topic_name} does not exist"), 400
        if not self.subs.is_subscriber_reg_with_topic(sub_id,topic_name):
            return dict(status="failure",message=f"Consumer:{sub_id} is not registered with Topic:{topic_name}"), 400
        idx=self.subs.get_curr_idx(sub_id)
        try:
            msg=self.tq.get_msg_for_topic_at_idx(topic_name,idx)
            self.subs.topic_consumed_increase_curr_idx(sub_id)
            return dict(status="success",message=msg), 200
        except KeyError:
            return dict(status="failure",message=f"Empty logs for Comsumer:{sub_id} for Topic:{topic_name}"), 400

    def Size(self,topic_name,combined_sub_id):
        try:
            sub_id=self.validate_id(combined_sub_id)
        except Exception as e:
            return dict(status="failure", message=str(e)), 400

        if not self.subs.is_valid_id(sub_id):
            return dict(status="failure",message=f"Consumer:{sub_id} does not exist"), 400
        if not self.tq.is_topic_exist(topic_name):
            return dict(status="failure",message=f"Topic:{topic_name} does not exist"), 400
        if not self.subs.is_subscriber_reg_with_topic(sub_id,topic_name):
            return dict(status="failure",message=f"Consumer:{sub_id} is not registered with Topic:{topic_name}"), 400
        curr_idx=self.subs.get_curr_idx(sub_id)
        size=self.tq.topic_qsize(topic_name,curr_idx)
        return dict(status="success",size=size), 200