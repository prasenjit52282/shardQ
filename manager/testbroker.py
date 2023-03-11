import random
import argparse
from library import ApiHandler

parser=argparse.ArgumentParser(description='example: python test_api.py --broker 10.110.10.216:5000')
parser.add_argument('--broker', type=str, help='broker address ip:port', required=True)
args = parser.parse_args()

myapi=ApiHandler(args.broker)
broker_name=args.broker.split(":")[0]

allpass=True

def run_test(fn,arg=(),shoulderr=False):
    global allpass
    try:
        return fn(*arg)
    except Exception as e:
        if shoulderr:
            print("AsExpected:",e)
        else:
            allpass=False
            print("Error:",e)

def can_send():
    return myapi.can_send()

def get_topics(should_have=None):
    res=myapi.get_topics()
    assert len(res)>0, 'ListTopics'
    if should_have!=None:
        assert should_have in res, f'{should_have} should be in the topic list'
    print("Pass: ListTopics", res)

def create_topic(topic):
    res=myapi.add_topics(topic)
    assert res==f'Topic:{topic} created successfully', "Error: CreateTopic"
    print("Pass: CreateTopic")

def reg_consumer(t):
    id=myapi.reg_consumer(t)
    assert type(id)==str,"RegisterConsumer"
    print("Pass: RegisterConsumer")
    return id

def reg_producer(t):
    id=myapi.reg_producer(t)
    assert type(id)==str,"RegisterProducer"
    print("Pass: RegisterProducer")
    return id

def produce(t,id):
    myapi.produce(t,id,"msg")
    print("Pass: Produce")

def consume(t,id):
    res=myapi.consume(t,id)
    assert res.message=="msg","Consume"
    print("Pass: Consume")

def get_size(t,id,should_be):
    res=myapi.get_size(t,id)
    print("Pass: Size")
    assert res==should_be,"Size"


if run_test(can_send):
    run_test(get_topics)
else:
    run_test(get_topics,shoulderr=True)                  #AsExpected: No Topic found

t1=f"mytopic{random.randint(0,1000)}"
run_test(create_topic,(t1,))
if 'secondtopic' not in myapi.get_topics():
    run_test(create_topic,("secondtopic",))

run_test(get_topics,(t1,))
run_test(reg_consumer,("anytopic",),shoulderr=True)      #AsExpected: Topic:anytopic does not exist
cid=run_test(reg_consumer,(t1,))
print("CID:",cid)
run_test(get_size,(t1,cid,0))
run_test(consume,(t1,cid),shoulderr=True)                #AsExpected: Empty logs for Comsumer:{} for Topic:mytopic{}
run_test(consume,("anytopic",cid),shoulderr=True)        #AsExpected: Topic:anytopic does not exist


pid=run_test(reg_producer,(t1,))
print("PID:",pid)
run_test(produce,(t1,pid))
run_test(produce,(t1,pid))
run_test(produce,("anytopic",pid),shoulderr=True)        #AsExpected: Topic:anytopic does not exist
run_test(produce,(t1,f'{broker_name}@-1'),shoulderr=True)                 #AsExpected: Producer:-1 does not exist
run_test(produce,("secondtopic",pid),shoulderr=True)     #AsExpected: Producer:2 is not registered with Topic:secondtopic



run_test(get_size,(t1,cid,2))
run_test(consume,(t1,f'{broker_name}@-1'),shoulderr=True)                 #AsExpected: Consumer:-1 does not exist
run_test(consume,(t1,'brokerX@-1'),shoulderr=True)                 #AsExpected:  broker part of the id does not match brokerX!={broker_name}
run_test(consume,("secondtopic",cid),shoulderr=True)     #AsExpected: Consumer:3 is not registered with Topic:secondtopic
run_test(consume,(t1,cid))
run_test(get_size,(t1,cid,1))

cid2=run_test(reg_consumer,(t1,))
run_test(get_size,(t1,cid2,0))
run_test(produce,(t1,pid))
run_test(get_size,(t1,cid,2))
run_test(get_size,(t1,cid2,1))


if allpass:
    print("------------>  All test cases are passed :)  <------------")
else:
    print("------------>  Some test cases are failed :(  <------------")