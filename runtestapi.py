import random
import argparse
from time import sleep
from myqueue import ApiHandler

parser=argparse.ArgumentParser(description='example: python test_api.py --broker 10.110.10.216')
parser.add_argument('--broker', type=str, help='broker address ip', required=True)
args = parser.parse_args()

myapi=ApiHandler(args.broker)

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
    status=myapi.can_send()
    print("Can send:", status)
    return status

def get_topics(should_have=None):
    res=myapi.get_topics()
    if len(res)==0:raise Exception("No topics are created yet")
    if should_have!=None:
        assert should_have in res, f'{should_have} should be in the topic list'
    print("Pass: ListTopics", res)

def create_topic(topic,part):
    res=myapi.add_topic(topic,part)
    assert 'created' in res, "Error: CreateTopic"
    print("Pass: CreateTopic")

def reg_consumer(t,part):
    id=myapi.reg_consumer(t,part)
    assert type(id)==int,"RegisterConsumer"
    print("Pass: RegisterConsumer")
    return id

def reg_producer(t,part):
    id=myapi.reg_producer(t,part)
    assert type(id)==int,"RegisterProducer"
    print("Pass: RegisterProducer")
    return id

def produce(id):
    res=myapi.produce(id,"msg")
    assert "Success" in res, "Not successfully produced msg"
    print("Pass: Produce")

def consume(id):
    res=myapi.consume(id)
    if res==None: raise Exception("Empty response consumed")
    assert res.message=="msg","Wrong msg Consumed"
    print("Pass: Consume")

def get_size(id,should_be):
    res=myapi.get_size(id)
    assert res==should_be,"Size"
    print("Pass: Size")

def get_brokers(should_have=None):
    res=myapi.get_brokers()
    if should_have!=None:
        assert should_have in res, f'{should_have} should be in the broker list'
    print("Pass: Brokers", res)
    return res

def add_brokers(bkr):
    res=myapi.add_broker(bkr)
    assert "added" in res, f'{bkr} not added'
    print("Pass: Add brokers", res)

def rm_brokers(bkr):
    res=myapi.rm_broker(bkr)
    print("Pass: rm brokers", res)

def test_brokers(bkr):
    res=myapi.test_broker(bkr)
    assert "All test cases are passed" in res, f'{bkr}  test not passed'
    print("Pass: test brokers")


if run_test(can_send):
    run_test(get_topics)
else:
    run_test(get_topics,shoulderr=True)                         #As Expected: topics={}

t1=f"T{random.randint(0,1000)}"
p1=f"P{random.randint(0,1000)}"

run_test(create_topic,(t1,p1))
run_test(create_topic,(t1,"P1"))
if 'secondtopic' not in myapi.get_topics():
    run_test(create_topic,("secondtopic","P1"))

run_test(get_topics,(t1,))
run_test(reg_consumer,("T3", "P1"),shoulderr=True)             #AsExpected: Topic:T3:P1 does not exist
cid=run_test(reg_consumer,(t1,p1))
run_test(get_size,(cid,0))
run_test(consume,(cid,),shoulderr=True)                        #AsExpected: Empty logs for Comsumer:{} for {cid} thus empty response


pid=run_test(reg_producer,(t1,None))
run_test(produce,(pid,)) 
run_test(produce,(pid,))
run_test(produce,(pid,))



run_test(get_size,(cid,2))
run_test(consume,(cid,))
run_test(get_size,(cid,1))

cid2=run_test(reg_consumer,(t1,None))
run_test(get_size,(cid2,0))
run_test(produce,(pid,))
run_test(produce,(pid,))
run_test(produce,(pid,))
run_test(get_size,(cid,2))
run_test(get_size,(cid2,3))


bkrs=run_test(get_brokers,('broker0',))
if 'broker1' not in bkrs:
    run_test(add_brokers,('broker1',))
    sleep(20)
run_test(test_brokers,('broker1',))
run_test(rm_brokers,('broker1',))
run_test(rm_brokers,('broker1',),shoulderr=True)                #AsExpected: broker1 does not exist
run_test(test_brokers,('broker1',),shoulderr=True)              #AsExpected: broker1 does not exist

if allpass:
    print("------------>  All test cases are passed :)  <------------")
else:
    print("------------>  Some test cases are failed :(  <------------")