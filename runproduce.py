import time
import random
import argparse
from myqueue import MyProducer,readProduceLog

parser=argparse.ArgumentParser(description='example: python runproduce.py --id 1  --topics T1:P1 T2:P3 --broker localhost --log_loc ./test')

parser.add_argument('--id', type=int, help='id of producer/consumer', required=True)
parser.add_argument('--topics', type=lambda e: e.split(':'), nargs='+', help='topics to produce/consume', required=True)
parser.add_argument('--broker', type=str, help='broker address ip:port', required=True)
parser.add_argument('--log_loc', type=str, help='log folder', required=True)

args = parser.parse_args()

def produce(pid=1,topics=[['T1', 'P1'], ['T2','*']],host='localhost',log_loc='./test'):
    producer = MyProducer(
        topics=topics,
        host=host)
    try:
        log_gen=readProduceLog(f'{log_loc}/producer_{pid}.txt')
        while True:
            for t,p in topics:
                topic,part,msg=next(log_gen)
                if p=='*':
                    if t!=topic:
                        raise Exception("log file:incompitable, make sure msg for topics are in order for a perticular time")
                elif t!=topic and p!=part:
                    raise Exception("log file:incompitable, make sure msg for topics are in order for a perticular time")
                producer.send(t,p,msg)
            time.sleep(random.random())
    except StopIteration:pass
    finally:
        producer.stop()

produce(args.id,args.topics,args.broker,args.log_loc)