import argparse
from myqueue import MyConsumer,writeComsumeLog

parser=argparse.ArgumentParser(description='example: python runconsume.py --id 1 --topics T1:P1 T2:P3 --broker localhost --log_loc ./test')

parser.add_argument('--id', type=int, help='id of producer/consumer', required=True)
parser.add_argument('--topics', type=lambda e: e.split(':'), nargs='+', help='topics to produce/consume', required=True)
parser.add_argument('--broker', type=str, help='broker address ip:port', required=True)
parser.add_argument('--log_loc', type=str, help='log folder', required=True)

args = parser.parse_args()

def consume(cid=1,topics=[['T1', 'P1'], ['T2','*']],host='localhost',log_loc='./test'):
    consumer = MyConsumer(
        topics=topics,
        host=host)
    try:
        writer=writeComsumeLog(f'{log_loc}/consumer_{cid}.txt')
        for msg in consumer.get_next():
            writer.writeline(f"{msg.topic}     {msg.message}")
    except:writer.close()
    finally:
        consumer.stop()

consume(args.id,args.topics,args.broker,args.log_loc)