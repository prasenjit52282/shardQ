import os
from library import Brokers,Publishers,Subscribers,Response
from flask import Flask, request
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

brokers=Brokers()
publ=Publishers()
subl=Subscribers()

persist=os.environ['PERSIST']

#--------------------- Handling Brokers ----------------------------------#
@app.route("/brokers",methods=["GET"])
def list_brokers():
    return Response(200, message=brokers.list, status='success')

@app.route("/brokers/add",methods=["GET"])
def add_broker():
    broker_name=request.args.get("broker_name")
    msg,status_code=brokers.add_broker(broker_name,persist)
    mode='success' if status_code==200 else 'failure'
    return Response(status_code, message=msg, status=mode)

@app.route("/brokers/rm",methods=["GET"])
def rm_brokers():
    broker_name=request.args.get("broker_name")
    msg,status_code=brokers.remove_broker(broker_name)
    mode='success' if status_code==200 else 'failure'
    return Response(status_code, message=msg, status=mode)

@app.route("/brokers/test",methods=["GET"])
def test_broker():
    broker_name=request.args.get("broker_name")
    msg,status_code=brokers.test_broker(broker_name)
    mode='success' if status_code==200 else 'failure'
    return Response(status_code, message=msg, status=mode)

#--------------------------------- Handling Topics -------------------------------#
@app.route("/topics",methods=["GET"])
def get_topics():
    return Response(200, message=brokers.curr_topics, status='success')

@app.route("/topics/add",methods=["POST"])
def add_topic():
    data=request.get_json()
    topic_name=data["topic"]
    part=data["part"]
    msg,status_code=brokers.add_topic(topic_name,part)
    mode='success' if status_code==200 else 'failure'
    return Response(status_code, message=msg, status=mode)

#--------------------------------- Handling Publishers ----------------------------#
@app.route("/producer/register",methods=["POST"])
def producer_registration():
    data=request.get_json()
    topic_name=data["topic"]
    part= data["part"] if "part" in data else None
    msg,status_code=brokers.producer_registration(topic_name,part,publ)
    mode='success' if status_code==200 else 'failure'
    return Response(status_code, message=msg, status=mode)

@app.route("/producer/produce",methods=["POST"])
def handle_produce():
    data=request.get_json()
    pub_id=int(data["producer_id"])
    msg=data["message"]
    TxP,nhop_pub_id=publ.translate(pub_id)
    bkr=nhop_pub_id.split('@')[0]
    msg,status_code=brokers.produce(bkr,TxP,nhop_pub_id,msg)
    mode='success' if status_code==200 else 'failure'
    return Response(status_code, message=msg, status=mode)

#---------------------------------- Handling Subscribers ----------------------------#
@app.route("/consumer/register",methods=["POST"])
def consumer_registration():
    data=request.get_json()
    topic_name=data["topic"]
    part= data["part"] if "part" in data else None
    msg,status_code=brokers.consumer_registration(topic_name,part,subl)
    mode='success' if status_code==200 else 'failure'
    return Response(status_code, message=msg, status=mode)

@app.route("/consumer/consume",methods=["GET"])
def handle_consume():
    sub_id=int(request.args.get("consumer_id"))
    TxP,nhop_sub_id=subl.translate(sub_id)
    bkr=nhop_sub_id.split('@')[0]
    msg,status_code=brokers.consume(bkr,TxP,nhop_sub_id)
    mode='success' if status_code==200 else 'failure'
    return Response(status_code, message=msg, topic=TxP.replace('x',':'), status=mode)

#------------------------------------ Size -------------------------------------------#
@app.route("/size",methods=["GET"])
def get_size():
    sub_id=int(request.args.get("consumer_id"))
    l=subl.translateAll(sub_id)
    summ=0
    note=""
    for TxP,nhop_sub_id in l:
        bkr=nhop_sub_id.split('@')[0]
        val,status=brokers.get_size(bkr,TxP,nhop_sub_id)
        if status==200:
            summ+=val
        else:
            note+=val+"\n"
    return Response(200, message=summ, note=note, status='success')



if __name__=='__main__':
    app.run(host='0.0.0.0',port=5000,debug=False)