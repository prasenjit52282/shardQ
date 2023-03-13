import os
from library import Brokers,Publishers,Subscribers
from flask import Flask, jsonify, request
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
    return jsonify(brokers.list), 200

@app.route("/brokers/add/<broker_name>",methods=["GET"])
def add_broker(broker_name):
    if broker_name in brokers.list: 
        return f"{broker_name} already exist", 400
    res=os.popen(f'sudo docker run --name {broker_name} --network mynet --network-alias {broker_name} -e BID={broker_name} -e PERSIST={persist}  -d broker:latest').read()
    if len(res)==0:
        return f"Unable to add {broker_name} - check manager logs", 400
    else:
        brokers.refresh()
        return f"successfully added {broker_name}", 200

@app.route("/brokers/rm/<broker_name>",methods=["GET"])
def rm_brokers(broker_name):
    if broker_name=='all':
        all_brokers=brokers.list
        for b in all_brokers:
            os.system(f'sudo docker stop {b} && sudo docker rm {b}')
        brokers.refreshTopics()
        return "Removed all brokers",200
    else:
        if broker_name not in brokers.list: 
            return f"{broker_name} does not exist", 400
        res=os.popen(f'sudo docker stop {broker_name} && sudo docker rm {broker_name}').read()
        if len(res)==0:
            return f"Unable to remove {broker_name} - check manager logs",400
        else:
            brokers.refreshTopics()
            return f"Removed {broker_name}", 200

@app.route("/brokers/test/<broker_name>",methods=["GET"])
def test_broker(broker_name):
    if broker_name not in brokers.list: 
        return f"{broker_name} does not exist", 400
    res=os.popen(f'bash /mgr/testbroker.sh {broker_name} 5000').read()
    if len(res)==0:
        return f"Unable to test {broker_name} - check manager logs",400
    else:
        return res, 200

#--------------------------------- Handling Topics -------------------------------#
@app.route("/topics",methods=["GET"])
def get_topics():
    return jsonify(brokers.curr_topics),200

@app.route("/topics/add/<topic_name>/<part>",methods=["GET"])
def add_topic(topic_name,part):
    return brokers.add_topic(topic_name,part)

#--------------------------------- Handling Publishers ----------------------------#
@app.route("/producer/register",methods=["POST"])
def producer_registration():
    data=request.get_json()
    topic_name=data["topic"]
    part= data["part"] if "part" in data else None
    return brokers.producer_registration(topic_name,part,publ)

@app.route("/producer/produce",methods=["POST"])
def handle_produce():
    data=request.get_json()
    pub_id=int(data["producer_id"])
    msg=data["message"]
    TxP,nhop_pub_id=publ.translate(pub_id)
    bkr=nhop_pub_id.split('@')[0]
    return brokers.produce(bkr,TxP,nhop_pub_id,msg)

#---------------------------------- Handling Subscribers ----------------------------#
@app.route("/consumer/register",methods=["POST"])
def consumer_registration():
    data=request.get_json()
    topic_name=data["topic"]
    part= data["part"] if "part" in data else None
    return brokers.consumer_registration(topic_name,part,subl)

@app.route("/consumer/consume/<consumer_id>",methods=["GET"])
def handle_consume(consumer_id):
    sub_id=int(consumer_id)
    TxP,nhop_sub_id=subl.translate(sub_id)
    bkr=nhop_sub_id.split('@')[0]
    return brokers.consume(bkr,TxP,nhop_sub_id)

#------------------------------------ Size -------------------------------------------#
@app.route("/size/<consumer_id>",methods=["GET"])
def get_size(consumer_id):
    sub_id=int(consumer_id)
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
    return str(summ),200 #add note later



if __name__=='__main__':
    app.run(host='0.0.0.0',port=5000,debug=False)