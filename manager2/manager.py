import os
import requests
from library import Brokers,Subscribers
from flask import Flask, jsonify, request
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

brokers=Brokers()
subl=Subscribers()

#--------------------- Handling Brokers ----------------------------------#
@app.route("/brokers",methods=["GET"])
def list_brokers():
    return jsonify(brokers.list), 200

#--------------------------------- Handling Topics -------------------------------#
@app.route("/topics",methods=["GET"])
def get_topics():
    return jsonify(brokers.curr_topics),200

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