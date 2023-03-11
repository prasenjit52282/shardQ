import os
from library import Brokers
from flask import Flask, jsonify, request
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

brokers=Brokers()
persist=os.environ['PERSIST']

#--------------------- Handling Brokers ----------------------------------#
@app.route("/brokers",methods=["GET"])
def list_brokers():
    return jsonify(brokers.list), 200

@app.route("/brokers/add/<broker_name>",methods=["GET"])
def add_broker(broker_name):
    res=os.popen(f'sudo docker run --name {broker_name} --network mynet --network-alias {broker_name} -e BID={broker_name} -e PERSIST={persist}  -d broker:latest').read()
    if len(res)==0:
        return f"Unable to add {broker_name} - check manager logs", 400
    else:
        return f"successfully added {broker_name}", 200

@app.route("/brokers/rm/<broker_name>",methods=["GET"])
def rm_brokers(broker_name):
    if broker_name=='all':
        all_brokers=brokers.list
        for b in all_brokers:
            os.system(f'sudo docker stop {b} && sudo docker rm {b}')
        return "Removed all brokers",200
    else:
        res=os.popen(f'sudo docker stop {broker_name} && sudo docker rm {broker_name}').read()
        if len(res)==0:
            return f"Unable to remove {broker_name} - check manager logs",400
        else:
            return f"Removed {broker_name}", 200

@app.route("/brokers/test/<broker_name>",methods=["GET"])
def test_broker(broker_name):
    res=os.popen(f'bash /mgr/testbroker.sh {broker_name} 5000').read()
    if len(res)==0:
        return f"Unable to test {broker_name} - check manager logs",400
    else:
        return res, 200

#--------------------------------- Handling Topics -------------------------------#
@app.route("/topics",methods=["GET"])
def get_topics():
    return jsonify(brokers.topics),200

@app.route("/topics/add/<topic_name>/<part>",methods=["GET"])
def add_topic(topic_name,part):
    return brokers.add_topic(topic_name,part)

#--------------------------------- Handling Publishers ----------------------------#
@app.route("/producer/register",methods=["POST"])
def producer_registration():
    data=request.get_json()
    topic_name=data["topic"]
    part= data["part"] if "part" in data else 'None'
    if part == 'None': return "No ready for part=None yet", 400
    return brokers.producer_registration(topic_name,part)

























if __name__=='__main__':
    app.run(host='0.0.0.0',port=5000,debug=False)