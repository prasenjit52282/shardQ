import os
from flask import Flask, jsonify, request
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

brokers=['broker0']
persist=os.environ['PERSIST']

@app.route("/brokers",methods=["GET"])
def list_brokers():
    return jsonify(brokers), 200

@app.route("/brokers/add/<broker_name>",methods=["GET"])
def add_broker(broker_name):
    res=os.popen(f'sudo docker run --name {broker_name} --network mynet --network-alias {broker_name} -e BID={broker_name} -e PERSIST={persist}  -d broker:latest').read()
    if len(res)==0:
        return f"Unable to add {broker_name} - check manager logs", 400
    else:
        brokers.append(broker_name)
        return f"successfully added {broker_name}", 200

@app.route("/brokers/rm/<broker_name>",methods=["GET"])
def rm_brokers(broker_name):
    if broker_name=='all':
        for b in brokers:
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





if __name__=='__main__':
    app.run(host='0.0.0.0',port=5000,debug=False)