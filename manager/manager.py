from crypt import methods
import os
from flask import Flask, jsonify, request
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

brokers=[]

@app.route("/add",methods=["POST"])
def add_broker():
    data=request.get_json()
    broker_name=data["broker_name"]
    try:
        os.system(f'docker run --name {broker_name} --network mynet --network-alias {broker_name} -e BID={broker_name} -e PERSIST=yes  -d broker:latest')
        brokers.append(broker_name)
        return f"successfully added {broker_name}", 200
    except Exception as e:
        return str(e), 400

@app.route("/brokers",methods=["GET"])
def list_brokers():
    return jsonify(brokers), 200


if __name__=='__main__':
    app.run(host='0.0.0.0',port=5000,debug=False)