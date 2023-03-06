from flask import Flask, jsonify, request
from manager import Manager
from helper import env_config
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

config=env_config()
mgr=Manager(config['persist'],config['broker_id'])

@app.route("/topics",methods=["GET","POST"])
def handle_topics():
    if request.method=="POST":
        topic_name=request.get_json()["topic_name"]
        response,status=mgr.CreateTopic(topic_name)
        return jsonify(response),status

    elif request.method=="GET":
        response,status=mgr.ListTopics()
        return jsonify(response),status


@app.route("/consumer/register",methods=["POST"])
def consumer_registration():
    topic_name=request.get_json()["topic"]
    response,status=mgr.RegisterConsumer(topic_name)
    return jsonify(response),status


@app.route("/producer/register",methods=["POST"])
def producer_registration():
    topic_name=request.get_json()["topic"]
    response,status=mgr.RegisterProducer(topic_name)
    return jsonify(response),status


@app.route("/producer/produce",methods=["POST"])
def handle_produce():
    data=request.get_json()
    topic_name=data["topic"]
    pub_id=data["producer_id"]
    msg=data["message"]
    response,status=mgr.Enqueue(topic_name,pub_id,msg)
    return jsonify(response),status


@app.route("/consumer/consume",methods=["GET"])
def handle_consume():
    topic_name=request.args.get("topic")
    sub_id=request.args.get("consumer_id")
    response,status=mgr.Dequeue(topic_name,sub_id)
    return jsonify(response),status


@app.route("/size",methods=["GET"])
def get_size():
    topic_name=request.args.get("topic")
    sub_id=request.args.get("consumer_id")
    response,status=mgr.Size(topic_name,sub_id)
    return jsonify(response),status


@app.errorhandler(Exception)
def handle_exception(e):
    return jsonify({"message":"Internal server Error: check params"}),500


if __name__=='__main__':
    app.run(host='0.0.0.0',port=5000,debug=False)