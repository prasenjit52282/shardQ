HOST=$1

curl -XGET "http://${HOST}:5000/brokers/add?broker_name=broker1";echo
sleep 20s

# #creating topics
curl -XPOST "http://${HOST}:5000/topics/add" -d '{"topic": "T1", "part": "P1"}' -H "Content-Type: application/json";echo
curl -XPOST "http://${HOST}:5000/topics/add" -d '{"topic": "T1", "part": "P2"}' -H "Content-Type: application/json";echo

curl -XPOST "http://${HOST}:5000/topics/add" -d '{"topic": "T2", "part": "P1"}' -H "Content-Type: application/json";echo
curl -XPOST "http://${HOST}:5000/topics/add" -d '{"topic": "T2", "part": "P2"}' -H "Content-Type: application/json";echo
#consumers
python runconsume.py --id 1 --topics T1:P1 T2:P1 T1:P2 T2:P2 --broker  ${HOST} --log_loc ./test/2P2C &
python runconsume.py --id 2 --topics T1:* T2:* --broker  ${HOST} --log_loc ./test/2P2C &
echo "2 consumers are started"

sleep 5s

#producers
python runproduce.py --id 1 --topics T1:P1 T2:P1 T1:P2 T2:P2 --broker  ${HOST} --log_loc ./test/2P2C &
python runproduce.py --id 2 --topics T1:* T2:* --broker  ${HOST} --log_loc ./test/2P2C &
echo "2 producers are started"

wait