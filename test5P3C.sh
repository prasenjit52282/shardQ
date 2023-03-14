HOST=$1

curl -XGET "http://${HOST}:5000/brokers/add?broker_name=broker1";echo
curl -XGET "http://${HOST}:5000/brokers/add?broker_name=broker2";echo
curl -XGET "http://${HOST}:5000/brokers/add?broker_name=broker3";echo
sleep 20s

# #creating topics
curl -XPOST "http://${HOST}:5000/topics/add" -d '{"topic": "T1", "part": "P1"}' -H "Content-Type: application/json";echo
curl -XPOST "http://${HOST}:5000/topics/add" -d '{"topic": "T1", "part": "P2"}' -H "Content-Type: application/json";echo

curl -XPOST "http://${HOST}:5000/topics/add" -d '{"topic": "T2", "part": "P1"}' -H "Content-Type: application/json";echo
curl -XPOST "http://${HOST}:5000/topics/add" -d '{"topic": "T2", "part": "P2"}' -H "Content-Type: application/json";echo

curl -XPOST "http://${HOST}:5000/topics/add" -d '{"topic": "T3", "part": "P1"}' -H "Content-Type: application/json";echo
curl -XPOST "http://${HOST}:5000/topics/add" -d '{"topic": "T3", "part": "P2"}' -H "Content-Type: application/json";echo
curl -XPOST "http://${HOST}:5000/topics/add" -d '{"topic": "T3", "part": "P3"}' -H "Content-Type: application/json";echo

#consumers
python runconsume.py --id 1 --topics T1:* T2:* T3:* --broker  ${HOST} --log_loc ./test/5P3C &
python runconsume.py --id 2 --topics T2:* T1:P1 --broker  ${HOST} --log_loc ./test/5P3C &
python runconsume.py --id 3 --topics T3:* T1:P2 --broker  ${HOST} --log_loc ./test/5P3C &
echo "3 consumers are started"

sleep 5s

#producers
python runproduce.py --id 1 --topics T1:* T3:P2 --broker  ${HOST} --log_loc ./test/5P3C &
python runproduce.py --id 2 --topics T1:P2 T2:P1 --broker  ${HOST} --log_loc ./test/5P3C &
python runproduce.py --id 3 --topics T2:* --broker  ${HOST} --log_loc ./test/5P3C &
python runproduce.py --id 4 --topics T2:P2 --broker  ${HOST} --log_loc ./test/5P3C &
python runproduce.py --id 5 --topics T3:* --broker  ${HOST} --log_loc ./test/5P3C &
echo "5 producers are started"

wait