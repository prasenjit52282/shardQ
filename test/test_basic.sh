curl -XPOST "http://localhost:5000/producer/register" -d '{"topic": "T3", "part": "P1"}' -H "Content-Type: application/json"; echo
curl -XPOST "http://localhost:5001/consumer/register" -d '{"topic": "T3", "part": "P1"}' -H "Content-Type: application/json"; echo

curl -XPOST "http://localhost:5000/producer/produce" -d '{"producer_id": "0", "message": "Bye1"}' -H "Content-Type: application/json"; echo
curl -XPOST "http://localhost:5000/producer/produce" -d '{"producer_id": "0", "message": "Bye2"}' -H "Content-Type: application/json"; echo

curl -XGET "http://localhost:5001/consumer/consume?consumer_id=0"; echo


curl -XPOST "http://localhost:5000/producer/produce" -d '{"producer_id": "0", "message": "Bye3"}' -H "Content-Type: application/json"; echo
curl -XPOST "http://localhost:5000/producer/produce" -d '{"producer_id": "0", "message": "Bye4"}' -H "Content-Type: application/json"; echo
curl -XPOST "http://localhost:5000/producer/produce" -d '{"producer_id": "0", "message": "Bye5"}' -H "Content-Type: application/json"; echo
curl -XPOST "http://localhost:5000/producer/produce" -d '{"producer_id": "0", "message": "Hello1"}' -H "Content-Type: application/json"; echo

curl -XGET "http://localhost:5001/consumer/consume?consumer_id=0"; echo
curl -XGET "http://localhost:5001/consumer/consume?consumer_id=0"; echo
curl -XGET "http://localhost:5001/consumer/consume?consumer_id=0"; echo


curl -XPOST "http://localhost:5000/producer/produce" -d '{"producer_id": "0", "message": "Hello2"}' -H "Content-Type: application/json"; echo
curl -XPOST "http://localhost:5000/producer/produce" -d '{"producer_id": "0", "message": "Hello3"}' -H "Content-Type: application/json"; echo
curl -XPOST "http://localhost:5000/producer/produce" -d '{"producer_id": "0", "message": "Hello4"}' -H "Content-Type: application/json"; echo

curl -XGET "http://localhost:5001/consumer/consume?consumer_id=0"; echo
curl -XGET "http://localhost:5001/consumer/consume?consumer_id=0"; echo

curl -XGET "http://localhost:5001/size?consumer_id=0"; echo

curl -XGET "http://localhost:5001/consumer/consume?consumer_id=0"; echo
curl -XGET "http://localhost:5001/consumer/consume?consumer_id=0"; echo
curl -XGET "http://localhost:5001/consumer/consume?consumer_id=0"; echo
curl -XGET "http://localhost:5001/consumer/consume?consumer_id=0"; echo
curl -XGET "http://localhost:5001/consumer/consume?consumer_id=0"; echo
curl -XGET "http://localhost:5001/consumer/consume?consumer_id=0"; echo

# testing rr policy
curl -XGET "http://localhost:5000/brokers/add?broker_name=broker1";echo
sleep 20s

curl -XPOST "http://localhost:5000/topics/add" -d '{"topic": "T1", "part": "P1"}' -H "Content-Type: application/json"; echo
curl -XPOST "http://localhost:5000/topics/add" -d '{"topic": "T1", "part": "P2"}' -H "Content-Type: application/json"; echo
curl -XPOST "http://localhost:5000/topics/add" -d '{"topic": "T1", "part": "P3"}' -H "Content-Type: application/json"; echo


curl -XGET "http://localhost:5000/brokers";echo
curl -XGET "http://localhost:5000/topics";echo

curl -XPOST "http://localhost:5000/producer/register" -d '{"topic": "T1"}' -H "Content-Type: application/json"; echo
curl -XPOST "http://localhost:5001/consumer/register" -d '{"topic": "T1"}' -H "Content-Type: application/json"; echo

curl -XPOST "http://localhost:5000/producer/produce" -d '{"producer_id": "1", "message": "P1"}' -H "Content-Type: application/json"; echo
curl -XPOST "http://localhost:5000/producer/produce" -d '{"producer_id": "1", "message": "P2"}' -H "Content-Type: application/json"; echo
curl -XPOST "http://localhost:5000/producer/produce" -d '{"producer_id": "1", "message": "P3"}' -H "Content-Type: application/json"; echo
curl -XPOST "http://localhost:5000/producer/produce" -d '{"producer_id": "1", "message": "P4"}' -H "Content-Type: application/json"; echo
curl -XPOST "http://localhost:5000/producer/produce" -d '{"producer_id": "1", "message": "P5"}' -H "Content-Type: application/json"; echo
curl -XPOST "http://localhost:5000/producer/produce" -d '{"producer_id": "1", "message": "P6"}' -H "Content-Type: application/json"; echo
curl -XPOST "http://localhost:5000/producer/produce" -d '{"producer_id": "1", "message": "P7"}' -H "Content-Type: application/json"; echo
curl -XPOST "http://localhost:5000/producer/produce" -d '{"producer_id": "1", "message": "P8"}' -H "Content-Type: application/json"; echo
curl -XPOST "http://localhost:5000/producer/produce" -d '{"producer_id": "1", "message": "P9"}' -H "Content-Type: application/json"; echo
curl -XPOST "http://localhost:5000/producer/produce" -d '{"producer_id": "1", "message": "P10"}' -H "Content-Type: application/json"; echo
curl -XPOST "http://localhost:5000/producer/produce" -d '{"producer_id": "1", "message": "P11"}' -H "Content-Type: application/json"; echo

curl -XGET "http://localhost:5001/size?consumer_id=1"; echo

curl -XGET "http://localhost:5001/consumer/consume?consumer_id=1"; echo
curl -XGET "http://localhost:5001/consumer/consume?consumer_id=1"; echo
curl -XGET "http://localhost:5001/consumer/consume?consumer_id=1"; echo
curl -XGET "http://localhost:5001/consumer/consume?consumer_id=1"; echo
curl -XGET "http://localhost:5001/consumer/consume?consumer_id=1"; echo
curl -XGET "http://localhost:5001/consumer/consume?consumer_id=1"; echo
curl -XGET "http://localhost:5001/consumer/consume?consumer_id=1"; echo
curl -XGET "http://localhost:5001/consumer/consume?consumer_id=1"; echo
curl -XGET "http://localhost:5001/consumer/consume?consumer_id=1"; echo
curl -XGET "http://localhost:5001/consumer/consume?consumer_id=1"; echo
curl -XGET "http://localhost:5001/consumer/consume?consumer_id=1"; echo
curl -XGET "http://localhost:5001/consumer/consume?consumer_id=1"; echo