Distributed Queue with Partitions and broker Manager

<!-- sudo docker run --name some-mysql -p 3306:3306 -e MYSQL_ROOT_PASSWORD=abc -d mysql:latest
sudo docker system prune -a
sudo docker build -f Dockerfile -t broker .
sudo docker run --name broker -p 5000:5000 -d broker:latest

sudo docker run --name broker1 -e BID=broker1 -e PERSIST=yes -p 5000:5000 -d broker:latest

os.system('sudo docker run --name broker1 -e BID=broker1 -e PERSIST=yes -p 5000:5000 -d broker:latest')
os.system('sudo docker stop broker2 && sudo docker rm broker2') -->

sudo docker run --name broker2 --network mynet --network-alias broker2 -e BID=broker2 -e PERSIST=yes  -d broker:latest

curl -XGET "http://localhost:5000/brokers/rm" 
curl -XPOST "http://localhost:5000/add" -d '{"broker_name": "broker1"}' -H "Content-Type: application/json"
curl -XPOST "http://localhost:5000/producer/register" -d '{"topic": "T3", "part": "P2"}' -H "Content-Type: application/json"

curl -XPOST "http://localhost:5000/producer/register" -d '{"topic": "T3"}' -H "Content-Type: application/json"