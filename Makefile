all: 
	sudo docker-compose up -d
clean:
	curl -XGET "http://localhost:5000/brokers/rm" 
	sudo docker-compose down --rmi all