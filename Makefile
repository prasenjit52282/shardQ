all:
	@echo give mode i.e make persist / make inmem to confgure the cluster

persist:
	@sudo docker-compose --env-file ./config/.env.persist up -d

inmem:
	@sudo docker-compose --env-file ./config/.env.inmem up -d

clean:
	@curl -XGET "http://localhost:5000/brokers/rm?broker_name=all" 
	@sudo docker-compose down --rmi all

prune:
	@sudo docker ps -aq | xargs sudo docker stop | xargs sudo docker rm
	@sudo docker system prune -a

restart:
	@sudo docker ps -aq | xargs sudo docker restart