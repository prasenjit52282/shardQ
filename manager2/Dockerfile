FROM mysql:8.0-debian

COPY deploy.sh /always-initdb.d/
COPY . /mgr
WORKDIR /mgr

RUN apt-get update
RUN apt-get install -y python3
RUN apt-get install -y python3-pip

RUN pip install --upgrade pip
RUN pip install -r requirements.txt
RUN chmod +x /mgr/custom-entry.sh

ENV MYSQL_ROOT_PASSWORD="abc"

EXPOSE 5000

ENTRYPOINT ["/mgr/custom-entry.sh"]
CMD ["mysqld"]