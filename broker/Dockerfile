FROM mysql:8.0-debian

COPY deploy.sh /always-initdb.d/
COPY . /bkr
WORKDIR /bkr

RUN apt-get update
RUN apt-get install -y python3
RUN apt-get install -y python3-pip

RUN pip install --upgrade pip
RUN pip install -r requirements.txt
RUN chmod +x /bkr/custom-entry.sh

ENV USER=theuser
RUN adduser --home /home/$USER --disabled-password --gecos GECOS $USER \
  && chsh -s /bin/zsh $USER
RUN touch /bkr/topicLog.db
RUN chown $USER /bkr/topicLog.db

USER $USER
ENV HOME=/home/$USER
ENV MYSQL_ROOT_PASSWORD="abc"

EXPOSE 5000

ENTRYPOINT ["/bkr/custom-entry.sh"]
CMD ["mysqld"]