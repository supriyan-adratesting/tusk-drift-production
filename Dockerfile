FROM ubuntu:22.04

RUN rm /bin/sh && ln -s /bin/bash /bin/sh

# Install system dependencies
RUN apt-get update && apt-get install -y iputils-ping net-tools nginx supervisor python3-pip python3-dev default-libmysqlclient-dev build-essential libssl-dev libffi-dev python3-setuptools python3-venv pkg-config

COPY second_careers_project /home/second_careers_project

RUN python3.10 -m venv /home/second_careers_project/venv_secondcareers

RUN /home/second_careers_project/venv_secondcareers/bin/pip install -r /home/second_careers_project/requirements.txt

RUN useradd --no-create-home nginx

RUN unlink /etc/nginx/sites-enabled/default

COPY server-conf/app /etc/nginx/sites-available/app
COPY server-conf/supervisord.conf /etc/supervisor/supervisord.conf

COPY server-conf/start.sh /usr/local/bin/start.sh

RUN chmod +x /usr/local/bin/start.sh

RUN ln -s /etc/nginx/sites-available/app /etc/nginx/sites-enabled

EXPOSE 80:80

WORKDIR /home/second_careers_project

# CMD ["/usr/bin/supervisord","-n","-c","/etc/supervisor/supervisord.conf"]
CMD ["/usr/local/bin/start.sh"]