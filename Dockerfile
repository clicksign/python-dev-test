FROM python:3.8

RUN apt-get update && apt-get install -y \
    build-essential && \
    rm -rf /var/lib/apt/lists/*
	
RUN mkdir -p /usr/src/app

WORKDIR /usr/src/app

#COPY requirements.txt /usr/src/app/
COPY . /usr/src/app

RUN pip3 install --no-cache-dir -r requirements.txt --upgrade

CMD [ "python", "trigger.py" ]