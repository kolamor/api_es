FROM python:3.7.7
LABEL version="0.1.0"
LABEL stage="beta"

COPY ./requirements.txt /app/requirements.txt
COPY ./start_serve /usr/sbin/start_serve
COPY ./ /app_es
WORKDIR /app_es

RUN pip3 install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt
RUN chmod +x /usr/sbin