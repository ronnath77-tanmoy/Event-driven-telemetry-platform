FROM python:3.12-slim

WORKDIR /consumer

COPY consumer/requirements.txt /consumer/requirements.txt
RUN pip install --no-cache-dir -r /consumer/requirements.txt

COPY consumer/ /consumer/

