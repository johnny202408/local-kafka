#!/bin/sh
echo Local Kafka -> local elasticsearch // Sanity check
echo make sure python virtual env is activated:
echo . venv/bin/activate
echo .
echo ---------------------------
echo writing message on kafka
echo ---------------------------
src/produce_message.py -n 2
echo .
sleep 2
echo ---------------------------
echo check on elasticsearch
echo ---------------------------
echo alias:
curl -X GET "http://localhost:9200/_cat/aliases?v" | grep widg
echo index:
curl -X GET "http://localhost:9200/_cat/indices?v" | grep widg
echo ---------------------------

