#!/bin/sh
echo ---------------------------
echo Local Kafka // Sanity check
echo ---------------------------
echo .
echo .
echo ---------------------------
echo writing message on kafka
echo ---------------------------
src/produce_message.py
echo .
echo ---------------------------
echo reading message from kafka
echo ---------------------------
src/read_one.py
echo .
echo ---------------------------
echo sanity check complete
echo visit Kafka UI at http://localhost:8080/
echo ---------------------------

