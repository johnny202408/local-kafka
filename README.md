# Kafka running on local containers


launch the kafka containers:  
```docker-compose up -d```  
    
please make sure to normally stop the kafka containers before machine shutdown / reboot:  
```docker-compose stop```  


add this entry to /etc/hosts
127.0.0.1 kafka

### Usage  
#### Start the stack:  
```docker-compose up -d```
#### Access Kafka UI:  
Visit http://localhost:8080.

#### Access Kafdrop:  
Visit http://localhost:9000.

prepare the python virtual environment:  
```
python3 -m venv venv
./venv/bin/activate
pip install -U -r requirements.txt
```  

generate json sample data:  
```python3 src/generate_samples.py```

stream one sample file into the kafka cluster:  
```python3 src/produce_message.py```

read one message from the kafka cluster:  
```python3 src/read_one.py```

---

written by me

