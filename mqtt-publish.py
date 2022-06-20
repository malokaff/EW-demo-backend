# python 3.6

import random
import time
import logging
import logging.handlers
import password

from paho.mqtt import client as mqtt_client
#from fstring import fstring


broker = '10.29.21.15'
port = 1883
topic = "python/mqtt-pensando"
# generate client ID with pub prefix randomly
id=format(random.randint(0, 1000))
#client_id = fstring('python-mqtt-{random.randint(0, 1000)}')
client_id = 'python-mqtt-' + id
username = 'mqtt'
password = password.pwd_mysql

def connect_mqtt():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            client.connected_flag = True #set flag
            my_logger.info("Connected to MQTT Broker!")
        else:
            my_logger.info("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(client_id,False)
    client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


def publish(client):
    msg_count = 0
    while True:
        if msg_count > 100:
           msg_count = 0
        time.sleep(1)
        #msg = f"messages: {msg_count}"
        count=format(msg_count)
        msg = 'message ' + count
        result = client.publish(topic, msg)
        # result: [0, 1]
        status = result[0]
        if status == 0:
            #print(f"Send `{msg}` to topic `{topic}`")
            msg = format(msg)
            #topic= format(topic)
            #print("Send " + msg + " to topic "+ topic)
        else:
            #topic= format(topic)
            my_logger.info("Failed to send message to topic " + topic)
        msg_count += 1


def run():
	mqtt_client.Client.connected_flag = False #create flag in class
	client = connect_mqtt()
	client.loop_start()
	while not client.connected_flag: #wait in loop
		my_logger.info("In wait loop")
		time.sleep(1)
	my_logger.info("in Main Loop")
	publish(client)
	client.loop_stop()    #Stop loop 
	client.disconnect() # disconnect



if __name__ == '__main__':
	LOG_FILENAME = '/home/pensando/logging-python.log'
	# definition du logging
	my_logger = logging.getLogger('MQTT_PYTHON')
	my_logger.setLevel(logging.DEBUG)

# definition de la taille des fichiers de logs, de la rotation et du format
	handler = logging.handlers.RotatingFileHandler(LOG_FILENAME, maxBytes=256000, backupCount=5)
	# create formatter
	formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
	# add formatter to handler
	handler.setFormatter(formatter)
	my_logger.addHandler(handler)
	try:
		run()
	except:
		my_logger.exception('Got exception on main handler')
		raise
