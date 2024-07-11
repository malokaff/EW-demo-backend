# python 3.6

import random
import time
import logging
import logging.handlers
import config
import mysql.connector
import time


from paho.mqtt import client as mqtt_client
#from fstring import fstring

sql_pass = config.pwd_mysql

broker = config.ip_mqttbroker
port = 1883
topic = "python/mqtt-pensando"
# generate client ID with pub prefix randomly
id=format(random.randint(0, 1000))
#client_id = fstring('python-mqtt-{random.randint(0, 1000)}')
client_id = 'python-mqtt-' + id
username = config.usr_mqtt
password = config.pwd_mysql
ratio_session = config.ratio_session

def connect_mqtt():
	def on_connect(client, userdata, flags, rc):
		if rc == 0:
			client.connected_flag = True #set flag
			my_logger.info("Connected to MQTT Broker!")
		else:
			my_logger.info("Failed to connect, return code %d\n", rc)

	client = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION1,client_id,False)
	client.username_pw_set(username, password)
	client.on_connect = on_connect
	try:
		client.connect(broker, port)
	except:
		my_logger.exception('Got exception in connect_mqtt function')
	return client
		
def publish(client,msg,topic):
	msg = msg
	mqtt_client.Client.connected_flag = False #create flag in class
	client.loop_start()
	while not client.connected_flag: #wait in loop
		my_logger.info("wait for mqtt connexion")
		time.sleep(1)
	my_logger.info("connexion mqtt ok")
	result = client.publish(topic, msg)
	# result: [0, 1]
	status = result[0]
	if status == 0:
		#print(f"Send `{msg}` to topic `{topic}`")
		msg = format(msg)
		#topic= format(topic)
		my_logger.info("Send " + msg + " to topic "+ topic)
	else:
		#topic= format(topic)
		my_logger.info("Failed to send message to topic " + topic)
	client.loop_stop()    #Stop loop 
	



def run():
	msg_count = 0
	nb_session = 0
	while True:
		time.sleep(1)
		if msg_count > 100:
			msg_count = 0
			time.sleep(1)
		count=format(msg_count)
		now = int( time.time() )
		now_format=format(now)
		msg = now_format +' - message ' + count
		client = connect_mqtt()
		publish(client, msg, "python/mqtt-pensando")
		if nb_session == ratio_session:
			client.disconnect() # disconnect
			nb_session = 0
		#updateValue(msg)
		msg_count += 1
	



if __name__ == '__main__':
	LOG_FILENAME = 'logging-python.log'
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
