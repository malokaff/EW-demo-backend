# python 3.6

import random
import time
import logging
import logging.handlers
import password
import mysql.connector
import time


from paho.mqtt import client as mqtt_client
#from fstring import fstring

sql_pass = password.pwd_mysql

broker = '10.29.21.15'
port = 1883
topic = "python/mqtt-pensando"
# generate client ID with pub prefix randomly
id=format(random.randint(0, 1000))
#client_id = fstring('python-mqtt-{random.randint(0, 1000)}')
client_id = 'python-mqtt-' + id
username = 'mqtt'
password = password.pwd_mysql

def updateValue(value):
	try:
		my_logger.info("updateValue: {}" + value)
		conn = mysql.connector.connect(host="10.29.21.15",user="root",password=sql_pass, database="MQTT")
		cursor = conn.cursor()
		global_connect_timeout = 'SET GLOBAL connect_timeout=10'
		global_wait_timeout = 'SET GLOBAL connect_timeout=10'
		global_interactive_timeout = 'SET GLOBAL connect_timeout=10'
		cursor.execute(global_connect_timeout) 
		cursor.execute(global_wait_timeout)
		cursor.execute(global_interactive_timeout)
		cursor.execute("""UPDATE `mqtt-value` SET value='%s' WHERE id='1'""" % (value))
		cursor.execute("commit")
		cursor.close()
	except mysql.connector.Error as err:
		my_logger.info("ERROR SQL :Something went wrong in function updateValue: {}", err)
		my_logger.exception('Got exception in updateValue function')


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
		
def publish(client,msg,topic):
	msg = msg
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



def run():
	msg_count = 0
	while True:
		time.sleep(1)
		if msg_count > 100:
			msg_count = 0
			time.sleep(1)
		count=format(msg_count)
		now = int( time.time() )
		now_format=format(now)
		msg = now_format +' - SQL ' + count
		#publish(client, msg, "python/mqtt-pensando")
		updateValue(msg)
		msg_count += 1
		
		


if __name__ == '__main__':
	LOG_FILENAME = '/home/pensando/logging-sql.log'
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
