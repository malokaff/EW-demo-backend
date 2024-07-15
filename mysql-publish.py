# python 3.6

import random
import time
import logging
import logging.handlers
import mysql.connector
import time
import config


from paho.mqtt import client as mqtt_client
#from fstring import fstring

mysql_server = config.ip_mysql
port = 1883
topic = "python/mqtt-pensando"
# generate client ID with pub prefix randomly
id=format(random.randint(0, 1000))
#client_id = fstring('python-mqtt-{random.randint(0, 1000)}')
client_id = 'python-mqtt-' + id
username = config.user_mysql
password = config.pwd_mysql
ratio_session = config.ratio_session

def updateValue(value,nb_session):
	try:
		my_logger.info("updateValue: {}" + value)
		conn = mysql.connector.connect(host=mysql_server,user=username,password=password, database="MQTT",connect_timeout=10)
		cursor = conn.cursor()
		#cursor.execute("SET GLOBAL connect_timeout=1")
		cursor.execute("""UPDATE `mqtt-value` SET value='%s' WHERE id='1'""" % (value))
		cursor.execute("commit")
		if nb_session == ratio_session:
			cursor.close()
			nb_session = 0
	except mysql.connector.Error as err:
		my_logger.info("ERROR SQL :Something went wrong in function updateValue: {}", err)
		my_logger.exception('Got exception in updateValue function')


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
		msg = now_format +' - SQL ' + count
		#publish(client, msg, "python/mqtt-pensando")
		updateValue(msg,nb_session)
		msg_count += 1
		
		


if __name__ == '__main__':
	LOG_FILENAME = 'logging-sql.log'
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
