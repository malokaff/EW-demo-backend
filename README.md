# introduction
Small app to simulate east-west trafic in 2 part to demo AMD Pensando technologies
backend part (this repository) and frontend part below
https://github.com/malokaff/EW-demo-frontend

This app require 3x VM :
- 1x VM for frontend 
- 1x VM for backend
- 1x VM for mysql and mosquitto broker server
Backend is running 2 python script to send SQL and MQTT data to mysql/mosquitto VM
Frontend is displaying data on a webpage

# EW-demo-backend
backend script to send MQTT message to broker
The backend server is the simpler one as your just need to install python and the following python module:
-	pip install paho-mqtt

on mqtt/mysql VM, you will need
-	Mysql
-	Apache / php (to use phpMyAdmin)
-	PhpMyAdmin
-	Mosquito (MQTT broker)



