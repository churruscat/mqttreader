#!/usr/bin/env python
#  
# read an mqtt broker, 
#        Save into an influxdb database
#        and, optional, resend the message to another mqtt broker
#read from mqttdbs.conf, which format is:

import argparse
import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient
import  json, math
from datetime import datetime
from time import time, altzone ,sleep
import os, socket, sys, subprocess, logging
from configparser import ConfigParser

dbport=8086
dbserver="influxdb"
dbname="iotdb"
dbuser=''
dbpassword=''
mqttbroker="mosquitto"
tipoLogging=['none','debug', 'info', 'warning', 'error' ,'critical']
clientes={
	"reader":{"clientId":"c_reader","broker":"127.0.0.1","port":1883,"name":"blank",
              "userid":"","password":"",
	          "subscribe_topic":"#","activo":True},
	"sender":{"clientId":"c_sender","broker":"","port":1883,"name":"blank",
              "userid":"","password":"",
	          "publish_topic":"meteo/envia",
			  "activo":False},
}

def db_insert(body):
	response=False
	try:
		client = InfluxDBClient(dbserver, dbport, dbuser, dbpassword, dbname)
		logging.info("connected to database")
	except:
		logging.warning("error connecting to database")
		logging.warning("host. %s \t port: %s \t, user:%s\t, password:%s\t, dbname:%s",dbserver, dbport, dbuser, dbpassword, dbname)
		return
	body1=json.loads(body)
	try:
		for clave in body1['fields'].copy():
			if (math.isnan(float( body1['fields'][clave]))):
				logging.warning("must delete : "+str(clave)+' = '+str(body1['fields'][clave]))
				del body1['fields'][clave]
			punto=json.loads('['+json.dumps(body1)+']')        
	except:
		logging.warning("error en registro: "+str(body))

	try:
		response=client.write_points(punto)
		logging.warning("Record stored:    "+str(response)+' ->'+str(punto))
	except:
		logging.warning("record discarded :"+str(response)+' ->'+str(punto))

# Funciones de Callback
def on_connect(mqttCliente, userdata, flags, rc):
	logging.info("Connected to broker")

 
def on_subscribe(mqttCliente, userdata, mid, granted_qos):
	logging.info("Subscribed OK; message "+str(mid)+"   qos= "+ str(granted_qos))
	sleep(1)

def on_disconnect(mqttCliente, userdata, rc):
	logging.info("Disconnected, rc= "+str(rc))    
	reconectate(mqttCliente)   

def on_publish(mqttCliente, userdata, mid):
	logging.info("message published "+ str(mid))   

def reconectate(mqttCliente):
	conectado=False
	while (not conectado):
		try:
			logging.info("reconnect  " )
			mqttCliente.reconnect()
			conectado=True
			sleep(2)
		except Exception as exErr:
			if hasattr(exErr, 'message'):
				logging.warning("Connection error 1 = "+ exErr.message)
			else:
				logging.warning("Connection error 2 = "+exErr)     
			sleep(30)

def arrancaEscritor(cola, puerto):
		global clientes
		clientes["sender"]["broker"]=cola
		clientes["sender"]["cliente"] = mqtt.Client( clean_session=True) 
		clientes["sender"]["cliente"].on_message    = on_message
		clientes["sender"]["cliente"].on_connect    = on_connect
		clientes["sender"]["cliente"].on_publish    = on_publish
		if (clientes["sender"]["userid"]!=''):
			clientes["sender"]["cliente"].username_pw_set(clientes["sender"]["userid"] , password=clientes["sender"]["password"])
		clientes["sender"]["cliente"].connect(cola,puerto)
		clientes["sender"]["cliente"].reconnect_delay_set(60, 600) 
		logging.info("SENDER=")
		logging.info(clientes["sender"])
	
def on_message(mqttCliente, userdata, message):
	global clientes
	try:
		medida=message.payload["measurement"]
		crudo=False
	except:
		logging.info("viene directamente de un sensor")
		crudo=True
		pass	
	#If it comes directly fom a sensor,Inadd meassurement and time
	if crudo:
		measurement=message.topic.split('/')[0]
		secs,usecs=divmod(time(),1)
		while (usecs<0.1):
			logging.info(str(usecs))
			usecs=usecs*10
		payload=json.loads(message.payload.decode())
		dato='{"measurement":"'+measurement+'","time":'+str(int(secs))+str(int(usecs*1000000000))+\
				',"fields":'+json.dumps(payload[0])+',"tags":'+json.dumps(payload[1])+'}'
	else :
		dato=message.payload[0]
		
	logging.info(dato)
	logging.info("salva en influxdb")
	db_insert(dato)
	if len(clientes["sender"]["broker"])>2:
		logging.info("preparo para enviar a mqtt remoto")
		#logging.info(measurement, json.dumps(payload))
		try:
			result, mid = clientes["sender"]["cliente"].publish(message.topic, json.dumps(payload), 1, True )
			logging.info("sent "+str(result))
		except Exception as exErr:
			if hasattr(exErr, 'message'):
				logging.warning("Connection error type 1 = "+ exErr.message)
			else:
				logging.warning("Connection error type 2 = "+exErr)				   
			sleep(30)
			arrancaEscritor(clientes["sender"]["broker"],clientes["sender"]["port"])
		#logging.info("sent to remote mqtt, result",result)

if __name__ == '__main__':
	parser = ConfigParser()
	parser.read('/etc/mqttdbs/mqttdbs.conf')
	if parser.has_section("mqtt_broker_read"):
		if parser.has_option("mqtt_broker_read","address"):
			clientes["reader"]["broker"]=parser.get("mqtt_broker_read","address")
		if parser.has_option("mqtt_broker_read","port"):
			clientes["reader"]["port"]=int(parser.get("mqtt_broker_read","port"))
		if parser.has_option("mqtt_broker_read","userid"):
			clientes["reader"]["userid"]=parser.get("mqtt_broker_read","userid")
		if parser.has_option("mqtt_broker_read","password"):
			clientes["reader"]["password"]=parser.get("mqtt_broker_read","password")
		if parser.has_option("mqtt_broker_read","subscribe_topic"):
			clientes["reader"]["subscribe_topic"]=parser.get("mqtt_broker_read","subscribe_topic")			

	if parser.has_section("mqtt_broker_send"):
		if parser.has_option("mqtt_broker_send","address"):
			clientes["sender"]["broker"]=parser.get("mqtt_broker_send","address")
		if parser.has_option("mqtt_broker_send","port"):
			clientes["sender"]["port"]=parser.get("mqtt_broker_send","port")
		if parser.has_option("mqtt_broker_send","userid"):
			clientes["sender"]["userid"]=parser.get("mqtt_broker_send","userid")
		if parser.has_option("mqtt_broker_send","password"):
			clientes["sender"]["password"]=parser.get("mqtt_broker_send","password")
		if parser.has_option("mqtt_broker_send","publish_topic"):
			clientes["sender"]["publish_topic"]=parser.get("mqtt_broker_send","publish_topic")

	if parser.has_section("log_level"):
		if parser.has_option("log_level","log_level"):	
			loglevel=parser.get("log_level","log_level")
		else:
			loglevel='warning'
	logging.basicConfig(stream=sys.stderr, format = '%(asctime)-15s  %(message)s', level=loglevel.upper())	
	if parser.has_section("database"):
		if parser.has_option("database","address"):
			dbserver=parser.get("database","address")
		if parser.has_option("database","dbname"):
			dbname=parser.get("database","dbname")
		if parser.has_option("database","userid"):
			dbuser=parser.get("database","userid")
		if parser.has_option("database","password"):
			dbpassword=parser.get("database","password")
	                         
	logging.info("IP addr: "+dbserver)

	## Define mqtt reader
	logging.info(clientes["reader"])
	print(clientes["reader"])
	clientes["reader"]["cliente"] = mqtt.Client(clientes["reader"]["clientId"],clean_session=False) 
	clientes["reader"]["cliente"].on_message    = on_message
	clientes["reader"]["cliente"].on_connect    = on_connect
	clientes["reader"]["cliente"].on_subscribe  = on_subscribe
	clientes["reader"]["cliente"].on_disconnect = on_disconnect
	if (clientes["reader"]["userid"]!=''):
		clientes["reader"]["cliente"].username_pw_set(clientes["reader"]["userid"] , password=clientes["reader"]["password"])
	clientes["reader"]["cliente"].connect(clientes["reader"]["broker"],clientes["reader"]["port"])
	clientes["reader"]["cliente"].reconnect_delay_set(30, 120) 
	clientes["reader"]["cliente"].subscribe(clientes["reader"]["subscribe_topic"],qos=1)
	logging.info("READER")
	logging.info(clientes["reader"])

	#and, if configured, client gateway that will resend messages to remote queue
	clientes["reader"]["cliente"].loop_start()                 #start the loop
	if (clientes["sender"]["broker"]!=''):
		arrancaEscritor(clientes["sender"]["broker"],clientes["sender"]["port"])
		clientes["sender"]["cliente"].loop_start()                 #start the loop

	try:
		while True:
			sleep(5)
	except (KeyboardInterrupt, SystemExit): #when you press ctrl+c
		print("\nKilling Thread...")
		clientes["reader"]["cliente"].loop_stop()                 #start the loop
		clientes["sender"]["cliente"].loop_stop()      
	print("Done.\nExiting.")

