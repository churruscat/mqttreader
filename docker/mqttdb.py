#!/usr/bin/env python
#  
# Lee de mqtt 
#        Salva en una bbdd influxdb
#        y resend a otro mqtt
import argparse
import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient
import  json, math
from datetime import datetime
from time import time, altzone ,sleep
import os, socket, sys, logging

dbport=8086
tipoLogging=['none','debug', 'info', 'warning', 'error' ,'critical']
clientes={
	"lector":{"clientId":"Cliente-Lector","broker":"127.0.0.1","port":1883,"name":"blank","subscribe_topic":"#","publishTopic":"meteo/envia",
						"updateTopic":"meteo/update","activo":True},
	"escritor":{"clientId":"Cliente-Escritor","broker":"","port":1883,"name":"blank","subscribe_topic":"","publishTopic":"meteo/envia",
						"updateTopic":"topic/update","activo":False},
}

def get_Host_name_IP(): 
	try: 
		host_name = socket.gethostname() 
		host_ip = socket.gethostbyname(host_name) 
		logging.info("Hostname :  " + host_name+"\t IP: "+host_ip) 
		return(host_ip)
	except: 
		return(False)

def db_insert(body):
	response=False
	try:
		client = InfluxDBClient(hostIPAddr, dbport, dbuser, dbpassword, dbname)
		logging.info("connected to database")
	except:
		logging.warning("error connecting to database")
		logging.warning("host. %s \t port: %s \t, user:%s\t, password:%s\t, dbname:%s",hostIPAddr, dbport, dbuser, dbpassword, dbname)
		return
	#try:
	body1=json.loads(body)
	try:
		for clave in body1['fields'].copy():
			if (math.isnan(float( body1['fields'][clave]))):
				logging.warning("hay que borrar :",clave+'='+str(body1['fields'][clave]))
				del body1['fields'][clave]
			punto=json.loads('['+json.dumps(body1)+']')        
	#logging.warning("punto: "+str(punto))
	#response = client.write_points(json.loads('['+body+']'))
	except:
		logging.warning("error en registro: "+str(body))

	try:
		response=client.write_points(punto)
		logging.warning("Record stored:    "+str(response)+' ->'+str(punto))
	except:
		logging.warning("record discarded :"+str(response)+' ->'+str(punto))
	'''		
	i=0
	while (response==False and i<5):
		response = client.write_points(punto)
		logging.warning("retry connection to db. Response: "+str(response))
		i+=1
		sleep(30)
		if i<6:
		logging.info("Record stored: "+str(response))
	else:
	#except:
	'''	

# Funciones de Callback
def on_connect(mqttCliente, userdata, flags, rc):
	logging.info("Connected to broker")

 
def on_subscribe(mqttCliente, userdata, mid, granted_qos):
	logging.info("Suscrito OK; mensaje "+str(mid)+"   qos= "+ str(granted_qos))
	sleep(1)

def on_disconnect(mqttCliente, userdata, rc):
	logging.info("Se ha desconectado, rc= "+str(rc))    
	reconectate(mqttCliente)   

def on_publish(mqttCliente, userdata, mid):
	logging.info("publicado el mensaje "+ str(mid))   

def reconectate(mqttCliente):
	conectado=False
	while (not conectado):
		try:
			logging.info("Me reconecto  " )
			mqttCliente.reconnect()
			conectado=True
			sleep(2)
		except Exception as exErr:
			if hasattr(exErr, 'message'):
				logging.warning("Error de conexion1 = "+ exErr.message)
			else:
				logging.warning("Error de conexion2 = "+exErr)     
			sleep(30)

def arrancaEscritor(cola, puerto):
		global clientes
		clientes["escritor"]["broker"]=cola
		clientes["escritor"]["cliente"] = mqtt.Client( clean_session=True) 
		clientes["escritor"]["cliente"].on_message    = on_message
		clientes["escritor"]["cliente"].on_connect    = on_connect
		clientes["escritor"]["cliente"].on_publish    = on_publish
		clientes["escritor"]["cliente"].connect(cola,puerto)
		#mqttEscritor.username_pw_set(username , password=token)
		clientes["escritor"]["cliente"].reconnect_delay_set(60, 600) 
		logging.info("ESCRITOR=")
		logging.info(clientes["escritor"])
	
def on_message(mqttCliente, userdata, message):
	#result, mid = clientes["escritor"]["cliente"].publish(message.topic, message, 1, True )
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
	if len(clientes["escritor"]["broker"])>2:
		logging.info("preparo para enviar a mqtt remoto")
		#logging.info(measurement, json.dumps(payload))
		try:
			result, mid = clientes["escritor"]["cliente"].publish(message.topic, json.dumps(payload), 1, True )
			logging.info("sent "+str(result))
		except Exception as exErr:
			if hasattr(exErr, 'message'):
				logging.warning("Connection error type 1 = "+ exErr.message)
			else:
				logging.warning("Connection error type 2 = "+exErr)				   
			sleep(30)
			arrancaEscritor(clientes["escritor"]["broker"],clientes["escritor"]["port"])
		#logging.info("sent to remote mqtt, result",result)

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Lee de mqtt y lo salva en database')
	parser.add_argument('-q', '--queue', nargs='?', default='',
						help='mqtt remote queue.')
	parser.add_argument('-u', '--user', nargs='?', default='',
						help='mqtt user.')
	parser.add_argument('-p', '--password', nargs='?', default='',
						help='mqtt Password.')														
	parser.add_argument('-v', '--verbose', nargs='?',
		choices=['none','debug', 'info', 'warning', 'error' ,'critical'],  default='warning',
							help='Verbose level. Default: warning')

	parser.add_argument('-s', '--dbuser', nargs='?', default='',
						help='Influxdb User name.')
	parser.add_argument('-a', '--dbpassword', nargs='?', default='',
						help='Influxdb Password.')
	parser.add_argument('-d', '--dbname', nargs='?', default='iotdb',
							help='Database name. Default: iotdb')                                                         

	args = parser.parse_args()
	dbpassword=args.dbpassword
	dbuser=args.dbuser
	dbname=args.dbname

	logging.basicConfig(stream=sys.stderr, format = '%(asctime)-15s  %(message)s', level=args.verbose.upper())
	hostIPAddr=get_Host_name_IP()
	logging.info("IP addr: "+hostIPAddr)

	## Defino el cliente mqtt para leer de la cola local 
	#clientes["lector"]["cliente"].username_pw_set(args.user,args.password)
	#clientes["lector"]["cliente"] = mqtt.Client(clientes["lector"]["clientId"], clean_session=True) 
	clientes["lector"]["broker"] = hostIPAddr
	clientes["lector"]["cliente"] = mqtt.Client(clientes["lector"]["clientId"],clean_session=False) 
	clientes["lector"]["cliente"].on_message    = on_message
	clientes["lector"]["cliente"].on_connect    = on_connect
	clientes["lector"]["cliente"].on_subscribe  = on_subscribe
	clientes["lector"]["cliente"].on_disconnect = on_disconnect
	clientes["lector"]["cliente"].on_publish    = on_publish
	clientes["lector"]["cliente"].connect(clientes["lector"]["broker"],clientes["lector"]["port"])
	clientes["lector"]["cliente"].reconnect_delay_set(30, 120) 
	clientes["lector"]["cliente"].subscribe(clientes["lector"]["subscribe_topic"],qos=1)
	logging.info("LECTOR")
	logging.info(clientes["lector"])

	#y ahora el cliente que reenvia los mensajes a una cola remota (si q <>'')
	clientes["lector"]["cliente"].loop_start()                 #start the loop
	if (args.queue!=''):
		arrancaEscritor(args.queue,clientes["escritor"]["port"])
		clientes["escritor"]["cliente"].loop_start()                 #start the loop

	try:
		while True:
			sleep(5)
	except (KeyboardInterrupt, SystemExit): #when you press ctrl+c
		print("\nKilling Thread...")
		clientes["lector"]["cliente"].loop_stop()                 #start the loop
		clientes["escritor"]["cliente"].loop_stop()      
	print("Done.\nExiting.")
