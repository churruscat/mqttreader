#!/usr/bin/env python
#  
# Lee de mqtt 
#        Salva en una bbdd influxdb
#        y resend a otro mqtt
import argparse
import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient
import  json
from datetime import datetime
from time import time, sleep, altzone
import os, socket, sys, logging

dbport=8086
tipoLogging=['none','debug', 'info', 'warning', 'error' ,'critical']
clientes={
	"lector":{"clientId":"Cliente-Lector","broker":"127.0.0.1","port":1883,"name":"blank","sub_topic":"/Subtopic","publishTopic":"GPS/envia",
						"updateTopic":"topic/update","activo":True},
	"escritor":{"clientId":"Cliente-Escritor","broker":"canmorras.duckdns.org","port":1883,"name":"blank","sub_topic":"Subtopic","publishTopic":"GPS/envia",
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
	logging.warning("body:"+body)	
	try:
		'''for clave in body1['fields'].copy():
			if (math.isnan(float( body1['fields'][clave]))):
				logging.warning("hay que borrar :",clave+'='+str(body1['fields'][clave]))
				del body1['fields'][clave]
		punto=json.loads('['+json.dumps(body1)+']')        
		logging.warning("punto: "+punto)
		'''
		response = client.write_points(json.loads('['+body+']'))
		while(response==False):
			response = client.write_points(punto)
			logging.warning("retry connection to db. Response: "+str(response))
			sleep(60)
		logging.info("Record stored: "+str(response))
	except:
		logging.warning("record discarded :"+str(response)+' ->'+body)

# Funciones de Callback
def on_connect(mqttCliente, userdata, flags, rc):
	logging.info("Connected to broker")
	mqttCliente.subscribe("#",qos=1)
	'''global conectado
	print("Codigo de retorno de connect rc: "+str(rc))
	conectado=True
	if rc==0:
		print("Me suscribo al primero obligatorio  " + host )
		mqttCliente.subscribe(mqttCliente["updateTopic"], 1)
	'''	
 
def on_subscribe(mqttCliente, userdata, mid, granted_qos):
	logging.info("Suscrito OK; mensaje "+str(mid)+"qos "+ str(granted_qos))
	time.sleep(1)

def on_disconnect(mqttCliente, userdata, rc):
	logging.info("Se ha desconectado, rc= "+str(rc))    
	reconectate()   

def on_publish(mqttCliente, userdata, mid):
	logging.info("publicado el mensaje "+ str(mid))   

def reconectate(mqttCliente):
	global conectado
	conectado=False
	while (not conectado):
		try:
			logging.error("Me reconecto  " )
			mqttCliente.reconnect()
			conectado=True
			time.sleep(2)
		except Exception as exErr:
			if hasattr(exErr, 'message'):
				logging.error("Error de conexion1 = "+ exErr.message)
			else:
				logging.error("Error de conexion2 = "+exErr)     
			time.sleep(30)

def on_message(mqttCliente, userdata, message):
	
	#result, mid = clientes["escritor"]["cliente"].publish(message.topic, message, 1, True )
	clave=message.topic.replace('/','.')
	measurement=message.topic.split('/')[0]
	#secs,usecs=divmod(time()-altzone,1) #local time
	secs,usecs=divmod(time(),1)
	while (usecs<0.1):
		logging.info(string(usecs))
		usecs=usecs*10
	payload=json.loads(message.payload.decode())
	dato='{"measurement":"'+measurement+'","time":'+str(int(secs))+str(int(usecs*1000000000))+\
			',"fields":'+json.dumps(payload[0])+',"tags":'+json.dumps(payload[1])+'}'
	logging.info(dato)
	logging.info("salva en influxdb")
	db_insert(dato)

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
							help='Verbose level')

	parser.add_argument('-s', '--dbuser', nargs='?', default='',
						help='Influxdb User name.')
	parser.add_argument('-a', '--dbpassword', nargs='?', default='',
						help='Influxdb Password.')
	parser.add_argument('-d', '--dbname', nargs='?', default='iotdb',
							help='Database name.')                                                         

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
	clientes["lector"]["cliente"] = mqtt.Client(clean_session=True) 
	clientes["lector"]["cliente"].on_message    = on_message
	clientes["lector"]["cliente"].on_connect    = on_connect
	clientes["lector"]["cliente"].on_subscribe  = on_subscribe
	clientes["lector"]["cliente"].on_disconnect = on_disconnect
	clientes["lector"]["cliente"].on_publish    = on_publish
	clientes["lector"]["cliente"].connect(clientes["lector"]["broker"],clientes["lector"]["port"])
	clientes["lector"]["cliente"].reconnect_delay_set(30, 120) 
	logging.info("LECTOR")
	logging.info(clientes["lector"])


	#y ahora el cliente que reenvia los mensajes a una cola remota (si q <>'')

	if (args.queue!=''):
		clientes["escritor"]["broker"]=args.queue
		clientes["escritor"]["cliente"] = mqtt.Client( clean_session=True) 
		clientes["escritor"]["cliente"].on_message    = on_message
		clientes["escritor"]["cliente"].on_connect    = on_connect
		#clientes["escritor"]["cliente"].on_subscribe  = on_subscribe
		clientes["escritor"]["cliente"].on_disconnect = on_disconnect
		clientes["escritor"]["cliente"].on_publish    = on_publish
		clientes["escritor"]["cliente"].connect(clientes["escritor"]["broker"],clientes["escritor"]["port"])
		#mqttEscritor.username_pw_set(username , password=token)
		clientes["escritor"]["cliente"].reconnect_delay_set(60, 600) 
		logging.info("ESCRITOR=")
		logging.info(clientes["escritor"])


	clientes["lector"]["cliente"].loop_forever()                 #start the loop
	clientes["escritor"]["cliente"].loop_forever()                 #start the loop
