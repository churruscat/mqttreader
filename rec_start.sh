#!/bin/bash 

while true
do
	while true
		do
		echo "lanzo mqttread"
		python -u /IOTServer/bin/mqttread.py -m warning >>/IOTServer/log/mqttread.log 2>&1
	done
done
exit 0
