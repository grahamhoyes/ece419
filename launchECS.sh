#!/usr/bin/bash

ant build-server-jar
ant build-ecs-jar

KV_SERVER_JAR=$(pwd)/KVServer.jar java -jar ECS.jar ecs.config $(hostname -I | awk '{print $1}') 2181