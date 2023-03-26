#!/bin/bash
classpath=./lib/gson-2.8.2.jar:./lib/logback-classic-1.2.3.jar:./lib/logback-core-1.2.3.jar:./lib/mongo-java-driver-3.6.3.jar:./lib/postgresql-42.2.6.jar:./lib/ojdbc6-11.2.0.1.0.jar:./lib/slf4j-api-1.7.25.jar:./:./CockroachETL.jar

#java -jar CockroachETL.jar $1 $2 $3 $4

java -cp $classpath -Dlogback.configurationFile=./conf/logback.xml com/mobil/main/CockroachETL $1 $2 $3 $4
