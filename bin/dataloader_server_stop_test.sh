#!/bin/bash

#working dir

time=`date +%Y-%m-%d,%H:%M:%S`

workDir=/home/hadoop/xa
logDir=${workDir}/log
runJar=${workDir}/yangborunJar

jar="dataloader_etl_testCoin.jar";

hadoopsh="/usr/lib/hadoop/bin/hadoop"


#***************
#run the dataloader 
main="com.xingcloud.dataloader.server.DataLoaderETLWatcherCoin"



hostliststr="dataloader0,dataloader1"
host=`echo ${hostliststr}|awk '{split($1,a,",");for(key in a)print a[key];}'`
for node in ${host} 
do
	echo ${node}
	echo "beforekill"
	ssh ${node} ps aux|grep $main|awk '{print$2}'
	pidlist=`ssh ${node} ps aux|grep $main|awk '{print$2}'`
for pid in $pidlist
do
echo $pid
ssh ${node} kill $pid
done
	echo "afterkill"
	ssh ${node} ps aux|grep $main|awk '{print$2}'
done




