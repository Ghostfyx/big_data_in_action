#!/bin/bash

# 假设安装目录为/usr/local,root登陆后
if [ ! -d "/usr/local/software" ];then
mkdir /usr/local/software
else
echo "文件夹已经存在"
fi
cd /usr/local/software
wget https://www.apache.org/dyn/closer.lua/spark/spark-2.4.5/spark-2.4.5-bin-hadoop2.7.tgz
tar -zxvf spark-2.4.5-bin-hadoop2.7.tgz -C /usr/local

