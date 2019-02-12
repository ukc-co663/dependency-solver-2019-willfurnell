#!/bin/bash
#rm -rf lib/*
#mkdir -p lib
#wget -O lib/fastjson-1.2.45.jar http://search.maven.org/remotecontent?filepath=com/alibaba/fastjson/1.2.45/fastjson-1.2.45.jar
apt update
apt install python3-pip -y
pip3 install --user networkx packaging matplotlib 
