#!/bin/sh
# This script will install python 3.7.6 on a RHEL or OEL.
# This script must be run as root

#Install Prereqs
yum -y install gcc openssl-devel bzip2-devel libffi-devel
​
#Install Python3.7
cd /usr/src
wget https://www.python.org/ftp/python/3.7.6/Python-3.7.6.tgz
tar xzf Python-3.7.6.tgz
cd Python-3.7.6
./configure --enable-optimizations
make altinstall
rm /usr/src/Python-3.7.6.tgz
​
#Install pip3.7
pip3.7 install --upgrade pip
pip install click
pip install rubrik_cdm
​
#Verify
python -V
python3.7 -V
pip -V
