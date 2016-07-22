#!/bin/bash

# Temporarily close off port 2181 breaking the zookeeper connection on a host.
# Used for testing what happens when the zookeeper connection breaks.

# Takes one argument: seconds to keep the port closed, default: 20.

set -e

iptables -I INPUT -p tcp --dport 2181 -j DROP 
iptables -I INPUT -p udp --dport 2181 -j DROP 
iptables -I OUTPUT -p tcp --dport 2181 -j DROP 
iptables -I OUTPUT -p udp --dport 2181 -j DROP 
sleep ${1:-20};
iptables -D INPUT -p tcp --dport 2181 -j DROP 
iptables -D INPUT -p udp --dport 2181 -j DROP 
iptables -D OUTPUT -p tcp --dport 2181 -j DROP 
iptables -D OUTPUT -p udp --dport 2181 -j DROP 
