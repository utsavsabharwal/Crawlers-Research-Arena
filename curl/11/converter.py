#!/usr/bin/env python
#coding:utf-8
# Author:   Utsav Sabharwal
# Purpose: To spread list of urls to different domain files
# Created: Tuesday 10 January 2012
import sys
filename=sys.argv[1]
f=open(filename).readlines()
for x in f:
    temp = x.split("\t")
    filename = "input/"+ temp[0]
    f1 = open(filename, "a+")
    f1.write((temp[1]))
    f1.close()


