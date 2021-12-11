#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT count(*) FROM RangeRatingsMetaData")
    p = cur.fetchall()
    count = p[0][0]
    res = []
    for i in range(count):
    	cur.execute("SELECT * FROM RangeRatingsPart{0} WHERE {1}<=Rating AND Rating<={2}".format(i,ratingMinValue,ratingMaxValue))
    	p = cur.fetchall()
    	for num in p:
    		num = list(num)
    		rname = ('rangeratingspart'+str(i))
            	num.insert(0,rname)
    		num = tuple(num)
    		res.append(num)

    cur.execute("SELECT * FROM RoundRobinRatingsMetaData")
    p = cur.fetchall()
    count = p[0][0]

    for i in range(count):
    	cur.execute("SELECT * FROM RoundRobinRatingsPart{0} WHERE {1}<=Rating AND Rating<={2}".format(i,ratingMinValue,ratingMaxValue))
    	p = cur.fetchall()
    	for num in p:
    		num = list(num)
    		rname = ('roundrobinratingspart'+str(i))
            	num.insert(0,rname)
    		num = tuple(num)
    		res.append(num)
    f = open('RangeQueryOut.txt', 'w')
    for line in res:
        f.write(','.join(str(s) for s in line))
        f.write('\n')
    f.close()
    return None

def PointQuery(ratingsTableName, ratingValue, openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT COUNT(*) FROM RangeRatingsMetaData")
    p = cur.fetchall()
    count = p[0][0]
    res = []
    for i in range(count):
    	cur.execute("SELECT * FROM RangeRatingsPart{0} WHERE {1}=Rating".format(i,ratingValue))
    	p = cur.fetchall()
    	for num in p:
    		num = list(num)
    		rname = ('rangeratingspart'+str(i))
            	num.insert(0,rname)
    		num = tuple(num)
    		res.append(num)

    cur.execute("SELECT * FROM RoundRobinRatingsMetadata")
    rr_num = cur.fetchone()[0]

    for i in range(rr_num):
    	cur.execute("SELECT * FROM RoundRobinRatingsPart{0} WHERE {1}=Rating".format(i,ratingValue))
    	p = cur.fetchall()
    	for num in p:
    		num = list(num)
    		rname = ('roundrobinratingspart'+str(i))
            	num.insert(0,rname)
    		num = tuple(num)
    		res.append(num)
    f = open('PointQueryOut.txt', 'w')
    for line in res:
        f.write(','.join(str(s) for s in line))
        f.write('\n')
    f.close()
    return None
