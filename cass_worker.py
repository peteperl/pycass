# cassworker.py
#  Cassandra worker functions built on top of pycass.py
#   Batch insert for Cassandra
#
########################################################

import os
import sys
import time
import traceback
import cPickle as pickle
import pycass

__author__ = 'peteperl'

casshost = '104.197.52.171'
keyspace = "demo"


# Insert a batch of data into Cassandra: 200-1000 entries is efficient
#  It is good to batch the insert of multiple metrics that come at the same time
#  'data' is a list of the data to insert
def insertBatch(cass, data, keyspace):
    cluster, cur = cass.getclustsess(keyspace)
    for d in data:
        timebucket = cass.insert_datasimple(cur, formatData(d))
    cur.shutdown()
    return timebucket


# Safe way to insert to Cassandra in case of problem connecting or other issues
def insertDataSafe(chost, keyspace, dcname, data):
    perfdata["clustername"] = dcname
    cass = cbcassandra.CBcassandra(chost, keyspace)  # Cassandra connection
    try:
        timebucket = insertBatch(cass, data, keyspace)
    except:
        time.sleep(1)
        try:
            timebucket = insertBatch(cass, data, keyspace)
        except:
            print traceback.print_exc()
            return False
        pass
    return timebucket


def insertData(dcname, data):
    insertDataSafe(casshost, keyspace, dcname, data)


# Write your own function to translate your data from the source to Cassandra schema
def formatData(d):
    # print d
    perfdata = {}
    perfdata["DCname"] = 'ClusterName'
    perfdata["platform"] = 'kubernetes'
    perfdata["vmname"] = d['vmname']
    perfdata["time_current"] = d['time']
    perfdata["pname"] = ''
    perfdata["pvalue"] = 0
    # print perfdata
    return perfdata


if __name__ == "__main__":
    print "*** Hello ***"
    ts = time.time()
    # cass_worker(casshost, 'testarchive', 'testcluster')
    cass = pycass.MYcassandra(casshost, keyspace)
    # cass.teardown()
    cass.setup_schema()
    print "*** DONE ***"
    tf = time.time()
    tt = tf - ts
    print "Time to setup Cassandra:"
    print tt
    print "*** DONE ***"
