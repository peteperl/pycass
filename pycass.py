#
# Cassandra - Python
#

import time
from cassandra.cluster import Cluster
import cPickle as pickle
import datetime

__author__ = 'peteperl'


class MYcassandra(object):
    '''
    Class to write data to Cassandra
    '''

    def __init__(self, chost, keyspace="demo"):
        """ initialize Class objects """
        self.chost = chost
        self.keyspace = keyspace
        self.cur = None  # Use this cursor with bulk requests. CAREFUL: Use this ONLY if each parallel MYcassandra requester makes its own worker.
        # Tables: see cassandra_schema.txt
        self.osarchivebulk = "data_archive_openstackbulk"
        self.osarchive = "data_archive_openstack"
        self.simple = "DataSimple"
        self.datastore = "DataStore"
        self.ds_meta = "dsMetaData"  # Row Partion of DataStore

    def getclustsess(self, keyspace=None):
        """ Return a Cluster instance and a session object """
        cluster = Cluster([self.chost])  # Cluster(['192.168.1.1', '192.168.1.2'])
        if keyspace:
            session = cluster.connect()
            session.set_keyspace(keyspace)
        else:
            session = cluster.connect()
        return cluster, session

    def open_cur(self):
        """ Opens cursor for bulk requests. Make sure to close_cur() """
        cluster, cur = self.getclustsess(self.keyspace)
        self.cur = cur

    def get_cur(self):
        """ Opens cursor for bulk requests. Make sure to cur.shutdown() """
        cluster, cur = self.getclustsess(self.keyspace)
        return cur

    def close_cur(self):
        self.cur.shutdown()

    def setup_schema(self):
        """ Creates our schema. Leave the flexiblility for now. """
        cluster, cur = self.getclustsess()
        CQLString = ("CREATE KEYSPACE " + self.keyspace +
                     " WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 3};")
        cur.execute(CQLString)
        cur.set_keyspace(self.keyspace)
        tables = {}
        # Specify the schema for any repos we need
        tables[self.osarchivebulk] = ("CREATE TABLE " + self.osarchivebulk +
                                      " (DCname_yymmddhh text, DCname text, "
                                      "ip text, time_current timestamp, vmsdata blob, "
                                      "PRIMARY KEY (DCname_yymmddhh, time_current));")
        tables[self.osarchive] = ("CREATE TABLE " + self.osarchive +
                                  " (DCname_yymmddhh text, DCname text, ip text, "
                                  "time_current timestamp, hostlist blob, hypervisors blob, "
                                  "hostnames blob, hostdata blob, vmsdata blob, "
                                  "PRIMARY KEY (DCname_yymmddhh, time_current));")
        # cur.execute(tables[self.osarchivebulk])
        # cur.execute(tables[self.osarchive])

        # This ignores Row Partition for now. Partition = 0.
        # I added platform: vmware, openstack, aws, rackpace, ...
        tables[self.simple] = ("""CREATE TABLE """ + self.simple + """ (
                        dc_name text,
                        time_bucket text,
                        partition int,
                        platform text,
                        vm_name text,
                        measurement_time timestamp,
                        perf_name text,
                        perf_data int,
                        PRIMARY KEY ((dc_name, time_bucket, partition), perf_name, vm_name, measurement_time)
                        ) WITH caching = 'all';""")
        cur.execute(tables[self.simple])
        # CREATE INDEX time_bucket_key ON DataSimple ("time_bucket");
        self.createindex(cur, self.simple, "dc_name")
        self.createindex(cur, self.simple, "time_bucket")
        self.createindex(cur, self.simple, "platform")
        self.createindex(cur, self.simple, "perf_name")
        self.createindex(cur, self.simple, "vm_name")

        # DataStore with partitions and other metadata
        # I added platform: vmware, openstack, aws, rackpace, ...
        tables[self.datastore] = ("""CREATE TABLE """ + self.datastore + """ (
                        dc_name text,
                        time_bucket text,
                        partition int,
                        platform text,
                        vm_name text,
                        measurement_time timestamp,
                        perf_name text,
                        perf_data int,
                        PRIMARY KEY ((dc_name, time_bucket, partition), perf_name, vm_name, measurement_time)
                        );""")
        cur.execute(tables[self.datastore])
        self.createindex(cur, self.datastore, "dc_name")
        self.createindex(cur, self.datastore, "time_bucket")
        self.createindex(cur, self.datastore, "platform")
        self.createindex(cur, self.datastore, "perf_name")
        self.createindex(cur, self.datastore, "vm_name")

        tables[self.ds_meta] = ("""CREATE TABLE """ + self.ds_meta + """ (
                        dc_name text,
                        time_bucket text,
                        partition int,
                        platform text,
                        num_partitions int,
                        max_len int,
                        oldest_ts timestamp,
                        PRIMARY KEY ((dc_name, time_bucket, partition))
                        );""")
        cur.execute(tables[self.ds_meta])
        self.createindex(cur, self.ds_meta, "dc_name")
        self.createindex(cur, self.ds_meta, "time_bucket")

        print "Keyspace created: " + self.keyspace
        cur.shutdown()

    def teardown(self):
        cluster, cur = self.getclustsess()
        # remove the keyspace so we can add it again to test
        cur.execute("DROP KEYSPACE " + self.keyspace + ";")
        print "Keyspace dropped: " + self.keyspace
        cur.shutdown()

    def createindex(self, cur, tableName, indexName):
        # remove the keyspace so we can add it again to test
        cur.execute("CREATE INDEX ON " + tableName + "(" + indexName + ");")

    def insert_osarchivebulk(self, cur, vmdata):
        # cluster, cur = self.getclustsess(self.keyspace)
        query = "INSERT INTO " + self.osarchivebulk + " (DCname_yymmddhh, DCname, ip, time_current, vmsdata) VALUES (%(DCname_yymmddhh)s, %(DCname)s, %(ip)s, %(time_current)s, %(vmsdata)s)"
        timenow = time.gmtime()
        current_hour = time.strftime("%Y%m%d%H", timenow)
        dcname = vmdata["DCname"]
        rowkey = dcname + "_" + current_hour
        print rowkey
        timen = datetime.datetime.utcnow()
        print timen

        values = {'DCname_yymmddhh': rowkey,
                  'DCname': dcname,
                  'ip': vmdata["ip"],
                  'time_current': timen,
                  'vmsdata': bytearray(pickle.dumps(vmdata["data"]), encoding="hex")}
        cur.execute(query, values)
        # cur.shutdown()
        print "INSERT into " + self.osarchivebulk

    def insert_osarchive(self, cur, vmdata):
        # cluster, cur = self.getclustsess(self.keyspace)
        query = "INSERT INTO " + self.osarchive + " (DCname_yymmddhh, DCname, ip, time_current, hostlist, hypervisors, hostnames, hostdata, vmsdata) VALUES (%(DCname_yymmddhh)s, %(DCname)s, %(ip)s, %(time_current)s, %(hostlist)s, %(hypervisors)s, %(hostnames)s, %(hostdata)s, %(vmsdata)s)"
        timenow = time.gmtime()
        current_hour = time.strftime("%Y%m%d%H", timenow)
        dcname = vmdata["DCname"]
        rowkey = dcname + "_" + current_hour
        print rowkey
        timen = datetime.datetime.utcnow()
        print timen
        vmsdata = vmdata["data"]

        values = {'DCname_yymmddhh': rowkey,
                  'DCname': dcname,
                  'ip': vmdata["ip"],
                  'time_current': timen,
                  'hostlist': bytearray(pickle.dumps(vmsdata["hostlist"]), encoding="hex"),
                  'hypervisors': bytearray(pickle.dumps(vmsdata["hypervisors"]), encoding="hex"),
                  'hostnames': bytearray(pickle.dumps(vmsdata["hostnames"]), encoding="hex"),
                  'hostdata': bytearray(pickle.dumps(vmsdata["hostdata"]), encoding="hex"),
                  'vmsdata': bytearray(pickle.dumps(vmsdata["vms"]), encoding="hex")}
        cur.execute(query, values)
        # cur.shutdown()
        print "INSERT into " + self.osarchivebulk

    def insert_datasimple(self, cur, vmdata):
        # cluster, cur = self.getclustsess(self.keyspace)
        query = "INSERT INTO " + self.simple + " (dc_name, time_bucket, partition, platform, vm_name, measurement_time, perf_name, perf_data) VALUES (%(DCname)s, %(timebucket)s, %(partition)s, %(platform)s, %(vmname)s, %(mtime)s, %(perfname)s, %(perfdata)s)"
        datatime = vmdata["time_current"]
        datadate = datetime.datetime.fromtimestamp(datatime)
        yymmddhh = datadate.strftime("%Y%m%d%H")
        # print yymmddhh
        # print vmdata
        timen = datetime.datetime.utcnow()

        values = {'DCname': vmdata["DCname"],
                  'timebucket': yymmddhh,
                  'partition': 0,
                  'platform': vmdata["platform"],
                  'vmname': vmdata["vmname"],
                  'mtime': datadate,
                  'perfname': vmdata["pname"],
                  'perfdata': vmdata["pvalue"]}

        # print query
        # print values
        cur.execute(query, values)
        # cur.shutdown()
        # print "INSERT into " + self.simple
        return yymmddhh

    def return_insert(self, vmdata):
        query = "INSERT INTO " + self.simple + " (dc_name, time_bucket, partition, platform, vm_name, measurement_time, perf_name, perf_data) "
        v = "VALUES (%(DCname)s, %(timebucket)s, %(partition)s, %(platform)s, %(vmname)s, %(mtime)s, %(perfname)s, %(perfdata)s)"
        datatime = vmdata["time_current"]
        datadate = datetime.datetime.fromtimestamp(datatime)
        yymmddhh = datadate.strftime("%Y%m%d%H")
        timen = datetime.datetime.utcnow()
        values = {'DCname': vmdata["DCname"],
                  'timebucket': yymmddhh,
                  'partition': 0,
                  'platform': vmdata["platform"],
                  'vmname': vmdata["vmname"],
                  'mtime': datadate,
                  'perfname': vmdata["pname"],
                  'perfdata': vmdata["pvalue"]}
        # print (query, values)
        # values = "VALUES (" + vmdata["DCname"] + ", " + yymmddhh + ", 0, " + vmdata["platform"] + ", " + vmdata["vmname"] + ", " + str(datadate) + ", " + vmdata["pname"] + ", " + str(vmdata["pvalue"]) + "); "
        qv = query + values
        print qv
        return qv

    def batch_insert(self, cur, qstr):
        query = "BEGIN BATCH " + qstr + "APPLY BATCH;"
        print query
        cur.execute(query)

    def store(self, vmdata):
        cluster, cur = self.getclustsess(self.keyspace)
        # self.insert_osarchivebulk(cur, vmdata)
        # self.insert_osarchive(cur, vmdata)
        # vizdata = self.computeVisualData(vmdata)
        # self.insert_visual(vizdata)
        self.insert_datasimple(cur, vmdata)
        cur.shutdown()

    def read_osarchivebulk(self):
        cluster, cur = self.getclustsess(self.keyspace)
        rows = cur.execute("SELECT * FROM " + self.osarchivebulk + ";")
        for row in rows:
            print(row)
        cur.shutdown()
        rowitem = rows[0]
        originaldict = pickle.loads(str(rowitem.vmsdata))
        print originaldict

    def read_osarchive(self):
        cluster, cur = self.getclustsess(self.keyspace)
        rows = cur.execute("SELECT * FROM " + self.osarchive + ";")
        for row in rows:
            print(row)
        cur.shutdown()
        rowitem = rows[0]
        originaldict = pickle.loads(str(rowitem.vmsdata))
        print originaldict

    def read_simple_all(self):
        cluster, cur = self.getclustsess(self.keyspace)
        rows = cur.execute("SELECT * FROM " + self.simple + " WHERE dc_name = 'my-cluster-2-DC' LIMIT 10;")
        for row in rows:
            print(row)
        cur.shutdown()

    def read_simple(self, getdc, timebucket, numdata):
        cluster, cur = self.getclustsess(self.keyspace)
        result = cur.execute(
            "SELECT * FROM " + self.simple + " WHERE dc_name = '" + getdc + "' AND time_bucket = '" + timebucket + "' LIMIT " + str(
                numdata) + " ALLOW FILTERING;")
        for row in result:
            print(row)
        cur.shutdown()
        print "******"
        return result

    def read_simple_time(self, timebucket, numdata):
        cluster, cur = self.getclustsess(self.keyspace)
        result = cur.execute("SELECT * FROM " + self.simple + " WHERE time_bucket = '" + timebucket + "' LIMIT " + str(
            numdata) + " ALLOW FILTERING;")
        cur.shutdown()
        return result

    def getTimebuckets(self, mydc, numbuckets):
        # cluster, cur = self.getclustsess(self.keyspace)
        timebuckets = ['2014031822', '2014031821', '2014031820', '2014031819', '2014031818', '2014031817', '2014031816',
                       '2014031815', '2014031814', '2014031813', '2014031812', '2014031811', '2014031810', '2014031809',
                       '2014031808', '2014031807', '2014031806', '2014031805', '2014031804', '2014031803', '2014031802',
                       '2014031801', '2014031800', '2014031799', '2014031798']
        # cqlstr = "SELECT * FROM " + self.simple + " WHERE dc_name = '" + getdc + "' AND time_bucket = '" + timebucket + "' AND partition=0 and perf_name = 'cpu.usagemhz.average' LIMIT " + str(numdata) + " ;"
        # result = cur.execute(cqlstr)
        # cur.shutdown()
        return timebuckets

    def read_simple_dc_cpu(self, getdc, timebucket, numdata):
        cluster, cur = self.getclustsess(self.keyspace)
        cqlstr = "SELECT * FROM " + self.simple + " WHERE dc_name = '" + getdc + "' AND time_bucket = '" + timebucket + "' AND partition=0 and perf_name = 'cpu.usagemhz.average' LIMIT " + str(
            numdata) + " ;"
        print cqlstr
        result = cur.execute(cqlstr)
        cur.shutdown()
        return result

    def read_simple_dc_mem(self, getdc, timebucket, numdata):
        cluster, cur = self.getclustsess(self.keyspace)
        cqlstr = "SELECT * FROM " + self.simple + " WHERE dc_name = '" + getdc + "' AND time_bucket = '" + timebucket + "' AND partition=0 and perf_name = 'mem.active.average' LIMIT " + str(
            numdata) + " ;"
        print cqlstr
        result = cur.execute(cqlstr)
        cur.shutdown()
        return result

    def read_simple_vmname(self, getdc):
        cur = self.cur
        cqlstr = "SELECT vm_name FROM DataSimple WHERE dc_name = '" + getdc + "' AND time_bucket = '2014031922' AND partition=0 and perf_name = 'cpu.usagemhz.average' LIMIT 10000;"
        result = cur.execute(cqlstr)
        return result

    def read_simple_dc_cpu_(self, getdc, timebucket, numdata):
        # Called by PrepareData.dcsummary()
        cur = self.cur
        cqlstr = "SELECT measurement_time, perf_data FROM " + self.simple + " WHERE dc_name = '" + getdc + "' AND time_bucket = '" + timebucket + "' AND partition=0 and perf_name = 'cpu.usagemhz.average' LIMIT " + str(
            numdata) + " ;"
        # print cqlstr
        result = cur.execute(cqlstr)
        return result

    def read_simple_dc_mem_(self, getdc, timebucket, numdata):
        # Called by PrepareData.dcsummary()
        cur = self.cur
        cqlstr = "SELECT measurement_time, perf_data FROM " + self.simple + " WHERE dc_name = '" + getdc + "' AND time_bucket = '" + timebucket + "' AND partition=0 and perf_name = 'mem.active.average' LIMIT " + str(
            numdata) + " ;"
        # print cqlstr
        result = cur.execute(cqlstr)
        return result

    def read_simple_dc_cpu_cur(self, cur, getdc, timebucket, numdata):
        # Called by PrepareData.dcsummary()
        cqlstr = "SELECT measurement_time, perf_data FROM " + self.simple + " WHERE dc_name = '" + getdc + "' AND time_bucket = '" + timebucket + "' AND partition=0 and perf_name = 'cpu.usagemhz.average' LIMIT " + str(
            numdata) + " ;"
        # print cqlstr
        result = cur.execute(cqlstr)
        return result

    def read_simple_dc_mem_cur(self, cur, getdc, timebucket, numdata):
        # Called by PrepareData.dcsummary()
        cqlstr = "SELECT measurement_time, perf_data FROM " + self.simple + " WHERE dc_name = '" + getdc + "' AND time_bucket = '" + timebucket + "' AND partition=0 and perf_name = 'mem.active.average' LIMIT " + str(
            numdata) + " ;"
        # print cqlstr
        result = cur.execute(cqlstr)
        return result

    # NEED TO CLUSTER AND INDEX by time
    def read_simple_dc_cpu_range(self, getdc, timebucket, ts, te, numdata):
        # Called by PrepareData.dcsummary()
        cur = self.cur
        cqlstr = "SELECT measurement_time, perf_data FROM " + self.simple + " WHERE dc_name = '" + getdc + "' AND time_bucket = '" + timebucket + "' AND partition=0 and perf_name = 'cpu.usagemhz.average' AND measurement_time > '" + str(
            datetime.datetime.fromtimestamp(ts)) + "' AND  measurement_time < '" + str(
            datetime.datetime.fromtimestamp(te)) + "' LIMIT " + str(numdata) + " ;"
        # print cqlstr
        result = cur.execute(cqlstr)
        return result

    def read_simple_dc_mem_range(self, getdc, timebucket, ts, te, numdata):
        # Called by PrepareData.dcsummary()
        cur = self.cur
        cqlstr = "SELECT measurement_time, perf_data FROM " + self.simple + " WHERE dc_name = '" + getdc + "' AND time_bucket = '" + timebucket + "' AND partition=0 and perf_name = 'mem.active.average' AND measurement_time > '" + str(
            datetime.datetime.fromtimestamp(ts)) + "' AND  measurement_time < '" + str(
            datetime.datetime.fromtimestamp(te)) + "' LIMIT " + str(numdata) + " ;"
        # print cqlstr
        result = cur.execute(cqlstr)
        return result

    def read_simple_vm_cpu_(self, getdc, vmname, timebucket, numdata):
        # Called by PrepareData.vmsummary()
        cur = self.cur
        cqlstr = "SELECT measurement_time, perf_data FROM " + self.simple + " WHERE dc_name = '" + getdc + "' AND time_bucket = '" + timebucket + "' AND partition=0 AND vm_name = '" + vmname + "' AND perf_name = 'cpu.usagemhz.average' LIMIT " + str(
            numdata) + " ;"
        # print cqlstr
        result = cur.execute(cqlstr)
        return result

    def read_simple_vm_mem_(self, getdc, vmname, timebucket, numdata):
        # Called by PrepareData.vmsummary()
        cur = self.cur
        cqlstr = "SELECT measurement_time, perf_data FROM " + self.simple + " WHERE dc_name = '" + getdc + "' AND time_bucket = '" + timebucket + "' AND partition=0 AND vm_name = '" + vmname + "' AND  perf_name = 'mem.active.average' LIMIT " + str(
            numdata) + " ;"
        # print cqlstr
        result = cur.execute(cqlstr)
        return result
