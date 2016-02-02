#!/usr/bin/env python
import random
import logging
import sys

log = logging.getLogger()
log.setLevel('ERROR')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

KEYSPACE = "bench"
TABLE = "table1"
SEED = random.randrange(0, 10000000, 1) 
args = sys.argv
IP = args[1]
NUMBER_ITEM = int(args[2])


def main(ip, number_item):


    cluster = Cluster([ip])
    session = cluster.connect()

    rows = session.execute("select keyspace_name from  system_schema.columns")
  
    if KEYSPACE not in [row[0] for row in rows]:
        #log.info("dropping existing keyspace...")
        #session.execute("DROP KEYSPACE " + KEYSPACE)

	    log.info("creating keyspace...")
	    session.execute("""
	        CREATE KEYSPACE %s
	        WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }
	        """ % KEYSPACE)

    log.info("setting keyspace...")
    session.set_keyspace(KEYSPACE)

    rows = session.execute("select table_name from  system_schema.columns;")

    if TABLE not in [row[0] for row in rows]:
	    log.info("creating table...")
	    session.execute("""
	        CREATE TABLE %s (
	            thekey text,
	            col1 text,
	            col2 text,
	            PRIMARY KEY (thekey, col1)
	        )
	        """ % TABLE)

    #query = SimpleStatement("""
    #    INSERT INTO mytable (thekey, col1, col2)
    #    VALUES (%(key)s, %(a)s, %(b)s)
    #    """, consistency_level=ConsistencyLevel.ONE)

    prepared = session.prepare("""
        INSERT INTO %s (thekey, col1, col2)
        VALUES (?, ?, ?)
        """ % TABLE)

    for i in range(SEED, SEED+number_item):
        log.info("inserting row %d" % i)
        #session.execute(query, dict(key="key%d" % i, a='a', b='b'))
        session.execute(prepared.bind(("key%d" % i, 'b', 'b')))

    #future = session.execute_async("SELECT * FROM mytable")
    #log.info("key\tcol1\tcol2")
    #log.info("---\t----\t----")

    #try:
    #    rows = future.result()
    #except Exception:
    #    log.exeception()

    #for row in rows:
    #    log.info('\t'.join(row))

    #session.execute("DROP KEYSPACE " + KEYSPACE)

if __name__ == "__main__":
    main(IP, NUMBER_ITEM)
