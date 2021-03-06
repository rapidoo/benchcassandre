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



def main(ip):

    cluster = Cluster([ip])
    session = cluster.connect()

    rows = session.execute("select keyspace_name from  system_schema.columns")
  
    if KEYSPACE in [row[0] for row in rows]:

        log.info("dropping existing keyspace...")
        session.execute("DROP KEYSPACE " + KEYSPACE)

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


if __name__ == "__main__":
    main(IP)
