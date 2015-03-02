#-------------------------------------------------------------------------------
# Name:        RunIntersectionTamer
# Purpose:     runs pre-process and tamer
#
# Author:      shostetter
#
# Created:     19/11/2014
# Requirements: Assumes Arcpy, MS SQL Server SDE and PYODBC ('2.1.10-beta01')
#-------------------------------------------------------------------------------
import os
from PreProcessALIS_DB import *
from ALIS_Node_Tamer import *


s=dt.now() #starts time
print 'Starting Pre-processing at', s

#get inputs from text time
ri = readInputs()
ri.read()

#base file names:
NodesFile = ri.data_dict['sql_server_db_name']+'.'+ri.data_dict['sql_server_db_schema']+'.Streets_ND_Junctions_SAMPLE'
StreetSegments = ri.data_dict['sql_server_db_name']+'.'+ri.data_dict['sql_server_db_schema']+'.StreetSegment_SAMPLE'
schema = ri.data_dict['sql_server_db_schema']
database_name = ri.data_dict['sql_server_db_name']
prefix = database_name+'.'+schema+'.'
root_folder = ri.data_dict['root_data_folder']
server_name = ri.data_dict['sql_server_name']

#run code
print 'Database Connections\\'+database_name+'.sde'
cleanUp = cleanUpGIS('Database Connections\\'+server_name+'.sde',
                     root_folder,
                     NodesFile,
                     StreetSegments,
                     schema)

#add new fields and populate
cleanUp.updateSegs(10.1)
cleanUp.updateNodes()

updateSQL  = update(server_name,
                    'SQL',
                    ri.data_dict['sql_server_db_username'],
                    ri.data_dict['sql_server_db_password'],
                    database_name
                    )
updateSQL.buildNodeStNames(NodesFile, StreetSegments, schema)


#end code
e = dt.now().now() #ends time
t= e - s
print 'Finished Pre-processing'
print t.seconds/60, " minutes\n\n"

print '\nSetup done, now starting tamer\n'
#-----RUN TAMER-----

s=dt.now()
print 'Starting Tamer at', s
print 'db name: %s, server: %s, prefix: %s' %(database_name,server_name, prefix)
db = database(server_name, 'SQL',
                    ri.data_dict['sql_server_db_username'],
                    ri.data_dict['sql_server_db_password'],
                    database_name)
print"""
            SELECT n.NODEID, n.STNAME, s.X, s.Y
            FROM """+NodesFile+""" as s
            INNER JOIN """+prefix+"""node_stnameFT_0 as n
                    ON s.NODEID = n.NODEID
            GROUP BY n.NODEID, n.STNAME, s.X, s.Y;
            """
db.getData("""
            SELECT n.NODEID, n.STNAME, s.X, s.Y
            FROM """+NodesFile+""" as s
            INNER JOIN """+prefix+"""node_stnameFT_0 as n
                    ON s.NODEID = n.NODEID
            GROUP BY n.NODEID, n.STNAME, s.X, s.Y;
            """)
node_list = db.output

ND = nodeData(NodesFile, prefix)
ND.populate(node_list)
ND.reverse()
ND.updateMaster(db)
ND.getTripples()
ND.getNearby(db)

ND.addPartialMatches(db)
ND.cleanUp(db)

GM = geocodeMaster(NodesFile, CWD)
GM.getCurrentMasters(db, ND)


    
e = dt.now()
t= e - s
print 'Finished Tamer'
print t.seconds/60.0, " minutes"

