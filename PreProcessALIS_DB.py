
#pre-processing for ALIS Tamer
import arcpy, os, math, csv
from datetime import datetime as dt
from DATA_ACCESS import db as database
from arcpy import env
import shutil


# baseline files
#________________________________________________________________________________________________
CWD = os.getcwd()
INPUT_FILE = 'sources.txt'

class readInputs(object):
    def __init__(self, data_path = CWD, data_list = INPUT_FILE):
        self.data_path = data_path
        self.data_list = data_list
        self.data_dict = {}

    def read (self):
        with open(os.path.join(self.data_path, self.data_list), 'r') as f:
            read_data = f.read()
            for i in read_data.split(','):
                self.data_dict[i.split('|')[0].strip()] = i.split('|')[1].strip()

class cleanUpGIS(object):
    def __str__(self):
        return """
                Sets up all of the database tables' structures (requires only arcpy)
                    - I lied, now it needs pyodbc via DATA_ACCESS -
                """

    def __init__(self, gdb, root_data_path, NodesFile = 'Streets_ND_Junctions', StreetSegments = 'StreetSegment', schema = 'dbo'):
        self.fgdb = gdb
        self.root_data_path = root_data_path
        self.NodesFile = NodesFile
        self.StreetSegments = StreetSegments
        self.schema = schema
        self.NodesDict ={}
        self.sqlFields = {}
        #use fgdb unless mannually changed
        env.workspace = self.fgdb

    def addFields(self, featureClass, fName, fType):
        '''Adds fields to feature class'''
        #this should be changed to SQL rather than arcpy!!!!!!
        arcpy.AddField_management(featureClass,fName,fType)

    def calculateFields(self,featureClass,fName,fValue, func =None):
        if func:
            print 'updating %s with %s in fc: %s' %(fName,fValue, featureClass)
            arcpy.CalculateField_management(featureClass,fName,fValue, "PYTHON_9.3", func)
        else:
            print 'updating %s with %s in fc: %s' %(fName,fValue, featureClass)
            arcpy.CalculateField_management(featureClass,fName,fValue, "PYTHON_9.3")

    def updateSegs(self, version = 10.0):
        '''Update and calculate From/To/Mid X,Ys of segments and add Node ID from To fields'''

        for field in [
            ["fX","!SHAPE.firstPoint.X!", "DOUBLE"],
                      ["fY", "!SHAPE.firstPoint.Y!", "DOUBLE"],
                      ["tX", "!SHAPE.lastPoint.X!", "DOUBLE"],
                      ["tY", "!SHAPE.lastPoint.Y!", "DOUBLE"],
                      ["mX", "!SHAPE.centroid.X!", "DOUBLE"],
                      ["mY", "!SHAPE.centroid.X!", "DOUBLE"],
                      ["FromNodeID", 0, "LONG"],
                      ["ToNodeID", 0, "LONG"],
                      ["fXY", '0', "TEXT"],
                      ["tXY", '0', "TEXT"]
                      ]:
            print field
            self.addFields(self.StreetSegments, field[0], field[2])
            #---------------this creates fields to allow attribute join to populate from and to nodes------------------
            if field[0] == "fXY":
                func = '''def add_XYs(field1, field2): return str(field1)+str(field2)'''
                self.calculateFields(self.StreetSegments,field[0],'add_XYs( !fX!, !fY!)', func)
            elif field[0] == "tXY":
                func = '''def add_XYs(field1, field2): return str(field1)+str(field2)'''
                self.calculateFields(self.StreetSegments,field[0],'add_XYs( !tX!, !tY!)', func)
            else:
                if version != 10.0:
                    print "Version is %f, replacing shape parsing strings" % version
                    coords = str(field[1]).replace(".firstPoint.X", ".extent.XMin")
                    coords = str(field[1]).replace(".firstPoint.Y", ".extent.YMin")
                    coords = str(field[1]).replace(".firstPoint.X", ".extent.XMax")
                    coords = str(field[1]).replace(".firstPoint.Y", ".extent.YMax")
                else:
                    coords = field[1]
                self.calculateFields(self.StreetSegments, field[0], field[1])


    def updateNodes(self):
        '''Adds and populates coordinates and adds MasterID fields'''
        for field in [
            ["NODEID","0", "LONG"],
            ["X","!SHAPE.centroid.X!", "DOUBLE"],
            ["Y","!SHAPE.centroid.Y!", "DOUBLE"],
            ["MasterID", "0", "LONG"],
            ["XY","0", "TEXT"]]:
            print 'adding %s' % field[0]
            self.addFields(self.NodesFile, field[0], field[2])
            print 'calculating %s' % field[0]
            ##---------------this creates field to allow attribute join to populate from and to nodes------------------
            if field[0] == "XY":
                func = '''def add_XYs(field1, field2): return str(field1)+str(field2)'''
                self.calculateFields(self.NodesFile,field[0],'add_XYs( !X!, !Y!)', func)
            elif field[0] == "NODEID":
                self.calculateFields(self.NodesFile,field[0],'!OBJECTID!+1')
            else:
                self.calculateFields(self.NodesFile, field[0], field[1])

    def createMissingNodes(self, sqlDB):
        '''REQUIRES PYODBC!!!!!!'''
        print 'Database is connected :%s' %str(sqlDB.isConnected)
        #update from and to NodeIDs - assumes the geogrpahy fields have been populated
        fromQry  = '''update '''+self.StreetSegments+'''
                set FromNodeID  = NODEID
                from '''+self.StreetSegments+''' join '''+self.NodesFile+''' on fxy = xy
                '''

        toQry  = '''update '''+self.StreetSegments+'''
                set ToNodeID  = NODEID
                from '''+self.StreetSegments+'''  join '''+self.NodesFile+''' on txy = xy
                '''
        sqlDB.updateData(fromQry)
        sqlDB.updateData(toQry)
        #generate missing nodes, segmentation is leading to missing nodes as they appear to be only where segments cross not where they terminate.
        missingQry = '''declare @m int
                set @m = (select max(NODEID) from '''+self.NodesFile+''');

                select 0 as ENABLED, 'N' as ISINTERSECTION,
                @m+( row_number() over (order by X, Y)) as NODEID,--<-this generates autonumber plus last node to continue NodeID#s
                X, Y ,0 as MasterID, cast(X as varchar) + cast(Y as varchar) as XY
                from (
                        select FromNodeID as NODEID, fX as X, fY as Y from '''+self.StreetSegments+'''
                        where FromNodeID = 0
                        union
                        select ToNodeID as NODEID, tX as X, tY as Y from '''+self.StreetSegments+'''
                        where ToNodeID = 0
                        ) as missingNodes
                group by NODEID, X, Y
                '''

        sqlDB.getData(missingQry)
        #create tempFC to hold missing - due to arc editor licence availibility at NYCDOT
        #get spatial reference
        sReference = arcpy.Describe(os.path.join(self.fgdb, self.NodesFile)).spatialReference
        #make shell
        try:
            print 'Creating FC: %s' % 'missingNodes'
            arcpy.CreateFeatureclass_management(self.fgdb, 'missingNodes', "POINT", "", "DISABLED", "DISABLED",sReference)
        except:
            print 'Missing FC exists, deleting...'
            arcpy.Delete_management('missingNodes')

            print 'Creating FC: %s' % 'missingNodes'
            arcpy.CreateFeatureclass_management(self.fgdb, 'missingNodes', "POINT", "", "DISABLED", "DISABLED",sReference)
        #add fields
        fields = [["ENABLED", "SHORT"],
                  ["ISINTERSECTION","TEXT"],
                  ["NODEID","LONG"],
                  ["X","DOUBLE"],
                  ["Y","DOUBLE"],
                  ["MasterID","LONG"],
                  ["XY","TEXT"],
                  ]
        for f in fields:
            arcpy.AddField_management('missingNodes',f[0],f[1])

        #create cursor
        rows = arcpy.InsertCursor(os.path.join(self.fgdb, 'missingNodes'))

        #add query results to tempFC
        for line in sqlDB.output:
            print 'Adding line for node %s' % str(line.NODEID)
            row = rows.newRow()
            row.shape = arcpy.Point(float(line.X),float(line.Y))
            row.ENABLED = int(line.ENABLED)
            row.ISINTERSECTION = line.ISINTERSECTION
            row.NODEID = int(line.NODEID)
            row.X = float(line.X)
            row.Y = float(line.Y)
            row.MasterID = int(line.MasterID)
            row.XY = line.XY
            rows.insertRow(row)
        try:
            del row
            del rows
        except:
            del rows
        print 'New FC created, append to server FC'



class update(database):
    def __str__(self):
        return "Adds necessary data to lookup table (requires pyodbc via database method)"

    def __init__(self, db_loc, db_Type = 'SQL', user_name= '', password= '', db_name= ''):
        database.__init__(self, db_loc, db_Type , user_name, password, db_name)
        self.dbprefix = db_name+'.'

    def buildNodeStNames(self, NodesFile = 'Streets_ND_Junctions', StreetSegments = 'StreetSegment', schema = 'dbo'):
        self.dbprefix+=schema+'.'

        '''Runs a series of SQL queries on database as part of the setup'''
        newTable  = """
                    CREATE TABLE """+self.dbprefix+"""node_stnameFT
                    (
                        OBJECTID int IDENTITY(1,1) PRIMARY KEY,
                        NODEID int,
                        STNAME char(100)
                    );
                    """
        newTable_0  = """
                    CREATE TABLE """+self.dbprefix+"""node_stnameFT_0
                    (
                        OBJECTID int IDENTITY(1,1) PRIMARY KEY,
                        NODEID int,
                        STNAME char(100)
                    );
                    """
        update_from_node_id = """
                            update """+self.dbprefix+"""[STREETSEGMENT_SAMPLE] 
                            set [FromNodeID] = [NODEID]
                            FROM """+self.dbprefix+"""[STREETSEGMENT_SAMPLE] as s
                            join  """+self.dbprefix+"""[STREETS_ND_JUNCTIONS_SAMPLE] as n
                            on s.fXY =XY;
                            """
        update_to_node_id = """
                            update """+self.dbprefix+"""[STREETSEGMENT_SAMPLE] 
                            set [ToNodeID] = [NODEID]
                            FROM """+self.dbprefix+"""[STREETSEGMENT_SAMPLE] as s
                            join  """+self.dbprefix+"""[STREETS_ND_JUNCTIONS_SAMPLE] as n
                            on s.tXY =XY;
                            """
        
        buildTable1 = """
                    INSERT INTO """+self.dbprefix+"""node_stnameFT ( NODEID, STNAME )

                    SELECT """+StreetSegments+""".FromNodeID AS NODEID,
                    """+StreetSegments+""".FULLSTNAME AS STNAME
                    FROM """+StreetSegments+"""
                    GROUP BY """+StreetSegments+""".FromNodeID, """+StreetSegments+""".FULLSTNAME

                    HAVING (Not (FULLSTNAME Like '%Unnamed Street%' Or FULLSTNAME Like '%Driveway%' Or
                    FULLSTNAME Like '%Ramp%' Or FULLSTNAME Like '%Connecting Road%' Or FULLSTNAME Like '%Alley%' Or
                    FULLSTNAME Like '%BOUNDARY%' Or FULLSTNAME Like '%BIKE%' Or
                    FULLSTNAME Like '%PEDESTRIAN%' Or FULLSTNAME Like '%ALLEY%' Or FULLSTNAME Like '% LINE' Or
                    FULLSTNAME Like '%WALK%' Or FULLSTNAME Like '%DRIVEWAY%' Or FULLSTNAME Like '%MALL%' Or
                    FULLSTNAME Like '%UNNAMED%' Or FULLSTNAME Like '%CONNECTOR%' Or
                    FULLSTNAME Like '%BIKE PATH%'))
                    """
        buildTable2 = """
                    INSERT INTO """+self.dbprefix+"""node_stnameFT ( NODEID, STNAME )

                    SELECT """+StreetSegments+""".ToNodeID AS NODEID,
                    """+StreetSegments+""".FULLSTNAME AS STNAME
                    FROM """+StreetSegments+"""
                    GROUP BY """+StreetSegments+""".ToNodeID, """+StreetSegments+""".FULLSTNAME

                    HAVING (Not (FULLSTNAME Like '%Unnamed Street%' Or FULLSTNAME Like '%Driveway%' Or
                    FULLSTNAME Like '%Ramp%' Or FULLSTNAME Like '%Connecting Road%' Or FULLSTNAME Like '%Alley%' Or
                    FULLSTNAME Like '%BOUNDARY%' Or FULLSTNAME Like '%BIKE%' Or
                    FULLSTNAME Like '%PEDESTRIAN%' Or FULLSTNAME Like '%ALLEY%' Or FULLSTNAME Like '% LINE' Or
                    FULLSTNAME Like '%WALK%' Or FULLSTNAME Like '%DRIVEWAY%' Or FULLSTNAME Like '%MALL%' Or
                    FULLSTNAME Like '%UNNAMED%' Or FULLSTNAME Like '%CONNECTOR%' Or
                    FULLSTNAME Like '%BIKE PATH%'))
                    """
        groupTable = """
                    INSERT INTO """+self.dbprefix+"""node_stnameFT_0 ( NODEID, STNAME )
                    SELECT """+self.dbprefix+"""node_stnameFT.NODEID, """+self.dbprefix+"""node_stnameFT.STNAME
                    FROM """+self.dbprefix+"""node_stnameFT
                    GROUP BY """+self.dbprefix+"""node_stnameFT.NODEID, """+self.dbprefix+"""node_stnameFT.STNAME;
                    """
        removeTemp = """drop table """+self.dbprefix+"""node_stnameFT"""
        doubleCheck = """
                    DELETE
                    FROM """+self.dbprefix+"""node_stnameFT_0
                    WHERE [STNAME] Like '%BOUNDARY%' Or
                    [STNAME] Like '%Unnamed%' Or
                    [STNAME] Like '%Driveway%' Or
                    [STNAME] Like '%Ramp%' Or
                    [STNAME] Like '%Connecting %' Or
                    [STNAME] Like '%Alley%' Or
                    [STNAME] Like '%BIKE%' Or
                    [STNAME] Like '%PEDESTRIAN%' Or
                    [STNAME] Like '%ALLEY%' Or
                    [STNAME] Like '% LINE' Or
                    [STNAME] Like '%WALK%' Or
                    [STNAME] Like '%DRIVEWAY%' Or
                    [STNAME] Like '%MALL%' Or
                    [STNAME] Like '%UNNAMED%' Or
                    [STNAME] Like '%CONNECTOR%' Or
                    [STNAME] Like '%BIKE PATH%'
                    """

        addMFT = "ALTER TABLE "+StreetSegments+" ADD  mft varchar(255)"

        queue = [newTable,
                 newTable_0,
                 update_from_node_id,
                 update_to_node_id,
                 buildTable1,
                 buildTable2,
                 groupTable,
                 removeTemp,
                 doubleCheck,
                 addMFT
                 ]
        self.connect()
        for qry in queue:
            print "Running query..."
            print qry
            self.updateData(qry)
        self.closeOut()


if __name__ == '__main__':
    s=dt.now() #starts time
    #run code
    ri = readInputs()
    ri.read()
    cleanUp = cleanUpGIS('Database Connections/'+ri.data_dict['sql_server_db_name']+'.sde',
                         ri.data_dict['root_data_folder'],
                         ri.data_dict['sql_server_db_name']+'.'+ri.data_dict['sql_server_db_schema']+'.Streets_ND_Junctions_SAMPLE',
                         ri.data_dict['sql_server_db_name']+'.'+ri.data_dict['sql_server_db_schema']+'.StreetSegment_SAMPLE',
                         ri.data_dict['sql_server_db_schema'])

    #add new fields and populate
    cleanUp.updateSegs(10.1)
    cleanUp.updateNodes()

    #sqlDB = database('SERVER', 'SQL', 'USER NAME','PASSWORD', 'DATABASE NAME')
##    sqlDB = database(ri.data_dict['sql_server_name'],
##                     'SQL',
##                     ri.data_dict['sql_server_db_username'],
##                     ri.data_dict['sql_server_db_password'],
##                     ri.data_dict['sql_server_db_name']
##                     )
##    cleanUp.createMissingNodes(sqlDB)

    updateSQL  = update(ri.data_dict['sql_server_name'],
                        'SQL',
                        ri.data_dict['sql_server_db_username'],
                        ri.data_dict['sql_server_db_password'],
                        ri.data_dict['sql_server_db_name'])
    updateSQL.buildNodeStNames(ri.data_dict['sql_server_db_name']+'.'+ri.data_dict['sql_server_db_schema']+'.Streets_ND_Junctions_SAMPLE',
                               ri.data_dict['sql_server_db_name']+'.'+ri.data_dict['sql_server_db_schema']+'.StreetSegment_SAMPLE',
                               ri.data_dict['sql_server_db_schema'])




    #end code
    e = dt.now().now() #ends time
    t= e - s
    print t.seconds/60, " minutes"

