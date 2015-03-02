
import os, math, csv
from datetime import datetime
from DATA_ACCESS import db as database
#Support functions

CWD = os.getcwd()
class geocodeMaster(object):
    def __str__(self):
        return """
                Based on the nodes (children) in the master node (parent) set
                returns the XY coordinates of the node (child) that is closest to the
                midpoint of all of the nodes (children) in the master set (parent)
            """
    def __init__(self, NodesFile, root_folder):
        self.masterNode={}
        self.masterCenter={}
        self.masterXY={}
        self.NodesFile = NodesFile
        self.out_file = os.path.join(root_folder,'MasterNodesGEO.csv')
        self.header = ['NODEID',
                       'MASTER_NODEID',
                       'nodeX',
                       'nodeY',
                       'masterX',
                       'masterY',
                       'STREET1',
                       'STREET2',
                       'STREET3',
                       'STREET4',
                       'STREET5'
                       ]

    def getCurrentMasters(self, db, nodeData):
        query  = """
                    SELECT MasterID, NODEID, X, Y
                    FROM """+self.NodesFile+"""
                    WHERE MasterID != 0
                """
        db.getData(query)
        for row in db.output:
            if row.MasterID not in self.masterNode.keys():
                self.masterNode[row.MasterID]=[row.NODEID]
            else:
                self.masterNode[row.MasterID].append(row.NODEID)
        self.getCenter(nodeData, db)




    def getCenter(self,nodeData, db):
        '''this is pulled from round1, should be refactored'''

        for master in self.masterNode.keys():
            x_list, y_list = [],[]
            for node in self.masterNode[master]:
                if len(self.masterNode[master])>1:
                    x,y = nodeData.geo[node][0],nodeData.geo[node][1]
                    x_list.append(x)
                    y_list.append(y)
                    aX, aY = listAvg(x_list), listAvg(y_list)
                    self.masterCenter[master]=[aX, aY]
                else:
                    self.masterCenter[master]=[nodeData.geo[node][0],nodeData.geo[node][1]]
        self.getClosest(nodeData, db)

    def getClosest(self,nodeData, db):
        for master in self.masterNode.keys():
            minNode = 0
            minDistance = 10.0**5
            for node in self.masterNode[master]:
                d = dist(float(nodeData.geo[node][0]),
                        float(nodeData.geo[node][1]),
                        float(self.masterCenter[master][0]),
                        float(self.masterCenter[master][1]))
                if d < minDistance:
                    minNode= node
                    minDistance = d
            self.masterXY[master]=minNode
        self.wrteOUT(nodeData)
        self.makeMasterTable(db, nodeData)

    def wrteOUT(self,nodeData):
        row_cnt=0
        #['NODEID', 'MASTER_NODEID', 'nodeX', 'nodeY', 'masterX', 'masterY']
        with open(self.out_file, 'wb') as csvfile:
            writer = csv.writer(csvfile, delimiter=',')
            writer.writerow(self.header)
            for parent in self.masterNode.keys():
                center = self.masterXY[parent]
                for child in self.masterNode[parent]:
                    out = [child,
                           parent,
                           nodeData.geo[child][0],
                           nodeData.geo[child][1],
                           nodeData.geo[center][0],
                           nodeData.geo[center][1]
                           ]
                    out+=nodeData.node_dict[child]
                    writer.writerow(out)
                    row_cnt+=1
        print str(row_cnt)+" rows were written to "+str(self.out_file)


    def makeMasterTable(self, db, nodeData):
        """create new masterNode table"""

        try:
            #clean up old table
            db.updateData("drop table MasterNodesGEO")

        except:
            pass
        #create empty table
        new_tbale = """
                    CREATE TABLE MasterNodesGEO
                    (
                        NODEID int
                      ,MASTER_NODEID int
                      ,nodeX float
                      ,nodeY float
                      ,masterX float
                      ,masterY float
                      ,STREET1 varchar(100)
                      ,STREET2 varchar(100)
                      ,STREET3 varchar(100)
                      ,STREET4 varchar(100)
                      ,STREET5 varchar(100)
                    );
                    """
        db.updateData(new_tbale)


        for parent in self.masterNode.keys():
            center = self.masterXY[parent]
            for child in self.masterNode[parent]:
                out = [child,
                       parent,
                       nodeData.geo[child][0],
                       nodeData.geo[child][1],
                       nodeData.geo[center][0],
                       nodeData.geo[center][1]
                       ]
                if len(nodeData.node_dict[child])>1:
                    qry = """INSERT INTO MasterNodesGEO (
                        NODEID, MASTER_NODEID, nodeX, nodeY, masterX, masterY, STREET1,STREET2)
                        VALUES ("""+str(child)+","+str(parent)+","+str(nodeData.geo[child][0])+","+str(nodeData.geo[child][1])+","+str(nodeData.geo[center][0])+","+str(nodeData.geo[center][1])+",'"+str(list(nodeData.node_dict[child])[0])+"','"+str(list(nodeData.node_dict[child])[1])+"');"
                try:
                    db.updateData(qry)
                except:
                    qry = """INSERT INTO MasterNodesGEO (
                        NODEID, MASTER_NODEID, nodeX, nodeY, masterX, masterY)
                        VALUES ("""+str(child)+","+str(parent)+","+str(nodeData.geo[child][0])+","+str(nodeData.geo[child][1])+","+str(nodeData.geo[center][0])+","+str(nodeData.geo[center][1])+");"
                    db.updateData(qry)

        print 'DONE'


class db(object):
    def __str__(self):
        return """
                pass sql query to getData returns results in list to output attribute
                pass sql query to updateData will run update query
                """
    def __init__(self, LOC, db_name, conn_type):
        self.db_loc = os.path.join(LOC, db_name)
        if conn_type == 'mdb':
            self.cnxn = pyodbc.connect('DRIVER={Microsoft Access Driver (*.mdb)};Dbq='+self.db_loc+';Uid=;Pwd=;')
        else:
            print 'Connection type unknown'
            exit()
        self.output=[]
        self.query =''

    def getData(self,sql_query):
        #store query
        self.query=sql_query
        #create the cursor
        cursor = self.cnxn.cursor()
        #run sql
        cursor.execute(sql_query)
        #store results
        self.output = cursor.fetchall()
        #delete the cusor
        del cursor

    def updateData(self,sql_query):
        #store query
        self.query=sql_query
        #create the cursor
        cursor = self.cnxn.cursor()
        #run sql
        cursor.execute(sql_query)
        #save changes
        self.cnxn.commit()
        #delete the cusor
        del cursor

    def updateNewDatabase(self, sql_query, conn_type= 'mdb', db_server = 'DOTDEVGISSQL01', user_name = 'CrashData_User', password = 'crashdata_pwd', db_name = 'CrashMap'):
        #store query
        self.query=sql_query
        #create the cursor
        if conn_type == 'mdb':
            altCnxn = pyodbc.connect('DRIVER={Microsoft Access Driver (*.mdb)};Dbq='+self.db_loc+';Uid=;Pwd=;')
        else:
            print 'Connection type unknown'
            exit()
        cursor = altCnxn.cursor()
        #run sql
        cursor.execute(sql_query)
        #save changes
        altCnxn.commit()
        #delete the cusor
        del cursor


class nodeData(object):
    def __str__(self):
        return """
                full node dataset storage object
                """

    def __init__(self, NodesFile = None, prefix = None):
        self.node_dict = {}
        self.name_dict = {}
        self.geo = {}
        self.newMatches={}
        self.moreThan2={}
        self.NodesFile = NodesFile
        self.prefix = prefix

    def populate(self, node_list):

        for row in node_list:
            self.geo[row.NODEID]=[row.X,row.Y]

            if row.NODEID not in self.node_dict.keys():
                self.node_dict[row.NODEID] = set([row.STNAME])
            else:
                self.node_dict[row.NODEID].add(row.STNAME)


    def reverse(self):
        '''reverses dictionary - street set: list of nodes'''
        for n in self.node_dict.keys():
            key = ''
            l = list(self.node_dict[n])
            while len(l) >0:
                key += str(l.pop())+'-'
            offset = 0
            key=key[:-1]
            result = 'FAIL'
            while result == 'FAIL':
                if key not in self.name_dict.keys():
                    self.name_dict[key]=[n]
                    print 'Trying %s : %s' %(str(key).strip(), str(n).strip())
                    result = 'PASS'
                else:
                    result = self.checkDist(n, self.name_dict[key])
                    if  result == 'PASS':
                        self.name_dict[key].append(n)
                        print 'Trying %s : %s' %(str(key).strip(), str(n).strip())
                    else:
                        offset+=1
                        key+=str(offset)

    def checkDist(self, test_node, node_set, check = 431):
        x_list, y_list = [],[]
        for i in node_set:
            x,y = self.geo[i][0],self.geo[i][1]
            x_list.append(x)
            y_list.append(y)
        #get average cooridinate of set
        aX, aY = listAvg(x_list), listAvg(y_list)
        distance = dist(float(self.geo[test_node][0]), float(self.geo[test_node][1]), float(aX), float(aY))
        if distance < check:
            return 'PASS'
        else:
            return 'FAIL'

    def updateMaster(self,db):
        '''update database with new masterID - original ID number - '''
        print 'Updating'
        new_ID = 1
        for streets in self.name_dict.keys():
            mID = str(self.name_dict[streets])[1:-1]
            #need to add a distance threshhold - for now looks like 431 feet - if greater then dont give it the same master
            print streets
            qry  = '''UPDATE '''+self.NodesFile+''' SET MasterID = '''+str(new_ID)+'''  WHERE NODEID In ('''+mID+''')'''
            new_ID+=1
            db.updateData(qry)

    def getTripples(self):
        '''creates the list of nodes with more than 2 names'''
        print 'Trippling'
        for n in self.node_dict.keys():
            if len(self.node_dict[n])>2:
                self.moreThan2[n]=[]

    def getNearby(self, db, feet = 300):
        print 'Getting nearby'
        #get coordinates of the 3+ name intersections
        for i in self.moreThan2.keys():
            print i
            qry = """select X, Y
                    FROM """+self.NodesFile+"""
                    where NODEID = """+str(i)
            db.getData(qry)

            #get nodes within 300' (+/- not exact, but close enough)
            #of the 3 name intersections
            x,y = db.output[0][0],db.output[0][1]
            qry = """select NODEID
                    FROM """+self.NodesFile+"""
                    where
                    X <= """+str(x+feet)+""" and
                    X >= """+str(x-feet)+""" and
                    Y <= """+str(y+feet)+""" and
                    Y >= """+str(y-feet)

            db.getData(qry)

            for near in db.output:
                #only keep intersecting street name sets
                if i in self.node_dict.keys() and near.NODEID in self.node_dict.keys() and len(self.node_dict[near.NODEID])>1:
                    if  self.node_dict[i].intersection(self.node_dict[near.NODEID])==self.node_dict[near.NODEID]:# => assume it is part of the center

#--------------------------------new check testing-----------------------------------------------------------
                        #olny take from larger set - order proxy
                        if len(self.node_dict[i])>len(self.node_dict[near.NODEID]):
#--------------------------------new check testing-----------------------------------------------------------
                            self.moreThan2[i].append(near.NODEID)

    def addPartialMatches(self, db):
        '''go through the moreThan2 update the children nodes master ID to the parent node's master id '''
        print 'Updating partial matches'

        for n in self.moreThan2.keys():
            qry1 = '''SELECT MasterID
                    FROM '''+self.NodesFile+'''
                    WHERE NODEID='''+str(n)
            db.getData(qry1)
            newMasterID = db.output[0][0]
            print n, self.moreThan2[n]
            if self.moreThan2[n] != []:
                qry2 = '''UPDATE '''+self.NodesFile+''' SET MasterID = '''+str(newMasterID)+''' WHERE NODEID In ('''+str(self.moreThan2[n])[1:-1]+''')'''
                print qry2
                db.updateData(qry2)

    def cleanUp(self, db):
        for n in self.node_dict.keys():
            if len(self.node_dict[n]) ==1:
                qry = '''UPDATE '''+self.NodesFile+''' SET MasterID = '''+str(0)+''' WHERE NODEID ='''+str(n)
                db.updateData(qry)

def dist(x1,y1,x2,y2):
    return math.fabs(math.sqrt((x1-x2)**2 + (y1-y2)**2 ))


def listAvg(nodeList):
    '''this is pulled from round1, should be refactored'''
    if len(nodeList)==1:
        return nodeList[0]
    else:
        return float(sum(nodeList))/len(nodeList)



#Set parameters for database with node information
#The database must contain two tables in the following conditions:

#Table1 is the comprehensive list of nodes in the geodatabase, in which each node represents an intersection
#Table1 - Name - Streets_ND_Junctions
#Table1 - Fields - NodeID, X, Y, MasterID

#Table2 is the list of all unique pairs of streets and Node IDs; so if a nodeID represents the intersection of two streets,
    #it will appear twice in this table, one record for each STName.
#Table2 - Name - node_stnameFT_0
#Table2 - Fields - NodeID, STName



if __name__ == '__main__':
    NodesFile = 'test.SHOSTETTER.Streets_ND_Junctions_SAMPLE'
    StreetSegments = 'test.SHOSTETTER.StreetSegment_SAMPLE'
    schema = 'SHOSTETTER'
    database_name = 'test'
    prefix = database_name+'.'+schema+'.'
    root_folder = CWD
    s=datetime.now()

    db = database('DOTDEVGISSQL01', 'SQL', '', '', database_name)
    print """
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


    nodeData = nodeData()
    nodeData.populate(node_list)
    nodeData.reverse()
    nodeData.updateMaster(db)
    nodeData.getTripples()
    nodeData.getNearby(db)
    nodeData.addPartialMatches(db)
    nodeData.cleanUp(db)

    geocodeMaster = geocodeMaster(NodesFile)
    geocodeMaster.getCurrentMasters(db, nodeData)

    e = datetime.now()
    t= e - s
    print t.seconds/60, " minutes"

