import pyodbc

class db(object):
    def __str__(self):
        return """
                pass sql query to getData returns results in list to output attribute
                pass sql query to updateData will run update query
                """
    def __init__(self, db_loc, db_Type = '', user_name= '', password= '', db_name= ''):

        self.db_loc = db_loc
        self.user_name = user_name
        self.password = password
        self.db_name = db_name
        self.db_Type = db_Type

        self.cnxn = ''#pyodbc.connect('DRIVER={Microsoft Access Driver (*.mdb)};Dbq='+self.db_loc+';Uid=;Pwd=;')
        self.output=[]
        self.query =''
        self.isConnected = False

    def connect(self):
        if self.db_Type.upper()[0] == 'M':
            self.cnxn = pyodbc.connect('DRIVER={Microsoft Access Driver (*.mdb)};Dbq='+str(self.db_loc)+';Uid=;Pwd=;')
        elif self.db_Type.upper()[0] == 'A':
            self.cnxn = pyodbc.connect('Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=%'+str(self.db_loc)+';')
        elif self.db_Type.upper()[0] =='S':
            self.cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+str(self.db_loc)+';DATABASE='+str(self.db_name)+';UID='+str(self.user_name)+';PWD='+str(self.password))
        self.isConnected = True

    def getData(self,sql_query):
        #connect to DB
        if self.isConnected == False:
            self.connect()
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
        #connect to DB
        if self.isConnected == False:
            self.connect()
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

    def closeOut(self):
        #close connection to database
        self.cnxn.close()
        self.isConnected = False



