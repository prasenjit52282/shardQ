import os
import numpy as np
import pandas as pd
import mysql.connector
from multiprocessing.dummy import Pool

class DataHandler:
    def __init__(self,columns=None,dtypes=None,is_SQL=False,SQL_handle=None,table_name=None):
        self.columns=columns
        self.dtypes=dtypes
        self.is_SQL=is_SQL
        self.SQL_handle=SQL_handle
        self._setup(table_name)

    def _setup(self,table_name):
        if not self.is_SQL:
            self.table=pd.DataFrame(columns=self.columns)
        else:
            self.table_name=self.SQL_handle.jobrunner.apply(self.SQL_handle.hasTable,(table_name,self.columns,self.dtypes))
    
    @property
    def Count(self):
        if not self.is_SQL:
            return self.table.shape[0]
        else:
            return self.SQL_handle.jobrunner.apply(self.SQL_handle.Count,(self.table_name,))

    def Insert(self,row):
        if not self.is_SQL:
            id=self.table.shape[0]
            self.table.loc[self.table.shape[0]]=row
        else:
            id=self.SQL_handle.jobrunner.apply(self.SQL_handle.Insert,(self.table_name,row))
        return id

    def Update(self,idx,col,val):
        if not self.is_SQL:
            self.table.loc[idx,col]=val
        else:
            self.SQL_handle.jobrunner.apply(self.SQL_handle.setVal,(self.table_name,idx,col,val))

    def GetAT(self,idx,col):
        if not self.is_SQL:
            return self.table.loc[idx,col]
        else:
            return self.SQL_handle.jobrunner.apply(self.SQL_handle.getVal,(self.table_name,idx,col))

    def IncrementBy(self,idx,col,by):
        if not self.is_SQL:
            self.table.loc[idx,col]+=by
        else:
            self.SQL_handle.jobrunner.apply(self.SQL_handle.IncrementBy,(self.table_name,idx,col,by))


class SQLHandler:
    def __init__(self,host='localhost',user='root',password='abc',db='dQdb'):
        self.jobrunner=Pool(1)
        self.host=host
        self.user=user
        self.password=password
        self.db=db

    def connect(self):
        connected=False
        while not connected:
            try:
                self.mydb = mysql.connector.connect(host=self.host,user=self.user,password=self.password)
                self.UseDB(self.db)
                connected=True
            except Exception:
                pass
    
    def query(self, sql):
        try:
            cursor = self.mydb.cursor()
            cursor.execute(sql)
        except Exception:
            self.connect()
            cursor = self.mydb.cursor()
            cursor.execute(sql)
        res=cursor.fetchall()
        cursor.close()
        self.mydb.commit()
        return res

    def UseDB(self,dbname=None):
        res=self.query("SHOW DATABASES")
        if dbname not in [r[0] for r in res]:
            self.query(f"CREATE DATABASE {dbname}")
        self.query(f"USE {dbname}")

    def DropDB(self,dbname=None):
        res=self.query("SHOW DATABASES")
        if dbname in [r[0] for r in res]:
            self.query(f"DROP DATABASE {dbname}")

    def hasTable(self,tabname=None,columns=None,dtypes=None):
        res=self.query("SHOW TABLES")
        if tabname not in [r[0] for r in res]:
            dmap={'int':'INT','str':'VARCHAR(32)'}
            col_config=''
            for c,d in zip(columns,dtypes):
                col_config+=f", {c} {dmap[d]}"
            self.query(f"CREATE TABLE {tabname} (id INT AUTO_INCREMENT PRIMARY KEY{col_config})")
        return tabname

    def getVal(self,table_name,idx,col):
        row=self.query(f"SELECT {col} FROM {table_name} where id={idx+1}")
        if len(row)==0:raise KeyError(f"Key:idx-{idx} is not found")
        else:return row[0][0]

    def setVal(self,table_name,idx,col,val):
        if type(val)==str:
            self.query(f"UPDATE {table_name} SET {col}='{val}' WHERE id={idx+1}")
        else:
            self.query(f"UPDATE {table_name} SET {col}={val} WHERE id={idx+1}")

    def IncrementBy(self,table_name,idx,col,by):
        self.query(f"UPDATE {table_name} SET {col}={col}+{by} WHERE id={idx+1}")

    def Insert(self,table_name,row):
        id=self.Count(table_name)
        row_str='0'
        for v in row:
            if type(v)==str:
                row_str+=f", '{v}'"
            else:
                v_reduced='NULL' if np.isnan(v) else v
                row_str+=f", {v_reduced}"
        self.query(f"INSERT INTO {table_name} VALUES ({row_str})")
        return id

    def getTopicTables(self,):
        res=self.query("SHOW TABLES")
        return [r[0] for r in res if r[0] not in ['subl','publ']]
    
    def Count(self,table_name):
        res=self.query(f"SELECT count(id) FROM {table_name}")
        return res[0][0]
    

def env_config():
    config={}
    config['persist']=True if os.environ['PERSIST']=='yes' else False
    config['broker_id']=os.environ['BID']
    return config