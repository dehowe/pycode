import pymysql.cursors
import pandas as pd
import time
import numpy as np
from datetime import datetime
#from apscheduler.schedulers.blocking import BlockingScheduler

class Database:
    def __init__(self, host, user, passwd, db, ):
        self.host = host
        self.db = db
        self.user = user
        self.passwd = passwd

    def db_connect(self):  # 连接数据库
        self.conn = pymysql.connect(host=self.host,
                               user=self.user,
                               password=self.passwd,
                               db=self.db, )
        self.cursor=self.conn.cursor() # 创建一个游标
    def close(self): #关闭数据库
        self.cursor.close()
        self.conn.close()

    def chaxun(self,sql):
        self.cursor.execute(sql)
        results=self.cursor.fetchall()# 获取查询的所有记录
        return results
    def insert_data(self,sql):
        self.cursor.execute(sql)
        # 提交
        self.conn.commit()

    def insert_datas(self,sql,list_datas):
        try:
            result = self.cursor.executemany(sql, list_datas)
            self.conn.commit()
            print('批量插入受影响的行数：', result)
        except Exception as e:
            print('db_insert_datas error: ', e.args)
    def clear_table(self,sql):
        self.cursor.execute(sql)
        # 提交
        self.conn.commit()
class Data_manager2():
    def __init__(self):
        self.lineid = 0

    def get_mysql_data(self, sql,db):
        results =db.chaxun(sql)
        return results

    def Zhanwei1(self, data, begin,num):
        for i in range(num):
            string1='qy'+str(8-i)+'accelerate'
            string2='qy'+str(8-i)+'stop_num'
            string3='qy'+str(8-i)+'mileage'
            string4='qy'+str(8-i)+'zs'
            string5='qy'+str(8-i)+'qy'
            string6='qy'+str(8-i)+'a'
            string7='qy'+str(8-i)+'v'
            data.insert(begin, string1, None)
            data.insert(begin, string2, None)
            data.insert(begin, string3, None)
            data.insert(begin, string4, None)
            data.insert(begin, string5, None)
            data.insert(begin, string6, None)
            data.insert(begin, string7, None)
        return data
    def Zhanwei2(self, data, begin,num):
        for i in range(num):
            string1='fz'+str(4-i)+'accelerate'
            string2='fz'+str(4-i)+'stop_num'
            string3='fz'+str(4-i)+'mileage'
            string4='fz'+str(4-i)+'zs'
            string5='fz'+str(4-i)+'fz'
            string6='fz'+str(4-i)+'a'
            string7='fz'+str(4-i)+'v'
            data.insert(begin, string1, None)
            data.insert(begin, string2, None)
            data.insert(begin, string3, None)
            data.insert(begin, string4, None)
            data.insert(begin, string5, None)
            data.insert(begin, string6, None)
            data.insert(begin, string7, None)
        return data
    def get_line15_031_data(self,db): #数据库读取line15_031数据，并合并
        line15QY1_sql = "select * from ys_line15_031_qy1data"
        line15QY2_sql = "select * from ys_line15_031_qy2data"
        line15QY3_sql = "select * from ys_line15_031_qy3data"
        line15QY4_sql = "select * from ys_line15_031_qy4data"
        line15FZ1_sql = "select * from ys_line15_031_fz1data"
        line15FZ2_sql = "select * from ys_line15_031_fz2data"

        line15QY1data = self.get_mysql_data(line15QY1_sql, db)
        line15QY2data = self.get_mysql_data(line15QY2_sql, db)
        line15QY3data = self.get_mysql_data(line15QY3_sql, db)
        line15QY4data = self.get_mysql_data(line15QY4_sql, db)
        line15FZ1data = self.get_mysql_data(line15FZ1_sql, db)
        line15FZ2data = self.get_mysql_data(line15FZ2_sql, db)

        if len(line15QY1data)!=0:
            line15QY1data_df = pd.DataFrame(list(line15QY1data))
            line15QY1data_df.columns=['date_qy1', 'qy1v', 'qy1a', 'qy1fz', 'qy1zs','qy1mileage','qy1stop_num','qy1accelerate']
            #line15QY1data_df1=line15QY1data_df[['date_qy1', 'qy1v', 'qy1a', 'qy1fz', 'qy1zs']]
            line15QY1data_df1 = line15QY1data_df.set_index('date_qy1')
        else:
            line15QY1data_df1=pd.DataFrame(columns=['qy1v', 'qy1a', 'qy1fz', 'qy1zs','qy1mileage','qy1stop_num','qy1accelerate'])
        if len(line15QY2data)!=0:
            line15QY2data_df = pd.DataFrame(list(line15QY2data))
            line15QY2data_df.columns=['date_qy2', 'qy2v', 'qy2a', 'qy2fz', 'qy2zs','qy2mileage','qy2stop_num','qy2accelerate']
            #line15QY2data_df1=line15QY2data_df[['date_qy2', 'qy2v', 'qy2a', 'qy2fz', 'qy2zs']]
            line15QY2data_df1 = line15QY2data_df.set_index('date_qy2')
        else:
            line15QY2data_df1=pd.DataFrame(columns=['qy2v', 'qy2a', 'qy2fz', 'qy2zs','qy2mileage','qy2stop_num','qy2accelerate'])
        if len(line15QY3data)!=0:
            line15QY3data_df = pd.DataFrame(list(line15QY3data))
            line15QY3data_df.columns=['date_qy3', 'qy3v', 'qy3a', 'qy3fz', 'qy3zs','qy3mileage','qy3stop_num','qy3accelerate']
            #line15QY3data_df1=line15QY3data_df[['date_qy3', 'qy3v', 'qy3a', 'qy3fz', 'qy3zs']]
            line15QY3data_df1 = line15QY3data_df.set_index('date_qy3')
        else:
            line15QY3data_df1=pd.DataFrame(columns=['qy3v', 'qy3a', 'qy3fz', 'qy3zs','qy3mileage','qy3stop_num','qy3accelerate'])
        if len(line15QY4data) != 0:
            line15QY4data_df = pd.DataFrame(list(line15QY3data))
            line15QY4data_df.columns = ['date_qy4', 'qy4v', 'qy4a', 'qy4fz', 'qy4zs', 'qy4mileage', 'qy4stop_num','qy4accelerate']
            # line15QY4data_df1=line15QY3data_df[['date_qy3', 'qy3v', 'qy3a', 'qy3fz', 'qy3zs']]
            line15QY4data_df1 = line15QY4data_df.set_index('date_qy4')
        else:
            line15QY4data_df1 = pd.DataFrame(columns=['qy4v', 'qy4a', 'qy4fz', 'qy4zs', 'qy4mileage', 'qy4stop_num', 'qy4accelerate'])
        if len(line15FZ1data)!=0:
            line15FZ1data_df = pd.DataFrame(list(line15FZ1data))
            line15FZ1data_df.columns=['date_fz1', 'fz1v', 'fz1a', 'fz1fz', 'fz1zs','fz1mileage','fz1stop_num','fz1accelerate']
            #line15FZ1data_df1=line15FZ1data_df[['date_fz1', 'fz1v', 'fz1a', 'fz1fz', 'fz1zs']]
            line15FZ1data_df1 = line15FZ1data_df.set_index('date_fz1')
        else:
            line15FZ1data_df1=pd.DataFrame(columns=['fz1v', 'fz1a', 'fz1fz', 'fz1zs','fz1mileage','fz1stop_num','fz1accelerate'])
        if len(line15FZ2data)!=0:
            line15FZ2data_df = pd.DataFrame(list(line15FZ2data))
            line15FZ2data_df.columns=['date_fz2', 'fz2v', 'fz2a', 'fz2fz', 'fz2zs','fz2mileage','fz2stop_num','fz2accelerate']
            #line15FZ2data_df1=line15FZ2data_df[['date_fz2', 'fz2v', 'fz2a', 'fz2fz', 'fz2zs']]
            line15FZ2data_df1 = line15FZ2data_df.set_index('date_fz2')
        else:
            line15FZ2data_df1=pd.DataFrame(columns=['fz2v', 'fz2a', 'fz2fz', 'fz2zs','fz2mileage','fz2stop_num','fz2accelerate'])

        result1 = line15QY1data_df1.join(line15QY2data_df1, how='outer')
        result2 = result1.join(line15QY3data_df1, how='outer')
        result2_1 = result2.join(line15QY4data_df1, how='outer')
        result3 = result2_1.join(line15FZ1data_df1, how='outer')
        result4 = result3.join(line15FZ2data_df1, how='outer')
        result4 = result4.reset_index()
        result4.rename(columns={'data_qy1': 'date'})
        result4 = self.Zhanwei1(result4, 29,4)
        result4 = self.Zhanwei2(result4, 71,2)
        result4.insert(0, 'trainid', '031')
        result4.insert(0, 'sline', '15')
        return result4
    def get_line13_417_data(self,db):
        line13QY1_sql = "select * from ys_line13_417_qy1data"
        line13QY2_sql = "select * from ys_line13_417_qy2data"
        line13QY3_sql = "select * from ys_line13_417_qy3data"
        line13FZ1_sql = "select * from ys_line13_417_fz1data"
        line13FZ2_sql = "select * from ys_line13_417_fz2data"

        line13QY1data = self.get_mysql_data(line13QY1_sql, db)
        line13QY2data = self.get_mysql_data(line13QY2_sql, db)
        line13QY3data = self.get_mysql_data(line13QY3_sql, db)
        line13FZ1data = self.get_mysql_data(line13FZ1_sql, db)
        line13FZ2data = self.get_mysql_data(line13FZ2_sql, db)

        if len(line13QY1data)!=0:
            line13QY1data_df = pd.DataFrame(list(line13QY1data))
            line13QY1data_df.columns=['date_qy1', 'qy1v', 'qy1a', 'qy1fz', 'qy1zs','qy1mileage','qy1stop_num','qy1accelerate']
            line13QY1data_df1 = line13QY1data_df.set_index('date_qy1')
        else:
            line13QY1data_df1=pd.DataFrame(columns=['qy1v', 'qy1a', 'qy1fz', 'qy1zs','qy1mileage','qy1stop_num','qy1accelerate'])
        if len(line13QY2data)!=0:
            line13QY2data_df = pd.DataFrame(list(line13QY2data))
            line13QY2data_df.columns=['date_qy2', 'qy2v', 'qy2a', 'qy2fz', 'qy2zs','qy2mileage','qy2stop_num','qy2accelerate']
            line13QY2data_df1 = line13QY2data_df.set_index('date_qy2')
        else:
            line13QY2data_df1=pd.DataFrame(columns=['qy2v', 'qy2a', 'qy2fz', 'qy2zs','qy2mileage','qy2stop_num','qy2accelerate'])
        if len(line13QY3data)!=0:
            line13QY3data_df = pd.DataFrame(list(line13QY3data))
            line13QY3data_df.columns=['date_qy3', 'qy3v', 'qy3a', 'qy3fz', 'qy3zs','qy3mileage','qy3stop_num','qy3accelerate']
            line13QY3data_df1 = line13QY3data_df.set_index('date_qy3')
        else:
            line13QY3data_df1=pd.DataFrame(columns=['qy3v', 'qy3a', 'qy3fz', 'qy3zs','qy3mileage','qy3stop_num','qy3accelerate'])
        if len(line13FZ1data)!=0:
            line13FZ1data_df = pd.DataFrame(list(line13FZ1data))
            line13FZ1data_df.columns=['date_fz1', 'fz1v', 'fz1a', 'fz1fz', 'fz1zs','fz1mileage','fz1stop_num','fz1accelerate']
            line13FZ1data_df1 = line13FZ1data_df.set_index('date_fz1')
        else:
            line13FZ1data_df1=pd.DataFrame(columns=['fz1v', 'fz1a', 'fz1fz', 'fz1zs','fz1mileage','fz1stop_num','fz1accelerate'])
        if len(line13FZ2data)!=0:
            line13FZ2data_df = pd.DataFrame(list(line13FZ2data))
            line13FZ2data_df.columns=['date_fz2', 'fz2v', 'fz2a', 'fz2fz', 'fz2zs','fz2mileage','fz2stop_num','fz2accelerate']
            line13FZ2data_df1 = line13FZ2data_df.set_index('date_fz2')
        else:
            line13FZ2data_df1=pd.DataFrame(columns=['fz2v', 'fz2a', 'fz2fz', 'fz2zs','fz2mileage','fz2stop_num','fz2accelerate'])

        result1 = line13QY1data_df1.join(line13QY2data_df1, how='outer')
        result2 = result1.join(line13QY3data_df1, how='outer')
        result3 = result2.join(line13FZ1data_df1, how='outer')
        result4 = result3.join(line13FZ2data_df1, how='outer')
        result4 = result4.reset_index()
        result4.rename(columns={'data_qy1': 'date'})
        result4 = self.Zhanwei1(result4, 22,5)
        result4 = self.Zhanwei2(result4, 71,2)
        result4.insert(0, 'trainid', '417')
        result4.insert(0, 'sline', '13')
        return result4
    def get_line6_009_data(self, db):  #数据库读取line6_009数据，并合并
        line6QY1_sql = "select * from ys_line6_009_qy1data"
        line6QY2_sql = "select * from ys_line6_009_qy2data"
        line6QY3_sql = "select * from ys_line6_009_qy3data"
        line6QY4_sql = "select * from ys_line6_009_qy4data"
        line6QY5_sql = "select * from ys_line6_009_qy5data"
        line6QY6_sql = "select * from ys_line6_009_qy6data"
        line6FZ1_sql = "select * from ys_line6_009_fz1data"
        line6FZ2_sql = "select * from ys_line6_009_fz2data"

        line6QY1data = self.get_mysql_data(line6QY1_sql, db)
        line6QY2data = self.get_mysql_data(line6QY2_sql, db)
        line6QY3data = self.get_mysql_data(line6QY3_sql, db)
        line6QY4data = self.get_mysql_data(line6QY4_sql, db)
        line6QY5data = self.get_mysql_data(line6QY5_sql, db)
        line6QY6data = self.get_mysql_data(line6QY6_sql, db)
        line6FZ1data = self.get_mysql_data(line6FZ1_sql, db)
        line6FZ2data = self.get_mysql_data(line6FZ2_sql, db)

        if len(line6QY1data) != 0:
            line6QY1data_df = pd.DataFrame(list(line6QY1data))
            line6QY1data_df.columns = ['date_qy1', 'qy1v', 'qy1a', 'qy1fz', 'qy1zs', 'qy1mileage', 'qy1stop_num', 'qy1accelerate']
            line6QY1data_df1 = line6QY1data_df.set_index('date_qy1')
        else:
            line6QY1data_df1 = pd.DataFrame(columns=['qy1v', 'qy1a', 'qy1fz', 'qy1zs', 'qy1mileage', 'qy1stop_num', 'qy1accelerate'])
        if len(line6QY2data) != 0:
            line6QY2data_df = pd.DataFrame(list(line6QY2data))
            line6QY2data_df.columns = ['date_qy2', 'qy2v', 'qy2a', 'qy2fz', 'qy2zs', 'qy2mileage', 'qy2stop_num','qy2accelerate']
            line6QY2data_df1 = line6QY2data_df.set_index('date_qy2')
        else:
            line6QY2data_df1 = pd.DataFrame(columns=['qy2v', 'qy2a', 'qy2fz', 'qy2zs', 'qy2mileage', 'qy2stop_num', 'qy2accelerate'])
        if len(line6QY3data) != 0:
            line6QY3data_df = pd.DataFrame(list(line6QY3data))
            line6QY3data_df.columns = ['date_qy3', 'qy3v', 'qy3a', 'qy3fz', 'qy3zs', 'qy3mileage', 'qy3stop_num','qy3accelerate']
            line6QY3data_df1 = line6QY3data_df.set_index('date_qy3')
        else:
            line6QY3data_df1 = pd.DataFrame(columns=['qy3v', 'qy3a', 'qy3fz', 'qy3zs', 'qy3mileage', 'qy3stop_num', 'qy3accelerate'])
        if len(line6QY4data) != 0:
            line6QY4data_df = pd.DataFrame(list(line6QY4data))
            line6QY4data_df.columns = ['date_qy4', 'qy4v', 'qy4a', 'qy4fz', 'qy4zs', 'qy4mileage', 'qy4stop_num', 'qy4accelerate']
            line6QY4data_df1 = line6QY4data_df.set_index('date_qy4')
        else:
            line6QY4data_df1 = pd.DataFrame(columns=['qy4v', 'qy4a', 'qy4fz', 'qy4zs', 'qy4mileage', 'qy4stop_num', 'qy4accelerate'])
        if len(line6QY5data) != 0:
            line6QY5data_df = pd.DataFrame(list(line6QY5data))
            line6QY5data_df.columns = ['date_qy5', 'qy5v', 'qy5a', 'qy5fz', 'qy5zs', 'qy5mileage', 'qy5stop_num','qy5accelerate']
            line6QY5data_df1 = line6QY5data_df.set_index('date_qy5')
        else:
            line6QY5data_df1 = pd.DataFrame(columns=['qy5v', 'qy5a', 'qy5fz', 'qy5zs', 'qy5mileage', 'qy5stop_num', 'qy5accelerate'])
        if len(line6QY6data) != 0:
            line6QY6data_df = pd.DataFrame(list(line6QY6data))
            line6QY6data_df.columns = ['date_qy6', 'qy6v', 'qy6a', 'qy6fz', 'qy6zs', 'qy6mileage', 'qy6stop_num','qy6accelerate']
            line6QY6data_df1 = line6QY6data_df.set_index('date_qy6')
        else:
            line6QY6data_df1 = pd.DataFrame(columns=['qy6v', 'qy6a', 'qy6fz', 'qy6zs', 'qy6mileage', 'qy6stop_num', 'qy6accelerate'])
        if len(line6FZ1data) != 0:
            line6FZ1data_df = pd.DataFrame(list(line6FZ1data))
            line6FZ1data_df.columns = ['date_fz1', 'fz1v', 'fz1a', 'fz1fz', 'fz1zs', 'fz1mileage', 'fz1stop_num', 'fz1accelerate']
            line6FZ1data_df1 = line6FZ1data_df.set_index('date_fz1')
        else:
            line6FZ1data_df1 = pd.DataFrame( columns=['fz1v', 'fz1a', 'fz1fz', 'fz1zs', 'fz1mileage', 'fz1stop_num', 'fz1accelerate'])
        if len(line6FZ2data) != 0:
            line6FZ2data_df = pd.DataFrame(list(line6FZ2data))
            line6FZ2data_df.columns = ['date_fz2', 'fz2v', 'fz2a', 'fz2fz', 'fz2zs', 'fz2mileage', 'fz2stop_num', 'fz2accelerate']
            line6FZ2data_df1 = line6FZ2data_df.set_index('date_fz2')
        else:
            line6FZ2data_df1 = pd.DataFrame(columns=['fz2v', 'fz2a', 'fz2fz', 'fz2zs', 'fz2mileage', 'fz2stop_num', 'fz2accelerate'])

        result1 = line6QY1data_df1.join(line6QY2data_df1, how='outer')
        result2 = result1.join(line6QY3data_df1, how='outer')
        result2_1 = result2.join(line6QY4data_df1, how='outer')
        result2_2 = result2_1.join(line6QY5data_df1, how='outer')
        result2_3 = result2_2.join(line6QY6data_df1, how='outer')
        result3 = result2_3.join(line6FZ1data_df1, how='outer')
        result4 = result3.join(line6FZ2data_df1, how='outer')
        result4 = result4.reset_index()
        result4.rename(columns={'data_qy1': 'date'})
        result4 = self.Zhanwei1(result4, 22, 2)
        result4 = self.Zhanwei2(result4, 71, 2)
        result4.insert(0, 'trainid', '009')
        result4.insert(0, 'sline', '6')
        return result4
    def get_line8_066_data(self, db): #数据库读取line8_066数据，并合并
        line8QY1_sql = "select * from ys_line8_066_qy1data"
        line8QY2_sql = "select * from ys_line8_066_qy2data"
        line8QY3_sql = "select * from ys_line8_066_qy3data"
        line8FZ1_sql = "select * from ys_line8_066_fz1data"
        line8FZ2_sql = "select * from ys_line8_066_fz2data"

        line8QY1data = self.get_mysql_data(line8QY1_sql, db)
        line8QY2data = self.get_mysql_data(line8QY2_sql, db)
        line8QY3data = self.get_mysql_data(line8QY3_sql, db)
        line8FZ1data = self.get_mysql_data(line8FZ1_sql, db)
        line8FZ2data = self.get_mysql_data(line8FZ2_sql, db)

        if len(line8QY1data) != 0:
            line8QY1data_df = pd.DataFrame(list(line8QY1data))
            line8QY1data_df.columns = ['date_qy1', 'qy1v', 'qy1a', 'qy1fz', 'qy1zs', 'qy1mileage', 'qy1stop_num', 'qy1accelerate']
            line8QY1data_df1 = line8QY1data_df.set_index('date_qy1')
        else:
            line8QY1data_df1 = pd.DataFrame(columns=['qy1v', 'qy1a', 'qy1fz', 'qy1zs', 'qy1mileage', 'qy1stop_num', 'qy1accelerate'])
        if len(line8QY2data) != 0:
            line8QY2data_df = pd.DataFrame(list(line8QY2data))
            line8QY2data_df.columns = ['date_qy2', 'qy2v', 'qy2a', 'qy2fz', 'qy2zs', 'qy2mileage', 'qy2stop_num', 'qy2accelerate']
            line8QY2data_df1 = line8QY2data_df.set_index('date_qy2')
        else:
            line8QY2data_df1 = pd.DataFrame(columns=['qy2v', 'qy2a', 'qy2fz', 'qy2zs', 'qy2mileage', 'qy2stop_num', 'qy2accelerate'])
        if len(line8QY3data) != 0:
            line8QY3data_df = pd.DataFrame(list(line8QY3data))
            line8QY3data_df.columns = ['date_qy3', 'qy3v', 'qy3a', 'qy3fz', 'qy3zs', 'qy3mileage', 'qy3stop_num','qy3accelerate']
            line8QY3data_df1 = line8QY3data_df.set_index('date_qy3')
        else:
            line8QY3data_df1 = pd.DataFrame(columns=['qy3v', 'qy3a', 'qy3fz', 'qy3zs', 'qy3mileage', 'qy3stop_num', 'qy3accelerate'])
        if len(line8FZ1data) != 0:
            line8FZ1data_df = pd.DataFrame(list(line8FZ1data))
            line8FZ1data_df.columns = ['date_fz1', 'fz1v', 'fz1a', 'fz1fz', 'fz1zs', 'fz1mileage', 'fz1stop_num','fz1accelerate']
            line8FZ1data_df1 = line8FZ1data_df.set_index('date_fz1')
        else:
            line8FZ1data_df1 = pd.DataFrame(columns=['fz1v', 'fz1a', 'fz1fz', 'fz1zs', 'fz1mileage', 'fz1stop_num', 'fz1accelerate'])
        if len(line8FZ2data) != 0:
            line8FZ2data_df = pd.DataFrame(list(line8FZ2data))
            line8FZ2data_df.columns = ['date_fz2', 'fz2v', 'fz2a', 'fz2fz', 'fz2zs', 'fz2mileage', 'fz2stop_num', 'fz2accelerate']
            line8FZ2data_df1 = line8FZ2data_df.set_index('date_fz2')
        else:
            line8FZ2data_df1 = pd.DataFrame(columns=['fz2v', 'fz2a', 'fz2fz', 'fz2zs', 'fz2mileage', 'fz2stop_num', 'fz2accelerate'])

        result1 = line8QY1data_df1.join(line8QY2data_df1, how='outer')
        result2 = result1.join(line8QY3data_df1, how='outer')
        result3 = result2.join(line8FZ1data_df1, how='outer')
        result4 = result3.join(line8FZ2data_df1, how='outer')
        result4 = result4.reset_index()
        result4.rename(columns={'data_qy1': 'date'})
        result4 = self.Zhanwei1(result4, 22, 5)
        result4 = self.Zhanwei2(result4, 71, 2)
        result4.insert(0, 'trainid', '066')
        result4.insert(0, 'sline', '8')
        return result4
    def get_line21_019_data(self, db):  # 数据库读取line21_019数据，并合并
        line21QY1_sql = "select * from ys_line21_019_qy1data"
        line21QY2_sql = "select * from ys_line21_019_qy2data"
        line21QY3_sql = "select * from ys_line21_019_qy3data"
        line21QY4_sql = "select * from ys_line21_019_qy3data"
        line21FZ1_sql = "select * from ys_line21_019_fz1data"
        line21FZ2_sql = "select * from ys_line21_019_fz2data"

        line21QY1data = self.get_mysql_data(line21QY1_sql, db)
        line21QY2data = self.get_mysql_data(line21QY2_sql, db)
        line21QY3data = self.get_mysql_data(line21QY3_sql, db)
        line21QY4data = self.get_mysql_data(line21QY4_sql, db)
        line21FZ1data = self.get_mysql_data(line21FZ1_sql, db)
        line21FZ2data = self.get_mysql_data(line21FZ2_sql, db)

        if len(line21QY1data) != 0:
            line21QY1data_df = pd.DataFrame(list(line21QY1data))
            line21QY1data_df.columns = ['date_qy1', 'qy1v', 'qy1a', 'qy1fz', 'qy1zs', 'qy1mileage', 'qy1stop_num','qy1accelerate']
            line21QY1data_df1 = line21QY1data_df.set_index('date_qy1')
        else:
            line21QY1data_df1 = pd.DataFrame(columns=['qy1v', 'qy1a', 'qy1fz', 'qy1zs', 'qy1mileage', 'qy1stop_num', 'qy1accelerate'])
        if len(line21QY2data) != 0:
            line21QY2data_df = pd.DataFrame(list(line21QY2data))
            line21QY2data_df.columns = ['date_qy2', 'qy2v', 'qy2a', 'qy2fz', 'qy2zs', 'qy2mileage', 'qy2stop_num','qy2accelerate']
            line21QY2data_df1 = line21QY2data_df.set_index('date_qy2')
        else:
            line21QY2data_df1 = pd.DataFrame(columns=['qy2v', 'qy2a', 'qy2fz', 'qy2zs', 'qy2mileage', 'qy2stop_num', 'qy2accelerate'])
        if len(line21QY3data) != 0:
            line21QY3data_df = pd.DataFrame(list(line21QY3data))
            line21QY3data_df.columns = ['date_qy3', 'qy3v', 'qy3a', 'qy3fz', 'qy3zs', 'qy3mileage', 'qy3stop_num', 'qy3accelerate']
            line21QY3data_df1 = line21QY3data_df.set_index('date_qy3')
        else:
            line21QY3data_df1 = pd.DataFrame(columns=['qy3v', 'qy3a', 'qy3fz', 'qy3zs', 'qy3mileage', 'qy3stop_num', 'qy3accelerate'])
        if len(line21QY4data) != 0:
            line21QY4data_df = pd.DataFrame(list(line21QY3data))
            line21QY4data_df.columns = ['date_qy4', 'qy4v', 'qy4a', 'qy4fz', 'qy4zs', 'qy4mileage', 'qy4stop_num', 'qy4accelerate']
            line21QY4data_df1 = line21QY4data_df.set_index('date_qy4')
        else:
            line21QY4data_df1 = pd.DataFrame(columns=['qy4v', 'qy4a', 'qy4fz', 'qy4zs', 'qy4mileage', 'qy4stop_num', 'qy4accelerate'])
        if len(line21FZ1data) != 0:
            line21FZ1data_df = pd.DataFrame(list(line21FZ1data))
            line21FZ1data_df.columns = ['date_fz1', 'fz1v', 'fz1a', 'fz1fz', 'fz1zs', 'fz1mileage', 'fz1stop_num', 'fz1accelerate']
            line21FZ1data_df1 = line21FZ1data_df.set_index('date_fz1')
        else:
            line21FZ1data_df1 = pd.DataFrame(columns=['fz1v', 'fz1a', 'fz1fz', 'fz1zs', 'fz1mileage', 'fz1stop_num', 'fz1accelerate'])
        if len(line21FZ2data) != 0:
            line21FZ2data_df = pd.DataFrame(list(line21FZ2data))
            line21FZ2data_df.columns = ['date_fz2', 'fz2v', 'fz2a', 'fz2fz', 'fz2zs', 'fz2mileage', 'fz2stop_num','fz2accelerate']
            line21FZ2data_df1 = line21FZ2data_df.set_index('date_fz2')
        else:
            line21FZ2data_df1 = pd.DataFrame(columns=['fz2v', 'fz2a', 'fz2fz', 'fz2zs', 'fz2mileage', 'fz2stop_num', 'fz2accelerate'])

        result1 = line21QY1data_df1.join(line21QY2data_df1, how='outer')
        result2 = result1.join(line21QY3data_df1, how='outer')
        result2_1 = result2.join(line21QY4data_df1, how='outer')
        result3 = result2_1.join(line21FZ1data_df1, how='outer')
        result4 = result3.join(line21FZ2data_df1, how='outer')
        result4 = result4.reset_index()
        result4.rename(columns={'data_qy1': 'date'})
        result4 = self.Zhanwei1(result4, 29, 4)
        result4 = self.Zhanwei2(result4, 71, 2)
        result4.insert(0, 'trainid', '019')
        result4.insert(0, 'sline', '21')
        return result4
    def get_line1_S403_data(self, db): #数据库读取line1_S403数据，并合并
        line1QY1_sql = "select * from ys_line1_s403_qy1data"
        line1QY2_sql = "select * from ys_line1_s403_qy2data"
        line1QY3_sql = "select * from ys_line1_s403_qy3data"
        line1FZ1_sql = "select * from ys_line1_s403_fz1data"
        line1FZ2_sql = "select * from ys_line1_s403_fz1data"

        line1QY1data = self.get_mysql_data(line1QY1_sql, db)
        line1QY2data = self.get_mysql_data(line1QY2_sql, db)
        line1QY3data = self.get_mysql_data(line1QY3_sql, db)
        line1FZ1data = self.get_mysql_data(line1FZ1_sql, db)
        line1FZ2data = self.get_mysql_data(line1FZ2_sql, db)

        if len(line1QY1data) != 0:
            line1QY1data_df = pd.DataFrame(list(line1QY1data))
            line1QY1data_df.columns = ['date_qy1', 'qy1v', 'qy1a', 'qy1fz', 'qy1zs', 'qy1mileage', 'qy1stop_num', 'qy1accelerate']
            line1QY1data_df1 = line1QY1data_df.set_index('date_qy1')
        else:
            line1QY1data_df1 = pd.DataFrame(columns=['qy1v', 'qy1a', 'qy1fz', 'qy1zs', 'qy1mileage', 'qy1stop_num', 'qy1accelerate'])
        if len(line1QY2data) != 0:
            line1QY2data_df = pd.DataFrame(list(line1QY2data))
            line1QY2data_df.columns = ['date_qy2', 'qy2v', 'qy2a', 'qy2fz', 'qy2zs', 'qy2mileage', 'qy2stop_num', 'qy2accelerate']
            line1QY2data_df1 = line1QY2data_df.set_index('date_qy2')
        else:
            line1QY2data_df1 = pd.DataFrame(columns=['qy2v', 'qy2a', 'qy2fz', 'qy2zs', 'qy2mileage', 'qy2stop_num', 'qy2accelerate'])
        if len(line1QY3data) != 0:
            line1QY3data_df = pd.DataFrame(list(line1QY3data))
            line1QY3data_df.columns = ['date_qy3', 'qy3v', 'qy3a', 'qy3fz', 'qy3zs', 'qy3mileage', 'qy3stop_num','qy3accelerate']
            line1QY3data_df1 = line1QY3data_df.set_index('date_qy3')
        else:
            line1QY3data_df1 = pd.DataFrame(columns=['qy3v', 'qy3a', 'qy3fz', 'qy3zs', 'qy3mileage', 'qy3stop_num', 'qy3accelerate'])
        if len(line1FZ1data) != 0:
            line1FZ1data_df = pd.DataFrame(list(line1FZ1data))
            line1FZ1data_df.columns = ['date_fz1', 'fz1v', 'fz1a', 'fz1fz', 'fz1zs', 'fz1mileage', 'fz1stop_num','fz1accelerate']
            line1FZ1data_df1 = line1FZ1data_df.set_index('date_fz1')
        else:
            line1FZ1data_df1 = pd.DataFrame(columns=['fz1v', 'fz1a', 'fz1fz', 'fz1zs', 'fz1mileage', 'fz1stop_num', 'fz1accelerate'])
        if len(line1FZ2data) != 0:
            line1FZ2data_df = pd.DataFrame(list(line1FZ2data))
            line1FZ2data_df.columns = ['date_fz2', 'fz2v', 'fz2a', 'fz2fz', 'fz2zs', 'fz2mileage', 'fz2stop_num', 'fz2accelerate']
            line1FZ2data_df1 = line1FZ2data_df.set_index('date_fz2')
        else:
            line1FZ2data_df1 = pd.DataFrame(columns=['fz2v', 'fz2a', 'fz2fz', 'fz2zs', 'fz2mileage', 'fz2stop_num', 'fz2accelerate'])

        result1 = line1QY1data_df1.join(line1QY2data_df1, how='outer')
        result2 = result1.join(line1QY3data_df1, how='outer')
        result3 = result2.join(line1FZ1data_df1, how='outer')
        result4 = result3.join(line1FZ2data_df1, how='outer')
        result4 = result4.reset_index()
        result4.rename(columns={'data_qy1': 'date'})
        result4 = self.Zhanwei1(result4, 22, 5)
        result4 = self.Zhanwei2(result4, 71, 2)
        result4.insert(0, 'trainid', 'S403')
        result4.insert(0, 'sline', '1')
        return result4

    def linedata_output_div(self,data_df,database):

        divide=len(data_df)//5000
        begin_index=0
        end_index=5000
        for i in range(divide):
            self.linedata_output(data_df.iloc[begin_index:end_index,:],database)
            begin_index=begin_index+5000
            end_index=end_index+5000
        self.linedata_output(data_df.iloc[begin_index:,:], database)

    def linedata_output(self, data_df, database):  # 异常数据导出到表
        data_df=data_df.fillna(0)  #将NAN替换为0
        #data_df = data_df.where(data_df.notnull(), None)
        data_df_np = data_df.values
        data_df_np=data_df_np.astype('str')
        data_df_list = data_df_np.tolist()
        sqls_insert_datas = "insert into kf_lines_data(sline,trainid,date,qy1v,qy1a,qy1qy,qy1zs,qy1mileage,qy1stop_num,qy1accelerate,qy2v,qy2a,qy2qy,qy2zs,qy2mileage,qy2stop_num,qy2accelerate,qy3v,qy3a,qy3qy,qy3zs,qy3mileage,qy3stop_num,qy3accelerate,qy4v,qy4a,qy4qy,qy4zs,qy4mileage,qy4stop_num,qy4accelerate,qy5v,qy5a,qy5qy,qy5zs,qy5mileage,qy5stop_num,qy5accelerate,qy6v,qy6a,qy6qy,qy6zs,qy6mileage,qy6stop_num,qy6accelerate,qy7v,qy7a,qy7qy,qy7zs,qy7mileage,qy7stop_num,qy7accelerate,qy8v,qy8a,qy8qy,qy8zs,qy8mileage,qy8stop_num,qy8accelerate,fz1v,fz1a,fz1fz,fz1zs,fz1mileage,fz1stop_num,fz1accelerate,fz2v,fz2a,fz2fz,fz2zs,fz2mileage,fz2stop_num,fz2accelerate,fz3v,fz3a,fz3fz,fz3zs,fz3mileage,fz3stop_num,fz3accelerate,fz4v,fz4a,fz4fz,fz4zs,fz4mileage,fz4stop_num,fz4accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
        database.insert_datas(sqls_insert_datas, data_df_list)
    def data_manager2_auto(self,db):
        print('原始数据处理功能启动..')
        print('处理line15_031中..')
        line15_031 = self.get_line15_031_data(db)
        self.linedata_output_div(line15_031,db)
        print('处理line13_417中..')
        line13_417 = self.get_line13_417_data(db)
        self.linedata_output_div(line13_417, db)
        print('处理line6_009中..')
        line16_009 = self.get_line6_009_data(db)
        self.linedata_output_div(line16_009, db)
        print('处理line8_066中..')
        line8_066 = self.get_line8_066_data(db)
        self.linedata_output_div(line8_066, db)
        print('处理line21_019中..')
        line21_019 = self.get_line21_019_data(db)
        self.linedata_output_div(line21_019, db)
        print('处理line1_S403中..')
        line1_S403 = self.get_line1_S403_data(db)
        self.linedata_output_div(line1_S403, db)
        print('原始数据处理功能结束')

class Data_manager3():
    def __init__(self):
        self.lineid = 0
    def get_mysql_data(self, sql,db):
        results =db.chaxun(sql)
        return results
    def timeHandle2(self,time_str):
        try:
            timeArray = time.strptime(time_str, "%Y-%m-%d %H:%M:%S")
            # 转换为时间戳
            time_stamp = int(time.mktime(timeArray))
        except:
            print('error')
        return time_stamp

    def datetime_toString(self,dt):
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    def timeHandle(self,time_str, len):
        time_str1 = []
        for i in range(len):
            try:
                timeArray = time.strptime(self.datetime_toString(time_str[i]), "%Y-%m-%d %H:%M:%S")
                # 转换为时间戳
                timeStamp = int(time.mktime(timeArray))
                time_str1.append(timeStamp)
                # time_str[i]=timeStamp
            except:
                print('error')
        return time_str1
    def timeHandle3(self,time_num):
        timeArray = time.localtime(time_num)
        otherStyleTime = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
        return otherStyleTime
    def Zhanwei1(self, data, begin,num):
        for i in range(num):
            string1='zs'+str(8-i)
            string2='qy'+str(8-i)
            data.insert(begin, string1, None)
            data.insert(begin, string2, None)
        return data
    def Zhanwei2(self, data, begin,num):
        for i in range(num):
            string1='fz'+str(4-i)
            data.insert(begin, string1, None)
        return data
    def year_change(self,data1):
        data_year = []
        data_month = []
        data_week = []
        data_day = []
        data_hour = []
        data_min = []
        for i in range(len(data1)):
            b = datetime.strptime(data1[i], '%Y-%m-%d %H:%M:%S').strftime('%Y')
            c = datetime.strptime(data1[i], '%Y-%m-%d %H:%M:%S').strftime('%m')
            d = datetime.strptime(data1[i], '%Y-%m-%d %H:%M:%S').strftime('%d')
            e = datetime.strptime(data1[i], '%Y-%m-%d %H:%M:%S').strftime('%H')
            f = datetime.strptime(data1[i], '%Y-%m-%d %H:%M:%S').strftime('%M')
            data_year.append(int(b))
            data_month.append(int(c))
            data_day.append(int(d))
            data_hour.append(int(e))
            data_min.append(int(f))
            d = self.get_week(int(b), int(c), int(d))
            data_week.append(d)
            # if d<=7:
            #     data_week.append(1)
            # elif d<=14:
            #     data_week.append(2)
            # elif d<=21:
            #     data_week.append(3)
            # else:
            #     data_week.append(4)
        array_to_matrix1 = np.mat(data_year)
        w = pd.DataFrame(array_to_matrix1)
        w = pd.DataFrame(w.values.T, index=w.columns, columns=w.index)  # 转置
        array_to_matrix1 = np.mat(data_month)
        x = pd.DataFrame(array_to_matrix1)
        x = pd.DataFrame(x.values.T, index=x.columns, columns=x.index)  # 转置
        array_to_matrix1 = np.mat(data_week)
        y = pd.DataFrame(array_to_matrix1)
        y = pd.DataFrame(y.values.T, index=y.columns, columns=y.index)  # 转置

        array_to_matrix1 = np.mat(data_day)
        w1 = pd.DataFrame(array_to_matrix1)
        w1 = pd.DataFrame(w1.values.T, index=w1.columns, columns=w1.index)  # 转置
        array_to_matrix1 = np.mat(data_hour)
        x1 = pd.DataFrame(array_to_matrix1)
        x1 = pd.DataFrame(x1.values.T, index=x1.columns, columns=x1.index)  # 转置
        array_to_matrix1 = np.mat(data_min)
        y1 = pd.DataFrame(array_to_matrix1)
        y1 = pd.DataFrame(y1.values.T, index=y1.columns, columns=y1.index)  # 转置
        return w, x, w1, x1, y1, y

    def get_week(self,year, month, day):
        days_month2 = 0
        if year % 4 == 0:
            days_month2 = 29
        else:
            days_month2 = 28
        days = 0
        if month == 1:
            days = day
        elif month == 2:
            days = 31 + day
        elif month == 3:
            days = 31 + days_month2 + day
        elif month == 4:
            days = 62 + days_month2 + day
        elif month == 5:
            days = 92 + days_month2 + day
        elif month == 6:
            days = 123 + days_month2 + day
        elif month == 7:
            days = 153 + days_month2 + day
        elif month == 8:
            days = 184 + days_month2 + day
        elif month == 9:
            days = 215 + days_month2 + day
        elif month == 10:
            days = 245 + days_month2 + day
        elif month == 11:
            days = 276 + days_month2 + day
        else:
            days = 306 + days_month2 + day
        week = days // 7 + 1
        return week
    def get_energy_day(self,data):
        if len(data) != 0:
            data_list=list(data)
            linexdata_timestr = [x[0] for x in data_list]
            energy=[x[3] for x in data_list]
            energy_re=[x[4] for x in data_list]
            QY_sum = []
            QY_re_sum = []
            Time_output = []
            day = self.timeHandle2(str(linexdata_timestr[0])) // 86400
            day_index = day * 86400-28800 + 86400
            j=0
            for i in range(len(linexdata_timestr)):
                #print('%.2f'%(i/len(linexdata_timestr)))
                time_stamp = self.timeHandle2(str(linexdata_timestr[i]))
                if time_stamp<day_index:
                    continue
                else:
                    QY_sum.append(energy[i] - energy[j])
                    QY_re_sum.append(energy_re[j]-energy_re[i])
                    Time_output.append(self.timeHandle3(day_index-86400))
                    day_index=day_index+86400
                    j=i
            array_to_matrix1 = np.mat(Time_output)
            Time_output_df = pd.DataFrame(array_to_matrix1)
            Time_output_df = pd.DataFrame(Time_output_df.values.T, index=Time_output_df.columns,columns=Time_output_df.index)  # 转置
            Time_output_df.columns = ['时间']
            array_to_matrix2 = np.mat(QY_sum)
            QY_sum_df = pd.DataFrame(array_to_matrix2)
            QY_sum_df = pd.DataFrame(QY_sum_df.values.T, index=QY_sum_df.columns,columns=QY_sum_df.index)  # 转置
            QY_sum_df.columns = ['qy']
            array_to_matrix3 = np.mat(QY_re_sum)
            QY_re_sum_df = pd.DataFrame(array_to_matrix3)
            QY_re_sum_df = pd.DataFrame(QY_re_sum_df.values.T, index=QY_re_sum_df.columns,columns=QY_re_sum_df.index)  # 转置
            QY_re_sum_df.columns = ['zs']
            result=pd.concat([Time_output_df, QY_sum_df, QY_re_sum_df], axis=1,join='inner')
        else:
            result=pd.DataFrame(columns=['qy_date', 'qy', 'zs'])
        return result
    def get_energy_min(self,data):
        if len(data) != 0:
            data_list = list(data)
            linexdata_timestr = [x[0] for x in data_list]
            energy = [x[3] for x in data_list]
            energy_re = [x[4] for x in data_list]
            QY_sum = []
            QY_re_sum = []
            Time_output = []
            day = self.timeHandle2(str(linexdata_timestr[0])) // 86400
            day_rest = self.timeHandle2(str(linexdata_timestr[0])) % 86400  # 如果时间大于8：00，index会归于下一天
            if day_rest < 43200:
                day_index = day * 86400 - 28800 + 60  # 每min
            else:
                day_index = (day + 1) * 86400 - 28800 + 60  # 每min
            j = 0
            time_end = (self.timeHandle2(str(linexdata_timestr[len(linexdata_timestr) - 1])) // 86400 + 1) * 86400 - 28800  # 补全min数据

            time_stamp = self.timeHandle(linexdata_timestr, len(linexdata_timestr))
            for i in range(len(linexdata_timestr)):
                # print('%.2f'%(i/len(linexdata_timestr)))
                # time_stamp = timeHandle2(str(linexdata_timestr[i]))
                if time_stamp[i] < day_index:
                    continue
                elif time_stamp[i] > day_index + 60:
                    while(time_stamp[i] >day_index + 60):
                        QY_sum.append(0)
                        QY_re_sum.append(0)
                        Time_output.append(self.timeHandle3(day_index - 60))
                        day_index = day_index + 60
                else:
                    QY_sum.append(energy[i] - energy[j])
                    QY_re_sum.append(energy_re[j] - energy_re[i])
                    Time_output.append(self.timeHandle3(day_index - 60))
                    # print(self.timeHandle3(day_index - 60))
                    # print(energy[i] - energy[j])
                    # print(energy_re[j] - energy_re[i])
                    day_index = day_index + 60
                    j = i
            last_num = int((time_end - day_index) / 60 + 1)
            for i in range(last_num):
                QY_sum.append(0)
                QY_re_sum.append(0)
                Time_output.append(self.timeHandle3(day_index - 60))
                day_index = day_index + 60

            array_to_matrix1 = np.mat(Time_output)
            Time_output_df = pd.DataFrame(array_to_matrix1)
            Time_output_df = pd.DataFrame(Time_output_df.values.T, index=Time_output_df.columns,
                                          columns=Time_output_df.index)  # 转置
            Time_output_df.columns = ['时间']
            array_to_matrix2 = np.mat(QY_sum)
            QY_sum_df = pd.DataFrame(array_to_matrix2)
            QY_sum_df = pd.DataFrame(QY_sum_df.values.T, index=QY_sum_df.columns, columns=QY_sum_df.index)  # 转置
            QY_sum_df.columns = ['qy']
            array_to_matrix3 = np.mat(QY_re_sum)
            QY_re_sum_df = pd.DataFrame(array_to_matrix3)
            QY_re_sum_df = pd.DataFrame(QY_re_sum_df.values.T, index=QY_re_sum_df.columns,
                                        columns=QY_re_sum_df.index)  # 转置
            QY_re_sum_df.columns = ['zs']
            result = pd.concat([Time_output_df, QY_sum_df, QY_re_sum_df], axis=1, join='inner')
        else:
            result = pd.DataFrame(columns=['qy_date', 'qy', 'zs'])
        return result
    def get_line15_031_data(self,db):  #15号线数据提取日能耗提取
        print('读取数据中..')
        linexdata_qy1_sql = "select * from ys_line15_031_qy1data"
        linexdata_qy1 = self.get_mysql_data(linexdata_qy1_sql, db)
        linexdata_qy2_sql = "select * from ys_line15_031_qy2data"
        linexdata_qy2 = self.get_mysql_data(linexdata_qy2_sql, db)
        linexdata_qy3_sql = "select * from ys_line15_031_qy3data"
        linexdata_qy3 = self.get_mysql_data(linexdata_qy3_sql, db)
        linexdata_qy4_sql = "select * from ys_line15_031_qy4data"
        linexdata_qy4 = self.get_mysql_data(linexdata_qy4_sql, db)
        linexdata_fz1_sql = "select * from ys_line15_031_fz1data"
        linexdata_fz1 = self.get_mysql_data(linexdata_fz1_sql, db)
        linexdata_fz2_sql = "select * from ys_line15_031_fz2data"
        linexdata_fz2 = self.get_mysql_data(linexdata_fz2_sql, db)
        print('处理中..')
        result_qy1=self.get_energy_min(linexdata_qy1) #提取min能耗
        result_qy1.columns=['qy1_date', 'qy1', 'zs1']
        result_qy1 = result_qy1.set_index('qy1_date')
        result_qy2=self.get_energy_min(linexdata_qy2) #提取min能耗
        result_qy2.columns = ['qy2_date', 'qy2', 'zs2']
        result_qy2 = result_qy2.set_index('qy2_date')
        result_qy3=self.get_energy_min(linexdata_qy3) #提取min能耗
        result_qy3.columns = ['qy3_date', 'qy3', 'zs3']
        result_qy3 = result_qy3.set_index('qy3_date')
        result_qy4=self.get_energy_min(linexdata_qy4) #提取min能耗
        result_qy4.columns = ['qy4_date', 'qy4', 'zs4']
        result_qy4 = result_qy4.set_index('qy4_date')
        result_fz1=self.get_energy_min(linexdata_fz1) #提取min能耗
        result_fz1.columns = ['fz1_date', 'fz1', 'fz1zs']
        result_fz1 = result_fz1.set_index('fz1_date')
        result_fz1 = result_fz1.drop(['fz1zs'], axis=1)
        result_fz2=self.get_energy_min(linexdata_fz2) #提取min能耗
        result_fz2.columns = ['fz2_date', 'fz2', 'fz2zs']
        result_fz2 = result_fz2.set_index('fz2_date')
        result_fz2 = result_fz2.drop(['fz2zs'], axis=1)
        #合并
        result1 = result_qy1.join(result_qy2, how='outer')
        result2 = result1.join(result_qy3, how='outer')
        result2_1 = result2.join(result_qy4, how='outer')
        result3 = result2_1.join(result_fz1, how='outer')
        result4 = result3.join(result_fz2, how='outer')
        result4 = result4.reset_index()
        result4.rename(columns={'qy1_date': 'date'}, inplace=True)
        result4 = self.Zhanwei1(result4, 9, 4)
        result4 = self.Zhanwei2(result4, 19, 2)
        # 计算：
        result4['qynh'] = result4['qy1'] + result4['qy2'] + result4['qy3'] + result4['qy4']
        result4['fznh'] = result4['fz1'] + result4['fz2']
        result4['zsnl'] = result4['zs1'] + result4['zs2'] + result4['zs3'] + result4['zs4']
        result4['lcsh'] = result4['qynh'] + result4['fznh'] - result4['zsnl']
        result4.insert(25, 'yxlc', None)
        result4.insert(26, 'yxlc_cab', None)
        result4.insert(27, 'cgl', None)
        result4.insert(28, 'rs', None)
        # 计算年月日
        result4.insert(1, 'year', '')
        result4.insert(2, 'month', '')
        result4.insert(3, 'day', '')
        result4.insert(4, 'hour', '')
        result4.insert(5, 'min', '')
        result4.insert(6, 'week', '')
        data = result4.iloc[:, 0].values
        result4['year'], result4['month'], result4['day'], result4['hour'], result4['min'], result4[
            'week'] = self.year_change(data)
        result4.insert(0, 'bid', '4')
        result4.insert(0, 'trainid', '15031')
        result4.insert(0, 'sline', '15')
        return result4
    def get_line13_417_data(self,db): #13号线数据提取日能耗提取
        print('读取数据中..')
        linexdata_qy1_sql = "select * from ys_line13_417_qy1data"
        linexdata_qy1 = self.get_mysql_data(linexdata_qy1_sql, db)
        linexdata_qy2_sql = "select * from ys_line13_417_qy2data"
        linexdata_qy2 = self.get_mysql_data(linexdata_qy2_sql, db)
        linexdata_qy3_sql = "select * from ys_line13_417_qy3data"
        linexdata_qy3 = self.get_mysql_data(linexdata_qy3_sql, db)
        linexdata_fz1_sql = "select * from ys_line13_417_fz1data"
        linexdata_fz1 = self.get_mysql_data(linexdata_fz1_sql, db)
        linexdata_fz2_sql = "select * from ys_line13_417_fz2data"
        linexdata_fz2 = self.get_mysql_data(linexdata_fz2_sql, db)
        result_qy1=self.get_energy_min(linexdata_qy1) #提取日能耗
        result_qy1.columns=['qy1_date', 'qy1', 'zs1']
        result_qy1 = result_qy1.set_index('qy1_date')
        result_qy2=self.get_energy_min(linexdata_qy2) #提取日能耗
        result_qy2.columns = ['qy2_date', 'qy2', 'zs2']
        result_qy2 = result_qy2.set_index('qy2_date')
        result_qy3=self.get_energy_min(linexdata_qy3) #提取日能耗
        result_qy3.columns = ['qy3_date', 'qy3', 'zs3']
        result_qy3 = result_qy3.set_index('qy3_date')
        result_fz1=self.get_energy_min(linexdata_fz1) #提取日能耗
        result_fz1.columns = ['fz1_date', 'fz1', 'fz1zs']
        result_fz1 = result_fz1.set_index('fz1_date')
        result_fz1 = result_fz1.drop(['fz1zs'], axis=1)
        result_fz2=self.get_energy_min(linexdata_fz2) #提取日能耗
        result_fz2.columns = ['fz2_date', 'fz2', 'fz2zs']
        result_fz2 = result_fz2.set_index('fz2_date')
        result_fz2 = result_fz2.drop(['fz2zs'], axis=1)
        #合并
        result1 = result_qy1.join(result_qy2, how='outer')
        result2 = result1.join(result_qy3, how='outer')
        result3 = result2.join(result_fz1, how='outer')
        result4 = result3.join(result_fz2, how='outer')
        result4 = result4.reset_index()
        result4.rename(columns={'qy1_date': 'date'}, inplace=True)
        result4 = self.Zhanwei1(result4, 7, 5)
        result4 = self.Zhanwei2(result4, 19, 2)
        # 计算：
        result4['qynh'] = result4['qy1'] + result4['qy2'] + result4['qy3']
        result4['fznh'] = result4['fz1'] + result4['fz2']
        result4['zsnl'] = result4['zs1'] + result4['zs2'] + result4['zs3']
        result4['lcsh'] = result4['qynh'] + result4['fznh'] - result4['zsnl']
        result4.insert(25, 'yxlc', None)
        result4.insert(26, 'yxlc_cab', None)
        result4.insert(27, 'cgl', None)
        result4.insert(28, 'rs', None)
        # 计算年月日
        result4.insert(1, 'year', '')
        result4.insert(2, 'month', '')
        result4.insert(3, 'day', '')
        result4.insert(4, 'hour', '')
        result4.insert(5, 'min', '')
        result4.insert(6, 'week', '')
        data = result4.iloc[:, 0].values
        result4['year'], result4['month'], result4['day'], result4['hour'], result4['min'], result4[
            'week'] = self.year_change(data)
        result4.insert(0, 'bid', '3')
        result4.insert(0, 'trainid', 'H417')
        result4.insert(0, 'sline', '13')
        return result4
    def get_line6_009_data(self,db):  # 6号线009数据提取min能耗提取
        print('读取数据中..')
        linexdata_qy1_sql = "select * from ys_line6_009_qy1data"
        linexdata_qy1 = self.get_mysql_data(linexdata_qy1_sql, db)
        linexdata_qy2_sql = "select * from ys_line6_009_qy2data"
        linexdata_qy2 = self.get_mysql_data(linexdata_qy2_sql, db)
        linexdata_qy3_sql = "select * from ys_line6_009_qy3data"
        linexdata_qy3 = self.get_mysql_data(linexdata_qy3_sql, db)
        linexdata_qy4_sql = "select * from ys_line6_009_qy4data"
        linexdata_qy4 = self.get_mysql_data(linexdata_qy4_sql, db)
        linexdata_qy5_sql = "select * from ys_line6_009_qy5data"
        linexdata_qy5 = self.get_mysql_data(linexdata_qy5_sql, db)
        linexdata_qy6_sql = "select * from ys_line6_009_qy6data"
        linexdata_qy6 = self.get_mysql_data(linexdata_qy6_sql, db)
        linexdata_fz1_sql = "select * from ys_line6_009_fz1data"
        linexdata_fz1 = self.get_mysql_data(linexdata_fz1_sql, db)
        linexdata_fz2_sql = "select * from ys_line6_009_fz2data"
        linexdata_fz2 = self.get_mysql_data(linexdata_fz2_sql, db)
        print('处理中..')
        result_qy1 = self.get_energy_min(linexdata_qy1)  # 提取日能耗
        result_qy1.columns = ['qy1_date', 'qy1', 'zs1']
        result_qy1 = result_qy1.set_index('qy1_date')
        result_qy2 = self.get_energy_min(linexdata_qy2)  # 提取日能耗
        result_qy2.columns = ['qy2_date', 'qy2', 'zs2']
        result_qy2 = result_qy2.set_index('qy2_date')
        result_qy3 = self.get_energy_min(linexdata_qy3)  # 提取日能耗
        result_qy3.columns = ['qy3_date', 'qy3', 'zs3']
        result_qy3 = result_qy3.set_index('qy3_date')
        result_qy4 = self.get_energy_min(linexdata_qy4)  # 提取日能耗
        result_qy4.columns = ['qy4_date', 'qy4', 'zs4']
        result_qy4 = result_qy4.set_index('qy4_date')
        result_qy5 = self.get_energy_min(linexdata_qy5)  # 提取日能耗
        result_qy5.columns = ['qy5_date', 'qy5', 'zs5']
        result_qy5 = result_qy5.set_index('qy5_date')
        result_qy6 = self.get_energy_min(linexdata_qy6)  # 提取日能耗
        result_qy6.columns = ['qy6_date', 'qy6', 'zs6']
        result_qy6 = result_qy6.set_index('qy6_date')
        result_fz1 = self.get_energy_min(linexdata_fz1)  # 提取日能耗
        result_fz1.columns = ['fz1_date', 'fz1', 'fz1zs']
        result_fz1 = result_fz1.set_index('fz1_date')
        result_fz1 = result_fz1.drop(['fz1zs'], axis=1)
        result_fz2 = self.get_energy_min(linexdata_fz2)  # 提取日能耗
        result_fz2.columns = ['fz2_date', 'fz2', 'fz2zs']
        result_fz2 = result_fz2.set_index('fz2_date')
        result_fz2 = result_fz2.drop(['fz2zs'], axis=1)
        # 合并
        result1 = result_qy1.join(result_qy2, how='outer')
        result2 = result1.join(result_qy3, how='outer')
        result2_1 = result2.join(result_qy4, how='outer')
        result2_2 = result2_1.join(result_qy5, how='outer')
        result2_3 = result2_2.join(result_qy6, how='outer')
        result3 = result2_3.join(result_fz1, how='outer')
        result4 = result3.join(result_fz2, how='outer')
        result4 = result4.reset_index()
        result4.rename(columns={'qy1_date': 'date'}, inplace=True)
        result4 = self.Zhanwei1(result4, 13, 2)
        result4 = self.Zhanwei2(result4, 19, 2)
        # 计算：
        result4['qynh'] = result4['qy1'] + result4['qy2'] + result4['qy3']+result4['qy4']+result4['qy5']+result4['qy6']
        result4['fznh'] = result4['fz1'] + result4['fz2']
        result4['zsnl'] = result4['zs1'] + result4['zs2'] + result4['zs3']+result4['zs4']+result4['zs5']+result4['zs6']
        result4['lcsh'] = result4['qynh'] + result4['fznh'] - result4['zsnl']
        result4.insert(25, 'yxlc', None)
        result4.insert(26, 'yxlc_cab', None)
        result4.insert(27, 'cgl', None)
        result4.insert(28, 'rs', None)
        # 计算年月日
        result4.insert(1, 'year', '')
        result4.insert(2, 'month', '')
        result4.insert(3, 'day', '')
        result4.insert(4, 'hour', '')
        result4.insert(5, 'min', '')
        result4.insert(6, 'week', '')
        data = result4.iloc[:, 0].values
        result4['year'], result4['month'], result4['day'], result4['hour'], result4['min'], result4[
            'week'] = self.year_change(data)
        result4.insert(0, 'bid', '1')
        result4.insert(0, 'trainid', '06009')
        result4.insert(0, 'sline', '6')
        return result4
    def get_line8_066_data(self,db):  # 8号线066数据提取日能耗提取
        print('读取数据中..')
        linexdata_qy1_sql = "select * from ys_line8_066_qy1data"
        linexdata_qy1 = self.get_mysql_data(linexdata_qy1_sql, db)
        linexdata_qy2_sql = "select * from ys_line8_066_qy2data"
        linexdata_qy2 = self.get_mysql_data(linexdata_qy2_sql, db)
        linexdata_qy3_sql = "select * from ys_line8_066_qy3data"
        linexdata_qy3 = self.get_mysql_data(linexdata_qy3_sql, db)
        linexdata_fz1_sql = "select * from ys_line8_066_fz1data"
        linexdata_fz1 = self.get_mysql_data(linexdata_fz1_sql, db)
        linexdata_fz2_sql = "select * from ys_line8_066_fz2data"
        linexdata_fz2 = self.get_mysql_data(linexdata_fz2_sql, db)
        print('处理中..')
        result_qy1 = self.get_energy_min(linexdata_qy1)  # 提取日能耗
        result_qy1.columns = ['qy1_date', 'qy1', 'zs1']
        result_qy1 = result_qy1.set_index('qy1_date')
        result_qy2 = self.get_energy_min(linexdata_qy2)  # 提取日能耗
        result_qy2.columns = ['qy2_date', 'qy2', 'zs2']
        result_qy2 = result_qy2.set_index('qy2_date')
        result_qy3 = self.get_energy_min(linexdata_qy3)  # 提取日能耗
        result_qy3.columns = ['qy3_date', 'qy3', 'zs3']
        result_qy3 = result_qy3.set_index('qy3_date')
        result_fz1 = self.get_energy_min(linexdata_fz1)  # 提取日能耗
        result_fz1.columns = ['fz1_date', 'fz1', 'fz1zs']
        result_fz1 = result_fz1.set_index('fz1_date')
        result_fz1 = result_fz1.drop(['fz1zs'], axis=1)
        result_fz2 = self.get_energy_min(linexdata_fz2)  # 提取日能耗
        result_fz2.columns = ['fz2_date', 'fz2', 'fz2zs']
        result_fz2 = result_fz2.set_index('fz2_date')
        result_fz2 = result_fz2.drop(['fz2zs'], axis=1)
        # 合并
        result1 = result_qy1.join(result_qy2, how='outer')
        result2 = result1.join(result_qy3, how='outer')
        result3 = result2.join(result_fz1, how='outer')
        result4 = result3.join(result_fz2, how='outer')
        result4 = result4.reset_index()
        result4.rename(columns={'qy1_date': 'date'}, inplace=True)
        result4 = self.Zhanwei1(result4, 7, 5)
        result4 = self.Zhanwei2(result4, 19, 2)
        # 计算：
        result4['qynh'] = result4['qy1'] + result4['qy2'] + result4['qy3']
        result4['fznh'] = result4['fz1'] + result4['fz2']
        result4['zsnl'] = result4['zs1'] + result4['zs2'] + result4['zs3']
        result4['lcsh'] = result4['qynh'] + result4['fznh'] - result4['zsnl']
        result4.insert(25, 'yxlc', None)
        result4.insert(26, 'yxlc_cab', None)
        result4.insert(27, 'cgl', None)
        result4.insert(28, 'rs', None)
        result4.insert(1, 'year', '')
        result4.insert(2, 'month', '')
        result4.insert(3, 'day', '')
        result4.insert(4, 'hour', '')
        result4.insert(5, 'min', '')
        result4.insert(6, 'week', '')
        data = result4.iloc[:, 0].values
        result4['year'], result4['month'], result4['day'], result4['hour'], result4['min'], result4['week'] = self.year_change(data)
        result4.insert(0, 'bid', '3')
        result4.insert(0, 'trainid', '08066')
        result4.insert(0, 'sline', '8')
        return result4
    def get_line21_019_data(self,db):  #房山线数据提取min能耗提取
        print('读取数据中..')
        linexdata_qy1_sql = "select * from ys_line21_019_qy1data"
        linexdata_qy1 = self.get_mysql_data(linexdata_qy1_sql, db)
        linexdata_qy2_sql = "select * from ys_line21_019_qy2data"
        linexdata_qy2 = self.get_mysql_data(linexdata_qy2_sql, db)
        linexdata_qy3_sql = "select * from ys_line21_019_qy3data"
        linexdata_qy3 = self.get_mysql_data(linexdata_qy3_sql, db)
        linexdata_qy4_sql = "select * from ys_line21_019_qy4data"
        linexdata_qy4 = self.get_mysql_data(linexdata_qy4_sql, db)
        linexdata_fz1_sql = "select * from ys_line21_019_fz1data"
        linexdata_fz1 = self.get_mysql_data(linexdata_fz1_sql, db)
        linexdata_fz2_sql = "select * from ys_line21_019_fz2data"
        linexdata_fz2 = self.get_mysql_data(linexdata_fz2_sql, db)
        print('处理中..')
        result_qy1=self.get_energy_min(linexdata_qy1) #提取日能耗
        result_qy1.columns=['qy1_date', 'qy1', 'zs1']
        result_qy1 = result_qy1.set_index('qy1_date')
        result_qy2=self.get_energy_min(linexdata_qy2) #提取日能耗
        result_qy2.columns = ['qy2_date', 'qy2', 'zs2']
        result_qy2 = result_qy2.set_index('qy2_date')
        result_qy3=self.get_energy_min(linexdata_qy3) #提取日能耗
        result_qy3.columns = ['qy3_date', 'qy3', 'zs3']
        result_qy3 = result_qy3.set_index('qy3_date')
        result_qy4=self.get_energy_min(linexdata_qy4) #提取日能耗
        result_qy4.columns = ['qy4_date', 'qy4', 'zs4']
        result_qy4 = result_qy4.set_index('qy4_date')
        result_fz1=self.get_energy_min(linexdata_fz1) #提取日能耗
        result_fz1.columns = ['fz1_date', 'fz1', 'fz1zs']
        result_fz1 = result_fz1.set_index('fz1_date')
        result_fz1 = result_fz1.drop(['fz1zs'], axis=1)
        result_fz2=self.get_energy_min(linexdata_fz2) #提取日能耗
        result_fz2.columns = ['fz2_date', 'fz2', 'fz2zs']
        result_fz2 = result_fz2.set_index('fz2_date')
        result_fz2 = result_fz2.drop(['fz2zs'], axis=1)
        #合并
        result1 = result_qy1.join(result_qy2, how='outer')
        result2 = result1.join(result_qy3, how='outer')
        result2_1 = result2.join(result_qy4, how='outer')
        result3 = result2_1.join(result_fz1, how='outer')
        result4 = result3.join(result_fz2, how='outer')
        result4 = result4.reset_index()
        result4.rename(columns={'qy1_date': 'date'}, inplace=True)
        result4 = self.Zhanwei1(result4, 9, 4)
        result4 = self.Zhanwei2(result4, 19, 2)
        # 计算：
        result4['qynh'] = result4['qy1'] + result4['qy2'] + result4['qy3'] + result4['qy4']
        result4['fznh'] = result4['fz1'] + result4['fz2']
        result4['zsnl'] = result4['zs1'] + result4['zs2'] + result4['zs3'] + result4['zs4']
        result4['lcsh'] = result4['qynh'] + result4['fznh'] - result4['zsnl']
        result4.insert(25, 'yxlc', None)
        result4.insert(26, 'yxlc_cab', None)
        result4.insert(27, 'cgl', None)
        result4.insert(28, 'rs', None)
        # 计算年月日
        result4.insert(1, 'year', '')
        result4.insert(2, 'month', '')
        result4.insert(3, 'day', '')
        result4.insert(4, 'hour', '')
        result4.insert(5, 'min', '')
        result4.insert(6, 'week', '')
        data = result4.iloc[:, 0].values
        result4['year'], result4['month'], result4['day'], result4['hour'], result4['min'], result4['week'] = self.year_change(data)
        result4.insert(0, 'bid', '2')
        result4.insert(0, 'trainid', 'FS019')
        result4.insert(0, 'sline', '21')
        return result4
    def get_line1_S403_data(self,db): #13号线数据提取日能耗提取
        print('读取数据中..')
        linexdata_qy1_sql = "select * from ys_line1_s403_qy1data"
        linexdata_qy1 = self.get_mysql_data(linexdata_qy1_sql, db)
        linexdata_qy2_sql = "select * from ys_line1_s403_qy2data"
        linexdata_qy2 = self.get_mysql_data(linexdata_qy2_sql, db)
        linexdata_qy3_sql = "select * from ys_line1_s403_qy3data"
        linexdata_qy3 = self.get_mysql_data(linexdata_qy3_sql, db)
        linexdata_fz1_sql = "select * from ys_line1_s403_fz1data"
        linexdata_fz1 = self.get_mysql_data(linexdata_fz1_sql, db)
        linexdata_fz2_sql = "select * from ys_line1_s403_fz2data"
        linexdata_fz2 = self.get_mysql_data(linexdata_fz2_sql, db)
        print('处理中..')
        result_qy1=self.get_energy_min(linexdata_qy1) #提取日能耗
        result_qy1.columns=['qy1_date', 'qy1', 'zs1']
        result_qy1 = result_qy1.set_index('qy1_date')
        result_qy2=self.get_energy_min(linexdata_qy2) #提取日能耗
        result_qy2.columns = ['qy2_date', 'qy2', 'zs2']
        result_qy2 = result_qy2.set_index('qy2_date')
        result_qy3=self.get_energy_min(linexdata_qy3) #提取日能耗
        result_qy3.columns = ['qy3_date', 'qy3', 'zs3']
        result_qy3 = result_qy3.set_index('qy3_date')
        result_fz1=self.get_energy_min(linexdata_fz1) #提取日能耗
        result_fz1.columns = ['fz1_date', 'fz1', 'fz1zs']
        result_fz1 = result_fz1.set_index('fz1_date')
        result_fz1 = result_fz1.drop(['fz1zs'], axis=1)
        result_fz2=self.get_energy_min(linexdata_fz2) #提取日能耗
        result_fz2.columns = ['fz2_date', 'fz2', 'fz2zs']
        result_fz2 = result_fz2.set_index('fz2_date')
        result_fz2 = result_fz2.drop(['fz2zs'], axis=1)
        #合并
        result1 = result_qy1.join(result_qy2, how='outer')
        result2 = result1.join(result_qy3, how='outer')
        result3 = result2.join(result_fz1, how='outer')
        result4 = result3.join(result_fz2, how='outer')
        result4 = result4.reset_index()
        result4.rename(columns={'qy1_date': 'date'}, inplace=True)
        result4 = self.Zhanwei1(result4, 7, 5)
        result4 = self.Zhanwei2(result4, 19, 2)
        # 计算：
        result4['qynh'] = result4['qy1'] + result4['qy2'] + result4['qy3']
        result4['fznh'] = result4['fz1'] + result4['fz2']
        result4['zsnl'] = result4['zs1'] + result4['zs2'] + result4['zs3']
        result4['lcsh'] = result4['qynh'] + result4['fznh'] - result4['zsnl']
        result4.insert(25, 'yxlc', None)
        result4.insert(26, 'yxlc_cab', None)
        result4.insert(27, 'cgl', None)
        result4.insert(28, 'rs', None)
        # 计算年月日
        result4.insert(1, 'year', '')
        result4.insert(2, 'month', '')
        result4.insert(3, 'day', '')
        result4.insert(4, 'hour', '')
        result4.insert(5, 'min', '')
        result4.insert(6, 'week', '')
        data = result4.iloc[:, 0].values
        result4['year'], result4['month'], result4['day'], result4['hour'], result4['min'], result4['week'] = self.year_change(data)
        result4.insert(0, 'bid', '2')
        result4.insert(0, 'trainid', 'S403')
        result4.insert(0, 'sline', '1')
        return result4
    def get_line7_016_data(self,db):  # 6号线009数据提取min能耗提取
        print('读取数据中..')
        linexdata_qy1_sql = "select * from ys_line7_016_qy1data"
        linexdata_qy1 = self.get_mysql_data(linexdata_qy1_sql, db)
        linexdata_qy2_sql = "select * from ys_line7_016_qy2data"
        linexdata_qy2 = self.get_mysql_data(linexdata_qy2_sql, db)
        linexdata_qy3_sql = "select * from ys_line7_016_qy3data"
        linexdata_qy3 = self.get_mysql_data(linexdata_qy3_sql, db)
        linexdata_qy4_sql = "select * from ys_line7_016_qy4data"
        linexdata_qy4 = self.get_mysql_data(linexdata_qy4_sql, db)
        linexdata_qy5_sql = "select * from ys_line7_016_qy5data"
        linexdata_qy5 = self.get_mysql_data(linexdata_qy5_sql, db)
        linexdata_qy6_sql = "select * from ys_line7_016_qy6data"
        linexdata_qy6 = self.get_mysql_data(linexdata_qy6_sql, db)
        linexdata_fz1_sql = "select * from ys_line7_016_fz1data"
        linexdata_fz1 = self.get_mysql_data(linexdata_fz1_sql, db)
        linexdata_fz2_sql = "select * from ys_line7_016_fz2data"
        linexdata_fz2 = self.get_mysql_data(linexdata_fz2_sql, db)
        print('处理中..')
        result_qy1 = self.get_energy_min(linexdata_qy1)  # 提取日能耗
        result_qy1.columns = ['qy1_date', 'qy1', 'zs1']
        result_qy1 = result_qy1.set_index('qy1_date')
        result_qy2 = self.get_energy_min(linexdata_qy2)  # 提取日能耗
        result_qy2.columns = ['qy2_date', 'qy2', 'zs2']
        result_qy2 = result_qy2.set_index('qy2_date')
        result_qy3 = self.get_energy_min(linexdata_qy3)  # 提取日能耗
        result_qy3.columns = ['qy3_date', 'qy3', 'zs3']
        result_qy3 = result_qy3.set_index('qy3_date')
        result_qy4 = self.get_energy_min(linexdata_qy4)  # 提取日能耗
        result_qy4.columns = ['qy4_date', 'qy4', 'zs4']
        result_qy4 = result_qy4.set_index('qy4_date')
        result_qy5 = self.get_energy_min(linexdata_qy5)  # 提取日能耗
        result_qy5.columns = ['qy5_date', 'qy5', 'zs5']
        result_qy5 = result_qy5.set_index('qy5_date')
        result_qy6 = self.get_energy_min(linexdata_qy6)  # 提取日能耗
        result_qy6.columns = ['qy6_date', 'qy6', 'zs6']
        result_qy6 = result_qy6.set_index('qy6_date')
        result_fz1 = self.get_energy_min(linexdata_fz1)  # 提取日能耗
        result_fz1.columns = ['fz1_date', 'fz1', 'fz1zs']
        result_fz1 = result_fz1.set_index('fz1_date')
        result_fz1 = result_fz1.drop(['fz1zs'], axis=1)
        result_fz2 = self.get_energy_min(linexdata_fz2)  # 提取日能耗
        result_fz2.columns = ['fz2_date', 'fz2', 'fz2zs']
        result_fz2 = result_fz2.set_index('fz2_date')
        result_fz2 = result_fz2.drop(['fz2zs'], axis=1)
        # 合并
        result1 = result_qy1.join(result_qy2, how='outer')
        result2 = result1.join(result_qy3, how='outer')
        result2_1 = result2.join(result_qy4, how='outer')
        result2_2 = result2_1.join(result_qy5, how='outer')
        result2_3 = result2_2.join(result_qy6, how='outer')
        result3 = result2_3.join(result_fz1, how='outer')
        result4 = result3.join(result_fz2, how='outer')
        result4 = result4.reset_index()
        result4.rename(columns={'qy1_date': 'date'}, inplace=True)
        result4 = self.Zhanwei1(result4, 13, 2)
        result4 = self.Zhanwei2(result4, 19, 2)
        # 计算：
        result4['qynh'] = result4['qy1'] + result4['qy2'] + result4['qy3']+result4['qy4']+result4['qy5']+result4['qy6']
        result4['fznh'] = result4['fz1'] + result4['fz2']
        result4['zsnl'] = result4['zs1'] + result4['zs2'] + result4['zs3']+result4['zs4']+result4['zs5']+result4['zs6']
        result4['lcsh'] = result4['qynh'] + result4['fznh'] - result4['zsnl']
        result4.insert(25, 'yxlc', None)
        result4.insert(26, 'yxlc_cab', None)
        result4.insert(27, 'cgl', None)
        result4.insert(28, 'rs', None)
        # 计算年月日
        result4.insert(1, 'year', '')
        result4.insert(2, 'month', '')
        result4.insert(3, 'day', '')
        result4.insert(4, 'hour', '')
        result4.insert(5, 'min', '')
        result4.insert(6, 'week', '')
        data = result4.iloc[:, 0].values
        result4['year'], result4['month'], result4['day'], result4['hour'], result4['min'], result4[
            'week'] = self.year_change(data)
        result4.insert(0, 'bid', '1')
        result4.insert(0, 'trainid', '07016')
        result4.insert(0, 'sline', '7')
        return result4
    def get_line23_437_data(self,db): #13号线数据提取日能耗提取
        print('读取数据中..')
        linexdata_qy1_sql = "select * from ys_line23_437_qy1data"
        linexdata_qy1 = self.get_mysql_data(linexdata_qy1_sql, db)
        linexdata_qy2_sql = "select * from ys_line23_437_qy2data"
        linexdata_qy2 = self.get_mysql_data(linexdata_qy2_sql, db)
        linexdata_qy3_sql = "select * from ys_line23_437_qy3data"
        linexdata_qy3 = self.get_mysql_data(linexdata_qy3_sql, db)
        linexdata_fz1_sql = "select * from ys_line23_437_fz1data"
        linexdata_fz1 = self.get_mysql_data(linexdata_fz1_sql, db)
        linexdata_fz2_sql = "select * from ys_line23_437_fz2data"
        linexdata_fz2 = self.get_mysql_data(linexdata_fz2_sql, db)
        print('处理中..')
        result_qy1=self.get_energy_min(linexdata_qy1) #提取日能耗
        result_qy1.columns=['qy1_date', 'qy1', 'zs1']
        result_qy1 = result_qy1.set_index('qy1_date')
        result_qy2=self.get_energy_min(linexdata_qy2) #提取日能耗
        result_qy2.columns = ['qy2_date', 'qy2', 'zs2']
        result_qy2 = result_qy2.set_index('qy2_date')
        result_qy3=self.get_energy_min(linexdata_qy3) #提取日能耗
        result_qy3.columns = ['qy3_date', 'qy3', 'zs3']
        result_qy3 = result_qy3.set_index('qy3_date')
        result_fz1=self.get_energy_min(linexdata_fz1) #提取日能耗
        result_fz1.columns = ['fz1_date', 'fz1', 'fz1zs']
        result_fz1 = result_fz1.set_index('fz1_date')
        result_fz1 = result_fz1.drop(['fz1zs'], axis=1)
        result_fz2=self.get_energy_min(linexdata_fz2) #提取日能耗
        result_fz2.columns = ['fz2_date', 'fz2', 'fz2zs']
        result_fz2 = result_fz2.set_index('fz2_date')
        result_fz2 = result_fz2.drop(['fz2zs'], axis=1)
        #合并
        result1 = result_qy1.join(result_qy2, how='outer')
        result2 = result1.join(result_qy3, how='outer')
        result3 = result2.join(result_fz1, how='outer')
        result4 = result3.join(result_fz2, how='outer')
        result4 = result4.reset_index()
        result4.rename(columns={'qy1_date': 'date'}, inplace=True)
        result4 = self.Zhanwei1(result4, 7, 5)
        result4 = self.Zhanwei2(result4, 19, 2)
        # 计算：
        result4['qynh'] = result4['qy1'] + result4['qy2'] + result4['qy3']
        result4['fznh'] = result4['fz1'] + result4['fz2']
        result4['zsnl'] = result4['zs1'] + result4['zs2'] + result4['zs3']
        result4['lcsh'] = result4['qynh'] + result4['fznh'] - result4['zsnl']
        result4.insert(25, 'yxlc', None)
        result4.insert(26, 'yxlc_cab', None)
        result4.insert(27, 'cgl', None)
        result4.insert(28, 'rs', None)
        # 计算年月日
        result4.insert(1, 'year', '')
        result4.insert(2, 'month', '')
        result4.insert(3, 'day', '')
        result4.insert(4, 'hour', '')
        result4.insert(5, 'min', '')
        result4.insert(6, 'week', '')
        data = result4.iloc[:, 0].values
        result4['year'], result4['month'], result4['day'], result4['hour'], result4['min'], result4['week'] = self.year_change(data)
        result4.insert(0, 'bid', '2')
        result4.insert(0, 'trainid', 'TQ437')
        result4.insert(0, 'sline', '23')
        return result4
    def lineedata_min_output(self, data_df, database):  # min数据导出
        data_df=data_df.fillna(0)  #将NAN替换为0
        #data_df = data_df.where(data_df.notnull(), None)
        data_df_np = data_df.values
        data_df_np=data_df_np.astype('str')
        data_df_list = data_df_np.tolist()
        sqls_insert_datas = "insert into sub_edata_min(sline,trainid,bid,date,year,month,day,hour,min,week,qy1,zs1,qy2,zs2,qy3,zs3,qy4,zs4,qy5,zs5,qy6,zs6,qy7,zs7,qy8,zs8,fz1,fz2,fz3,fz4,qynh,fznh,zsnl,lcsh,yxlc,yxlc_cab,cgl,rs) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
        database.insert_datas(sqls_insert_datas, data_df_list)
    def linedata_min_output_div(self, data_df, database):
        divide = len(data_df) // 1440
        begin_index = 0
        end_index = 1440
        for i in range(divide):
            self.lineedata_min_output(data_df.iloc[begin_index:end_index, :], database)
            begin_index = begin_index + 1440
            end_index = end_index + 1440
        self.lineedata_min_output(data_df.iloc[begin_index:, :], database)

    def lineedata_hour_output(self, data_df, database):  # 小时数据导出到表
        data_df=data_df.fillna(0)  #将NAN替换为0
        #data_df = data_df.where(data_df.notnull(), None)
        data_df_np = data_df.values
        data_df_np=data_df_np.astype('str')
        data_df_list = data_df_np.tolist()
        sqls_insert_datas = "insert into sub_edata_hour(sline,trainid,bid,date,year,month,day,hour,min,week,qy1,zs1,qy2,zs2,qy3,zs3,qy4,zs4,qy5,zs5,qy6,zs6,qy7,zs7,qy8,zs8,fz1,fz2,fz3,fz4,qynh,fznh,zsnl,lcsh,yxlc,yxlc_cab,cgl,rs) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
        database.insert_datas(sqls_insert_datas, data_df_list)
    def linedata_hour_output_div(self, data_df, database):
        divide = len(data_df) // 24
        begin_index = 0
        end_index = 24
        for i in range(divide):
            self.lineedata_hour_output(data_df.iloc[begin_index:end_index, :], database)
            begin_index = begin_index + 24
            end_index = end_index + 24
        self.lineedata_hour_output(data_df.iloc[begin_index:, :], database)
    def lineedata_day_output(self, data_df, database):  # day数据导出到表
        data_df = data_df.fillna(0)  # 将NAN替换为0
        # data_df = data_df.where(data_df.notnull(), None)
        data_df_np = data_df.values
        data_df_np = data_df_np.astype('str')
        data_df_list = data_df_np.tolist()
        sqls_insert_datas = "insert into sub_edata_day(sline,trainid,bid,date,year,month,day,hour,min,week,qy1,zs1,qy2,zs2,qy3,zs3,qy4,zs4,qy5,zs5,qy6,zs6,qy7,zs7,qy8,zs8,fz1,fz2,fz3,fz4,qynh,fznh,zsnl,lcsh,yxlc,yxlc_cab,cgl,rs) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
        database.insert_datas(sqls_insert_datas, data_df_list)
    def linedata_day_output_div(self, data_df, database):
        divide = len(data_df)
        begin_index = 0
        end_index = 1
        for i in range(divide):
            self.lineedata_day_output(data_df.iloc[begin_index:end_index, :], database)
            begin_index = begin_index + 1
            end_index = end_index + 1
        self.lineedata_day_output(data_df.iloc[begin_index:, :], database)

    def get_hour_data(self,data_csv):
        if(len(data_csv)!=0):
            data_csv = data_csv.fillna(0)
            result = []
            data_index = 60 - 1
            j = 0
            for i in range(len(data_csv)):
                if i == data_index:
                    data = data_csv.iloc[i - 59, :].copy()
                    data['qy1'] = self.get_sum_data(data_csv.iloc[j:i + 1, 10].values)
                    data['zs1'] = self.get_sum_data(data_csv.iloc[j:i + 1, 11].values)
                    data['qy2'] = self.get_sum_data(data_csv.iloc[j:i + 1, 12].values)
                    data['zs2'] = self.get_sum_data(data_csv.iloc[j:i + 1, 13].values)
                    data['qy3'] = self.get_sum_data(data_csv.iloc[j:i + 1, 14].values)
                    data['zs3'] = self.get_sum_data(data_csv.iloc[j:i + 1, 15].values)
                    data['qy4'] = self.get_sum_data(data_csv.iloc[j:i + 1, 16].values)
                    data['zs4'] = self.get_sum_data(data_csv.iloc[j:i + 1, 17].values)
                    data['qy5'] = self.get_sum_data(data_csv.iloc[j:i + 1, 18].values)
                    data['zs5'] = self.get_sum_data(data_csv.iloc[j:i + 1, 19].values)
                    data['qy6'] = self.get_sum_data(data_csv.iloc[j:i + 1, 20].values)
                    data['zs6'] = self.get_sum_data(data_csv.iloc[j:i + 1, 21].values)
                    data['qy7'] = self.get_sum_data(data_csv.iloc[j:i + 1, 22].values)
                    data['zs7'] = self.get_sum_data(data_csv.iloc[j:i + 1, 23].values)
                    data['qy8'] = self.get_sum_data(data_csv.iloc[j:i + 1, 24].values)
                    data['zs8'] = self.get_sum_data(data_csv.iloc[j:i + 1, 25].values)
                    data['fz1'] = self.get_sum_data(data_csv.iloc[j:i + 1, 26].values)
                    data['fz2'] = self.get_sum_data(data_csv.iloc[j:i + 1, 27].values)
                    data['fz3'] = self.get_sum_data(data_csv.iloc[j:i + 1, 28].values)
                    data['fz4'] = self.get_sum_data(data_csv.iloc[j:i + 1, 29].values)
                    data['qynh'] = self.get_sum_data(data_csv.iloc[j:i + 1, 30].values)
                    data['fznh'] = self.get_sum_data(data_csv.iloc[j:i + 1, 31].values)
                    data['zsnl'] = self.get_sum_data(data_csv.iloc[j:i + 1, 32].values)
                    data['lcsh'] = self.get_sum_data(data_csv.iloc[j:i + 1, 33].values)
                    result.append(data)
                    j = j + 60
                    data_index = data_index + 60
                else:
                    continue
            array_to_matrix1 = np.mat(result)
            w = pd.DataFrame(array_to_matrix1)
            # w = pd.DataFrame(w.values.T, index=w.columns, columns=w.index)  # 转置
            w.columns = ['sline', 'trainid', 'bid', 'date', 'year', 'month', 'day', 'hour', 'min', 'week', 'qy1', 'zs1',
                         'qy2', 'zs2', 'qy3', 'zs3', 'qy4', 'zs4', 'qy5', 'zs5', 'qy6', 'zs6', 'qy7', 'zs7', 'qy8', 'zs8',
                         'fz1', 'fz2', 'fz3', 'fz4', 'qynh', 'fznh', 'zsnl', 'lcsh', 'yxlc', 'yxlc_cab', 'cgl', 'rs']
            return w
        else:
            w = pd.DataFrame(columns=['sline', 'trainid', 'bid', 'date', 'year', 'month', 'day', 'hour', 'min', 'week', 'qy1', 'zs1',
                         'qy2', 'zs2', 'qy3', 'zs3', 'qy4', 'zs4', 'qy5', 'zs5', 'qy6', 'zs6', 'qy7', 'zs7', 'qy8', 'zs8',
                         'fz1', 'fz2', 'fz3', 'fz4', 'qynh', 'fznh', 'zsnl', 'lcsh', 'yxlc', 'yxlc_cab', 'cgl', 'rs'])
            return w

    def get_day_data(self,data_csv):
        if(len(data_csv)!=0):
            data_csv = data_csv.fillna(0)
            result = []
            data_index = 60 * 24 - 1
            j = 0
            for i in range(len(data_csv)):
                if i == data_index:
                    data = data_csv.iloc[i - 60 * 24 + 1, :].copy()
                    data['qy1'] = self.get_sum_data(data_csv.iloc[j:i + 1, 10].values)
                    data['zs1'] = self.get_sum_data(data_csv.iloc[j:i + 1, 11].values)
                    data['qy2'] = self.get_sum_data(data_csv.iloc[j:i + 1, 12].values)
                    data['zs2'] = self.get_sum_data(data_csv.iloc[j:i + 1, 13].values)
                    data['qy3'] = self.get_sum_data(data_csv.iloc[j:i + 1, 14].values)
                    data['zs3'] = self.get_sum_data(data_csv.iloc[j:i + 1, 15].values)
                    data['qy4'] = self.get_sum_data(data_csv.iloc[j:i + 1, 16].values)
                    data['zs4'] = self.get_sum_data(data_csv.iloc[j:i + 1, 17].values)
                    data['qy5'] = self.get_sum_data(data_csv.iloc[j:i + 1, 18].values)
                    data['zs5'] = self.get_sum_data(data_csv.iloc[j:i + 1, 19].values)
                    data['qy6'] = self.get_sum_data(data_csv.iloc[j:i + 1, 20].values)
                    data['zs6'] = self.get_sum_data(data_csv.iloc[j:i + 1, 21].values)
                    data['qy7'] = self.get_sum_data(data_csv.iloc[j:i + 1, 22].values)
                    data['zs7'] = self.get_sum_data(data_csv.iloc[j:i + 1, 23].values)
                    data['qy8'] = self.get_sum_data(data_csv.iloc[j:i + 1, 24].values)
                    data['zs8'] = self.get_sum_data(data_csv.iloc[j:i + 1, 25].values)
                    data['fz1'] = self.get_sum_data(data_csv.iloc[j:i + 1, 26].values)
                    data['fz2'] = self.get_sum_data(data_csv.iloc[j:i + 1, 27].values)
                    data['fz3'] = self.get_sum_data(data_csv.iloc[j:i + 1, 28].values)
                    data['fz4'] = self.get_sum_data(data_csv.iloc[j:i + 1, 29].values)
                    data['qynh'] = self.get_sum_data(data_csv.iloc[j:i + 1, 30].values)
                    data['fznh'] = self.get_sum_data(data_csv.iloc[j:i + 1, 31].values)
                    data['zsnl'] = self.get_sum_data(data_csv.iloc[j:i + 1, 32].values)
                    data['lcsh'] = self.get_sum_data(data_csv.iloc[j:i + 1, 33].values)
                    result.append(data)
                    j = j + 60 * 24
                    data_index = data_index + 60 * 24
                else:
                    continue
            array_to_matrix1 = np.mat(result)
            w = pd.DataFrame(array_to_matrix1)
            # w = pd.DataFrame(w.values.T, index=w.columns, columns=w.index)  # 转置
            w.columns = ['sline', 'trainid', 'bid', 'date', 'year', 'month', 'day', 'hour', 'min', 'week', 'qy1', 'zs1',
                         'qy2', 'zs2', 'qy3', 'zs3', 'qy4', 'zs4', 'qy5', 'zs5', 'qy6', 'zs6', 'qy7', 'zs7', 'qy8', 'zs8',
                         'fz1', 'fz2', 'fz3', 'fz4', 'qynh', 'fznh', 'zsnl', 'lcsh', 'yxlc', 'yxlc_cab', 'cgl', 'rs']
            return w
        else:
            w = pd.DataFrame(columns=['sline', 'trainid', 'bid', 'date', 'year', 'month', 'day', 'hour', 'min', 'week', 'qy1', 'zs1',
                         'qy2', 'zs2', 'qy3', 'zs3', 'qy4', 'zs4', 'qy5', 'zs5', 'qy6', 'zs6', 'qy7', 'zs7', 'qy8', 'zs8',
                         'fz1', 'fz2', 'fz3', 'fz4', 'qynh', 'fznh', 'zsnl', 'lcsh', 'yxlc', 'yxlc_cab', 'cgl', 'rs'])
            return w
    def get_sum_data(self,data):
        sum = 0
        # data=data.reset_index(drop=True)
        for i in range(len(data)):
            sum = sum + data[i]
        return sum


    def data_manager3_auto(self,db):
        print('提取分钟能耗功能启动..')
        print('处理line15_031中..')
        #提取分钟、小时、天数据
        # result_line15_031_min = self.get_line15_031_data(db)
        # result_line15_031_hour=self.get_hour_data(result_line15_031_min)
        # result_line15_031_day=self.get_day_data(result_line15_031_min)
        # self.linedata_min_output_div(result_line15_031_min, db)
        # self.linedata_hour_output_div(result_line15_031_hour, db)
        # self.linedata_day_output_div(result_line15_031_day, db)

        print('处理line13_417中..')
        # result_line13_417_min = self.get_line13_417_data(db)
        # result_line13_417_hour=self.get_hour_data(result_line13_417_min)
        # result_line13_417_day=self.get_day_data(result_line13_417_min)
        # self.linedata_min_output_div(result_line13_417_min, db)
        # self.linedata_hour_output_div(result_line13_417_hour, db)
        # self.linedata_day_output_div(result_line13_417_day, db)

        # print('处理line6_009中..')
        # result_line6_009_min = self.get_line6_009_data(db)
        # result_line6_009_hour = self.get_hour_data(result_line6_009_min)
        # result_line6_009_day = self.get_day_data(result_line6_009_min)
        # self.linedata_min_output_div(result_line6_009_min, db)
        # self.linedata_hour_output_div(result_line6_009_hour, db)
        # self.linedata_day_output_div(result_line6_009_day, db)
        #
        # print('处理line8_066中..')
        # result_line8_066_min = self.get_line8_066_data(db)
        # result_line8_066_hour = self.get_hour_data(result_line8_066_min)
        # result_line8_066_day = self.get_day_data(result_line8_066_min)
        # self.linedata_min_output_div(result_line8_066_min, db)
        # self.linedata_hour_output_div(result_line8_066_hour, db)
        # self.linedata_day_output_div(result_line8_066_day, db)
        #
        # print('处理line21_019中..')
        # result_line21_019_min = self.get_line21_019_data(db)
        # result_line21_019_hour = self.get_hour_data(result_line21_019_min)
        # result_line21_019_day = self.get_day_data(result_line21_019_min)
        # self.linedata_min_output_div(result_line21_019_min, db)
        # self.linedata_hour_output_div(result_line21_019_hour, db)
        # self.linedata_day_output_div(result_line21_019_day, db)
        #
        #
        # print('处理line1_S403中..')
        # result_line1_S403_min = self.get_line1_S403_data(db)
        # result_line1_S403_hour = self.get_hour_data(result_line21_019_min)
        # result_line1_S403_day = self.get_day_data(result_line21_019_min)
        # self.linedata_min_output_div(result_line1_S403_min, db)
        # self.linedata_hour_output_div(result_line1_S403_hour, db)
        # self.linedata_day_output_div(result_line1_S403_day, db)
        #
        # print('处理line7_016中..')
        # result_line7_016_min = self.get_line7_016_data(db)
        # result_line7_016_hour = self.get_hour_data(result_line7_016_min)
        # result_line7_016_day = self.get_day_data(result_line7_016_min)
        # self.linedata_min_output_div(result_line7_016_min, db)
        # self.linedata_hour_output_div(result_line7_016_hour, db)
        # self.linedata_day_output_div(result_line7_016_day, db)

        print('处理line23_437中..')
        result_line23_437_min = self.get_line23_437_data(db)
        result_line23_437_hour = self.get_hour_data(result_line23_437_min)
        result_line23_437_day = self.get_day_data(result_line23_437_min)
        self.linedata_min_output_div(result_line23_437_min, db)
        self.linedata_hour_output_div(result_line23_437_hour, db)
        self.linedata_day_output_div(result_line23_437_day, db)
        print('日能耗提取结束')
    def clear_all_table(self,db):
        #15号线031
        sql_line15_031 = 'truncate table ys_line15_031_qy1data'  # 清空
        db.clear_table(sql_line15_031)
        sql_line15_031 = 'truncate table ys_line15_031_qy2data'
        db.clear_table(sql_line15_031)
        sql_line15_031 = 'truncate table ys_line15_031_qy3data'
        db.clear_table(sql_line15_031)
        sql_line15_031 = 'truncate table ys_line15_031_qy4data'
        db.clear_table(sql_line15_031)
        sql_line15_031 = 'truncate table ys_line15_031_fz1data'
        db.clear_table(sql_line15_031)
        sql_line15_031 = 'truncate table ys_line15_031_fz2data'
        db.clear_table(sql_line15_031)
        #13号线417
        sql_line13_417 = 'truncate table ys_line13_417_qy1data'
        db.clear_table(sql_line13_417)
        sql_line13_417 = 'truncate table ys_line13_417_qy2data'
        db.clear_table(sql_line13_417)
        sql_line13_417 = 'truncate table ys_line13_417_qy3data'
        db.clear_table(sql_line13_417)
        sql_line13_417 = 'truncate table ys_line13_417_fz1data'
        db.clear_table(sql_line13_417)
        sql_line13_417 = 'truncate table ys_line13_417_fz2data'
        db.clear_table(sql_line13_417)
        #6号线009
        sql_line6_009 = 'truncate table ys_line6_009_qy1data'
        db.clear_table(sql_line6_009)
        sql_line6_009 = 'truncate table ys_line6_009_qy2data'
        db.clear_table(sql_line6_009)
        sql_line6_009 = 'truncate table ys_line6_009_qy3data'
        db.clear_table(sql_line6_009)
        sql_line6_009 = 'truncate table ys_line6_009_qy4data'
        db.clear_table(sql_line6_009)
        sql_line6_009 = 'truncate table ys_line6_009_qy5data'
        db.clear_table(sql_line6_009)
        sql_line6_009 = 'truncate table ys_line6_009_qy6data'
        db.clear_table(sql_line6_009)
        sql_line6_009 = 'truncate table ys_line6_009_fz1data'
        db.clear_table(sql_line6_009)
        sql_line6_009 = 'truncate table ys_line6_009_fz2data'
        db.clear_table(sql_line6_009)
        #8号线066
        sql_line8_066 = 'truncate table ys_line8_066_qy1data'
        db.clear_table(sql_line8_066)
        sql_line8_066 = 'truncate table ys_line8_066_qy2data'
        db.clear_table(sql_line8_066)
        sql_line8_066 = 'truncate table ys_line8_066_qy3data'
        db.clear_table(sql_line8_066)
        sql_line8_066 = 'truncate table ys_line8_066_fz1data'
        db.clear_table(sql_line8_066)
        sql_line8_066 = 'truncate table ys_line8_066_fz2data'
        db.clear_table(sql_line8_066)
        #房山线019
        sql_line21_019 = 'truncate table ys_line21_019_qy1data'
        db.clear_table(sql_line21_019)
        sql_line21_019 = 'truncate table ys_line21_019_qy2data'
        db.clear_table(sql_line21_019)
        sql_line21_019 = 'truncate table ys_line21_019_qy3data'
        db.clear_table(sql_line21_019)
        sql_line21_019 = 'truncate table ys_line21_019_qy4data'
        db.clear_table(sql_line21_019)
        sql_line21_019 = 'truncate table ys_line21_019_fz1data'
        db.clear_table(sql_line21_019)
        sql_line21_019 = 'truncate table ys_line21_019_fz2data'
        db.clear_table(sql_line21_019)
        #1号线S403
        sql_line1_S403 = 'truncate table ys_line1_s403_qy1data'
        db.clear_table(sql_line1_S403)
        sql_line1_S403 = 'truncate table ys_line1_s403_qy2data'
        db.clear_table(sql_line1_S403)
        sql_line1_S403 = 'truncate table ys_line1_s403_qy3data'
        db.clear_table(sql_line1_S403)
        sql_line1_S403 = 'truncate table ys_line1_s403_fz1data'
        db.clear_table(sql_line1_S403)
        sql_line1_S403 = 'truncate table ys_line1_s403_fz2data'
        db.clear_table(sql_line1_S403)
def config(path):
    with open(path, 'rt') as csvfile:
        data_csv = pd.read_csv(csvfile,header=0)
    start_date=data_csv.iloc[0,0][11:]
    end_date=data_csv.iloc[1,0][9:]
    days=data_csv.iloc[2,0][5:]
    return start_date,end_date,days
def Main():
    db=Database('219.242.115.140','root','BJTUMysql5.7','test') #数据库连接
    #db=Database('219.242.115.140','root','BJTUMysql5.7','ys_data_test') #数据库连接
    conn = db.db_connect()
    #原始数据合并入库
    #dm2 = Data_manager2()
    #dm2.data_manager2_auto(db)
    #分钟能耗提取
    dm3 = Data_manager3()
    dm3.data_manager3_auto(db)
    #清空表
    #dm3.clear_all_table(db)
    print('tick')
    db.close()  # 关闭数据库
# Main()
# if __name__ == '__main__':
#     con=config('config.txt')
#     scheduler = BlockingScheduler()
#     scheduler.add_job(Main, 'interval', start_date=con[0],end_date=con[1],days=int(con[2]))
#     try:
#         scheduler.start()
#     except (KeyboardInterrupt, SystemExit):
#         pass



