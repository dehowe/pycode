import pymysql.cursors
import pandas as pd
import time
import os
import csv
import numpy as np

class Database:  #数据库类，封装数据库操作方法
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
    def build_tabel(self): #建表判断
        create_table_sql = '''CREATE TABLE IF NOT EXISTS
                `ys_line21_019_qy4data` (
               `date` datetime NOT NULL,
               `v` bigint(255) DEFAULT NULL,
               `a` bigint(255) DEFAULT NULL,
               `energy_add` bigint(255) DEFAULT NULL,
               `energy_re` bigint(255) DEFAULT NULL,
               `mileage` bigint(255) DEFAULT NULL,
               `stop_num` bigint(255) DEFAULT NULL,
               `accelerate` bigint(255) DEFAULT NULL,
                PRIMARY KEY (`date`)
            ) ENGINE=InnoDB DEFAULT CHARSET=latin1
            '''
        self.cursor.execute(create_table_sql)
        print('创建数据库表成功！')
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
    def delete_data(self,sql):
        try:
            self.cursor.execute(sql)  # 像sql语句传递参数
            # 提交
            self.conn.commit()
        except Exception as e:
            # 错误回滚
            self.conn.rollback()
        finally:
            self.conn.close()

    # 批量删除数据
    def delete_datas(self,sql, list_datas):
        try:
            result = self.cursor.executemany(sql, list_datas)
            self.conn.commit()
            print('批量删除受影响的行数：', result)
        except Exception as e:
            print('db_delete_datas error: ', e.args)
class Data_manager1: #数据管理类，封装数据处理方法
    def __init__(self):
        self.line_id=0
        self.qy_fz_id=0
        self.train_id=0
        self.name=0
    def get_line_qyfz_id(self,name):
        self.name=name
        #获取线路id
        if name[0:2]=='FT':
            self.line_id=15
        elif name[0:2]=='TH':
            self.line_id=13
        elif name[0:2]=='SI':
            self.line_id=6
        elif name[0:2]=='EI':
            self.line_id=8
        elif name[0:2]=='FS': #房山线
            self.line_id=21
        elif name[0:2]=='FB':#复八线，1号线
            self.line_id=1
        elif name[0:2]=='SE':#7号线
            self.line_id=7
        elif name[0:2] =='TQ': #八通线
            self.line_id = 23
        elif name[0:2] =='S1': #八通线
            self.line_id = 17
        else:
            self.line_id=-1
        #获取QY或FZ QY1:1  QY2:2  QY3:3  QY4:4  QY5:5  QY6:6  QY7:7  QY8:8  FZ1:9  FZ2:10   FZ3:11  FZ4:12
        if name[6:9]=='QY1':
            self.qy_fz_id=1
        elif name[6:9]=='QY2':
            self.qy_fz_id=2
        elif name[6:9]=='QY3':
            self.qy_fz_id=3
        elif name[6:9]=='QY4':
            self.qy_fz_id=4
        elif name[6:9]=='QY5':
            self.qy_fz_id=5
        elif name[6:9]=='QY6':
            self.qy_fz_id=6
        elif name[6:9]=='QY7':
            self.qy_fz_id=7
        elif name[6:9]=='QY8':
            self.qy_fz_id=8
        elif name[6:9]=='FZ1':
            self.qy_fz_id=9
        elif name[6:9]=='FZ2':
            self.qy_fz_id=10
        elif name[6:9]=='FZ3':
            self.qy_fz_id=11
        elif name[6:9]=='FZ4':
            self.qy_fz_id=12
        elif name[6:9]=='FZ5':
            self.qy_fz_id=13
        elif name[6:9]=='FZ6':
            self.qy_fz_id=14
        elif name[6:9]=='FZ7':
            self.qy_fz_id=15
        elif name[6:9]=='FZ8':
            self.qy_fz_id=16
        elif name[6:9]=='XF1':
            self.qy_fz_id=17
        elif name[6:9]=='XF2':
            self.qy_fz_id=18
        elif name[6:9]=='XF3':
            self.qy_fz_id=19
        elif name[6:9]=='XF4':
            self.qy_fz_id=20
        elif name[6:9]=='XF5':
            self.qy_fz_id=21
        elif name[6:9]=='XF6':
            self.qy_fz_id=22
        elif name[6:9]=='XF7':
            self.qy_fz_id=23
        elif name[6:9]=='XF8':
            self.qy_fz_id=24
        else:
            self.qy_fz_id=-1
        self.train_id=name[2:5]
        return self.line_id,self.qy_fz_id,self.train_id
    def energyfile_output(self,database):
        # 获取线路文件夹
        if self.line_id==1:
            path='/home/cj/dt/FB/'
        elif self.line_id==6:
            path='/home/cj/dt/SI/'
        elif self.line_id==7:
            path='/home/cj/dt/SE/'
        elif self.line_id==8:
            path='/home/cj/dt/EI/'
        elif self.line_id==13:
            path='/home/cj/dt/TH/'
        elif self.line_id==15:
            path='/home/cj/dt/FT/'
        elif self.line_id==21:
            path='/home/cj/dt/FS/'
        elif self.line_id==23:
            path='/home/cj/dt/TQ/'
        elif self.line_id==17:
            path='/home/cj/dt/S1/'
        else:
            path=''
        # sqls_insert_datas = "insert into energyfile_ftp(path,filename) values(%s,%s);"%(path,self.name)
        # database.insert_data(sqls_insert_datas)
        if(self.line_id!=-1):
            if(self.line_id==17):
                name = [self.name]
                sqls_insert_datas = "insert into energyfile_ftp_s1(path,filename,flag) values('%s','%s',%s);" % (path, self.name, '0')
                database.insert_data(sqls_insert_datas)
            else:
                name=[self.name]
                sqls_insert_datas = "insert into energyfile_ftp(path,filename,flag) values('%s','%s',%s);"%(path,self.name,'0')
                database.insert_data(sqls_insert_datas)
    def get_data(self,path):  #处理数据，分为正常数据和异常数据
        with open(path, 'rt') as csvfile:
            data_csv = pd.read_csv(csvfile,header=None)
            time_str = data_csv.iloc[:, 0].values
        csvfile.close()
        data_csv.columns = ['时间', '电压', '电流', '累积能耗', '再生能耗','累积走行里程','站编号','加速度']
        time_length = len(time_str)
        data_correct=[]
        data_error=[]
        data_last=0
        for i in range(time_length):
            #print('%.2f'%(i/time_length))
            try:
                timeArray=time.strptime(time_str[i], "%Y-%m-%d %H:%M:%S")  #判断是否为标准时间格式
                time_stamp = int(time.mktime(timeArray))
                if i==0:
                    data_correct.append(data_csv.iloc[0,:])
                    data_last=time_stamp
                elif time_stamp<=data_last:                  #判断是否重复或者异常
                    data_error.append(data_csv.iloc[i, :])
                    data_last=time_stamp
                else:
                    data_correct.append(data_csv.iloc[i,:])
                    data_last=time_stamp
            except :
                data_error.append(data_csv.iloc[i,:])
        return data_correct,data_error   #返回正常值和异常值
    def error_data_output(self,data_error,database):  #异常数据导出到表
        if len(data_error) != 0:
            data_error_df=pd.DataFrame(data_error)
            data_error_df.insert(0, 'qy_fz_name', self.qy_fz_id)
            data_error_df.insert(0, 'trainid', self.train_id)
            data_error_df.insert(0, 'sline', self.line_id)
            data_error_df.insert(11, 'filename', self.name)
            data_error_df=data_error_df.fillna(0)  #将NAN替换为0
            data_error_np = data_error_df.values
            data_error_list = data_error_np.tolist()
            sqls_insert_datas = "insert into ys_error_data(sline,trainid,qy_fz_name,date,v,a,energy_add,energy_re,mileage,stop_num,accelerate,filename) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
            database.insert_datas(sqls_insert_datas,data_error_list)
    def correct_data_output(self,data_correct,database): #正常数据导出到表
        data_correct_df=pd.DataFrame(data_correct)
        data_correct_df=data_correct_df.fillna(0)  #将NAN替换为0
        data_correct_np=data_correct_df.values
        data_correct_list = data_correct_np.tolist()
        #判断不同线路，不同QYFZ，导出到相应的表
        if self.line_id==15:  #15号线
            if self.train_id=='031':
                if self.qy_fz_id==1:
                    sqls_insert_datas = "insert into ys_line15_031_qy1data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
                elif self.qy_fz_id==2:
                    sqls_insert_datas = "insert into ys_line15_031_qy2data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
                elif self.qy_fz_id==3:
                    sqls_insert_datas = "insert into ys_line15_031_qy3data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
                elif self.qy_fz_id==4:
                    sqls_insert_datas = "insert into ys_line15_031_qy4data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
                elif self.qy_fz_id==9:
                    sqls_insert_datas = "insert into ys_line15_031_fz1data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
                elif self.qy_fz_id == 10:
                    sqls_insert_datas = "insert into ys_line15_031_fz2data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
            else: #如果都不满足，导出文件名到一个未知文件表
                sqls_insert_datas = "insert into ys_unknownfile(filename) values(%s);"
                database.insert_datas(sqls_insert_datas,[self.name])
        elif self.line_id==13: #13号线
            if self.train_id=='417':
                if self.qy_fz_id == 1:
                    sqls_insert_datas = "insert into ys_line13_417_qy1data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
                elif self.qy_fz_id == 2:
                    sqls_insert_datas = "insert into ys_line13_417_qy2data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
                elif self.qy_fz_id == 3:
                    sqls_insert_datas = "insert into ys_line13_417_qy3data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
                elif self.qy_fz_id == 9:
                    sqls_insert_datas = "insert into ys_line13_417_fz1data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
                elif self.qy_fz_id == 10:
                    sqls_insert_datas = "insert into ys_line13_417_fz2data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
            else:  # 如果都不满足，导出文件名到一个未知文件表
                sqls_insert_datas = "insert into ys_unknownfile(filename) values(%s);"
                database.insert_datas(sqls_insert_datas, [self.name])
        elif self.line_id==6: #6号线
            if self.train_id=='009':
                if self.qy_fz_id == 1:
                    sqls_insert_datas = "insert into ys_line6_009_qy1data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
                elif self.qy_fz_id == 2:
                    sqls_insert_datas = "insert into ys_line6_009_qy2data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
                elif self.qy_fz_id == 3:
                    sqls_insert_datas = "insert into ys_line6_009_qy3data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
                elif self.qy_fz_id == 4:
                    sqls_insert_datas = "insert into ys_line6_009_qy4data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
                elif self.qy_fz_id == 5:
                    sqls_insert_datas = "insert into ys_line6_009_qy5data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
                elif self.qy_fz_id == 6:
                    sqls_insert_datas = "insert into ys_line6_009_qy6data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
                elif self.qy_fz_id == 9:
                    sqls_insert_datas = "insert into ys_line6_009_fz1data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
                elif self.qy_fz_id == 10:
                    sqls_insert_datas = "insert into ys_line6_009_fz2data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
            else:  # 如果都不满足，导出文件名到一个未知文件表
                sqls_insert_datas = "insert into ys_unknownfile(filename) values(%s);"
                database.insert_datas(sqls_insert_datas, [self.name])
        elif self.line_id==8: #8号线
            if self.train_id=='066':
                if self.qy_fz_id == 1:
                    sqls_insert_datas = "insert into ys_line8_066_qy1data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
                elif self.qy_fz_id == 2:
                    sqls_insert_datas = "insert into ys_line8_066_qy2data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
                elif self.qy_fz_id == 3:
                    sqls_insert_datas = "insert into ys_line8_066_qy3data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
                elif self.qy_fz_id == 9:
                    sqls_insert_datas = "insert into ys_line8_066_fz1data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
                elif self.qy_fz_id == 10:
                    sqls_insert_datas = "insert into ys_line8_066_fz2data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
            elif self.train_id=='079':
                if self.qy_fz_id == 1:
                    sqls_insert_datas = "insert into ys_line8_079_qy1data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
                elif self.qy_fz_id == 2:
                    sqls_insert_datas = "insert into ys_line8_079_qy2data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
                elif self.qy_fz_id == 3:
                    sqls_insert_datas = "insert into ys_line8_079_qy3data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
                elif self.qy_fz_id == 9:
                    sqls_insert_datas = "insert into ys_line8_079_fz1data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
                elif self.qy_fz_id == 10:
                    sqls_insert_datas = "insert into ys_line8_079_fz2data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
            else:  # 如果都不满足，导出文件名到一个未知文件表
                sqls_insert_datas = "insert into ys_unknownfile(filename) values(%s);"
                database.insert_datas(sqls_insert_datas, [self.name])
        elif self.line_id==21: #房山线
            if self.train_id=='019':
                if self.qy_fz_id == 1:
                    sqls_insert_datas = "insert into ys_line21_019_qy1data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
                elif self.qy_fz_id == 2:
                    sqls_insert_datas = "insert into ys_line21_019_qy2data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
                elif self.qy_fz_id == 3:
                    sqls_insert_datas = "insert into ys_line21_019_qy3data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
                elif self.qy_fz_id == 4:
                    sqls_insert_datas = "insert into ys_line21_019_qy4data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
                elif self.qy_fz_id == 9:
                    sqls_insert_datas = "insert into ys_line21_019_fz1data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
                elif self.qy_fz_id == 10:
                    sqls_insert_datas = "insert into ys_line21_019_fz2data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
            elif self.train_id=='001':
                if self.qy_fz_id == 1:
                    sqls_insert_datas = "insert into ys_line21_001_qy1data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
                elif self.qy_fz_id == 2:
                    sqls_insert_datas = "insert into ys_line21_001_qy2data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
                elif self.qy_fz_id == 3:
                    sqls_insert_datas = "insert into ys_line21_001_qy3data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
                elif self.qy_fz_id == 4:
                    sqls_insert_datas = "insert into ys_line21_001_qy4data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
                elif self.qy_fz_id == 9:
                    sqls_insert_datas = "insert into ys_line21_001_fz1data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
                elif self.qy_fz_id == 10:
                    sqls_insert_datas = "insert into ys_line21_001_fz2data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
            else:  # 如果都不满足，导出文件名到一个未知文件表
                sqls_insert_datas = "insert into ys_unknownfile(filename) values(%s);"
                database.insert_datas(sqls_insert_datas, [self.name])
        elif self.line_id==1: #1号线（复八线）
            if self.train_id=='403':
                if self.qy_fz_id == 1:
                    sqls_insert_datas = "insert into ys_line1_s403_qy1data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
                elif self.qy_fz_id == 2:
                    sqls_insert_datas = "insert into ys_line1_s403_qy2data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
                elif self.qy_fz_id == 3:
                    sqls_insert_datas = "insert into ys_line1_s403_qy3data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
                elif self.qy_fz_id == 9:
                    sqls_insert_datas = "insert into ys_line1_s403_fz1data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
                elif self.qy_fz_id == 10:
                    sqls_insert_datas = "insert into ys_line1_s403_fz2data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
            else:  # 如果都不满足，导出文件名到一个未知文件表
                sqls_insert_datas = "insert into ys_unknownfile(filename) values(%s);"
                database.insert_datas(sqls_insert_datas, [self.name])
        elif self.line_id==7: #7号线
            if self.train_id=='016':
                if self.qy_fz_id == 1:
                    sqls_insert_datas = "insert into ys_line7_016_qy1data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
                elif self.qy_fz_id == 2:
                    sqls_insert_datas = "insert into ys_line7_016_qy2data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
                elif self.qy_fz_id == 3:
                    sqls_insert_datas = "insert into ys_line7_016_qy3data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
                elif self.qy_fz_id == 4:
                    sqls_insert_datas = "insert into ys_line7_016_qy4data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
                elif self.qy_fz_id == 5:
                    sqls_insert_datas = "insert into ys_line7_016_qy5data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
                elif self.qy_fz_id == 6:
                    sqls_insert_datas = "insert into ys_line7_016_qy6data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
                elif self.qy_fz_id == 9:
                    sqls_insert_datas = "insert into ys_line7_016_fz1data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
                elif self.qy_fz_id == 10:
                    sqls_insert_datas = "insert into ys_line7_016_fz2data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
            elif self.train_id=='032':
                if self.qy_fz_id == 1:
                    sqls_insert_datas = "insert into ys_line7_032_qy1data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
                elif self.qy_fz_id == 2:
                    sqls_insert_datas = "insert into ys_line7_032_qy2data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
                elif self.qy_fz_id == 3:
                    sqls_insert_datas = "insert into ys_line7_032_qy3data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
                elif self.qy_fz_id == 4:
                    sqls_insert_datas = "insert into ys_line7_032_qy4data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
                elif self.qy_fz_id == 5:
                    sqls_insert_datas = "insert into ys_line7_032_qy5data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
                elif self.qy_fz_id == 6:
                    sqls_insert_datas = "insert into ys_line7_032_qy6data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
                elif self.qy_fz_id == 9:
                    sqls_insert_datas = "insert into ys_line7_032_fz1data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
                elif self.qy_fz_id == 10:
                    sqls_insert_datas = "insert into ys_line7_032_fz2data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
            else:  # 如果都不满足，导出文件名到一个未知文件表
                sqls_insert_datas = "insert into ys_unknownfile(filename) values(%s);"
                database.insert_datas(sqls_insert_datas, [self.name])
        elif self.line_id==23: #八通线
            if self.train_id=='437':
                if self.qy_fz_id == 1:
                    sqls_insert_datas = "insert into ys_line23_437_qy1data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
                elif self.qy_fz_id == 2:
                    sqls_insert_datas = "insert into ys_line23_437_qy2data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
                elif self.qy_fz_id == 3:
                    sqls_insert_datas = "insert into ys_line23_437_qy3data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
                elif self.qy_fz_id == 9:
                    sqls_insert_datas = "insert into ys_line23_437_fz1data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
                elif self.qy_fz_id == 10:
                    sqls_insert_datas = "insert into ys_line23_437_fz2data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                    database.insert_datas(sqls_insert_datas, data_correct_list)
            else:  # 如果都不满足，导出文件名到一个未知文件表
                sqls_insert_datas = "insert into ys_unknownfile(filename) values(%s);"
                database.insert_datas(sqls_insert_datas, [self.name])
        elif self.line_id==17: #S1线
            if self.qy_fz_id == 1:
                sqls_insert_datas = "insert into ys_line17_010_qy1data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                database.insert_datas(sqls_insert_datas, data_correct_list)
            elif self.qy_fz_id == 2:
                sqls_insert_datas = "insert into ys_line17_010_qy2data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                database.insert_datas(sqls_insert_datas, data_correct_list)
            elif self.qy_fz_id == 3:
                sqls_insert_datas = "insert into ys_line17_010_qy3data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                database.insert_datas(sqls_insert_datas, data_correct_list)
            elif self.qy_fz_id == 4:
                sqls_insert_datas = "insert into ys_line17_010_qy4data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                database.insert_datas(sqls_insert_datas, data_correct_list)
            elif self.qy_fz_id == 5:
                sqls_insert_datas = "insert into ys_line17_010_qy5data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                database.insert_datas(sqls_insert_datas, data_correct_list)
            elif self.qy_fz_id == 6:
                sqls_insert_datas = "insert into ys_line17_010_qy6data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                database.insert_datas(sqls_insert_datas, data_correct_list)
            elif self.qy_fz_id == 9:
                sqls_insert_datas = "insert into ys_line17_010_fz1data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                database.insert_datas(sqls_insert_datas, data_correct_list)
            elif self.qy_fz_id == 10:
                sqls_insert_datas = "insert into ys_line17_010_fz2data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                database.insert_datas(sqls_insert_datas, data_correct_list)
            elif self.qy_fz_id == 11:
                sqls_insert_datas = "insert into ys_line17_010_fz3data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                database.insert_datas(sqls_insert_datas, data_correct_list)
            elif self.qy_fz_id == 12:
                sqls_insert_datas = "insert into ys_line17_010_fz4data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                database.insert_datas(sqls_insert_datas, data_correct_list)
            elif self.qy_fz_id == 13:
                sqls_insert_datas = "insert into ys_line17_010_fz5data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                database.insert_datas(sqls_insert_datas, data_correct_list)
            elif self.qy_fz_id == 14:
                sqls_insert_datas = "insert into ys_line17_010_fz6data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                database.insert_datas(sqls_insert_datas, data_correct_list)
            elif self.qy_fz_id == 17:
                sqls_insert_datas = "insert into ys_line17_010_xf1data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                database.insert_datas(sqls_insert_datas, data_correct_list)
            elif self.qy_fz_id == 18:
                sqls_insert_datas = "insert into ys_line17_010_xf2data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                database.insert_datas(sqls_insert_datas, data_correct_list)
            elif self.qy_fz_id == 19:
                sqls_insert_datas = "insert into ys_line17_010_xf3data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                database.insert_datas(sqls_insert_datas, data_correct_list)
            elif self.qy_fz_id == 20:
                sqls_insert_datas = "insert into ys_line17_010_xf4data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                database.insert_datas(sqls_insert_datas, data_correct_list)
            elif self.qy_fz_id == 21:
                sqls_insert_datas = "insert into ys_line17_010_xf5data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                database.insert_datas(sqls_insert_datas, data_correct_list)
            elif self.qy_fz_id == 22:
                sqls_insert_datas = "insert into ys_line17_010_xf6data(date,v,a,energy_add,energy_re,mileage,stop_num,accelerate) values(%s,%s,%s,%s,%s,%s,%s,%s);"
                database.insert_datas(sqls_insert_datas, data_correct_list)
        else:
            sqls_insert_datas = "insert into ys_unknownfile(filename) values(%s);"
            database.insert_datas(sqls_insert_datas, [self.name])
def get_mvb_data(path):  #数据
    with open(path, 'rt') as csvfile:
        data_csv = pd.read_csv(csvfile,header=None)
    csvfile.close()
    data_csv.columns = ['date_mvb','date','mileage','station_now','station_next','speed','gradient','mc1_fullload_rate','mc1_noload_mass','m2_fullload_rate','m2_noload_mass','m3_fullload_rate','m3_noload_mass','m4_fullload_rate','m4_noload_mass','m5_fullload_rate','m5_noload_mass','mc6_fullload_rate','mc6_noload_mass','train_mass','mc1_motor_speed','mc1_motor_a','mc1_motor_v','mc1_motor_flag','m2_motor_speed','m2_motor_a','m2_motor_v','m2_motor_flag','m3_motor_speed','m3_motor_a','m3_motor_v','m3_motor_flag','m4_motor_speed','m4_motor_a','m4_motor_v','m4_motor_flag','m5_motor_speed','m5_motor_a','m5_motor_v','m5_motor_flag','mc6_motor_speed','mc6_motor_a','mc6_motor_v','mc6_motor_flag','tra_force','mc1_ebf','m2_ebf','m3_ebf','m4_ebf','m5_ebf','mc6_ebf','mc1_abf','m2_abf','m3_abf','m4_abf','m5_abf','mc6_abf','motor_tem']
    return data_csv
def mvb_data_output(data_mvb,database):  #异常数据导出到表
    if len(data_mvb) != 0:
        data_mvb_df=pd.DataFrame(data_mvb)
        data_mvb_df=data_mvb_df.fillna(0)  #将NAN替换为0
        data_mvb_np = data_mvb_df.values
        data_mvb_list = data_mvb_np.tolist()
        sqls_insert_datas = "insert into ys_line17_001_mvbdata(date_mvb,date,mileage,station_now,station_next,speed,gradient,mc1_fullload_rate,mc1_noload_mass,m2_fullload_rate,m2_noload_mass,m3_fullload_rate,m3_noload_mass,m4_fullload_rate,m4_noload_mass,m5_fullload_rate,m5_noload_mass,mc6_fullload_rate,mc6_noload_mass,train_mass,mc1_motor_speed,mc1_motor_a,mc1_motor_v,mc1_motor_flag,m2_motor_speed,m2_motor_a,m2_motor_v,m2_motor_flag,m3_motor_speed,m3_motor_a,m3_motor_v,m3_motor_flag,m4_motor_speed,m4_motor_a,m4_motor_v,m4_motor_flag,m5_motor_speed,m5_motor_a,m5_motor_v,m5_motor_flag,mc6_motor_speed,mc6_motor_a,mc6_motor_v,mc6_motor_flag,tra_force,mc1_ebf,m2_ebf,m3_ebf,m4_ebf,m5_ebf,mc6_ebf,mc1_abf,m2_abf,m3_abf,m4_abf,m5_abf,mc6_abf,motor_tem) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
        database.insert_datas(sqls_insert_datas,data_mvb_list)
def get_warning_data(path):  #数据
    with open(path, 'rt') as csvfile:
        data_csv = pd.read_csv(csvfile,header=None,encoding='gbk',sep=' ')
    csvfile.close()
    data_csv.columns = ['date','data1','message']
    for i in range(len(data_csv)):
        data_csv.iloc[i,0]=data_csv.iloc[i,0]+' '+data_csv.iloc[i,1]
    data_csv = data_csv.drop(['data1'], axis=1)
    return data_csv
def get_line_qyfz_id(name):
    #获取线路id
    if name[0:2]=='FT':
        line_id=15
    elif name[0:2]=='TH':
        line_id=13
    elif name[0:2]=='SI':
        line_id=6
    elif name[0:2]=='EI':
        line_id=8
    elif name[0:2]=='FS': #房山线
        line_id=21
    elif name[0:2]=='FB':#复八线，1号线
        line_id=1
    elif name[0:2]=='SE':#7号线
        line_id=7
    elif name[0:2]=='TQ': #八通线
        line_id = 23
    elif name[0:2]=='S1': #八通线
        line_id = 17
    else:
        line_id=-1
    #获取QY或FZ QY1:1  QY2:2  QY3:3  QY4:4  QY5:5  QY6:6  QY7:7  QY8:8  FZ1:9  FZ2:10   FZ3:11  FZ4:12
    qy_fz_id=name[5:6]
    train_id=name[2:5]
    return line_id,qy_fz_id,train_id

def warning_data_output(data_warn,lineid, qy_fz_id, train_id,database):  #异常数据导出到表

    if len(data_warn) != 0:
        data_warning_df=pd.DataFrame(data_warn)
        data_warning_df=data_warning_df.fillna(0)  #将NAN替换为0
        data_warning_df.columns=['date','message']
        data_warning_df['sline']=lineid
        data_warning_df['train_id']=train_id
        data_warning_df['qy_fz_id']=qy_fz_id
        data_warning_np = data_warning_df.values
        data_warning_list = data_warning_np.tolist()
        sqls_insert_datas = "insert into ys_warning_data(date,message,sline,trainid,qy_fz_id) values(%s,%s,%s,%s,%s);"
        database.insert_datas(sqls_insert_datas,data_warning_list)

def Main_serve1(path):
    dm = Data_manager1()
    name = os.path.basename(path)
    lineid, qy_fz_id, train_id = dm.get_line_qyfz_id(name)  # 获取数据文件信息
    # db = Database('localhost', 'root', 'a5029731', 'test')  # 数据库连接
    db = Database('219.242.115.140', 'root', 'BJTUMysql5.7', 'ys_data_test')  # 数据库连接
    conn = db.db_connect()
    dm.energyfile_output(db)
    data_correct,data_error=dm.get_data(path) #数据处理，分为正常数据和异常数据
    #db.build_tabel()#建表,如数据库中缺少对应表，可更改参数手动设置建表
    dm.error_data_output(data_error,db)       #异常数据入库
    dm.correct_data_output(data_correct,db)   #正常数据入库
    db.close()#关闭数据库
def Main_serve2(path):  #MVB数据
    dm = Data_manager1()
    data = get_mvb_data(path)
    name = os.path.basename(path)
    lineid, qy_fz_id, train_id = dm.get_line_qyfz_id(name)  # 获取数据文件信息
    db=Database('219.242.115.140','root','BJTUMysql5.7','ys_data_test') #数据库连接
    conn = db.db_connect()
    dm.energyfile_output(db)
    mvb_data_output(data,db)    #mvb数据入库
    db.close()#关闭数据库

def Main_serve3(path):  #告警数据
    name = os.path.basename(path)
    lineid, qy_fz_id, train_id = get_line_qyfz_id(name)  # 获取数据文件信息
    data = get_warning_data(path)
    db=Database('219.242.115.140','root','BJTUMysql5.7','ys_data_test') #数据库连接
    conn = db.db_connect()
    warning_data_output(data,lineid, qy_fz_id, train_id,db)    #告警数据入库
    db.close()#关闭数据库


path='E:\server\\SE0325QY420210113052430'
Main_serve1(path)
# path='D:\线路能耗评估\S10106MVB20201203045440'
# Main_serve2(path)
# path='D:\线路能耗评估\\FS0011ALM20201119114026'
# Main_serve3(path)

