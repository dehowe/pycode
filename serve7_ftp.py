from ftplib import FTP
import pymysql.cursors
import pandas as pd

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

class Data_manager1():
    def __init__(self):
        self.lineid = 0

    def get_mysql_data(self, sql,db):
        results =db.chaxun(sql)
        return results
    def get_path_filename(self,db):
        file_sql = "select * from energyfile_ftp WHERE flag=0"
        filedata = self.get_mysql_data(file_sql, db)
        return filedata

#连接ftp
def ftpconnect( host, port, username, password):
    ftp = FTP()
    ftp.connect(host, port)
    ftp.login(username, password)
    return ftp

#上传文件
def uploadfile( ftp, remotepath, localpath):
    bufsize = 1024
    fp = open(localpath, 'rb')
    ftp.storbinary('STOR ' + remotepath, fp, bufsize)
    ftp.set_debuglevel(0)
    fp.close()

def Main():
    #Hi
    db = Database('219.242.115.140', 'root', 'BJTUMysql5.7', 'ys_data_test')  # 数据库连接
    conn = db.db_connect()
    dm=Data_manager1()
    filedata=dm.get_path_filename(db)
    #返回ftp对象
    try:
        ftp = ftpconnect("xyu7570600001.my3w.com", 21, "xyu7570600001", "fireMount@")
        #ftp = ftpconnect("39.105.191.99", 35001, "jdata", "KLsad*&1)_p")
    except Exception as e:
        print('error: ', e.args)

    #显示目录下所有目录信息
    #ftp.dir()
    #上传文件
    if(len(filedata)>0):
        for i in range(len(filedata)):
            if(filedata[i][1]!=''):
                local_path=filedata[i][1]+filedata[i][2]
                #remote_path='/'+filedata[i][2]
                remote_path='/'+filedata[i][2]
                uploadfile(ftp,remote_path,local_path)
                print(local_path)
        #标记已发送文件
        filedata_df = pd.DataFrame(filedata)
        filedata_df.columns=['id','path','filename','flag']
        filedata_df = filedata_df.drop(['id'], axis=1)
        filedata_df['flag']=1
        filedata_np = filedata_df.values
        filedata_list = filedata_np.tolist()
        #清除标记
        delet_sql = "DELETE FROM energyfile_ftp WHERE flag=0"  # 删除
        db.cursor.execute(delet_sql)
        #导入标记
        sqls_insert_datas = "insert into energyfile_ftp(path,filename,flag) values(%s,%s,%s);"
        db.insert_datas(sqls_insert_datas, filedata_list)
        db.conn.commit()
    ftp.close()
    db.close()#关闭数据库


Main()