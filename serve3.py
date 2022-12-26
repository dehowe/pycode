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

def get_data(path):  #数据
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
def Main_serve1(path):
    data = get_data(path)
    db=Database('219.242.115.140','root','BJTUMysql5.7','ys_data_test') #数据库连接
    conn = db.db_connect()
    mvb_data_output(data,db)    #mvb数据入库
    db.close()#关闭数据库

Main_serve1('D:\线路能耗评估\MVB\FT0314MVB20200811123147.txt')