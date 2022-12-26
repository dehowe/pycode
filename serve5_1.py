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
class Data_manager4():
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
    def get_sum_data(self,data):
        sum = 0
        # data=data.reset_index(drop=True)
        for i in range(len(data)):
            sum = sum + data[i]
        return sum
    def get_month_data(self,data):
        if len(data) != 0:
            array_to_matrix1 = np.mat(data)
            data_csv = pd.DataFrame(array_to_matrix1)
            data_csv.columns = ['id', 'sline', 'trainid', 'bid', 'date', 'year', 'month', 'day', 'hour', 'min',
                                'week', 'yxlc','qynh', 'fznh', 'zsnl', 'lcsh', 'yxlc_cab', 'cgl','rs', 'qy1',
                                'zs1','qy2', 'zs2', 'qy3', 'zs3', 'qy4', 'zs4', 'qy5', 'zs5', 'qy6', 'zs6',
                                'qy7', 'zs7', 'qy8','zs8', 'fz1', 'fz2', 'fz3', 'fz4', 'flag']
            data_csv = data_csv.drop(['id','flag'], axis=1)
            yxlc=data_csv.pop('yxlc')
            qynh=data_csv.pop('qynh')
            fznh=data_csv.pop('fznh')
            zsnl=data_csv.pop('zsnl')
            lcsh = data_csv.pop('lcsh')
            yxlc_cab = data_csv.pop('yxlc_cab')
            cgl = data_csv.pop('cgl')
            rs = data_csv.pop('rs')
            data_csv.insert(30,'qynh',qynh)
            data_csv.insert(31,'fznh',fznh)
            data_csv.insert(32,'zsnl',zsnl)
            data_csv.insert(33,'lcsh',lcsh)
            data_csv.insert(34, 'yxlc', yxlc)
            data_csv.insert(35, 'yxlc_cab', yxlc_cab)
            data_csv.insert(36, 'cgl', cgl)
            data_csv.insert(37, 'rs', rs)
            data_csv = data_csv.fillna(0)

            month_begin = data_csv.iloc[0, 5]
            result = []
            j = 0
            for i in range(len(data_csv)):
                month_index = data_csv.iloc[i, 5]
                if month_begin != month_index:
                    data = data_csv.iloc[j, :].copy()
                    data['qy1'] = self.get_sum_data(data_csv.iloc[j:i, 10].values)
                    data['zs1'] = self.get_sum_data(data_csv.iloc[j:i, 11].values)
                    data['qy2'] = self.get_sum_data(data_csv.iloc[j:i, 12].values)
                    data['zs2'] = self.get_sum_data(data_csv.iloc[j:i, 13].values)
                    data['qy3'] = self.get_sum_data(data_csv.iloc[j:i, 14].values)
                    data['zs3'] = self.get_sum_data(data_csv.iloc[j:i, 15].values)
                    data['qy4'] = self.get_sum_data(data_csv.iloc[j:i, 16].values)
                    data['zs4'] = self.get_sum_data(data_csv.iloc[j:i, 17].values)
                    data['qy5'] = self.get_sum_data(data_csv.iloc[j:i, 18].values)
                    data['zs5'] = self.get_sum_data(data_csv.iloc[j:i, 19].values)
                    data['qy6'] = self.get_sum_data(data_csv.iloc[j:i, 20].values)
                    data['zs6'] = self.get_sum_data(data_csv.iloc[j:i, 21].values)
                    data['qy7'] = self.get_sum_data(data_csv.iloc[j:i, 22].values)
                    data['zs7'] = self.get_sum_data(data_csv.iloc[j:i, 23].values)
                    data['qy8'] = self.get_sum_data(data_csv.iloc[j:i, 24].values)
                    data['zs8'] = self.get_sum_data(data_csv.iloc[j:i, 25].values)
                    data['fz1'] = self.get_sum_data(data_csv.iloc[j:i, 26].values)
                    data['fz2'] = self.get_sum_data(data_csv.iloc[j:i, 27].values)
                    data['fz3'] = self.get_sum_data(data_csv.iloc[j:i, 28].values)
                    data['fz4'] = self.get_sum_data(data_csv.iloc[j:i, 29].values)
                    data['qynh'] = self.get_sum_data(data_csv.iloc[j:i, 30].values)
                    data['fznh'] = self.get_sum_data(data_csv.iloc[j:i, 31].values)
                    data['zsnl'] = self.get_sum_data(data_csv.iloc[j:i, 32].values)
                    data['lcsh'] = self.get_sum_data(data_csv.iloc[j:i, 33].values)
                    data['yxlc'] = self.get_sum_data(data_csv.iloc[j:i, 34].values)
                    data['rs'] = 0
                    result.append(data)
                    j = i
                    month_begin = month_index
                else:
                    continue
            if (len(result) > 0):
                array_to_matrix1 = np.mat(result)
                w = pd.DataFrame(array_to_matrix1)
                # w = pd.DataFrame(w.values.T, index=w.columns, columns=w.index)  # 转置
                w.columns = ['sline', 'trainid', 'bid', 'date', 'year', 'month', 'day', 'hour', 'min', 'week', 'qy1',
                             'zs1',
                             'qy2', 'zs2', 'qy3', 'zs3', 'qy4', 'zs4', 'qy5', 'zs5', 'qy6', 'zs6', 'qy7', 'zs7', 'qy8',
                             'zs8',
                             'fz1', 'fz2', 'fz3', 'fz4', 'qynh', 'fznh', 'zsnl', 'lcsh', 'yxlc', 'yxlc_cab', 'cgl',
                             'rs']
                return w
            else:
                w = pd.DataFrame(
                    columns=['sline', 'trainid', 'bid', 'date', 'year', 'month', 'day', 'hour', 'min', 'week', 'qy1',
                             'zs1',
                             'qy2', 'zs2', 'qy3', 'zs3', 'qy4', 'zs4', 'qy5', 'zs5', 'qy6', 'zs6', 'qy7', 'zs7', 'qy8',
                             'zs8',
                             'fz1', 'fz2', 'fz3', 'fz4', 'qynh', 'fznh', 'zsnl', 'lcsh', 'yxlc', 'yxlc_cab', 'cgl',
                             'rs'])
                return w





    def data_manager3_auto(self,db):
        print('提取月能耗功能启动..')
        # 提取分钟、小时、天数据

        print('处理line1_S403中..')
        line1_403_data_day_sql = "select * from sub_edata_day WHERE sline='1' and trainid='S403'"
        line1_403_data = self.get_mysql_data(line1_403_data_day_sql, db)
        result_line1_403_week = self.get_month_data(line1_403_data)
        self.linedata_week_output_div(result_line1_403_week, db)

        print('处理line6_009中..')
        line6_009_data_day_sql = "select * from sub_edata_day WHERE sline='6' and trainid='06009'"
        line6_009_data = self.get_mysql_data(line6_009_data_day_sql, db)
        result_line6_009_week = self.get_month_data(line6_009_data)
        self.linedata_week_output_div(result_line6_009_week, db)
        print('处理line7_016中..')
        line7_016_data_day_sql = "select * from sub_edata_day WHERE sline='7'and trainid='07016'"
        line7_016_data = self.get_mysql_data(line7_016_data_day_sql, db)
        result_line7_016_week = self.get_month_data(line7_016_data)
        self.linedata_week_output_div(result_line7_016_week, db)
        print('处理line7_032中..')
        line7_032_data_day_sql = "select * from sub_edata_day WHERE sline='7'and trainid='07032'"
        line7_032_data = self.get_mysql_data(line7_032_data_day_sql, db)
        result_line7_032_week = self.get_month_data(line7_032_data)
        self.linedata_week_output_div(result_line7_032_week, db)
        print('处理line8_066中..')
        line8_066_data_day_sql = "select * from sub_edata_day WHERE sline='8' and trainid='08066'"
        line8_066_data = self.get_mysql_data(line8_066_data_day_sql, db)
        result_line8_066_week = self.get_month_data(line8_066_data)
        self.linedata_week_output_div(result_line8_066_week, db)
        print('处理line8_079中..')
        line8_079_data_day_sql = "select * from sub_edata_day WHERE sline='8' and trainid='08079'"
        line8_079_data = self.get_mysql_data(line8_079_data_day_sql, db)
        result_line8_079_week = self.get_month_data(line8_079_data)
        self.linedata_week_output_div(result_line8_079_week, db)
        print('处理line13_417中..')
        line13_417_data_day_sql = "select * from sub_edata_day WHERE sline='13' and trainid='H417'"
        line13_417_data = self.get_mysql_data(line13_417_data_day_sql, db)
        result_line13_417_week = self.get_month_data(line13_417_data)
        self.linedata_week_output_div(result_line13_417_week, db)
        print('处理line15_031中..')
        line15_031_data_day_sql = "select * from sub_edata_day WHERE sline='15'"
        line15_031_data = self.get_mysql_data(line15_031_data_day_sql, db)
        result_line15_031_week=self.get_month_data(line15_031_data)
        self.linedata_week_output_div(result_line15_031_week, db)

        print('处理line17_010中..')
        line17_010_data_day_sql = "select * from sub_edata_day WHERE sline='17'"
        line17_010_data = self.get_mysql_data(line17_010_data_day_sql, db)
        result_line17_010_week = self.get_month_data(line17_010_data)
        self.linedata_week_output_div(result_line17_010_week, db)

        print('处理line21_019中..')
        line21_019_data_day_sql = "select * from sub_edata_day WHERE sline='21' and trainid='FS019'"
        line21_019_data = self.get_mysql_data(line21_019_data_day_sql, db)
        result_line21_019_week = self.get_month_data(line21_019_data)
        self.linedata_week_output_div(result_line21_019_week, db)
        print('处理line21_001中..')
        line21_001_data_day_sql = "select * from sub_edata_day WHERE sline='21' and trainid='FS001'"
        line21_001_data = self.get_mysql_data(line21_001_data_day_sql, db)
        result_line21_001_week = self.get_month_data(line21_001_data)
        self.linedata_week_output_div(result_line21_001_week, db)

        print('处理line23_437中..')
        line23_437_data_day_sql = "select * from sub_edata_day WHERE sline='23'"
        line23_437_data = self.get_mysql_data(line23_437_data_day_sql, db)
        result_line23_437_week = self.get_month_data(line23_437_data)
        self.linedata_week_output_div(result_line23_437_week, db)
        print('月能耗提取结束')
    def lineedata_week_output(self, data_df, database):  # day数据导出到表
        data_df = data_df.fillna(0)  # 将NAN替换为0
        # data_df = data_df.where(data_df.notnull(), None)
        data_df_np = data_df.values
        data_df_np = data_df_np.astype('str')
        data_df_list = data_df_np.tolist()
        sqls_insert_datas = "insert into sub_edata_month(sline,trainid,bid,date,year,month,day,hour,min,week,qy1,zs1,qy2,zs2,qy3,zs3,qy4,zs4,qy5,zs5,qy6,zs6,qy7,zs7,qy8,zs8,fz1,fz2,fz3,fz4,qynh,fznh,zsnl,lcsh,yxlc,yxlc_cab,cgl,rs) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
        database.insert_datas(sqls_insert_datas, data_df_list)
    def linedata_week_output_div(self, data_df, database):
        divide = len(data_df)
        begin_index = 0
        end_index = 1
        for i in range(divide):
            self.lineedata_week_output(data_df.iloc[begin_index:end_index, :], database)
            begin_index = begin_index + 1
            end_index = end_index + 1
        self.lineedata_week_output(data_df.iloc[begin_index:, :], database)
    def clear_week_table(self,db):
        sql_linex_week = 'truncate table sub_edata_month'  # 清空
        db.clear_table(sql_linex_week)

def Main():
    db=Database('219.242.115.140','root','BJTUMysql5.7','ebj') #数据库连接
    #db=Database('219.242.115.140','root','BJTUMysql5.7','ys_data_test') #数据库连接
    conn = db.db_connect()
    #月能耗提取
    dm4 = Data_manager4()
    dm4.clear_week_table(db)
    dm4.data_manager3_auto(db)
    #清空表
    db.close()  # 关闭数据库

Main()