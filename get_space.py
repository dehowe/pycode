import os
from os.path import join, getsize
import pymysql.cursors
import datetime  # 引入time模块

def get_space_use(dir1,dir2):
    text_list=[]
    data_list=[]
    time_list=[]
    ticks = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    val = os.popen('df -h /home/cj').readlines()  # 返回字符串类型的结果
    space_all=int(val[1][25:28])  #存储空间
    space_use=int(val[1][31:34])  #已用空间
    #print(space_all)
    #print(space_use)
    str1='存储空间'
    str2='已用空间'
    text_list.append(str1)
    text_list.append(str2)
    data_list.append(space_all)
    data_list.append(space_use)
    time_list.append(ticks)
    time_list.append(ticks)

    dir_array=''
    space_list=[]
    dir_list=[]
    size1 = 0
    for root, dirs, files in os.walk(dir1):
        size1 += sum([getsize(join(root, name)) for name in files])
    size1_G=round(size1 / 1024 / 1024/1024,3)  #未解析文件空间
    str3='未解析文件占用空间'
    text_list.append(str3)
    data_list.append(size1_G)
    time_list.append(ticks)

    for root, dirs, files in os.walk(dir2):
        print(root)  # 当前目录路径
        print(dirs)  # 当前路径下所有子目录
        dir_array=dirs
        break
    for i in range(len(dir_array)):
        size = 0
        dir_cal=dir2+'/'+str(dir_array[i])
        for root, dirs, files in os.walk(dir_cal):
            size += sum([getsize(join(root, name)) for name in files])
        size_G=round(size / 1024 / 1024/1024,3)
        space_list.append(size_G)
        dir_list.append(dir_array[i])

        line_id=''
        if str(dir_array[i])=='FT':
            line_id='15号线'
        elif str(dir_array[i])=='TH':
            line_id='13号线'
        elif str(dir_array[i])=='SI':
            line_id='6号线'
        elif str(dir_array[i])=='EI':
            line_id='8号线'
        elif str(dir_array[i])=='FS': #房山线
            line_id='房山线'
        elif str(dir_array[i])=='FB':#复八线，1号线
            line_id='1号线'
        elif str(dir_array[i])=='SE':#7号线
            line_id='7号线'
        elif str(dir_array[i]) =='TQ': #八通线
            line_id = '八通线'
        elif str(dir_array[i])=='S1': #八通线
            line_id = 'S1线'
        elif str(dir_array[i]) == 'BJ':  # 八通线
            line_id = '测试'
        else:
            line_id='未知'
        str4 = line_id+'文件占用空间'
        text_list.append(str4)
        data_list.append(size_G)
        time_list.append(ticks)

    for i in range(len(text_list)):
        print(text_list[i])
        print(data_list[i])

    result = list(zip(text_list, data_list,time_list))
    return result


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
        self.cursor = self.conn.cursor()  # 创建一个游标

    def close(self):  # 关闭数据库
        self.cursor.close()
        self.conn.close()

    def chaxun(self, sql):
        self.cursor.execute(sql)
        results = self.cursor.fetchall()  # 获取查询的所有记录
        return results

    def insert_data(self, sql):
        self.cursor.execute(sql)
        # 提交
        self.conn.commit()

    def insert_datas(self, sql, list_datas):
        try:
            result = self.cursor.executemany(sql, list_datas)
            self.conn.commit()
            print('批量插入受影响的行数：', result)
        except Exception as e:
            print('db_insert_datas error: ', e.args)

    def clear_table(self, sql):
        self.cursor.execute(sql)
        # 提交
        self.conn.commit()
db=Database('219.242.115.140','root','BJTUMysql5.7','ebj') #数据库连接
conn = db.db_connect()
sql_clear = 'truncate table kf_space'  # 清空
db.clear_table(sql_clear)

sqls_insert_datas = "insert into kf_space(message,value,date) values(%s,%s,%s);"
result=get_space_use('/home/cj/data','/home/cj/dt')
print(result)
db.insert_datas(sqls_insert_datas, result)
db.close()  # 关闭数据库


