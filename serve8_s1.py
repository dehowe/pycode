import os
import pandas as pd
import time
import datetime
import numpy as np
import shutil

def timeHandle(time_str,len):
    time_str1=[]
    for i in range(len):
        try:
            timeArray = time.strptime(time_str[i], "%Y-%m-%d %H:%M:%S")
            # 转换为时间戳
            timeStamp = int(time.mktime(timeArray))
            time_str1.append(timeStamp)
            #time_str[i]=timeStamp
        except:
            print(i)
    return time_str1

def timeHandle4(time_num,len):
    for i in range(len):
        timeArray = time.localtime(time_num)
        # 转换为时间字符串
        otherStyleTime = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
        time_num[i]=otherStyleTime
    return time_num

def timeHandle2(time_str):
    timeArray = time.strptime(time_str, "%Y-%m-%d %H:%M:%S")
     # 转换为时间戳
    time_stamp = int(time.mktime(timeArray))
    return time_stamp

def timeHandle3(time_num):
    timeArray = time.localtime(time_num)
    otherStyleTime = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
    return otherStyleTime
def s1_file_Combine(path_input,path_output): #整合csv
    if not os.path.exists(path_output):
        os.mkdir(path_output)
    filenames = os.listdir(path_input)
    for i in filenames:
        qy_fz_id = i[6:9]
        train_id = i[2:5]
        excel_path=path_input+'/'+i
        if(train_id=='010'):
            if(qy_fz_id=='QY1'):
                f = open(excel_path, 'rb')
                data = pd.read_csv(f)  #
                data.to_csv(path_output+'qy1.csv', index=False, mode='a', header=False, )
            elif(qy_fz_id=='QY2'):
                f = open(excel_path, 'rb')
                data = pd.read_csv(f)  #
                data.to_csv(path_output + 'qy2.csv', index=False, mode='a', header=False, )
            elif (qy_fz_id == 'QY3'):
                f = open(excel_path, 'rb')
                data = pd.read_csv(f)  #
                data.to_csv(path_output + 'qy3.csv', index=False, mode='a', header=False, )
            elif (qy_fz_id == 'QY4'):
                f = open(excel_path, 'rb')
                data = pd.read_csv(f)  #
                data.to_csv(path_output + 'qy4.csv', index=False, mode='a', header=False, )
            elif (qy_fz_id == 'QY5'):
                f = open(excel_path, 'rb')
                data = pd.read_csv(f)  #
                data.to_csv(path_output + 'qy5.csv', index=False, mode='a', header=False, )
            elif (qy_fz_id == 'QY6'):
                f = open(excel_path, 'rb')
                data = pd.read_csv(f)  #
                data.to_csv(path_output + 'qy6.csv', index=False, mode='a', header=False, )
            elif (qy_fz_id == 'FZ1'):
                f = open(excel_path, 'rb')
                data = pd.read_csv(f)  #
                data.to_csv(path_output + 'fz1.csv', index=False, mode='a', header=False, )
            elif (qy_fz_id == 'FZ2'):
                f = open(excel_path, 'rb')
                data = pd.read_csv(f)  #
                data.to_csv(path_output + 'fz2.csv', index=False, mode='a', header=False, )
            elif (qy_fz_id == 'FZ3'):
                f = open(excel_path, 'rb')
                data = pd.read_csv(f)  #
                data.to_csv(path_output + 'fz3.csv', index=False, mode='a', header=False, )
            elif (qy_fz_id == 'FZ4'):
                f = open(excel_path, 'rb')
                data = pd.read_csv(f)  #
                data.to_csv(path_output + 'fz4.csv', index=False, mode='a', header=False, )
            elif (qy_fz_id == 'FZ5'):
                f = open(excel_path, 'rb')
                data = pd.read_csv(f)  #
                data.to_csv(path_output + 'fz5.csv', index=False, mode='a', header=False, )
            elif (qy_fz_id == 'FZ6'):
                f = open(excel_path, 'rb')
                data = pd.read_csv(f)  #
                data.to_csv(path_output + 'fz6.csv', index=False, mode='a', header=False, )
            elif (qy_fz_id == 'XF1'):
                f = open(excel_path, 'rb')
                data = pd.read_csv(f)  #
                data.to_csv(path_output + 'xf1.csv', index=False, mode='a', header=False, )
            elif (qy_fz_id == 'XF2'):
                f = open(excel_path, 'rb')
                data = pd.read_csv(f)  #
                data.to_csv(path_output + 'xf2.csv', index=False, mode='a', header=False, )
            elif (qy_fz_id == 'XF3'):
                f = open(excel_path, 'rb')
                data = pd.read_csv(f)  #
                data.to_csv(path_output + 'xf3.csv', index=False, mode='a', header=False, )
            elif (qy_fz_id == 'XF4'):
                f = open(excel_path, 'rb')
                data = pd.read_csv(f)  #
                data.to_csv(path_output + 'xf4.csv', index=False, mode='a', header=False, )
            elif (qy_fz_id == 'XF5'):
                f = open(excel_path, 'rb')
                data = pd.read_csv(f)  #
                data.to_csv(path_output + 'xf5.csv', index=False, mode='a', header=False, )
            elif (qy_fz_id == 'XF6'):
                f = open(excel_path, 'rb')
                data = pd.read_csv(f)  #
                data.to_csv(path_output + 'xf6.csv', index=False, mode='a', header=False, )
            elif (qy_fz_id == 'MVB'):
                f = open(excel_path, 'rb')
                data = pd.read_csv(f)  #
                data.to_csv(path_output + 'mvb.csv', index=False, mode='a', header=False, )
        print(i)

def year_change(data1):
    data_year = []
    data_month = []
    data_week = []
    data_day = []
    data_hour = []
    data_min = []
    for i in range(len(data1)):
        b = datetime.datetime.strptime(data1[i], '%Y-%m-%d %H:%M:%S').strftime('%Y')
        c = datetime.datetime.strptime(data1[i], '%Y-%m-%d %H:%M:%S').strftime('%m')
        d = datetime.datetime.strptime(data1[i], '%Y-%m-%d %H:%M:%S').strftime('%d')
        e = datetime.datetime.strptime(data1[i], '%Y-%m-%d %H:%M:%S').strftime('%H')
        f = datetime.datetime.strptime(data1[i], '%Y-%m-%d %H:%M:%S').strftime('%M')
        data_year.append(int(b))
        data_month.append(int(c))
        data_day.append(int(d))
        data_hour.append(int(e))
        data_min.append(int(f))
        d=get_week(int(b),int(c),int(d))
        data_week.append(d)
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
def get_week(year,month,day):
    y, week, d = datetime.date(year, month, day).isocalendar()
    return week
def S1_Combine_s(path):  #合并数据
    if not os.path.exists(path+'秒\\'):
        os.mkdir(path+'秒\\')
    path_out=path+'秒\\'
    try:
        for i in range(18):  #提取s数据
            if(i<6):
                pathi=path+'缓存\\qy'+str(i+1)+'.csv'
                pathi_out=path+'缓存\\qy'+str(i+1)+'_s.csv'
            elif(i<12):
                pathi = path + '缓存\\fz' + str(i -5) + '.csv'
                pathi_out = path + '缓存\\fz' + str(i -5) + '_s.csv'
            else:
                pathi = path + '缓存\\xf' + str(i -11) + '.csv'
                pathi_out = path + '缓存\\xf' + str(i -11) + '_s.csv'
            with open(pathi) as csvfile:
                data_csv = pd.read_csv(csvfile, header=None)
            csvfile.close()
            data_csv.columns = ['date', 'v', 'a', 'lei', 'zai', 'mileage', 'stop_num','accelerate']

            data_csv = data_csv.drop(['mileage', 'stop_num', 'accelerate'], axis=1)
            data_csv['qy'] = abs(data_csv['lei'].diff(-1))
            data_csv['zs'] = data_csv['zai'].diff(-1)
            data_csv = data_csv.drop(['lei', 'zai'], axis=1)
            data_csv = data_csv.fillna(0)  # 将NAN替换为0
            data_csv.to_csv(pathi_out, index=0, header=0)  # 导出解析结果，修改名称或路径
    except:
        return -1
    try:
        with open(path+'缓存\\qy1_s.csv', 'rt') as csvfile:
            data_csv11 = pd.read_csv(csvfile,header=None)
        csvfile.close()
        data_csv11.columns = ['date', 'm1_qy_v', 'm1_qy_a', 'm1_qy', 'm1_zs']
        data_csv11 = data_csv11.set_index("date")

        with open(path+'缓存\\fz1_s.csv', 'rt') as csvfile:
            data_csv12 = pd.read_csv(csvfile,header=None)
        csvfile.close()
        data_csv12.columns = ['date', 'm1_fz_v', 'm1_fz_a', 'm1_fz', 'm1_zai']
        data_csv12 = data_csv12.drop(['m1_zai'], axis=1)
        data_csv12 = data_csv12.set_index("date")

        with open(path+'缓存\\xf1_s.csv', 'rt') as csvfile:
            data_csv13 = pd.read_csv(csvfile,header=None)
        csvfile.close()
        data_csv13.columns = ['date', 'm1_xf_v', 'm1_xf_a', 'm1_xf', 'm1_zai']
        data_csv13 = data_csv13.drop(['m1_zai'], axis=1)
        data_csv13 = data_csv13.set_index("date")

        with open(path+'缓存\\qy2_s.csv', 'rt') as csvfile:
            data_csv21 = pd.read_csv(csvfile,header=None)
        csvfile.close()
        data_csv21.columns = ['date', 'm2_qy_v', 'm2_qy_a', 'm2_qy', 'm2_zs']
        data_csv21 = data_csv21.set_index("date")

        with open(path+'缓存\\fz2_s.csv', 'rt') as csvfile:
            data_csv22 = pd.read_csv(csvfile,header=None)
        csvfile.close()
        data_csv22.columns = ['date', 'm2_fz_v', 'm2_fz_a', 'm2_fz', 'm2_zai']
        data_csv22 = data_csv22.drop(['m2_zai'], axis=1)
        data_csv22 = data_csv22.set_index("date")

        with open(path+'缓存\\xf2_s.csv', 'rt') as csvfile:
            data_csv23 = pd.read_csv(csvfile,header=None)
        csvfile.close()
        data_csv23.columns = ['date', 'm2_xf_v', 'm2_xf_a', 'm2_xf', 'm2_zai']
        data_csv23 = data_csv23.drop(['m2_zai'], axis=1)
        data_csv23 = data_csv23.set_index("date")

        with open(path+'缓存\\qy3_s.csv', 'rt') as csvfile:
            data_csv31 = pd.read_csv(csvfile,header=None)
        csvfile.close()
        data_csv31.columns = ['date', 'm3_qy_v', 'm3_qy_a', 'm3_qy', 'm3_zs']
        data_csv31 = data_csv31.set_index("date")

        with open(path+'缓存\\fz3_s.csv', 'rt') as csvfile:
            data_csv32 = pd.read_csv(csvfile,header=None)
        csvfile.close()
        data_csv32.columns = ['date', 'm3_fz_v', 'm3_fz_a', 'm3_fz', 'm3_zai']
        data_csv32 = data_csv32.drop(['m3_zai'], axis=1)
        data_csv32 = data_csv32.set_index("date")

        with open(path+'缓存\\xf3_s.csv', 'rt') as csvfile:
            data_csv33 = pd.read_csv(csvfile,header=None)
        csvfile.close()
        data_csv33.columns = ['date', 'm3_xf_v', 'm3_xf_a', 'm3_xf', 'm3_zai']
        data_csv33 = data_csv33.drop(['m3_zai'], axis=1)
        data_csv33 = data_csv33.set_index("date")

        with open(path + '缓存\\qy4_s.csv', 'rt') as csvfile:
            data_csv41 = pd.read_csv(csvfile, header=None)
        csvfile.close()
        data_csv41.columns = ['date', 'm4_qy_v', 'm4_qy_a', 'm4_qy', 'm4_zs']
        data_csv41 = data_csv41.set_index("date")

        with open(path + '缓存\\fz4_s.csv', 'rt') as csvfile:
            data_csv42 = pd.read_csv(csvfile, header=None)
        csvfile.close()
        data_csv42.columns = ['date', 'm4_fz_v', 'm4_fz_a', 'm4_fz', 'm4_zai']
        data_csv42 = data_csv42.drop(['m4_zai'], axis=1)
        data_csv42 = data_csv42.set_index("date")

        with open(path + '缓存\\xf4_s.csv', 'rt') as csvfile:
            data_csv43 = pd.read_csv(csvfile, header=None)
        csvfile.close()
        data_csv43.columns = ['date', 'm4_xf_v', 'm4_xf_a', 'm4_xf', 'm4_zai']
        data_csv43 = data_csv43.drop(['m4_zai'], axis=1)
        data_csv43 = data_csv43.set_index("date")

        with open(path + '缓存\\qy5_s.csv', 'rt') as csvfile:
            data_csv51 = pd.read_csv(csvfile, header=None)
        csvfile.close()
        data_csv51.columns = ['date', 'm5_qy_v', 'm5_qy_a', 'm5_qy', 'm5_zs']
        data_csv51 = data_csv51.set_index("date")

        with open(path + '缓存\\fz5_s.csv', 'rt') as csvfile:
            data_csv52 = pd.read_csv(csvfile, header=None)
        csvfile.close()
        data_csv52.columns = ['date', 'm5_fz_v', 'm5_fz_a', 'm5_fz', 'm5_zai']
        data_csv52 = data_csv52.drop(['m5_zai'], axis=1)
        data_csv52 = data_csv52.set_index("date")

        with open(path + '缓存\\xf5_s.csv', 'rt') as csvfile:
            data_csv53 = pd.read_csv(csvfile, header=None)
        csvfile.close()
        data_csv53.columns = ['date', 'm5_xf_v', 'm5_xf_a', 'm5_xf', 'm5_zai']
        data_csv53 = data_csv53.drop(['m5_zai'], axis=1)
        data_csv53 = data_csv53.set_index("date")

        with open(path + '缓存\\qy6_s.csv', 'rt') as csvfile:
            data_csv61 = pd.read_csv(csvfile, header=None)
        csvfile.close()
        data_csv61.columns = ['date', 'm6_qy_v', 'm6_qy_a', 'm6_qy', 'm6_zs']
        data_csv61 = data_csv61.set_index("date")

        with open(path + '缓存\\fz6_s.csv', 'rt') as csvfile:
            data_csv62 = pd.read_csv(csvfile, header=None)
        csvfile.close()
        data_csv62.columns = ['date', 'm6_fz_v', 'm6_fz_a', 'm6_fz', 'm6_zai']
        data_csv62 = data_csv62.drop(['m6_zai'], axis=1)
        data_csv62 = data_csv62.set_index("date")

        with open(path + '缓存\\xf6_s.csv', 'rt') as csvfile:
            data_csv63 = pd.read_csv(csvfile, header=None)
        csvfile.close()
        data_csv63.columns = ['date', 'm6_xf_v', 'm6_xf_a', 'm6_xf', 'm6_zai']
        data_csv63 = data_csv63.drop(['m6_zai'], axis=1)
        data_csv63 = data_csv63.set_index("date")

        result1 = data_csv11.join(data_csv12, how='inner')
        result2 = result1.join(data_csv13, how='inner')
        result3 = result2.join(data_csv21, how='inner')
        result4 = result3.join(data_csv22, how='inner')
        result5 = result4.join(data_csv23, how='inner')
        result6 = result5.join(data_csv31, how='inner')
        result7 = result6.join(data_csv32, how='inner')
        result8 = result7.join(data_csv33, how='inner')
        result9 = result8.join(data_csv41, how='inner')
        result10 = result9.join(data_csv42, how='inner')
        result11 = result10.join(data_csv43, how='inner')
        result12 = result11.join(data_csv51, how='inner')
        result13 = result12.join(data_csv52, how='inner')
        result14 = result13.join(data_csv53, how='inner')
        result15 = result14.join(data_csv61, how='inner')
        result16 = result15.join(data_csv62, how='inner')
        result17 = result16.join(data_csv63, how='inner')
        result17 = result17.fillna(0)  # 将NAN替换为0
        result17['qynh']=result17['m1_qy']+result17['m2_qy']+result17['m3_qy']+result17['m4_qy']+result17['m5_qy']+result17['m6_qy']
        result17['zsnl']=result17['m1_zs']+result17['m2_zs']+result17['m3_zs']+result17['m4_zs']+result17['m5_zs']+result17['m6_zs']
        result17['fznh']=result17['m1_fz']+result17['m2_fz']+result17['m3_fz']+result17['m4_fz']+result17['m5_fz']+result17['m6_fz']
        result17['xfnh']=result17['m1_xf']+result17['m2_xf']+result17['m3_xf']+result17['m4_xf']+result17['m5_xf']+result17['m6_xf']
        result17['znh']=result17['qynh']+result17['xfnh']+result17['fznh']
        result17['m1_all']=result17['m1_qy']+result17['m1_fz']+result17['m1_xf']
        result17['m2_all']=result17['m2_qy']+result17['m2_fz']+result17['m2_xf']
        result17['m3_all']=result17['m3_qy']+result17['m3_fz']+result17['m3_xf']
        result17['m4_all']=result17['m4_qy']+result17['m4_fz']+result17['m4_xf']
        result17['m5_all']=result17['m5_qy']+result17['m5_fz']+result17['m5_xf']
        result17['m6_all']=result17['m6_qy']+result17['m6_fz']+result17['m6_xf']
    except:
        return -1
    #mvb数据
    try:
        with open(path + '缓存\\mvb.csv', 'rt') as csvfile:
            data_csv_mvb = pd.read_csv(csvfile, header=None)
        csvfile.close()
        data_csv_mvb.columns = ['date_mvb','date', 'mileage', 'station_now', 'station_next', 'speed','gradient','mc1_fullload_rate','mc1_noload_mass','m2_fullload_rate','m2_noload_mass','m3_fullload_rate','m3_noload_mass','m4_fullload_rate','m4_noload_mass','m5_fullload_rate','m5_noload_mass','mc6_fullload_rate','mc6_noload_mass','train_mass','mc1_motor_speed','mc1_motor_a','mc1_motor_v','mc1_motor_flag','m2_motor_speed','m2_motor_a','m2_motor_v','m2_motor_flag','m3_motor_speed','m3_motor_a','m3_motor_v','m3_motor_flag','m4_motor_speed','m4_motor_a','m4_motor_v','m4_motor_flag','m5_motor_speed','m5_motor_a','m5_motor_v','m5_motor_flag','mc6_motor_speed','mc6_motor_a','mc6_motor_v','mc6_motor_flag','tra_force','mc1_ebf','m2_ebf','m3_ebf','m4_ebf','m5_ebf','mc6_ebf','mc1_abf','m2_abf','m3_abf','m4_abf','m5_abf','mc6_abf','motor_tem']
        #data_csv_mvb = data_csv_mvb.drop(['date_mvb','mc1_motor_speed','mc1_motor_a','mc1_motor_v','mc1_motor_flag','m2_motor_speed','m2_motor_a','m2_motor_v','m2_motor_flag','m3_motor_speed','m3_motor_a','m3_motor_v','m3_motor_flag','m4_motor_speed','m4_motor_a','m4_motor_v','m4_motor_flag','m5_motor_speed','m5_motor_a','m5_motor_v','m5_motor_flag','mc6_motor_speed','mc6_motor_a','mc6_motor_v','mc6_motor_flag','tra_force','mc1_ebf','m2_ebf','m3_ebf','m4_ebf','m5_ebf','mc6_ebf','mc1_abf','m2_abf','m3_abf','m4_abf','m5_abf','mc6_abf','motor_tem'], axis=1)
        data_csv_mvb = data_csv_mvb.drop(['date_mvb'], axis=1)  #以计量装置时间戳合并
        data_csv_mvb['speed']=data_csv_mvb['speed']/100
        data_csv_mvb['accelerate'] = data_csv_mvb['speed'].diff(-1)
        data_csv_mvb['distance'] = abs(data_csv_mvb['speed'])
        data_csv_mvb['passenger']=round(abs(data_csv_mvb['train_mass']-13660)/6,2)
        data_csv_mvb = data_csv_mvb.fillna(0)  # 将NAN替换为0
        data_csv_mvb = data_csv_mvb.set_index("date")
    except:
        return -1
    result18 = result17.join(data_csv_mvb, how='inner')
    result18['dh_p'] ='0'
    result18['dh_c'] ='0'
    result18['flag']='0'
    result18 = result18.reset_index()
    result18=result18.rename(columns={'index': 'date'})
    result18.insert(1, 'year', '')
    result18.insert(2, 'month', '')
    result18.insert(3, 'day', '')
    result18.insert(4, 'hour', '')
    result18.insert(5, 'min', '')
    result18.insert(6, 'week', '')
    data = result18.iloc[:, 0].values
    result18['year'], result18['month'], result18['day'], result18['hour'], result18['min'], result18['week'] = year_change(
        data)
    result18.insert(0, 'trainid', '010')
    result18.insert(0, 'sline', 'S1')
    name=get_name(str(result18.iloc[0,2]),str(result18.iloc[len(result18)-1,2]))+'秒原始数据.csv'
    result18.to_csv(path+'缓存\\'+name, index=0, header=0)  # 导出解析结果，修改名称或路径

#def get_correct_data(path,path_out):  #补充数据,使时间连续
    try:
        #with open('D:\线路能耗评估\\2020年11-12月份数据\S1test\缓存\\S1010_202011020000_202011070429秒原始数据.csv', 'rt') as csvfile:
        with open(path+'缓存\\'+name, 'rt') as csvfile:
            data_csv = pd.read_csv(csvfile,header=None)
            time_str = data_csv.iloc[:, 2].values
        csvfile.close()
    except:
        return -1
    data_csv.columns = ['sline','trainid','date','year','month','day','hour','min','week','m1_qy_v','m1_qy_a','m1_qy','m1_zs','m1_fz_v','m1_fz_a','m1_fz','m1_xf_v','m1_xf_a','m1_xf','m2_qy_v','m2_qy_a','m2_qy','m2_zs','m2_fz_v','m2_fz_a','m2_fz','m2_xf_v','m2_xf_a','m2_xf','m3_qy_v','m3_qy_a','m3_qy','m3_zs','m3_fz_v','m3_fz_a','m3_fz','m3_xf_v','m3_xf_a','m3_xf','m4_qy_v','m4_qy_a','m4_qy','m4_zs','m4_fz_v','m4_fz_a','m4_fz','m4_xf_v','m4_xf_a','m4_xf','m5_qy_v','m5_qy_a','m5_qy','m5_zs','m5_fz_v','m5_fz_a','m5_fz','m5_xf_v','m5_xf_a','m5_xf','m6_qy_v','m6_qy_a','m6_qy','m6_zs','m6_fz_v','m6_fz_a','m6_fz','m6_xf_v','m6_xf_a','m6_xf','qynh','zsnl','fznh','xfnh','znh','m1_all','m2_all','m3_all','m4_all','m5_all','m6_all','mileage', 'station_now', 'station_next', 'speed','gradient','mc1_fullload_rate','mc1_noload_mass','m2_fullload_rate','m2_noload_mass','m3_fullload_rate','m3_noload_mass','m4_fullload_rate','m4_noload_mass','m5_fullload_rate','m5_noload_mass','mc6_fullload_rate','mc6_noload_mass','train_mass','mc1_motor_speed','mc1_motor_a','mc1_motor_v','mc1_motor_flag','m2_motor_speed','m2_motor_a','m2_motor_v','m2_motor_flag','m3_motor_speed','m3_motor_a','m3_motor_v','m3_motor_flag','m4_motor_speed','m4_motor_a','m4_motor_v','m4_motor_flag','m5_motor_speed','m5_motor_a','m5_motor_v','m5_motor_flag','mc6_motor_speed','mc6_motor_a','mc6_motor_v','mc6_motor_flag','tra_force','mc1_ebf','m2_ebf','m3_ebf','m4_ebf','m5_ebf','mc6_ebf','mc1_abf','m2_abf','m3_abf','m4_abf','m5_abf','mc6_abf','motor_tem','accelerate','distance','passenger','dh_p','dh_c','flag']
    timestamp_begin = timeHandle2(time_str[0])
    timestamp_index=timestamp_begin
    data_correct=[]

    for i in range(len(data_csv)):
        print(i/len(data_csv))
        try:
            timeArray = time.strptime(time_str[i], "%Y-%m-%d %H:%M:%S")  # 判断是否为标准时间格式
            time_stamp = int(time.mktime(timeArray))
            if(time_stamp==timestamp_index):
                data_correct.append(data_csv.iloc[i, :])
                timestamp_index = timestamp_index + 1
            elif(time_stamp>timestamp_index):
                while(time_stamp>timestamp_index-1):
                    insert_data=data_csv.iloc[i, :].copy()
                    insert_data=get_copy(insert_data)
                    #insert_data = insert.iloc[0, :].copy()
                    insert_data['date']=timeHandle3(timestamp_index)
                    data_correct.append(insert_data)
                    timestamp_index = timestamp_index + 1
                    print(timeHandle3(timestamp_index))
            else:
                continue

        except:
            return -1
    data_correct_df = pd.DataFrame(data_correct)
    data_correct_df = data_correct_df.reset_index(drop=True)
    data_correct_df = data_correct_df.fillna(0)  # 将NAN替换为0
    #分整天输出

    s_time_begin=timeHandle2(str(data_correct_df.iloc[0,2]))//86400
    day_rest=timeHandle2(str(data_correct_df.iloc[0,2])) % 86400 #如果时间大于8：00，index会归于下一天
    if day_rest<57600:
        min_time_index = (s_time_begin) * 86400 - 28800
    else:
        min_time_index = (s_time_begin + 1) * 86400 - 28800

    time_num_index=timeHandle(data_correct_df.iloc[:,2],len(data_correct_df))
    begin_index=0
    name_index=0
    for i in range(len(time_num_index)):
        if(time_num_index[i]>=min_time_index):
            if i!=0:
                if i-begin_index>7200:
                    name = get_name(str(data_correct_df.iloc[name_index, 2]), str(data_correct_df.iloc[i, 2]))
                    data_correct_df.iloc[begin_index:i, :].to_csv(path_out + name + '_s.csv', index=0, header=1)
                    begin_index=i
                    name_index=i
                else:
                    name_index=i
            min_time_index=min_time_index+86400
    if len(data_correct_df)-begin_index>7200:
        name = get_name(str(data_correct_df.iloc[name_index, 2]), str(data_correct_df.iloc[len(data_correct_df)-1, 2]))
        data_correct_df.iloc[begin_index:, :].to_csv(path_out + name + '_s.csv', index=0, header=1)
    data_correct_df.to_csv(path+'缓存\\'+'sdata.csv', index=0, header=0)

def get_copy(insert):
    insert['m1_qy'] = '0'
    insert['m1_zs'] = '0'
    insert['m1_fz'] = '0'
    insert['m1_xf'] = '0'
    insert['m2_qy'] = '0'
    insert['m2_zs'] = '0'
    insert['m2_fz'] = '0'
    insert['m2_xf'] = '0'
    insert['m3_qy'] = '0'
    insert['m3_zs'] = '0'
    insert['m3_fz'] = '0'
    insert['m3_xf'] = '0'
    insert['m4_qy'] = '0'
    insert['m4_zs'] = '0'
    insert['m4_fz'] = '0'
    insert['m4_xf'] = '0'
    insert['m5_qy'] = '0'
    insert['m5_zs'] = '0'
    insert['m5_fz'] = '0'
    insert['m5_xf'] = '0'
    insert['m6_qy'] = '0'
    insert['m6_zs'] = '0'
    insert['m6_fz'] = '0'
    insert['m6_xf'] = '0'
    insert['date'] = '0'
    insert['m1_all'] = '0'
    insert['m2_all'] = '0'
    insert['m3_all'] = '0'
    insert['m4_all'] = '0'
    insert['m5_all'] = '0'
    insert['m6_all'] = '0'
    insert['qynh'] = '0'
    insert['fznh'] = '0'
    insert['xfnh'] = '0'
    insert['zsnl'] = '0'
    insert['znh'] = '0'
    insert['m1_all'] = '0'
    insert['m2_all'] = '0'
    insert['m3_all'] = '0'
    insert['m4_all'] = '0'
    insert['m5_all'] = '0'
    insert['m6_all'] = '0'
    insert['accelerate'] = '0'
    insert['distance'] = '0'
    insert['dh_p'] = '0'
    insert['dh_c'] = '0'
    insert['flag'] = '1'
    return insert
def get_name(time_str1,time_str2):
    time_str1_y = datetime.datetime.strptime(time_str1, '%Y-%m-%d %H:%M:%S').strftime('%Y')
    time_str1_m = datetime.datetime.strptime(time_str1, '%Y-%m-%d %H:%M:%S').strftime('%m')
    time_str1_d = datetime.datetime.strptime(time_str1, '%Y-%m-%d %H:%M:%S').strftime('%d')
    time_str1_h = datetime.datetime.strptime(time_str1, '%Y-%m-%d %H:%M:%S').strftime('%H')
    time_str1_mi = datetime.datetime.strptime(time_str1, '%Y-%m-%d %H:%M:%S').strftime('%M')
    time_str2_y = datetime.datetime.strptime(time_str2, '%Y-%m-%d %H:%M:%S').strftime('%Y')
    time_str2_m = datetime.datetime.strptime(time_str2, '%Y-%m-%d %H:%M:%S').strftime('%m')
    time_str2_d = datetime.datetime.strptime(time_str2, '%Y-%m-%d %H:%M:%S').strftime('%d')
    time_str2_h = datetime.datetime.strptime(time_str2, '%Y-%m-%d %H:%M:%S').strftime('%H')
    time_str2_mi = datetime.datetime.strptime(time_str2, '%Y-%m-%d %H:%M:%S').strftime('%M')
    name='S1010_'+time_str1_y+time_str1_m+time_str1_d+time_str1_h+time_str1_mi+'_'+time_str2_y+time_str2_m+time_str2_d+time_str2_h+time_str2_mi
    return name

def move_data(path):
    try:
        path_from='E:\server\dt\\S1\\'
        path_out1=path+'data_deal\S1\\'
        if not os.path.exists(path+'data_deal\\'):
            os.mkdir(path+'data_deal\\')
        if not os.path.exists(path + 'data_deal\\缓存\\'):
            os.mkdir(path + 'data_deal\\缓存\\')
        #else:
        #   shutil.rmtree(path + 'data_deal\\缓存\\')  # 清空缓存文件夹
        if not os.path.exists(path_out1):
            os.mkdir(path_out1)
        path_out2=path+'data_deal\\缓存\\data\\'
        if not os.path.exists(path_out2):
            os.mkdir(path_out2)
        filenames = os.listdir(path_from)
        for i in filenames:
            file_path = path_from + '/' + i
            shutil.copyfile(file_path, path_out1+'/'+i)
            shutil.move(file_path, path_out2)
    except:
        shutil.rmtree(path_out)  # 清空缓存文件夹
        print('原始数据异常')


path='E:\server\\'
path_in=path+'data_deal\缓存\data\\'
path_out=path+'data_deal\缓存\\'
try:
    shutil.rmtree(path_out)  # 清空缓存文件夹
except:
    print('缓存文件夹清空')
move_data(path) #转移数据
s1_file_Combine(path_in,path_out)  #合并数据
flag=S1_Combine_s(path+'data_deal\\')
if flag==-1:
    print('处理异常')
    shutil.rmtree(path_out)  # 清空缓存文件夹


#print(flag)
#path='D:\线路能耗评估\\2020年10-11月份数据\S1testcsv\\秒\\S1010_2020-10-25_2020-10-30秒数据1.csv'
#path='D:\线路能耗评估\\2020年10-11月份数据\S1testcsv\\秒\\1.csv'
#get_correct_data(path_out,path_out)

#chaifen('D:\线路能耗评估\\2020年11-12月份数据\S1test\缓存\S1010_202010270403_202011162350秒原始数据.csv','D:\线路能耗评估\\2020年11-12月份数据\S1test\缓存\\')


