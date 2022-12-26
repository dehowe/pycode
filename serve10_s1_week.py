import os
import pandas as pd
import time
import datetime
import numpy as np
import shutil

def timeHandle(time_str,len):
    time_str1=[]
    for i in range(len):
        timeArray = time.strptime(time_str[i], "%Y-%m-%d %H:%M:%S")
        # 转换为时间戳
        timeStamp = int(time.mktime(timeArray))
        time_str1.append(timeStamp)
        #time_str[i]=timeStamp
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

def s1_week_Combine(path): #整合csv
    if not os.path.exists(path + '周\\'):
        os.mkdir(path + '周\\')
    path_input=path+'天\\'
    path_out=path+'周\\'
    # filenames = os.listdir(path_input)
    # for i in filenames:
    #     excel_path=path_input+'/'+i
    #     f = open(excel_path, 'rb')
    #     data = pd.read_csv(f)  #
    #     data.to_csv(path+'\\缓存\\mindata.csv', index=False, mode='a', header=0)
    #     print(i)
    try:
        with open(path_input + 'S1010_day.csv', 'rt') as csvfile:
            data_csv = pd.read_csv(csvfile, header=0)
        csvfile.close()
    except:
        return -2
    data_csv.columns = ['sline','trainid','date','year','month','day','hour','min','week','m1_qy_v','m1_qy_a','m1_qy','m1_zs','m1_fz_v','m1_fz_a','m1_fz','m1_xf_v','m1_xf_a','m1_xf',
                        'm2_qy_v','m2_qy_a','m2_qy','m2_zs','m2_fz_v','m2_fz_a','m2_fz','m2_xf_v','m2_xf_a','m2_xf',
                        'm3_qy_v','m3_qy_a','m3_qy','m3_zs','m3_fz_v','m3_fz_a','m3_fz','m3_xf_v','m3_xf_a','m3_xf',
                        'm4_qy_v','m4_qy_a','m4_qy','m4_zs','m4_fz_v','m4_fz_a','m4_fz','m4_xf_v','m4_xf_a','m4_xf',
                        'm5_qy_v','m5_qy_a','m5_qy','m5_zs','m5_fz_v','m5_fz_a','m5_fz','m5_xf_v','m5_xf_a','m5_xf',
                        'm6_qy_v','m6_qy_a','m6_qy','m6_zs','m6_fz_v','m6_fz_a','m6_fz','m6_xf_v','m6_xf_a','m6_xf',
                        'qynh','zsnl','fznh','xfnh','znh','m1_all','m2_all','m3_all','m4_all','m5_all','m6_all',
                        'mileage', 'station_now', 'station_next', 'speed','gradient','mc1_fullload_rate','mc1_noload_mass',
                        'm2_fullload_rate','m2_noload_mass','m3_fullload_rate','m3_noload_mass','m4_fullload_rate','m4_noload_mass',
                        'm5_fullload_rate','m5_noload_mass','mc6_fullload_rate','mc6_noload_mass','train_mass','mc1_motor_speed','mc1_motor_a',
                        'mc1_motor_v','mc1_motor_flag','m2_motor_speed','m2_motor_a','m2_motor_v','m2_motor_flag','m3_motor_speed','m3_motor_a',
                        'm3_motor_v','m3_motor_flag','m4_motor_speed','m4_motor_a','m4_motor_v','m4_motor_flag','m5_motor_speed','m5_motor_a',
                        'm5_motor_v','m5_motor_flag','mc6_motor_speed','mc6_motor_a','mc6_motor_v','mc6_motor_flag','tra_force','mc1_ebf','m2_ebf',
                        'm3_ebf','m4_ebf','m5_ebf','mc6_ebf','mc1_abf','m2_abf','m3_abf','m4_abf','m5_abf','mc6_abf','motor_tem','accelerate','distance',
                        'passenger','dh_p','dh_c','flag']

    week_begin = data_csv.iloc[0, 8]
    result = []
    j=0
    for i in range(len(data_csv)):
        week_index=data_csv.iloc[i, 8]
        if week_index != week_begin:
                data = data_csv.iloc[j, :].copy()
                data['m1_qy'] = get_sum_data(data_csv.iloc[j:i + 1, 11].values)
                data['m1_zs'] = get_sum_data(data_csv.iloc[j:i + 1, 12].values)
                data['m1_fz'] = get_sum_data(data_csv.iloc[j:i + 1, 15].values)
                data['m1_xf'] = get_sum_data(data_csv.iloc[j:i + 1, 18].values)
                data['m2_qy'] = get_sum_data(data_csv.iloc[j:i + 1, 21].values)
                data['m2_zs'] = get_sum_data(data_csv.iloc[j:i + 1, 22].values)
                data['m2_fz'] = get_sum_data(data_csv.iloc[j:i + 1, 25].values)
                data['m2_xf'] = get_sum_data(data_csv.iloc[j:i + 1, 28].values)
                data['m3_qy'] = get_sum_data(data_csv.iloc[j:i + 1, 31].values)
                data['m3_zs'] = get_sum_data(data_csv.iloc[j:i + 1, 32].values)
                data['m3_fz'] = get_sum_data(data_csv.iloc[j:i + 1, 35].values)
                data['m3_xf'] = get_sum_data(data_csv.iloc[j:i + 1, 38].values)
                data['m4_qy'] = get_sum_data(data_csv.iloc[j:i + 1, 41].values)
                data['m4_zs'] = get_sum_data(data_csv.iloc[j:i + 1, 42].values)
                data['m4_fz'] = get_sum_data(data_csv.iloc[j:i + 1, 45].values)
                data['m4_xf'] = get_sum_data(data_csv.iloc[j:i + 1, 48].values)
                data['m5_qy'] = get_sum_data(data_csv.iloc[j:i + 1, 51].values)
                data['m5_zs'] = get_sum_data(data_csv.iloc[j:i + 1, 52].values)
                data['m5_fz'] = get_sum_data(data_csv.iloc[j:i + 1, 55].values)
                data['m5_xf'] = get_sum_data(data_csv.iloc[j:i + 1, 58].values)
                data['m6_qy'] = get_sum_data(data_csv.iloc[j:i + 1, 61].values)
                data['m6_zs'] = get_sum_data(data_csv.iloc[j:i + 1, 62].values)
                data['m6_fz'] = get_sum_data(data_csv.iloc[j:i + 1, 65].values)
                data['m6_xf'] = get_sum_data(data_csv.iloc[j:i + 1, 68].values)
                data['qynh'] = get_sum_data(data_csv.iloc[j:i + 1, 69].values)
                data['zsnl'] = get_sum_data(data_csv.iloc[j:i + 1, 70].values)
                data['fznh'] = get_sum_data(data_csv.iloc[j:i + 1, 71].values)
                data['xfnh'] = get_sum_data(data_csv.iloc[j:i + 1, 72].values)
                data['znh'] = get_sum_data(data_csv.iloc[j:i + 1, 73].values)
                data['m1_all'] = data['m1_qy'] + data['m1_fz'] + data['m1_xf']
                data['m2_all'] = data['m2_qy'] + data['m2_fz'] + data['m2_xf']
                data['m3_all'] = data['m3_qy'] + data['m3_fz'] + data['m3_xf']
                data['m4_all'] = data['m4_qy'] + data['m4_fz'] + data['m4_xf']
                data['m5_all'] = data['m5_qy'] + data['m5_fz'] + data['m5_xf']
                data['m6_all'] = data['m6_qy'] + data['m6_fz'] + data['m6_xf']
                distance = round(get_sum_data(data_csv.iloc[j:i + 1, 137].values), 2)
                passenger = round(get_sum_data(data_csv.iloc[j:i + 1, 138].values), 2)
                data['distance'] = distance
                data['passenger'] = passenger
                if distance != 0:
                    data['dh_p'] = round((data['qynh'] + data['fznh'] + data['xfnh']) / passenger, 2)
                else:
                    data['dh_p'] = '0'
                if passenger != 0:
                    data['dh_c'] = round((data['qynh'] + data['fznh'] + data['xfnh']) / distance, 2)
                else:
                    data['dh_c'] = '0'
                data['flag'] = 0
                week_begin=week_index
                result.append(data)
                j = i

        else:
            continue
    length = len(result)
    if (length != 0):
        # w = pd.DataFrame(w.values.T, index=w.columns, columns=w.index)  # 转置
        array_to_matrix1 = np.mat(result)
        data_correct_df = pd.DataFrame(array_to_matrix1)
        data_correct_df.columns = ['sline','trainid','date','year','month','day','hour','min','week','m1_qy_v','m1_qy_a','m1_qy','m1_zs','m1_fz_v','m1_fz_a','m1_fz','m1_xf_v','m1_xf_a','m1_xf',
                        'm2_qy_v','m2_qy_a','m2_qy','m2_zs','m2_fz_v','m2_fz_a','m2_fz','m2_xf_v','m2_xf_a','m2_xf',
                        'm3_qy_v','m3_qy_a','m3_qy','m3_zs','m3_fz_v','m3_fz_a','m3_fz','m3_xf_v','m3_xf_a','m3_xf',
                        'm4_qy_v','m4_qy_a','m4_qy','m4_zs','m4_fz_v','m4_fz_a','m4_fz','m4_xf_v','m4_xf_a','m4_xf',
                        'm5_qy_v','m5_qy_a','m5_qy','m5_zs','m5_fz_v','m5_fz_a','m5_fz','m5_xf_v','m5_xf_a','m5_xf',
                        'm6_qy_v','m6_qy_a','m6_qy','m6_zs','m6_fz_v','m6_fz_a','m6_fz','m6_xf_v','m6_xf_a','m6_xf',
                        'qynh','zsnl','fznh','xfnh','znh','m1_all','m2_all','m3_all','m4_all','m5_all','m6_all',
                        'mileage', 'station_now', 'station_next', 'speed','gradient','mc1_fullload_rate','mc1_noload_mass',
                        'm2_fullload_rate','m2_noload_mass','m3_fullload_rate','m3_noload_mass','m4_fullload_rate','m4_noload_mass',
                        'm5_fullload_rate','m5_noload_mass','mc6_fullload_rate','mc6_noload_mass','train_mass','mc1_motor_speed','mc1_motor_a',
                        'mc1_motor_v','mc1_motor_flag','m2_motor_speed','m2_motor_a','m2_motor_v','m2_motor_flag','m3_motor_speed','m3_motor_a',
                        'm3_motor_v','m3_motor_flag','m4_motor_speed','m4_motor_a','m4_motor_v','m4_motor_flag','m5_motor_speed','m5_motor_a',
                        'm5_motor_v','m5_motor_flag','mc6_motor_speed','mc6_motor_a','mc6_motor_v','mc6_motor_flag','tra_force','mc1_ebf','m2_ebf',
                        'm3_ebf','m4_ebf','m5_ebf','mc6_ebf','mc1_abf','m2_abf','m3_abf','m4_abf','m5_abf','mc6_abf','motor_tem','accelerate','distance',
                        'passenger','dh_p','dh_c','flag']
        data_correct_df.to_csv(path_out + 'S1010_week.csv', index=0, header=1)
    #shutil.rmtree(path + '缓存\\')  # 清空缓存文件夹

def get_sum_data(data):
    sum = 0
    # data=data.reset_index(drop=True)
    for i in range(len(data)):
        sum = sum + data[i]
    return sum

path='E:\server\data_deal\\'
s1_week_Combine(path)
