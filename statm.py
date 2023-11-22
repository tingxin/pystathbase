import threading
import time
import sys
import happybase
import random
from datetime import datetime
import uuid
from datetime import datetime, timedelta
import os
import json


def split_big_range(begin_prefix: str, end_prefix: str)->list:
    datelength=10
    result = list()
    result.append(begin_prefix)
    begin_dt_index = begin_prefix.rfind('_')
    if begin_dt_index < 0:
        return [begin_prefix, end_prefix]
    begin_dt_str = begin_prefix[begin_dt_index+1:begin_dt_index+datelength+1]

    end_dt_index = end_prefix.rfind('_')
    end_dt_str = end_prefix[end_dt_index+1:end_dt_index+datelength+1]

    format_string = "%Y%m%d"
    format2_string = "%Y%m%d%H"
    
    begin_date = datetime.strptime(begin_dt_str, format_string)
    end_date = datetime.strptime(end_dt_str, format_string)

    current_date = begin_date + timedelta(hours=1)
    begin_f = begin_prefix[:begin_dt_index+1]
    while current_date < end_date:
        current_str = current_date.strftime(format2_string)
        new_sp = begin_f + current_str
        result.append(new_sp)
        current_date = current_date+ timedelta(hours=1)
    result.append(end_prefix)

    return result
    
    


def exe_check(host_source: str, table_name: str, b_prefix: str, e_prefix: str, cache: dict, max_count=1000000):
    
    
    hour = 1000*60*60 
    host = host_source.split(':')

    range_fix = split_big_range(b_prefix, e_prefix)
    for index_fix in range(0, len(range_fix)-1):

        begin_prefix = range_fix[index_fix]
        end_prefix = range_fix[index_fix+1]
        # 分批的原因是服务端rpc只能连接1分钟，但是服务的目前还不能改
        connection = happybase.Connection(host[0], port=int(host[1]), timeout=hour)
        connection.open()
        table = connection.table(table_name)


        # scan_filter = f"RowFilter(>, 'binaryprefix:{begin_prefix}') AND RowFilter(<, 'binaryprefix:{end_prefix}')"
        # 构造扫描器，并应用过滤器
        try:
            print(f"{host_source} bescagin scan table {table_name} in {begin_prefix} and {end_prefix}")
            ca = dict()
            scanner = table.scan(row_start=begin_prefix,row_stop=end_prefix,sorted_columns=True)
            counter = 0
            for key, data in scanner:
                ca[key] = data
                counter += 1
                if counter % 10000 == 0:
                    print(f"正在扫描 {counter} 行")

        except Exception as e:
            ca.clear()
            print(e)
            print(f'{begin_prefix} and {end_prefix} contain too much rows(> {max_count})')

        cache.update(ca)
        del ca
        connection.close()

def compare(task_fix,host_source, host_target, table_name:str, result1: dict, result2: dict, begin_prefix, end_prefix):

    l1 = len(result1)
    l2 = len(result2)

    sourceset = set(result1.keys())
    targetset = set(result2.keys())

    notin_target_keys = sourceset.difference(targetset)
    notin_source_keys = targetset.difference(sourceset)
    intersation = sourceset.intersection(targetset)

    with open(f'{os.getcwd()}/compare_{task_fix}.txt', 'a') as file:
        file.write(f'{table_name} 在 {host_source} 在范围{begin_prefix} 和 {end_prefix} 扫描了: {l1}\n')
        file.write(f'{table_name} 在 {host_target} 在范围{begin_prefix} 和 {end_prefix} 扫描了: {l2}\n')

        is_true = True
        if l1 != l2:
            file.write('校验结果：集群数据不一致\n')
            is_true = False

        if len(notin_target_keys) > 0:
            is_true = False
            file.write(f'如下key 在{host_source} 但是不在 {host_target}:\n')
            for key in notin_target_keys:
                file.write(f'{key}\n')
            file.write('\n')

        if len(notin_source_keys) > 0:
            is_true = False
            file.write(f'如下key 在{host_target} 但是不在 {host_source}:\n')
            for key in notin_source_keys:
                file.write(f'{key}\n')
            file.write('\n')

        if len(intersation) > 0:
            notmatch_list = list()
            for key in intersation:
                if result1[key] != result2[key]:
                    notmatch_list.append(f'{key}:\n {result1[key]}\n{result2[key]}\n')
            
            if len(notmatch_list) > 0:
                file.write(f'如下key 在两个集群都存在， 但是不一致,共计{len(notmatch_list)}:\n')
                for item in notmatch_list:
                    file.write(item)
                    file.write('\n')
        if is_true:
            file.write(f'校验结果：{table_name} 在两个集群在范围{begin_prefix} 和 {end_prefix} 数据验证全部正确\n')


        file.write('\t\n')



def run(task_fix, host_source, host_target, table_name, begin_prefix, end_prefix, stat_count):
    # 创建两个线程
    cache1 = dict()
    cache2 = dict()
    thread1 = threading.Thread(target=exe_check, args=(host_source, table_name, begin_prefix, end_prefix,cache1, stat_count,))
    thread2 = threading.Thread(target=exe_check, args=(host_target, table_name, begin_prefix, end_prefix,cache2, stat_count,))

    # 启动线程
    print(f"begin scan table {table_name} between {host_source} and {host_target} in {begin_prefix} and {end_prefix}")
    thread1.start()
    thread2.start()

    # 等待两个线程执行完毕
    thread1.join()
    thread2.join()

    print(f"begin compare table {table_name} between {host_source} and {host_target} in {begin_prefix} and {end_prefix}")
    compare(task_fix,host_source, host_target, table_name, cache1,cache2, begin_prefix, end_prefix)
    print(f"end compare table {table_name} between {host_source} and {host_target} in {begin_prefix} and {end_prefix}")

    del cache1
    del cache2

        
        

def main():
    arguments = sys.argv
    conf_path = f"{os.getcwd()}/{arguments[1]}"
    print(conf_path)
    # 读取 JSON 文件
    with open(conf_path, 'r') as file:
        # 加载 JSON 数据
        conf = json.load(file)

    host_source = conf['host_source']
    host_target = conf['host_target']
    max_count = conf['max_count']
    tables = conf['tables']

    current_time = datetime.now()+timedelta(hours=8)
    # 将当前时间格式化为字符
    formatted_time = current_time.strftime("%Y-%m-%d_%H_%M_%S")

    for table in tables:
        start_time = time.time()
        table_name = table['name']
        begin_prefix = table['begin_prefix']
        end_prefix = table['end_prefix']

        run(formatted_time, host_source, host_target, table_name, begin_prefix, end_prefix, max_count)
        
        end_time = time.time()

        # 计算程序运行时间
        elapsed_time = end_time - start_time
        print(f"校验表{table_name} 耗时 {elapsed_time:.2f} 秒")


main()
