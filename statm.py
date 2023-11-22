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


def exe_check(host_source: str, table_name: str, begin_prefix: str, end_prefix: str, cache: dict, max_count=1000000):
    hour = 1000*60*60 
    host = host_source.split(':')
    connection = happybase.Connection(host[0], port=int(host[1]), timeout=hour)
    connection.open()

    table = connection.table(table_name)
    ca = cache

    scan_filter = f"RowFilter(>, 'binaryprefix:{begin_prefix}') AND RowFilter(<, 'binaryprefix:{end_prefix}')"
    # 构造扫描器，并应用过滤器
    try:
        scanner = table.scan(row_start=begin_prefix,row_stop=end_prefix)
        counter = 0
        for key, data in scanner:
            ca[key] = data
            counter += 1
            if counter > max_count:
                break
            if counter % 10000 == 0:
                print(f"正在扫描 {counter} 行")

    except Exception as e:
        ca.clear()
        print(e)
        print(f'{begin_prefix} and {end_prefix} contain too much rows(> {max_count})')

    connection.close()


def compare(task_fix, table_name:str, result1: dict, result2: dict, begin_prefix, end_prefix):

    l1 = len(result1)
    l2 = len(result2)
    big_name = '集群1'
    small_name = '集群2'
    big = result1
    small = result2
    if l1 < l2:
        big, small = result2, result1
        big_name,small_name = small_name, big_name

    bigset = set(big.keys())
    smallset = set(small)

    notin_small_keys = bigset.difference(smallset)
    notin_big_keys = smallset.difference(bigset)
    intersation = bigset.intersection(smallset)
    



    with open(f'{os.getcwd()}/compare_{task_fix}.txt', 'a') as file:
        file.write(f'{table_name} 在集群1 在范围{begin_prefix} 和 {end_prefix} 扫描了: {l1}\n')
        file.write(f'{table_name} 在集群2 在范围{begin_prefix} 和 {end_prefix} 扫描了: {l2}\n')

        is_true = True
        if l1 != l2:
            file.write('校验结果：集群数据不一致\n')
            is_true = False

        if len(notin_small_keys) > 0:
            is_true = False
            file.write(f'如下key 在{big_name} 但是不在 {small_name}:\n')
            for key in notin_small_keys:
                file.write(f'{key}\n')
            file.write('\n')

        if len(notin_big_keys) > 0:
            is_true = False
            file.write(f'如下key 在{small_name} 但是不在 {big_name}:\n')
            for key in notin_big_keys:
                file.write(f'{key}\n')
            file.write('\n')

        if len(intersation) > 0:
            notmatch_count = 0
            notmatch_list = list()
            for key in big:
                if big[key] != small[key]:
                    notmatch_count +=1
                    notmatch_list.append(f'{key}:\n {big[key]}\n{small[key]}\n')
            
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
    compare(task_fix, table_name, cache1,cache2, begin_prefix, end_prefix)
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
