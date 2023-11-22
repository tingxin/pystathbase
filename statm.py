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

bach_count = 10000
get_bach_count = 100

def compare_get(result:dict, a:dict, b:dict):
    t = list()
    for row in b:
        rkey = row[0]
        data = row[1]
        if a[rkey] != data:
            result[rkey] = f'{rkey}不一致:\n{a[rkey]}\n{data}\n'
        t.append(rkey)
    return t


def exe_check(host_source: str, host_target:str, table_name: str, begin_prefix: str, end_prefix: str, result:dict,max_count=1000000):
    start_time = time.time()
    hour = 1000 * 60 * 60
    host = host_source.split(':')

    # 分批的原因是服务端rpc只能连接1分钟，但是服务的目前还不能改
    connection = happybase.Connection(host[0], port=int(host[1]), timeout=hour)
    connection.open()
    table = connection.table(table_name)

    # 构造扫描器，并应用过滤器
    cache = dict()
    try:
        total_count = 0
        row_start = begin_prefix

        while True:
            print(f"{host_source} begin scan scan table {table_name} in {row_start} and {end_prefix}")
            scanner = table.scan(row_start=row_start, row_stop=end_prefix, sorted_columns=True, limit=bach_count)
            one_count = 0
            for key, data in scanner:
                cache[key] = data
                one_count += 1
                row_start = key

            total_count = len(cache)
            print(f"正在扫描 {total_count} 行,新增 {one_count}")
            # 包括上次的row key
            if one_count <= 1:
                break
            if total_count > max_count:
                break

    except Exception as e:
        print(e)

    connection.close()

    host = host_target.split(':')

    # 分批的原因是服务端rpc只能连接1分钟，但是服务的目前还不能改
    connection = happybase.Connection(host[0], port=int(host[1]), timeout=hour)
    connection.open()
    table = connection.table(table_name)

    request = list()
    counter = 0
    gen = cache.keys()
    total_resp = list()
    for key in gen:
        request.append(key)
        counter +=1
        if counter == get_bach_count:
            resp = table.rows(request)
            t = compare_get(result, cache, resp)
            total_resp.extend(t)
            request = list()

    if len(request) > 0:
        resp = table.rows(request)
        t = compare_get(result, cache, resp)
        total_resp.extend(t)

    diff = set(cache.keys()).difference(set(total_resp))
    for key in diff:
        result[key] = f'{key} 在集群{host_source} 但是不在{host_target}\n'

    end_time = time.time()

        # 计算程序运行时间
    elapsed_time = end_time - start_time
    print(f"校验表{table_name} 耗时 {elapsed_time:.2f} 秒")


def main():
    arguments = sys.argv
    if len(arguments) < 2:
        print("缺少配置文件")
        return

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

    current_time = datetime.now() + timedelta(hours=8)
    # 将当前时间格式化为字符
    formatted_time = current_time.strftime("%Y-%m-%d_%H_%M_%S")

    result_list = list()
    t = list()
    for table in tables:
        
        table_name = table['name']
        begin_prefix = table['begin_prefix']
        end_prefix = table['end_prefix']
        result = dict()
        th = threading.Thread(target=exe_check,
                                 args=(host_source, host_target, table_name, begin_prefix, end_prefix,result, max_count,))
        
        result_list.append((table,result))
        t.append(th)
        th.start()
    
    for item in t:
        item.join()

    with open(f'{os.getcwd()}/compare_{formatted_time}.txt', 'a') as file:
        for tup in result_list:
            table = tup[0]
            table_name = table['name']
            begin_prefix = table['begin_prefix']
            end_prefix = table['end_prefix']
            result = tup[1]

            if len(result) > 0:
                file.write(f'{table_name} 在 {host_source} 在范围{begin_prefix} 和 {end_prefix}\n')
                for key in result:
                    file.write(result[key])
                file.write('\n')



main()
