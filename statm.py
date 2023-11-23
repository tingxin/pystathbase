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

get_bach_count = 2000
need_remedy = False

def compare_and_fix(tb_name:str, result:list, source:dict, target, host_target):
    notmatch_source=dict()
    keys = set()
    
    for row in target:
        key = row[0]
        data = row[1]
        keys.add(key)
        if source[key] != data:
            notmatch_source[key]= data

    
    print(len(source))
    print(len(keys))

    table = None
    connection = None
    if need_remedy:
        hour = 1000 * 60 * 60
        host = host_target.split(':')
        connection = happybase.Connection(host[0], port=int(host[1]), timeout=hour)
        table = connection.table(tb_name)

    result.append(f'{tb_name} 共从源扫描{len(source)}\n')
    result.append(f'{tb_name} 共从目标扫描{len(keys)}\n')
    if len(notmatch_source) > 0:
        result.append(f'{tb_name} 不一致数据：')
        if need_remedy:
            result.append(f'共需修正 {len(notmatch_source)}条数据\n')

        for key in notmatch_source:
            result.append(f'{key} 不一致\n期望{source[key]}\n{notmatch_source[key]}\n')
            if need_remedy:
                print(f"正在修正 {tb_name} 数据 {key}")
                t = table.put(key, source[key])
            
    notin_keys = set(source.keys()).difference(keys)
    if len(notin_keys) > 0:
        result.append(f'{tb_name} 缺失数据：')
        if need_remedy:
            result.append(f'共需补充 {len(notin_keys)}条数据\n')
        for key in notin_keys:
            result.append(f'{key} 缺失\n')
            if need_remedy:
                print(f"正在补充 {tb_name} 数据 {key}")
                t = table.put(key, source[key])

    if need_remedy:
        connection.close()



def exe_check(host_source: str, host_target: str, table_name: str, begin_prefix: str, end_prefix: str, result: list,
              max_count=1000000):
    start_time = time.time()
    hour = 1000 * 60 * 60
    host = host_source.split(':')

    connection1 = happybase.Connection(host[0], port=int(host[1]), timeout=hour)
    connection1.scanner_timeout = hour*2
    table1 = connection1.table(table_name)

    # 构造扫描器，并应用过滤器
    cache = dict()
    try:
        print(f"{host_source} begin scan table {table_name} in {begin_prefix} and {end_prefix}")
        scanner = table1.scan(row_start=begin_prefix, row_stop=end_prefix, sorted_columns=True, limit=max_count)
        total_count = 0
        for key, data in scanner:
            cache[key] = data
            total_count += 1
            if total_count % 10000==0:
                print(f"正在扫描 {total_count} 行")

    except Exception as e:
        print(e)

    host = host_target.split(':')

    # 分批的原因是服务端rpc只能连接1分钟，但是服务的目前还不能改
    resp_result = list()
    try:
        request = list()
        counter = 0
        bach_counter = 0
        gen = cache.keys()
        
        for key in gen:
            request.append(key)
            counter += 1
            bach_counter+=1
            if bach_counter == get_bach_count:
                # connection 超过服务的超时，但是客户端不知道，继续get数据可能造成socket问题
                # 所以笨办法每次都重启一个链接
                connection2 = happybase.Connection(host[0], port=int(host[1]), timeout=hour)
                table2 = connection2.table(table_name)
                resp = table2.rows(request)
                resp_result.extend(resp)
                connection2.close()
                print(f"正在从{host_target} 扫描 并比较 {table_name} 第{counter} 行")
                request = list()
                bach_counter = 0

        if len(request) > 0:
            connection2 = happybase.Connection(host[0], port=int(host[1]), timeout=hour)
            table2 = connection2.table(table_name)
            resp = table2.rows(request)
            resp_result.extend(resp)
            connection2.close()

    except Exception as e:
        print(e)
        print("比对行数次数太多，请适当减少max_count")

    compare_and_fix(table_name, result,cache,resp_result,host_target)

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"校验表{table_name} 耗时 {elapsed_time:.2f} 秒")
    del cache
    resp_result = None


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
    remedy = False
    if 'remedy' in conf:
        remedy = bool(conf['remedy'])
    global need_remedy
    need_remedy = remedy

    tables = conf['tables']
    current_time = datetime.now() + timedelta(hours=8)
    # 将当前时间格式化为字符
    formatted_time = current_time.strftime("%Y-%m-%d_%H_%M_%S")

    result_list = list()
    t = list()
    start_time = time.time()
    for table in tables:
        table_name = table['name'].strip('\n\t')
        begin_prefix = table['begin_prefix']
        end_prefix = table['end_prefix']
        result = list()
        th = threading.Thread(target=exe_check,
                              args=(host_source, host_target, table_name, begin_prefix, end_prefix, result, max_count,))

        result_list.append((table, result))
        t.append(th)
        th.start()

    for item in t:
        item.join()

    with open(f'{os.getcwd()}/compare_{formatted_time}.txt', 'a') as file:
        for tup in result_list:
            table = tup[0]
            table_name = table['name'].strip('\t').strip('\n')
            begin_prefix = table['begin_prefix']
            end_prefix = table['end_prefix']
            
            result = tup[1]
            file.write(f'分析报告：{table_name} 在 {host_source} 在范围{begin_prefix} 和 {end_prefix}\n')
            for item in result:
                file.write(item)
            if len(result) <=2:
                file.write('校验结果：全部正确\n')
            
            file.write('\n')



    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"全部任务耗时 {elapsed_time:.2f} 秒")

main()
