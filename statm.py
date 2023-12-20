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
import hashlib

bach_count = 5000
get_bach_count = 1000
need_remedy = False
time_out_hour = 1000 * 60 * 60 * 10


def compare_and_fix(tb_name: str, result: list, source: dict, target, host_target):
    not_match_source = dict()
    keys = set()

    for row in target:
        key = row[0]
        data = row[1]
        keys.add(key)

        if source[key] != data:
            not_match_source[key] = data

    # print(list(keys)[0:20])

    table = None
    connection = None
    if need_remedy:
        hour = 1000 * 60 * 60
        host = host_target.split(':')
        connection = happybase.Connection(host[0], port=int(host[1]), timeout=hour)
        table = connection.table(tb_name)

    if len(not_match_source) > 0:
        result.append(f'{tb_name} 不一致数据：')
        if need_remedy:
            result.append(f'共需修正 {len(not_match_source)}条数据\n')

        for key in not_match_source:
            result.append(f'{key} 不一致\n期望{source[key]}\n{not_match_source[key]}\n')
            if need_remedy:
                print(f"{datetime.now()}:正在修正 {tb_name} 数据 {key}")
                table.put(key, source[key])

    not_in_keys = set(source.keys()).difference(keys)
    if len(not_in_keys) > 0:
        result.append(f'{tb_name} 缺失数据：')
        if need_remedy:
            result.append(f'共需补充 {len(not_in_keys)}条数据\n')
        for key in not_in_keys:
            result.append(f'{key} 缺失\n')
            if need_remedy:
                print(f"{datetime.now()}:正在补充 {tb_name} 数据 {key}")
                table.put(key, source[key])

    if need_remedy:
        connection.close()


def get_target_data(host_target: str, table_name, source_cache: dict):
    print(f"{datetime.now()}:开始从{host_target} 扫描 并比较 {table_name}")
    host = host_target.split(':')
    resp_result = list()
    if len(source_cache) <=0:
        return resp_result
    try:
        connection2 = happybase.Connection(host[0], port=int(host[1]), timeout=time_out_hour)
        table2 = connection2.table(table_name)

        request = list()
        gen = source_cache.keys()

        counter = 0
        bach_counter = 0

        for key in gen:
            request.append(key)
            counter += 1
            bach_counter += 1
            if bach_counter == get_bach_count:
                start_time = time.time()
                resp = table2.rows(request)
                resp_result.extend(resp)
                end_time = time.time()
                elapsed_time = end_time - start_time
                print(f"{datetime.now()}:正在从{host_target} 扫描 并比较 {table_name} 第{counter} 行, 耗时 {elapsed_time:.2f} 秒")
                request = list()
                bach_counter = 0

        if len(request) > 0:
            resp = table2.rows(request)
            resp_result.extend(resp)

        connection2.close()

    except Exception as e:
        print(f"get_target_data:\n{e}")
    return  resp_result


def exe_check(host_source: str, host_target: str, table_name: str, begin_prefix: str, end_prefix: str, result: list):
    start_time = time.time()
    host = host_source.split(':')

    # 构造扫描器，并应用过滤器

    total_count = 0
    total_target = 0
    row_start = begin_prefix
    while True:
        cache = dict()
        try:
            connection1 = happybase.Connection(host[0], port=int(host[1]), timeout=time_out_hour)
            connection1.scanner_timeout = time_out_hour
            table1 = connection1.table(table_name)
            print(f"{datetime.now()}:{host_source} begin scan scan table {table_name} in {row_start} and {end_prefix}")
            scanner = table1.scan(row_start=row_start, row_stop=end_prefix, sorted_columns=True, limit=bach_count)
            one_count = 0
            for key, data in scanner:
                cache[key] = data
                one_count += 1
                row_start = key.decode('utf-8')


            row_start = row_start + '0'
            total_count += one_count
            print(f"{datetime.now()}:正在扫描 {total_count} 行,新增 {one_count}")
            if one_count <= 0:
                break
        except Exception as e:
            print('end scan')
            print(e)
        finally:
            print(f"{datetime.now()}:{host_source} end scan table {table_name} in {begin_prefix} and {end_prefix}")
            connection1.close()

        resp_result = get_target_data(host_target, table_name, cache)
        total_target += len(resp_result)
        compare_and_fix(table_name, result, cache, resp_result, host_target)

        del cache
        time.sleep(2)

    result.insert(0, f'{table_name} 共从目标扫描{total_target}\n')
    result.insert(0, f'{table_name} 共从源扫描{total_count}\n')


    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"{datetime.now()}:校验表{table_name} 耗时 {elapsed_time:.2f} 秒")




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

    remedy = False
    if 'remedy' in conf:
        remedy = bool(conf['remedy'])
    global need_remedy
    need_remedy = remedy

    max_workers = 4
    if 'max_workers' in conf:
        max_workers = int(conf['max_workers'])

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
                              args=(host_source, host_target, table_name, begin_prefix, end_prefix, result,))

        result_list.append((table, result))
        t.append(th)
        th.start()
        time.sleep(2)

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
            if len(result) <= 2:
                file.write('校验结果：全部正确\n')

            file.write('\n')

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"全部任务耗时 {elapsed_time:.2f} 秒")


main()
