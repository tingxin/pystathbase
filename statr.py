import redis
import sys
import json
import os
import threading
import happybase


def exe_check(host_source: str, table_name: str, begin_prefix: str, end_prefix: str, cache: dict, max_count=1000000):
    hour = 1000 * 60 * 60
    host = host_source.split(':')

    # 分批的原因是服务端rpc只能连接1分钟，但是服务的目前还不能改
    connection = happybase.Connection(host[0], port=int(host[1]), timeout=hour)
    connection.open()
    table = connection.table(table_name)

    # 构造扫描器，并应用过滤器

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


def run(task_fix, host_source, host_target, table_name, begin_prefix, end_prefix, stat_count):
    # 创建两个线程
    cache1 = dict()
    cache2 = dict()
    thread1 = threading.Thread(target=exe_check,
                               args=(host_source, patten,  cache1, stat_count,))
    thread2 = threading.Thread(target=exe_check,
                               args=(host_target, patten,  cache2, stat_count,))

    # 启动线程
    print(f"begin scan table {table_name} between {host_source} and {host_target} in {begin_prefix} and {end_prefix}")
    thread1.start()
    thread2.start()

    # 等待两个线程执行完毕
    thread1.join()
    thread2.join()

    print(
        f"begin compare table {table_name} between {host_source} and {host_target} in {begin_prefix} and {end_prefix}")
    compare(task_fix, host_source, host_target, table_name, cache1, cache2, begin_prefix, end_prefix)
    print(f"end compare table {table_name} between {host_source} and {host_target} in {begin_prefix} and {end_prefix}")

    del cache1
    del cache2


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

    r = redis.Redis(host='localhost', port=6380, username='dvora', password='redis', decode_responses=True)
    r.ping()
    r.keys(pattern="*")
