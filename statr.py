import redis
import sys
import json
import os
import threading


def run(task_fix, host_source, host_target, table_name, begin_prefix, end_prefix, stat_count):
    # 创建两个线程
    cache1 = dict()
    cache2 = dict()
    thread1 = threading.Thread(target=exe_check,
                               args=(host_source, table_name, begin_prefix, end_prefix, cache1, stat_count,))
    thread2 = threading.Thread(target=exe_check,
                               args=(host_target, table_name, begin_prefix, end_prefix, cache2, stat_count,))

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
