import redis
import sys
import json
import os



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
