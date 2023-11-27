# PyStatHbase

使用happybase 库，通过抽样的方式 校验两个hbase集群相同数据表的数据是否一致

# 快速使用
# 使用
1. 建议创建一个新的python3 运行环境
```
python3 -m venv new_py_env
source ~/new_py_env/bin/active
```
2. 安装依赖
```shell
pip install -r requirement.txt
```
2. 修改conf.json
```json
{
    "host_source": "172.31.13.109:9090", # 源端的主节点地址+端口
    "host_target": "172.31.12.101:9090", # 目标端的主节点地址+端口
    "remedy": true, # 校验中，发现目标端数据和远端不一致后（不一致或者缺失），是否要修复目标端数据
    "tables": [
        {
            "name": "sales", # 表名
            "begin_prefix": "01f41001", #  范围前缀
            "end_prefix": "0259d608" # 范围后缀
        },
        {
            "name": "sales",
            "begin_prefix": "00fe7daf",
            "end_prefix": "01f5ca24-1d3e"
        },
        {
            "name": "app",
            "begin_prefix": "v1_20220311232",
            "end_prefix": "v3_20220311232"
        }
    ]
}

```
3. 保证程序的运行环境和两个数据库集群网络通
```
telnet xxxx 3306
```

4. 运行代码
```shell
python3 statm.py conf.json
```
