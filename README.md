##官方 http://docs.mongodb.org/ecosystem/tutorial/getting-started-with-hadoop/

##mongo-haoop项目地址 https://github.com/mongodb/mongo-hadoop

##该代码托管 https://github.com/cclient/mongo_hadoop_map-reduce

原业务 由nodejs+async编写,较耗时且分布式实现复杂,改为map、reduce实现。

Mongo collection 数据格式

clientMac和url 先拼在一起，再按mac长度分割
要求 计算每个apMac下每个clientMac top100的url

数据流程 

mongodb 原始数据
{
    "_id" : ObjectId("54d83f3548c9bc218e056ce6"),
    "apMac" : "aa:bb:cc:dd:ee:ff",
    "proto" : "http",
    "url" : "extshort.weixin.qq.com",
    "clientMac" : "ff:ee:dd:cc:bb:aa"
}
map:因(clientMac长度固定,按字符分拆clientMac和url即可,因此未用分隔符)
[{apmac:[clientmac+url]}]
例
[{"aa:bb:cc:dd:ee:ff":[ff:ee:dd:cc:bb:aaextshort.weixin.qq.com]}]

compine:

如果有相同的client+url 则统计个数，以|分隔
[{apmca:[clientmac+url|url_num]}]
例
[{"aa:bb:cc:dd:ee:ff":[ff:ee:dd:cc:bb:aaextshort.weixin.qq.com|100]}]

reducer:
按mac长度和"|"分隔出clientMac、url、url_num

聚合取url_num sum,取top100

最后结果如下

因为不同版本的mongo对 key里的.号处理方式不同(字符串或子对象的key),实际执行时.都先统一替换为}再处理,为方便理解,文档依然用.

{
    "_id": "00:21:26:00:0A:FF",
    "aa:bb:cc:1c:b9:8f": {
        "c.tieba.baidu.com": 1,
        "short.weixin.qq.com": 1,
        "get.sogou.com": 1,
        "md.openapi.360.cn": 1,
        "74.125.235.224": 1,
        "mmbiz.qpic.cn": 1,
        "tb.himg.baidu.com": 1
    },
    "cc:bb:aa:d5:30:8a": {
        "captive.apple.com": 2,
        "www.airport.us": 1,
        "www.itools.info": 2,
        "www.thinkdifferent.us": 1,
        "www.ibook.info": 1
    },
    "ee:ee:bb:78:31:74": {
        "www.itools.info": 1,
        "www.ibook.info": 1
    }
}
