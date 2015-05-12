# mongo_hadoop_map-reduce
##官方 http://docs.mongodb.org/ecosystem/tutorial/getting-started-with-hadoop/

##mongo-haoop项目地址 https://github.com/mongodb/mongo-hadoop

##该代码托管 https://github.com/cclient/mongo_hadoop_map-reduce

原分析 由nodejs+async编写

用游标迭代查询mongo数据库，分析数据

因数据量较大，目前执行分析任务耗时4个小时，这只是极限数据量的1%

为优化，采用hadoop-mongo 方案

优点：mongo只能单机单线程（不作shard的情况），hadoop-mongo可以集群处理。

缺点：查了些资料，发现不能设计mongodb 的输入条件，猜想是因为该中间件是直接读数据文件，并根据数据文件分割数据，加查询条件则不容易分割，只能作全表分析。

    若要设定条件，有以下两种思路。

   1可以先以query,dump下来mongo表（collection）A中的数据，再restore到单独的表B中，对表B执行hadoop任务。

   2可以在mapper任务中，验证条件，符合条件才context.wirte(Writable,Writable)，不符合的跳过。

   3按1的思路拓展mongo-hadoop中间件，在map之前，添加任务，先分析出一张临时表，在reducer之后，删除该表（……时间充分再提代码）。

完成代码


近期一直写的脚本语言，再回头写点JAVA，好悲催，感觉很受限制。
 
初步代码 很粗糙
Mongo collection 数据格式

{
    "_id" : ObjectId("54d83f3548c9bc218e056ce6"),"apMac" : "aa:bb:cc:dd:ee:ff","proto" : "http",
    "url" : "extshort.weixin.qq.comhttp",
    "clientMac" : "ff:ee:dd:cc:bb:aa"
}
 

clientMac和url 先拼在一起，再按mac长度分割

数据流程 

orgin->map

map:[{"aa:bb:cc:dd:ee:ff":[ff:ee:dd:cc:bb:aaextshort.weixin.qq.comhttp]}]
 

假如是多条数据则 

map:[{"aa:bb:cc:dd:ee:ff":["ff:ee:dd:cc:bb:aaextshort.weixin.qq.comhttp","ff:ee:dd:cc:bb:aaextshort.weixin.qq.comhttp1","ff:ee:dd:cc:bb:aaextshort.weixin.qq.comhttp2"]}]
map->compine

如果有相同的client+url 则统计个数，以|分隔

compine:[{"aa:bb:cc:dd:ee:ff":[ff:ee:dd:cc:bb:aaextshort.weixin.qq.comhttp|100]}]
compine->reducer

reducer中 按mac长度分割出 clientMac url 再按“|”分割出 个数

统计前每个clientMac的前100条

reduce:

复制代码
{
    "_id": "00:21:26:00:0A:FF",
    "aa:bb:cc:1c:b9:8f": {
        "c}tieba}baidu}com|": 1,
        "short}weixin}qq}comhttp:|": 1,
        "get}sogou}com|": 1,
        "md}openapi}360}cn|": 1,
        "74}125}235}224|": 1,
        "mmbiz}qpic}cn|": 1,
        "tb}himg}baidu}com|": 1
    },
    "cc:bb:aa:d5:30:8a": {
        "captive}apple}com|": 2,
        "www}airport}us|": 1,
        "www}itools}info|": 2,
        "www}thinkdifferent}us|": 1,
        "www}ibook}info|": 1
    },
    "ee:ee:bb:78:31:74": {
        "www}itools}info|": 1,
        "www}ibook}info|": 1
    }
    
}
