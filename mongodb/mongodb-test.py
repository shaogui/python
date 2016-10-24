#!/usr/local/bin/python
# -*- coding:utf-8 -*-
import pymongo
import time
import datetime
import random
import threading
import sys
import getopt

#连接mongodb
client=pymongo.MongoClient('10.132.53.174', 27017)
#切换db库
db=client.test     #or  db = connection['local'] 

#时间记录器
def func_time(func):
        def _wrapper(*args,**kwargs):
                start = time.time()
                func(*args,**kwargs)
                print func.__name__,'run:',time.time()-start
        return _wrapper

#函数装饰
#个人理解：insert(num)函数包装到func_time(func)函数里面,也就是说func=insert(num)
#           根据 return _wrapper得出func=_wrapper,而_wrapper是一个内置函数,所以func变相的成为一个内置函数
#           由于前面已经得知func=insert(num),所以_wrapper=insert(num)
#           由于装饰器的执行顺序是由下而上,所以顺序就是：func==insert(num)=_wrapper
#           insert(num)=_wrapper
#           func=insert(num)
#           func=_wrapper

#插入数据
def insert(num,num_end):
    collection = db.test
    for x in xrange(num,num_end):
            post = {
                "_id" : str(x),
                "author": "Mike"+str(x),
                "text": "My first blog post!",
                "tags": ["mongodb", "python", "pymongo"],
                "date": datetime.datetime.utcnow()
                }
            try:
                collection.insert(post)
            except Exception,err:
                print err

#修改数据
def update(num,num_end):
    collection=db.test
    for x in xrange(num,num_end):
        rand=random.randint(num,num_end)
        collection.update({"author" : "Mike"+str(rand)},{'$set' : {"tags" : [ "mongodb"+str(rand), "python"+str(rand), "pymongo"+str(rand) ]}})
        
#查询数据
def select(num,num_end):
    collection = db.test
    for x in xrange(num,num_end): 
        rand = random.randint(num,num_end)
        collection.find({"author": "Mike"+str(rand)})

#多线程
@func_time
def xianchen(num,process,status):
    threadpool = []
    for i in xrange(process):
        th = threading.Thread(target=status,args=(i*num,i*num+num))
        threadpool.append(th)
    for th in threadpool:
        th.start()
    for th in threadpool:
        threading.Thread.join(th)

#命令行参数提示
def usage():
    print 'PyTest.py usage:'
    print '\t-h, --help: print help message.'
    print '\t-v, --version: print script version'
    print '\t-s, --status: insert,update,select'
    print '\t-n, --num: 条数'
    print '\t-p, --process: thread num'

def version():
    print 'mongodb stress test 1.0'

#命令行参数接收
def main():
    status_ = 'none'
    status = eval('insert')
    num = 0
    process = 0
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hvs:n:p:",["help","version","status=","num=","process="])
    except Exception,err:
        print str(err)
        sys.exit(2)
    for o, a in opts:
        if o in ("-h", "--help"):
            usage()
            sys.exit(1)
        elif o in ("-v", "--version"):
            version()
            sys.exit(0)
        elif o in ("-s", "--status"):
            status = eval(a)
            status_ = a
        elif o in ("-n", "--num"):
            num = a
        elif o in ("-p", "--process"):
            process = a
    print "status:",status_,"\tnum:",num,"\tprocess:",process
    if status_ == 'none':
        print "status is default\nno run sql"
    else:
        xianchen(int(num),int(process),status)
    
    
if __name__ == '__main__':
    main()
        
    
