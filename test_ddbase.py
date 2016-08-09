from ddbase import DDBase, main
import os
import time
import random
import string
import socket
import json


def randstr(n, seed=None):
    if seed:
        random.seed(seed)
    size = random.randint(0, n)
    return ''.join(random.choice(string.ascii_uppercase) for x in xrange(size))

def test1():
    n = 1024
    n = 100
    # delete everything
    if os.path.exists("ddbase.ddb"):
        os.remove("ddbase.ddb")
    if os.path.exists("ddbase.ddb.table"):
        os.remove("ddbase.ddb.table")
    # create a big key
    big = randstr(1024*n)
    s1 = time.time()
    # start new db
    ddb = DDBase()
    for i in range(n):
        k = "test%i" % i
        v = big
        ddb.set(k, v)
    ddb.close()
    # start new instace and check
    s2 = time.time()
    ddb = DDBase()
    for i in range(n):
        k = "test%i" % i
        v = big
        assert ddb.get(k) == v
    ddb.close()
    s3 = time.time()
    print s3-s1, "write"
    print s3-s2, "read"


def test2():
    n = 1000
    big = randstr(1024)
    # tcp api
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(('localhost', 12000))

    t = {}

    for i in range(n):
        k = "test%i" % i
        #print k
        v = randstr(8000)
        t[k] = v
        #print "    ", v
        cmd = {'cmd': 'set', 'key': k, 'size': len(v)}
        s.sendall(json.dumps(cmd)+"\n")
        s.sendall(v)
    s.close()


    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(('localhost', 12000))
    f = s.makefile()
    for i in range(n):
        k = "test%i" % i
        #print "get", k
        cmd = {'cmd': 'get', 'key': k}
        f.write(json.dumps(cmd)+"\n")
        f.flush()
        record = json.loads(f.readline())
        value = f.read(record['size'])
        assert record['key'] == k
        #print "    ", len(value)
        #print "    ", len(t[k])
        assert value == t[k]


def test3():
    # tcp api
    t = {}

    while True:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(('localhost', 12000))
        f = s.makefile()

        # write random
        k = "test%i" % random.randint(0, 1000)
        v = "*" * random.randint(0, 16000)
        if random.randint(0, 10) == 1:
            v = "*" * random.randint(0, 1600000)
        #v = randstr(1024*64)
        #v = randstr(80)

        t[k] = v
        cmd = {'cmd': 'set', 'key': k, 'size': len(v)}
        f.write(json.dumps(cmd)+"\n")
        f.write(v)
        f.flush()

        if not t:
            continue

        #for k,v in t.iteritems():
        for i in range(1):
            # read random
            k = random.choice(t.keys())
            cmd = {'cmd': 'get', 'key': k}
            f.write(json.dumps(cmd)+"\n")
            f.flush()
            record = json.loads(f.readline())
            value = f.read(record['size'])
            #print k
            #print "    ", t[k]
            #print "    ", value
            assert record['key'] == k
            assert value == t[k]

        s.close()


#test1()
#test2()
test3()
