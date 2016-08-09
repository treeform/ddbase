import mmap
import os
import json
from gevent import socket
from gevent.server import StreamServer
import time


SCALE = 512


class DDBase:
    """ main ddbase class """

    def __init__(self, file_name = "ddbase.ddb"):
        """ creats a ddbase instnace """
        self.file_name = file_name
        self.table_file_name = file_name + ".table"
        if not os.path.exists(file_name):
            self.create()
        # read table defintion file
        self.start_mmap()
        self.read_table()
        #self.check_vaccum()
        self.compute_free()

    def start_mmap(self):
        # set up big mmemory mapped file
        self.ddb_file = open(self.file_name, "r+b")
        self.mmap = mmap.mmap(self.ddb_file.fileno(), 0)
        # set up table file for append
        self.table_file = open(self.table_file_name, "a")

    def compute_free(self):
        self.free = ["."] * (self.mmap.size() / SCALE)
        for record in self.table.itervalues():
            self.set_free(record, "#")

    def set_free(self, record, char='#'):
        if record['size'] == 0:
            return
        start = record['offset'] / SCALE
        end = (record['offset'] + record['size']) / SCALE
        if end == len(self.free): end -= 1
        for i in range(start, end + 1):
            self.free[i] = char

    def have_free(self, size):
        size = size / SCALE
        #print "free", size
        for x in xrange(len(self.free)-size):
            if self.free[x] == ".":
                for y in xrange(1, size+1):
                    #print " check", x, y, free[x+y]
                    if self.free[x+y] != ".":
                        break
                else:
                    return x*SCALE
        return None

    def read_table(self):
        """ read table defintions """
        self.table = {}
        with open(self.table_file_name , "r") as table_file:
            for line in table_file:
                record = json.loads(line)
                key = record['key']
                self.table[key] = record
        print "keys", len(self.table)

    def check_vaccum(self):
        """ check if we need to vaccum """
        # if deleted space is more then twise of the current size
        if self.free.count(".") > self.free.count("#")*.50:
            print "vaccuming"
            self.vaccum()
            self.compute_free()

    def vaccum(self):
        """ creates a new file without deleted or empty entries """
        start = time.time()
        new_table = {}
        new_ddb_file = open(self.file_name + ".new", 'w')
        new_table_file = open(self.table_file_name + ".new", 'w')
        new_ddb_file.write(chr(0)*SCALE)
        offset = SCALE
        for record in self.table.itervalues():
            if record['size'] == 0:
                continue
            key = record['key']
            new_record = dict(record)
            new_table[key] = new_record
            new_record['offset'] = offset
            value = self.get(key)
            new_ddb_file.write(value)
            offset += len(value)
            slak = SCALE - offset % SCALE
            if slak:
                new_ddb_file.write(chr(0)*slak)
                offset += slak
            new_table_file.write(json.dumps(new_record)+'\n')
        self.close()
        new_ddb_file.close()
        new_table_file.close()
        os.rename(self.file_name + ".new", self.file_name)
        os.rename(self.table_file_name + ".new", self.table_file_name)
        self.start_mmap()
        self.table = new_table
        self.compute_free()
        print "vaccumed in", time.time() - start, "seconds"

    def create(self):
        """ creates new fresh db files """
        with open(self.file_name , "wb") as f:
            f.write(chr(0)*SCALE)
        with open(self.table_file_name, "wb") as f:
            pass

    def set(self, key, value):
        """ saves a key """
        bytes = value.encode('latin-1')
        size = len(bytes)

        if key in self.table:
            record = self.table[key]
            self.set_free(record, '.')
            record['size'] = 0
        else:
            record = {
                'key': key,
                'offset': 0,
                'size': 0
            }
            self.table[key] = record

        offset = self.have_free(size)
        offset = None
        if offset is None:
            offset = self.expand(size)
        record['offset'] = offset
        record['size'] = size
        self.mmap[offset:offset+size] = bytes
        self.table_file.write(json.dumps(record)+'\n')
        self.flush()
        self.set_free(record, '#')
        self.check_vaccum()

    def expand(self, size):
        bsize = int(size/SCALE + 1)
        size = bsize * SCALE
        mmap_size = self.mmap.size()
        self.mmap.resize(mmap_size + size)
        self.free.extend(["."]*bsize )
        return mmap_size

    def get(self, key):
        """ gets a key """
        if key not in self.table:
            return ""
        record = self.table[key]
        offset = record['offset']
        size = record['size']
        return self.mmap[offset : offset + size]

    def flush(self):
        """ flushes every thing to disk """
        self.mmap.flush()
        self.table_file.flush()

    def close(self):
        """ closes all files safely """
        self.mmap.close()
        self.ddb_file.close()
        self.table_file.close()


def main(host='', port=12000):
    ddb = DDBase()

    def tcp_api(sock, address):
        try:
            fp = sock.makefile()
            while True:
                line = fp.readline()
                if line:
                    #print line
                    cmd = json.loads(line)
                    key = cmd['key']
                    if cmd['cmd'] == 'set':
                        value = fp.read(cmd['size'])
                        ddb.set(key, value)
                    elif cmd['cmd'] == 'get':
                        value = ddb.get(key)
                        record = {'key':key, 'size':len(value)}
                        fp.write(json.dumps(record)+"\n")
                        fp.write(value)
                    fp.flush()
                else:
                    break
            sock.shutdown(socket.SHUT_WR)
            sock.close()
        except socket.error, e:
            print e
            return

    server = StreamServer((host, port), tcp_api)
    try:
        server.serve_forever()
    except:
        ddb.close()
        print "ddb saved"


if __name__ == "__main__":
    main()
