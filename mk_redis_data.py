import struct
import itertools
import random
import uuid
import sys
import redis

num_keys = int(sys.argv[1])

r = redis.Redis()


buf = struct.pack('4096B', *itertools.islice(itertools.cycle(range(97, 115)), 4096))

keys = []

n = 0
pipe = r.pipeline()
for i in range(num_keys):
    key = uuid.uuid4().bytes
    keys.append(key)

    pipe.set(key, buf[0:random.randrange(1024, 4096)])

    n += 1
    if n == 1024:
        pipe.execute()
        pipe = r.pipeline()
        n = 0

pipe.execute()


with open('redis.toc', 'w') as toc:
    n = 0
    chunk = []
    for key in keys:
        chunk.append(key + '\x00\x00\x00\x00')

        if n == 1024:
            toc.write(''.join(chunk))
            chunk = []

    toc.write(''.join(chunk))
