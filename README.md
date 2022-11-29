# Parse Redis AOF

This script will parse a [Redis append only file](https://redis.io/docs/management/persistence/) and print out each command on a separate line.

```
$ ./parse-redis-aof.py appendonly.aof
SELECT  1
MULTI
incrby  stat:processed  0
...
```

**NOTE:** Only tested with an AOF file from Redis 5.x. May not work correctly with Redis 7's multi-part AOF mechanism.
