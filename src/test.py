
import redis
import msgpack
import time
from threading import Thread



def lua_ws_aggregate():

    ## aggregates the synonims for all the pages passes as parameter
    lua = """

    local ind = KEYS[1]

    local t = {}
    for i =1, 200 do
        redis.call('GET', table.concat({'aaaaa', tostring(i)}, ' '))
        table.insert(t, i)
    end

    local sum = 0
    for i =1, 100 do
        table.sort(t, function(a, b) return a > b end)
        for _, w in pairs(t) do
            sum = sum + w
        end
        local t2 = cmsgpack.pack(t)
    end

    local t = {}
    t['sum'] = sum
    t['ind'] = ind
    local oo = cmsgpack.pack(t)

    return oo
    """

    return lua


def prepare():

    r = redis.Redis()
    for i in range(0, 2000):
        r.set('aaaaa {}'.format(i), i)

    fun_sha = r.script_load(lua_ws_aggregate())

    return fun_sha


def call_lua(r, fun_sha, glob, ind):

    start_time = int(round(time.time() * 1000))

    args = [ind]

    res = r.evalsha(fun_sha, 1, *args)

    res = msgpack.loads(res)
    tot_time = int(round(time.time() * 1000) - start_time)

    glob.append((ind, ">>>> {}, {} ms".format(res['sum'], tot_time)))




def test():

    print "Preparation..."
    fun_sha = prepare()
    r = redis.Redis()

    ths = []
    glob = []
    for i in range(100):
        ths.append(Thread(target=call_lua, args=(r, fun_sha, glob, i)))

    start_time = int(round(time.time() * 1000))

    for t in ths:
        t.start()
        t.join()

    tot_time = int(round(time.time() * 1000) - start_time)

    h=dict([(ind, True) for ind, _ in glob])
    for i in range(100):
        if i not in h:
            raise Exception('results are not consistent')


    print "Glob size: {} {} >> {} ms".format(len(glob), glob[10][1], tot_time)
    print "Done"


## ---

test()






