import json
from functools import partial

from .util import RedisScript, read_lua_file


class RedisQuery:
    card = None
    _meta_info = None
    script_dep = {'script_dependency': ('build_query', 'move2set')}

    def zism(self, r):
        return r is not None

    def sism(self, r):
        return r

    @property
    def meta_info(self):
        if self._meta_info is None:
            self._meta_info = json.dumps(self.backend.meta(self.meta))
        return self._meta_info

    def _build(self, pipe=None, **kwargs):
        # Accumulate a query
        if pipe is None:
            pipe = self.backend.client.pipeline()
        self.pipe = pipe
        qs = self.queryelem
        backend = self.backend
        key, meta, keys, args = None, self.meta, [], []
        pkname = meta.pkname()
        for child in qs:
            if getattr(child, 'backend', None) == backend:
                lookup, value = 'set', child
            else:
                lookup, value = child
            if lookup == 'set':
                be = value.backend_query(pipe=pipe)
                keys.append(be.query_key)
                args.extend(('set', be.query_key))
            else:
                if isinstance(value, tuple):
                    value = self.dump_nested(*value)
                args.extend((lookup, '' if value is None else value))
        temp_key = True
        if qs.keyword == 'set':
            if qs.name == pkname and not args:
                key = backend.basekey(meta, 'id')
                temp_key = False
            else:
                key = backend.tempkey(meta)
                keys.insert(0, key)
                backend.odmrun(pipe, 'query', meta, keys, self.meta_info,
                               qs.name, *args)
        else:
            key = backend.tempkey(meta)
            p = 'z' if meta.ordering else 's'
            pipe.execute_script('move2set', keys, p)
            if qs.keyword == 'intersect':
                command = getattr(pipe, p+'interstore')
            elif qs.keyword == 'union':
                command = getattr(pipe, p+'unionstore')
            elif qs.keyword == 'diff':
                command = getattr(pipe, p+'diffstore')
            else:
                raise ValueError('Could not perform %s operation' % qs.keyword)
            command(key, keys)
        where = self.queryelem.data.get('where')
        # where query
        if where:
            # First key is the current key
            keys.insert(0, key)
            if not temp_key:
                temp_key = True
                key = backend.tempkey(meta)
            # Second key is the destination key (which can be the current
            # key if it is temporary key)
            keys.insert(0, key)
            backend.where_run(pipe, self.meta_info, keys, *where)
        #
        # If we are getting a field (for a subsequent query maybe)
        # unwind the query and store the result
        gf = qs._get_field
        if gf and gf != pkname:
            field_attribute = meta.dfields[gf].attname
            bkey = key
            if not temp_key:
                temp_key = True
                key = backend.tempkey(meta)
            okey = backend.basekey(meta, OBJ, '*->' + field_attribute)
            pipe.sort(bkey, by='nosort', get=okey, store=key)
            self.card = getattr(pipe, 'llen')
        if temp_key:
            pipe.expire(key, self.expire)
        self.query_key = key

    def _execute_query(self):
        '''Execute the query without fetching data.

        Returns the number of elements in the query.
        '''
        pipe = self.pipe
        if not self.card:
            if self.meta.ordering:
                self.ismember = getattr(self.backend.client, 'zrank')
                self.card = getattr(pipe, 'zcard')
                self._check_member = self.zism
            else:
                self.ismember = getattr(self.backend.client, 'sismember')
                self.card = getattr(pipe, 'scard')
                self._check_member = self.sism
        else:
            self.ismember = None
        self.card(self.query_key)
        result = yield pipe.execute()
        yield result[-1]

    def order(self, last):
        '''Perform ordering with respect model fields.'''
        desc = last.desc
        field = last.name
        nested = last.nested
        nested_args = []
        while nested:
            meta = nested.model._meta
            nested_args.extend((self.backend.basekey(meta), nested.name))
            last = nested
            nested = nested.nested
        method = 'ALPHA' if last.field.internal_type == 'text' else ''
        if field == last.model._meta.pkname():
            field = ''
        return {'field': field,
                'method': method,
                'desc': desc,
                'nested': nested_args}

    def dump_nested(self, value, nested):
        nested_args = []
        if nested:
            for name, meta in nested:
                if meta:
                    meta = self.backend.basekey(meta)
                nested_args.extend((name, meta))
        return json.dumps((value, nested_args))

    def _has(self, val):
        r = self.ismember(self.query_key, val)
        return self._check_member(r)

    def get_redis_slice(self, slic):
        if slic:
            start = slic.start or 0
            stop = slic.stop
        else:
            start = 0
            stop = None
        return start, stop

    def _items(self, slic):
        # Unwind the database query by creating a list of arguments for
        # the load_query lua script
        backend = self.backend
        meta = self.meta
        name = ''
        order = ()
        start, stop = self.get_redis_slice(slic)
        if self.queryelem.ordering:
            order = self.order(self.queryelem.ordering)
        elif meta.ordering:
            name = 'DESC' if meta.ordering.desc else 'ASC'
        elif start or stop is not None:
            order = self.order(meta.get_sorting(meta.pkname()))
        # Wen using the sort algorithm redis requires the number of element
        # not the stop index
        if order:
            name = 'explicit'
            N = self.execute_query()
            if stop is None:
                stop = N
            elif stop < 0:
                stop += N
            if start < 0:
                start += N
            stop -= start
        elif stop is None:
            stop = -1
        get = self.queryelem._get_field
        fields_attributes = None
        pkname_tuple = (meta.pk.name,)
        # if the get_field is available, we only load that field
        if get:
            if slic:
                raise QuerySetError('Cannot slice a queryset in conjunction '
                                    'with get_field. Use load_only instead.')
            if get == meta.pk.name:
                fields_attributes = fields = pkname_tuple
            else:
                fields, fields_attributes = meta.backend_fields((get,))
        else:
            fields = self.queryelem.fields or None
            if fields:
                fields = unique_tuple(fields,
                                      self.queryelem.select_related or ())
            if fields == pkname_tuple:
                fields_attributes = fields
            elif fields:
                fields, fields_attributes = meta.backend_fields(fields)
            else:
                fields_attributes = ()
        options = {'ordering': name,
                   'order': order,
                   'start': start,
                   'stop': stop,
                   'fields': fields_attributes,
                   'related': dict(self.related_lua_args()),
                   'get': get}
        joptions = json.dumps(options)
        options.update({'fields': fields,
                        'fields_attributes': fields_attributes})
        return backend.odmrun(backend.client, 'load', meta, (self.query_key,),
                              self.meta_info, joptions, **options)

    def related_lua_args(self):
        '''Generator of load_related arguments'''
        related = self.queryelem.select_related
        if related:
            meta = self.meta
            for rel in related:
                field = meta.dfields[rel]
                relmodel = field.relmodel
                bk = self.backend.basekey(relmodel._meta) if relmodel else ''
                fields = list(related[rel])
                if meta.pkname() in fields:
                    fields.remove(meta.pkname())
                    if not fields:
                        fields.append('')
                ftype = field.type if field in meta.multifields else ''
                data = {'field': field.attname, 'type': ftype,
                        'bk': bk, 'fields': fields}
                yield field.name, data


class RedisStructure(BackendStructure):

    def __init__(self, *args, **kwargs):
        super(RedisStructure, self).__init__(*args, **kwargs)
        instance = self.instance
        field = instance.field
        if field:
            model = field.model
            if instance._pkvalue:
                id = self.backend.basekey(model._meta, 'obj',
                                          instance._pkvalue, field.name)
            else:
                id = self.backend.basekey(model._meta, 'struct', field.name)
        else:
            id = '%s.%s' % (instance._meta.name, instance.id)
        self.id = id

    @property
    def is_pipeline(self):
        return self.client.is_pipeline

    def delete(self):
        return self.client.delete(self.id)


class String(RedisStructure):

    def flush(self):
        cache = self.instance.cache
        result = None
        data = cache.getvalue()
        if data:
            self.client.append(self.id, data)
            result = True
        return result

    def size(self):
        return self.client.strlen(self.id)

    def incr(self, num=1):
        return self.client.incr(self.id, num)


class Set(RedisStructure):

    def flush(self):
        cache = self.instance.cache
        result = None
        if cache.toadd:
            self.client.sadd(self.id, *cache.toadd)
            result = True
        if cache.toremove:
            self.client.srem(self.id, *cache.toremove)
            result = True
        return result

    def size(self):
        return self.client.scard(self.id)

    def items(self):
        return self.client.smembers(self.id)


class Zset(RedisStructure):
    '''Redis ordered set structure'''
    def flush(self):
        cache = self.instance.cache
        result = None
        if cache.toadd:
            flat = cache.toadd.flat()
            self.client.zadd(self.id, *flat)
            result = True
        if cache.toremove:
            flat = tuple((el[1] for el in cache.toremove))
            self.client.zrem(self.id, *flat)
            result = True
        return result

    def get(self, score):
        r = self.range(score, score, withscores=False)
        if r:
            if len(r) > 1:
                return r
            else:
                return r[0]

    def items(self):
        return self.irange(withscores=True)

    def values(self):
        return self.irange(withscores=False)

    def size(self):
        return self.client.zcard(self.id)

    def rank(self, value):
        return self.client.zrank(self.id, value)

    def count(self, start, stop):
        return self.client.zcount(self.id, start, stop)

    def range(self, start, end, withscores=True, **options):
        return self.backend.execute(
            self.client.zrangebyscore(self.id, start, end,
                                      withscores=withscores, **options),
            partial(self._range, withscores))

    def irange(self, start=0, stop=-1, desc=False, withscores=True, **options):
        return self.backend.execute(
            self.client.zrange(self.id, start, stop, desc=desc,
                               withscores=withscores, **options),
            partial(self._range, withscores))

    def ipop_range(self, start, stop=None, withscores=True, **options):
        '''Remove and return a range from the ordered set by rank (index).'''
        return self.backend.execute(
            self.client.zpopbyrank(self.id, start, stop,
                                   withscores=withscores, **options),
            partial(self._range, withscores))

    def pop_range(self, start, stop=None, withscores=True, **options):
        '''Remove and return a range from the ordered set by score.'''
        return self.backend.execute(
            self.client.zpopbyscore(self.id, start, stop,
                                    withscores=withscores, **options),
            partial(self._range, withscores))

    # PRIVATE
    def _range(self, withscores, result):
        if withscores:
            return [(score, v) for v, score in result]
        else:
            return result


class List(RedisStructure):

    def pop_front(self):
        return self.client.lpop(self.id)

    def pop_back(self):
        return self.client.rpop(self.id)

    def block_pop_front(self, timeout):
        value = yield self.client.blpop(self.id, timeout)
        if value:
            yield value[1]

    def block_pop_back(self, timeout):
        value = yield self.client.brpop(self.id, timeout)
        if value:
            yield value[1]

    def flush(self):
        cache = self.instance.cache
        result = None
        if cache.front:
            self.client.lpush(self.id, *cache.front)
            result = True
        if cache.back:
            self.client.rpush(self.id, *cache.back)
            result = True
        return result

    def size(self):
        return self.client.llen(self.id)

    def range(self, start=0, end=-1):
        return self.client.lrange(self.id, start, end)


class Hash(RedisStructure):

    def flush(self):
        cache = self.instance.cache
        result = None
        if cache.toadd:
            self.client.hmset(self.id, cache.toadd)
            result = True
        if cache.toremove:
            self.client.hdel(self.id, *cache.toremove)
            result = True
        return result

    def size(self):
        return self.client.hlen(self.id)

    def get(self, key):
        return self.client.hget(self.id, key)

    def pop(self, key):
        pi = self.is_pipeline
        p = self.client if pi else self.client.pipeline()
        p.hget(self.id, key).hdel(self.id, key)
        if not pi:
            result = yield p.execute()
            yield result[0]

    def remove(self, *fields):
        return self.client.hdel(self.id, *fields)

    def __contains__(self, key):
        return self.client.hexists(self.id, key)

    def keys(self):
        return self.client.hkeys(self.id)

    def values(self):
        return self.client.hvals(self.id)

    def items(self):
        return self.client.hgetall(self.id)


class TS(RedisStructure):
    '''Redis timeseries implementation is based on the ts.lua script'''
    def flush(self):
        cache = self.instance.cache
        result = None
        if cache.toadd:
            result = self.client.execute_script('ts_commands', (self.id,),
                                                'add', *cache.toadd.flat())
        if cache.toremove:
            raise NotImplementedError('Cannot remove. TSDEL not implemented')
        return result

    def __contains__(self, timestamp):
        return self.client.execute_script('ts_commands', (self.id,), 'exists',
                                          timestamp)

    def size(self):
        return self.client.execute_script('ts_commands', (self.id,), 'size')

    def count(self, start, stop):
        return self.client.execute_script('ts_commands', (self.id,), 'count',
                                          start, stop)

    def times(self, time_start, time_stop, **kwargs):
        return self.client.execute_script('ts_commands', (self.id,), 'times',
                                          time_start, time_stop, **kwargs)

    def itimes(self, start=0, stop=-1, **kwargs):
        return self.client.execute_script('ts_commands', (self.id,), 'itimes',
                                          start, stop, **kwargs)

    def get(self, dte):
        return self.client.execute_script('ts_commands', (self.id,),
                                          'get', dte)

    def rank(self, dte):
        return self.client.execute_script('ts_commands', (self.id,),
                                          'rank', dte)

    def pop(self, dte):
        return self.client.execute_script('ts_commands', (self.id,),
                                          'pop', dte)

    def ipop(self, index):
        return self.client.execute_script('ts_commands', (self.id,),
                                          'ipop', index)

    def range(self, time_start, time_stop, **kwargs):
        return self.client.execute_script('ts_commands', (self.id,), 'range',
                                          time_start, time_stop, **kwargs)

    def irange(self, start=0, stop=-1, **kwargs):
        return self.client.execute_script('ts_commands', (self.id,), 'irange',
                                          start, stop, **kwargs)

    def pop_range(self, time_start, time_stop, **kwargs):
        return self.client.execute_script('ts_commands', (self.id,),
                                          'pop_range',
                                          time_start, time_stop, **kwargs)

    def ipop_range(self, start=0, stop=-1, **kwargs):
        return self.client.execute_script('ts_commands', (self.id,),
                                          'ipop_range', start, stop, **kwargs)


class NumberArray(RedisStructure):

    def flush(self):
        cache = self.instance.cache
        result = None
        if cache.back:
            self.client.execute_script('numberarray_pushback', (self.id,),
                                       *cache.back)
            result = True
        return result

    def get(self, index):
        return self.client.execute_script('numberarray_getset', (self.id,),
                                          'get', index+1)

    def set(self, value):
        return self.client.execute_script('numberarray_getset', (self.id,),
                                          'set', index+1, value)

    def range(self):
        return self.client.execute_script('numberarray_all_raw', (self.id,),)

    def resize(self, size, value=None):
        if value is not None:
            argv = (size, value)
        else:
            argv = (size,)
        return self.client.execute_script('numberarray_resize', (self.id,),
                                          *argv)

    def size(self):
        return self.client.strlen(self.id)//8


class ts_commands(RedisScript):
    script = (read_lua_file('commands.timeseries'),
              read_lua_file('tabletools'),
              read_lua_file('ts'))


class numberarray_resize(RedisScript):
    script = (read_lua_file('numberarray'),
              '''return array:new(KEYS[1]):resize(unpack(ARGV))''')


class numberarray_all_raw(RedisScript):
    script = (read_lua_file('numberarray'),
              '''return array:new(KEYS[1]):all_raw()''')


class numberarray_getset(RedisScript):
    script = (read_lua_file('numberarray'),
              '''local a = array:new(KEYS[1])
if ARGV[1] == 'get' then
    return a:get(ARGV[2],true)
else
    a:set(ARGV[2],ARGV[3],true)
end''')


class numberarray_pushback(RedisScript):
    script = (read_lua_file('numberarray'),
              '''local a = array:new(KEYS[1])
for _,v in ipairs(ARGV) do
    a:push_back(v,true)
end''')
