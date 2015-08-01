from collections import namedtuple

from pulsar.utils.string import native_str
from pulsar.utils.structures import AttributeDictionary

from .util import RedisScript, read_lua_file
from .errors import OperationalError

MIN_FLOAT = -1.e99

############################################################################
#    prefixes for data
OBJ = 'obj'     # the hash table for a instance
TMP = 'tmp'     # temporary key
ODM_SCRIPTS = ('odmrun', 'move2set', 'zdiffstore')
############################################################################


RedisKey = AttributeDictionary
instance_session_result = namedtuple('instance_session_result',
                                     'iid persistent id deleted score')
session_data = namedtuple('session_data',
                          'meta dirty deletes queries structures')
session_result = namedtuple('session_result', 'meta results')
redis_connection = namedtuple('redis_connection', 'address db')


def decode(value, encoding):
    if isinstance(value, bytes):
        return value.decode(encoding)
    else:
        return value


def pairs_to_dict(response, encoding):
    "Create a dict given a list of key/value pairs"
    it = iter(response)
    return dict(((k.decode(encoding), v) for k, v in zip(it, it)))


def script_callback(response, script=None, **options):
    if script:
        return script.callback(response, **options)
    else:
        return response


def parse_info(response):
    '''Parse the response of Redis's INFO command into a Python dict.
    In doing so, convert byte data into unicode.'''
    info = {}
    response = response.decode('utf-8')

    def get_value(value):
        if ',' and '=' not in value:
            return value
        sub_dict = {}
        for item in value.split(','):
            k, v = item.split('=')
            try:
                sub_dict[k] = int(v)
            except ValueError:
                sub_dict[k] = v
        return sub_dict
    data = info
    for line in response.splitlines():
        keyvalue = line.split(':')
        if len(keyvalue) == 2:
            key, value = keyvalue
            try:
                data[key] = int(value)
            except ValueError:
                data[key] = get_value(value)
        else:
            data = {}
            info[line[2:]] = data
    return info


def dict_update(original, data):
    target = original.copy()
    target.update(data)
    return target


#    BATTERY INCLUDED REDIS SCRIPTS
class countpattern(RedisScript):
    script = '''\
return # redis.call('keys', ARGV[1])
'''

    def preprocess_args(self, client, args):
        if args and client.prefix:
            args = tuple(('%s%s' % (client.prefix, a) for a in args))
        return args


class delpattern(countpattern):
    script = '''\
local n = 0
for i,key in ipairs(redis.call('keys', ARGV[1])) do
  n = n + redis.call('del', key)
end
return n
'''


class zpop(RedisScript):
    script = read_lua_file('commands.zpop')

    def callback(self, response, withscores=False, **options):
        if not response or not withscores:
            return response
        return zip(response[::2], map(float, response[1::2]))


class zdiffstore(RedisScript):
    script = read_lua_file('commands.zdiffstore')


class move2set(RedisScript):
    script = (read_lua_file('commands.utils'),
              read_lua_file('commands.move2set'))


class keyinfo(RedisScript):
    script = read_lua_file('commands.keyinfo')

    def preprocess_args(self, client, args):
        if args and client.prefix:
            a = ['%s%s' % (client.prefix, args[0])]
            a.extend(args[1:])
            args = tuple(a)
        return args

    def callback(self, response, redis_client=None, **options):
        client = redis_client
        if client.is_pipeline:
            client = client.client
        encoding = 'utf-8'
        all_keys = []
        for key, typ, length, ttl, enc, idle in response:
            key = key.decode(encoding)[len(client.prefix):]
            key = RedisKey(key=key, client=client,
                           type=typ.decode(encoding),
                           length=length,
                           ttl=ttl if ttl != -1 else False,
                           encoding=enc.decode(encoding),
                           idle=idle)
            all_keys.append(key)
        return all_keys


class OdmRun(RedisScript):
    script = (read_lua_file('tabletools'),
              # timeseries must be included before utils
              read_lua_file('commands.timeseries'),
              read_lua_file('commands.utils'),
              read_lua_file('odm'))
    required_scripts = ODM_SCRIPTS

    def callback(self, response, meta=None, backend=None, odm_command=None,
                 **opts):
        if odm_command == 'delete':
            res = (instance_session_result(r, False, r, True, 0)
                   for r in response)
            return session_result(meta, res)
        elif odm_command == 'commit':
            res = self._wrap_commit(response, **opts)
            return session_result(meta, res)
        elif odm_command == 'load':
            return self.load_query(response, backend, meta, **opts)
        elif odm_command == 'structure':
            return self.flush_structure(response, backend, meta, **opts)
        else:
            return response

    def _wrap_commit(self, response, iids=None, redis_client=None, **options):
        for id, iid in zip(response, iids):
            id, flag, info = id
            if int(flag):
                yield instance_session_result(iid, True, id, False,
                                              float(info))
            else:
                msg = info.decode(redis_client.encoding)
                yield OperationalError(msg)

    def load_query(self, response, backend, meta, get=None, fields=None,
                   fields_attributes=None, redis_client=None, **options):
        if get:
            tpy = meta.dfields.get(get).to_python
            return [tpy(v, backend) for v in response]
        else:
            data, related = response
            encoding = redis_client.encoding
            data = self.build(data, meta, fields, fields_attributes, encoding)
            related_fields = {}
            if related:
                for fname, rdata, fields in related:
                    fname = native_str(fname, encoding)
                    fields = tuple(native_str(f, encoding) for f in fields)
                    related_fields[fname] =\
                        self.load_related(meta, fname, rdata, fields, encoding)
            return backend.objects_from_db(meta, data, related_fields)

    def build(self, response, meta, fields, fields_attributes, encoding):
        fields = tuple(fields) if fields else None
        if fields:
            if len(fields) == 1 and fields[0] in (meta.pkname(), ''):
                for id in response:
                    yield id, (), {}
            else:
                for id, fdata in response:
                    yield id, fields, dict(zip(fields_attributes, fdata))
        else:
            for id, fdata in response:
                yield id, None, pairs_to_dict(fdata, encoding)

    def load_related(self, meta, fname, data, fields, encoding):
        '''Parse data for related objects.'''
        field = meta.dfields[fname]
        if field in meta.multifields:
            fmeta = field.structure_class()._meta
            if fmeta.name in ('hashtable', 'zset'):
                return ((native_str(id, encoding),
                         pairs_to_dict(fdata, encoding)) for
                        id, fdata in data)
            else:
                return ((native_str(id, encoding), fdata) for
                        id, fdata in data)
        else:
            # this is data for stdmodel instances
            return self.build(data, meta, fields, fields, encoding)


class check_structures(RedisScript):
    script = read_lua_file('structures')
