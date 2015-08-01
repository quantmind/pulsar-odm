import json

from .utils import unique_tuple
from .errors import OperationalError
from .scripts import OBJ


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
                raise OperationalError('Cannot slice a queryset in '
                                       'conjunction with get_field. '
                                       'Use load_only instead.')
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
