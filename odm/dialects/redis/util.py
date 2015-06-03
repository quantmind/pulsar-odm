import os
from hashlib import sha1


p = os.path
DEFAULT_LUA_PATH = p.join(p.dirname(p.abspath(__file__)), 'lua')


class RedisScriptMeta(type):

    def __new__(cls, name, bases, attrs):
        super_new = super(RedisScriptMeta, cls).__new__
        abstract = attrs.pop('abstract', False)
        new_class = super_new(cls, name, bases, attrs)
        if not abstract:
            o = new_class(new_class.script, new_class.__name__)
            new_class._scripts[o.name] = o
        return new_class


def read_lua_file(dotted_module, path=None, context=None):
    '''Load lua script file
    '''
    path = path or DEFAULT_LUA_PATH
    bits = dotted_module.split('.')
    bits[-1] += '.lua'
    name = os.path.join(path, *bits)
    with open(name) as f:
        data = f.read()
    if context:
        data = data.format(context)
    return data


def single_result(name, result):
    return (name, result.__class__.__name__,
            None, None, None, None, None)


class RedisScript(metaclass=RedisScriptMeta):
    '''Class which helps the sending and receiving lua scripts.

    It uses the ``evalsha`` command.

    .. attribute:: script

        The lua script to run

    .. attribute:: required_scripts

        A list/tuple of other :class:`RedisScript` names required by this
        script to properly execute.

    .. attribute:: sha1

        The SHA-1_ hexadecimal representation of :attr:`script` required by the
        ``EVALSHA`` redis command. This attribute is evaluated by the library,
        it is not set by the user.

    .. _SHA-1: http://en.wikipedia.org/wiki/SHA-1
    '''
    abstract = True
    script = None
    _scripts = {}
    required_scripts = ()

    def __init__(self, script, name):
        if isinstance(script, (list, tuple)):
            script = '\n'.join(script)
        self._name = name
        self.script = script
        rs = set((name,))
        rs.update(self.required_scripts)
        self.required_scripts = rs

    @property
    def name(self):
        return self._name

    @property
    def sha1(self):
        if not hasattr(self, '_sha1'):
            self._sha1 = sha1(self.script.encode('utf-8')).hexdigest()
        return self._sha1

    def __repr__(self):
        return self.name if self.name else self.__class__.__name__
    __str__ = __repr__

    def preprocess_args(self, client, args):
        return args

    def callback(self, response, **options):
        '''Called back after script execution.

        This is the only method user should override when writing a new
        :class:`RedisScript`. By default it returns ``response``.

        :parameter response: the response obtained from the script execution.
        :parameter options: Additional options for the callback.
        '''
        return response
