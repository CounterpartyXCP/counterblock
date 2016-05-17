import logging
import collections
import gevent.pool
import gevent.util

CORE_FIRST_PRIORITY = 65535  # arbitrary, must be > 1000, as custom plugins utilize the range of <= 1000
CORE_LAST_PRIORITY = -1  # arbitrary, must be < 0


class GreenletGroupWithExceptionCatching(gevent.pool.Group):
    """See https://gist.github.com/progrium/956006"""

    def __init__(self, *args):
        super(GreenletGroupWithExceptionCatching, self).__init__(*args)
        self._error_handlers = {}

    def _wrap_errors(self, func):
        """Wrap a callable for triggering error handlers

        This is used by the greenlet spawn methods so you can handle known
        exception cases instead of gevent's default behavior of just printing
        a stack trace for exceptions running in parallel greenlets.

        """
        def wrapped_f(*args, **kwargs):
            exceptions = tuple(self._error_handlers.keys())
            try:
                return func(*args, **kwargs)
            except exceptions as exception:
                for type in self._error_handlers:
                    if isinstance(exception, type):
                        handler, greenlet = self._error_handlers[type]
                        self._wrap_errors(handler)(exception, greenlet)
                return exception
        return wrapped_f

    def catch(self, type, handler):
        """Set an error handler for exceptions of `type` raised in greenlets"""
        self._error_handlers[type] = (handler, gevent.getcurrent())

    def spawn(self, func, *args, **kwargs):
        parent = super(GreenletGroupWithExceptionCatching, self)
        func_wrap = self._wrap_errors(func)
        return parent.spawn(func_wrap, *args, **kwargs)

    def spawn_later(self, seconds, func, *args, **kwargs):
        parent = super(GreenletGroupWithExceptionCatching, self)
        func_wrap = self._wrap_errors(func)
        # spawn_later doesn't exist in pool.Group, so let's implement it below
        greenlet = parent.greenlet_class(func_wrap, *args, **kwargs)
        parent.add(greenlet)
        greenlet.start_later(seconds)
        return greenlet


def start_task(func, delay=None):
    def raise_in_handling_greenlet(error, greenlet):
        greenlet.throw(error)
    group = GreenletGroupWithExceptionCatching()
    # robbyd: comment this out for now, for better tracebacks
    #group.catch(Exception, raise_in_handling_greenlet)
    group.spawn(func) if not delay else group.spawn_later(delay, func)
    return group


class Dispatcher(collections.MutableMapping):
    """ API Method dispatcher.

    Dictionary like object which holds map method_name to method.

    """

    def __init__(self, prototype=None):
        """ Build method dispatcher.

        :param prototype: Initial method mapping.
        :type prototype: None or object or dict

        """
        self.method_map = dict()

        if prototype is not None:
            self.build_method_map(prototype)

    def __getitem__(self, key):
        return self.method_map[key]

    def __setitem__(self, key, value):
        self.method_map[key] = value

    def __delitem__(self, key):
        del self.method_map[key]

    def __len__(self):
        return len(self.method_map)

    def __iter__(self):
        return iter(self.method_map)

    def __repr__(self):
        return repr(self.method_map)

    def add_method(self, f, name=None):
        """ Add a method to the dispatcher.

        :param callable f: Callable to be added.
        :param name: Name to register
        :type name: None or str

        When used as a decorator keep callable object unmodified.
        """
        self.method_map[name or f.__name__] = f
        return f

    def build_method_map(self, prototype):
        """ Add prototype methods to the dispatcher.

        :param prototype: Method mapping.
        :type prototype: None or object or dict

        If given prototype is a dictionary then all callable objects
        will be added to dispatcher.  If given prototype is an object
        then all public methods will be used.

        """
        if not isinstance(prototype, dict):
            prototype = dict((method, getattr(prototype, method))
                             for method in dir(prototype)
                             if not method.startswith('_'))

        for attr, method in list(prototype.items()):
            if isinstance(method, collections.Callable):
                self[attr] = method


class Processor(Dispatcher):
    logger = logging.getLogger(__name__)

    def __init__(self, prototype=None):
        self.active_functions_data = None
        super(Processor, self).__init__(prototype=prototype)

    def subscribe(self, name=None, priority=0, enabled=True):
        self.active_functions_data = None  # needs refresh

        def inner(f):
            default = f.__name__
            if(f.__module__ not in [
               'lib.processor.messages', 'lib.processor.startup',
               'lib.processor.caughtup', 'lib.processor.blocks']):
                default = "{0}.{1}".format(f.__module__, f.__name__)
            self.method_map[name or default] = {
                'function': f, 'priority': priority, 'enabled': enabled, 'name': name or default}
            return f
        return inner

    def add_method(*args, **kwargs):
        return subscribe(*args, **kwargs)

    def __repr__(self):
        return str(self.method_map)

    def active_functions(self):
        if not self.active_functions_data:
            self.active_functions_data = sorted((func for func in list(self.values()) if func['enabled']), key=lambda x: x['priority'], reverse=True)
        return self.active_functions_data

    def run_active_functions(self, *args, **kwargs):
        for func in self.active_functions():
            self.logger.debug('starting {}'.format(func['name']))
            func['function'](*args, **kwargs)

MessageProcessor = Processor()
MempoolMessageProcessor = Processor()
BlockProcessor = Processor()
StartUpProcessor = Processor()
CaughtUpProcessor = Processor()
RollbackProcessor = Processor()
API = Dispatcher()
