import logging
import collections

CORE_FIRST_PRIORITY = 65535 #arbitrary, must be > 1000, as custom plugins utilize the range of <= 1000
CORE_LAST_PRIORITY = -1 #arbitrary, must be < 0

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

        for attr, method in prototype.items():
            if callable(method):
                self[attr] = method


class Processor(Dispatcher):
    def subscribe(self, name=None, priority=0, enabled=True):
        def inner(f): 
            default = f.__name__
            if f.__module__ not in ['lib.processor.messages', 'lib.processor.startup',
                'lib.processor.caughtup', 'lib.processor.blocks' ]: default = "{0}.{1}".format(f.__module__, f.__name__)
            self.method_map[name or default] = {
                'function': f, 'priority': priority, 'enabled': enabled, 'name': name or default}
            return f
        return inner
    
    def add_method(*args, **kwargs): 
        return subscribe(*args, **kwargs)
        
    def __repr__(self):
        return str(self.method_map)
    
    #use iteritems instead ? 
    def active_functions(self): 
        return sorted((func for func in self.values() if func['enabled']), key=lambda x: x['priority'], reverse=True)
    
    def run_active_functions(self): 
        for func in self.active_functions(): 
            logging.debug('starting {}'.format(func['name']))
            func['function']()
            
MessageProcessor = Processor()
BlockProcessor = Processor()
StartUpProcessor= Processor()
CaughtUpProcessor = Processor()                
API = Dispatcher() 