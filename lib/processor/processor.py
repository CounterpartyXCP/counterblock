from lib.apihandler import Dispatcher

class Processor(Dispatcher):
    def subscribe(self, name=None, priority=0, enabled=True):
        def inner(f): 
            default = f.__name__
            if f.__module__ not in ['lib.processor.messages', 'lib.processor.startup', 'lib.processor.caughtup', 'lib.processor.blocks' ]: default = "{0}.{1}".format(f.__module__, f.__name__)
            self.method_map[name or default] = {'function': f, 'priority': priority, 'enabled': enabled, 'name': name or default}
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
        for func in active_functions(): 
            logging.debug('starting {}'.format(func['name']))
            func['function']()
            
MessageProcessor = Processor()
BlockProcessor = Processor()
StartUpProcessor= Processor()
CaughtUpProcessor = Processor() 
