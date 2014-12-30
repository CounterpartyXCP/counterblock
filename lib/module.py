import os
import sys
import imp
import logging
from configobj import ConfigObj

from lib import config

def load_all():
    """Loads 3rd party plugin modules (note that this does not yet run startup processors, etc)
    """
    def load_module(module_path): 
        logging.info('Loading Plugin Module %s' % module_path)
        module_path_only = os.path.join(*module_path.split('/')[:-1])
        module_path_full = os.path.join(os.path.dirname(
            os.path.abspath(os.path.join(__file__, os.pardir))), module_path_only)
        module_name = module_path.split('/')[-1]
        f, fl, dsc = imp.find_module(module_name, [module_path_full,])
        imp.load_module(module_name, f, fl, dsc)
        logging.debug('Plugin Module Loaded %s' % module_name)
        
    def get_mod_params_dict(params):
        if not isinstance(params, list):
            params = [params] 
        params_dict = {} 
        
        try: 
            params_dict['priority'] = float(params[0])
        except:
            params_dict['enabled'] = False if "false" == params[0].lower() else True
        
        if len(params) > 1: 
            try:
                params_dict['priority'] = float(params[1]) 
            except:
                params_dict['enabled'] = False if "false" == params[1].lower() else True
        return params_dict

    #Read counterblockd_module.conf
    module_conf = ConfigObj(os.path.join(config.DATA_DIR, 'counterblockd_module.conf'))
    for key, container in module_conf.items():
        if key == 'LoadModule':
            for module, user_settings in container.items(): 
                try:
                    params = get_mod_params_dict(user_settings)
                    if params['enabled'] is True:
                        load_module(module) 
                except:
                    logging.warn("Failed to load Module %s" % module)
        elif 'Processor' in key:
            try:
                processor_functions = processor.__dict__[key]
            except: 
                logging.warn("Invalid config header %s in counterblockd_module.conf" % key)
                continue
            #print(processor_functions)
            for func_name, user_settings in container.items(): 
                #print(func_name, user_settings)
                if func_name in processor_functions:
                    params = get_mod_params_dict(user_settings)
                    #print(func_name, params)
                    for param_name, param_value in params.items(): 
                        processor_functions[func_name][param_name] = param_value
                else:
                    logging.warn("Attempted to configure a non-existent processor %s" %func_name)
            logging.debug(processor_functions)

def toggle(mod, enabled=True):
    try:
        imp.find_module(mod)
    except: 
        print("Unable to find module %s"  % mod)
        return
    mod_config_path = os.path.join(config.DATA_DIR, 'counterblockd_module.conf')
    module_conf = ConfigObj(mod_config_path)
    try:
        try:
            if module_conf['LoadModule'][mod][0] in ['True', 'False']: 
                module_conf['LoadModule'][mod][0] = enabled
            else: module_conf['LoadModule'][mod][1] = enabled
        except: module_conf['LoadModule'][mod].insert(0, enabled)
    except: 
        if not "LoadModule" in module_conf: module_conf['LoadModule'] = {}
        module_conf['LoadModule'][mod] = enabled 
    module_conf.write()
    print("%s Module %s" %("Enabled" if enabled else "Disabled", mod))
    
def list_all():
    mod_config_path = os.path.join(config.DATA_DIR, 'counterblockd_module.conf')
    module_conf = ConfigObj(mod_config_path)
    for name, modules in module_conf.items(): 
        print("Configuration for %s" %name)
        for module, settings in modules.items(): 
            print("     %s %s: %s" %(("Module" if name == "LoadModule" else "Function"), module, settings))
