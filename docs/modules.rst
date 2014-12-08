Modular Functionality
=============================

Counterblockd allows the default set of processors and/or event handlers to be disabled or re-prioritized. Additional processors and/or event handlers can also be added. 

configuration file
----------------------
All configurations are contained in ``counterblockd_module.conf`` by default, which should be placed in to the `counterblockd data-dir`. To load a custom module specify the module path under ``[LoadModule]`` relative to the `counterblockd base-dir`. 
i.e. 
::
    [LoadModule]
    'lib/vendor' = True
    
To change the default behavior for Counterblockd modules/events, change the corresponding processor config (note: function names must be exact). 

To disable a processor.
::
    #(must be bool)
    [BlockProcessor]
    generate_wallet_stats = False
To change a processor's priority.  
::
    #(must be int) 
    [MessageProcessor]
    parse_issuance = 5
To change priority and enable. 
::
    #(tuple, order does not matter)
    [MessageProcessor]
    parse_issuance = 5, True 
    parse_issuance = True, 5

Commandline functions
-----------------------------

To enable a custom module in Counterblockd:  
::
    python counterblockd.py enmod 'lib/vendor'
To disable a loaded module:
::
    python counterblockd.py dismod 'lib/vendor' 
To list loaded modules and processors:
:: 
    python counterblockd.py listmod

Adding Custom Methods
-----------------------------------
For Adding custom methods to built in Counterblockd processors. The general syntax is:
::
    from processors import <processor_name> 
        @<processor_name>.subscribe(enabled=<bool>, priority=<int>)

If not specified, the defaults are ``enabled=true, priority=0``. When a processor is triggered methods are run in order of priority from the highest. 
::
    @<Processor>.subscribe()

``MessageProcessor`` runs once for each message as obtained from `counterpartyd`, ``msg`` will pass the message in the same format as the ``get_messages`` counterpartyd api method, msg_data corresponds to ``json.loads(msg['bindings'])``. 
::
    @MessageProcessor.subscribe(enabled=True, priority=90) 
    def custom_received_xcp_alert(msg, msg_data):
        if msg and not msg['category'] == 'sends': return
        if not msg_data['destination'] in MY_ADDRESS_LIST: return
        if not msg_data['asset'] == 'XCP': return 
        print('Received %s XCP at My Address %s from %s' %((float(msg_data['quantity'])/10**8), msg_data['destination'], msg_data['source']))
        return

``BlockProcessor`` run once per new block, after all ``MessageProcessor`` functions have completed. 
::
    @BlockProcessor.subscribe(priority=0) 
    def alertBlock(): 
        print('Finished processing messages for this block') 

A number of changing variables that a module may need to access are stored in ``config.state`` - For example if you want to run a process for every new block (but not when counterblockd is catching up). 
::
    @BlockProcessor.subscribe() 
    def my_custom_block_event(): 
        if not (config.state['last_processed_block']['block_index'] - config.state['my_latest_block']['block_index']) == 1: 
            return
        #Do stuff here
    
``StartUpProcessor`` runs once on Counterblockd startup. 
::
    @StartUpProcessor.subscribe()
    def my_db_config(): 
        config.my_db = pymongo.Connection()['my_db'] 

``CaughtUpProcessor`` runs once when Counterblockd catches up to the latest Counterpartyd block. 
::
    @CaughtUpProcessor.subscribe()
    def caughtUpAlert(): 
        print('Counterblockd is now caught up to Counterpartyd!') 

To add a method from a module to the API dispatcher: 
::
    from lib.apihandler import dispatcher
    
    #(note that the dispatcher add_method does not take arguments) 
    @dispatcher.subscribe
    def my_foo_api_method(): 
        return 'bar' 
