counterblockd Plugin Module Functionality
=============================================

``counterblockd`` allows the default set of processors and/or event handlers to be disabled or re-prioritized.
Additional processors and/or event handlers can also be added. This allows developers to easily extend the
capabilities of ``counterblockd``, as well as deactivate unused portions of the system as necessary.

configuration file
----------------------
All configurations are contained in ``counterblockd_module.conf`` by default, which should be placed in to the
`counterblockd data-dir`. To load a custom module specify the module path under ``[LoadModule]`` relative to
the `counterblockd base-dir`. i.e.
::
    [LoadModule]
    'lib/vendor' = True
    
To change the default behavior for ``counterblockd`` modules/events, change the corresponding processor config.
(Note: function names must be exact.) 

To disable a processor:
::
    #(must be bool)
    [BlockProcessor]
    generate_wallet_stats = False

To change a processor's priority:
::
    #(must be int) 
    [MessageProcessor]
    parse_issuance = 5
    
To change priority and enable:
::
    #(tuple, order does not matter)
    [MessageProcessor]
    parse_issuance = 5, True 
    parse_issuance = True, 5

Command-line functions
-----------------------------

To enable a custom module in ``counterblockd``:
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
For Adding custom methods to built in ``counterblockd`` processors. The general syntax is:

.. code-block:: python
    from lib.processor import <processor_name> 
        @<processor_name>.subscribe(enabled=<bool>, priority=<int>)

If not specified, the defaults are ``enabled=true, priority=0``.
When a processor is triggered methods are run in order of priority from the highest.
*Please note that any priority less than ``0`` or greater than ``1000`` is reserved for internal ``counterblock``
functionality, and custom plugins should only utilize priority settings under this number.*

.. code-block:: python
    @<Processor>.subscribe()

``MessageProcessor`` runs once for each message as obtained from `counterpartyd`, ``msg`` will pass the message
 in the same format as the ``get_messages`` counterpartyd api method, msg_data corresponds to ``json.loads(msg['bindings'])``. 

.. code-block:: python
    @MessageProcessor.subscribe(enabled=True, priority=90) 
    def custom_received_xcp_alert(msg, msg_data):
        if msg['category'] != 'sends': return
        if message['status'] != 'valid': return
        if not msg_data['destination'] in MY_ADDRESS_LIST: return
        if not msg_data['asset'] == 'XCP': return 
        print('Received %s XCP at My Address %s from %s' %(
        	(float(msg_data['quantity'])/10**8), msg_data['destination'], msg_data['source']))
        return

Note that with ``MessageProcessor`` handlers, you can return ``'continue'`` to prevent the running of further MessageProcessors (i.e.
of lesser priority than the current one) for the message being currently processed.

``BlockProcessor`` run once per new block, after all ``MessageProcessor`` functions have completed. 

.. code-block:: python
    @BlockProcessor.subscribe(priority=0) 
    def alertBlock(): 
        print('Finished processing messages for this block') 

A number of changing variables that a module may need to access are stored in ``config.state`` - For example if you
want to run a process for every new block (but not when counterblockd is catching up). 

.. code-block:: python
    @BlockProcessor.subscribe() 
    def my_custom_block_event(): 
        if not (config.state['cpd_latest_block']['block_index'] - config.state['my_latest_block']['block_index']) == 1: 
            return
        #Do stuff here
    
``StartUpProcessor`` runs once on ``counterblockd`` startup. 

.. code-block:: python
    @StartUpProcessor.subscribe()
    def my_db_config(): 
        config.my_db = pymongo.Connection()['my_db'] 

``CaughtUpProcessor`` runs once when ``counterblockd`` catches up to the latest Counterpartyd block. 

.. code-block:: python
    @CaughtUpProcessor.subscribe()
    def caughtUpAlert(): 
        print('counterblockd is now caught up to Counterpartyd!') 

``RollbackProcessor`` runs whenever the ``counterblockd`` database is rolled back (either due to a blockchain
reorg, or an explicit rollback command being specified to ``counterblockd`` via the command line). 

.. code-block:: python
    @RollbackProcessor.subscribe()
    def rollbackAlert(max_block_index): 
        print('counterblockd block database rolled back! Anything newer than block index %i removed!' % max_block_index) 

To add a method from a module to the API dispatcher: 

.. code-block:: python
    from lib.processor import API
    
    #(note that the dispatcher add_method does not take arguments) 
    @API.add_method
    def my_foo_api_method(): 
        return 'bar' 
