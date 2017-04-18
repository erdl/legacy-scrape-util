#!/usr/bin/env python3
from src.core.data_utils import Row, get_uid_generator, check_config
from src.core.error_utils import error_template
import requests
import time

# an experimental shortcut to nicer errors :)
webctrl_error = error_template('`webctrl` data-acquisition step')

# Primary entry point for webctrl scrape.
# Returns data & updated state.
def acquire(project,config,state):
    state = setup(project,config,state)
    nonce = state['nonce']
    sensors = config['sensor']
    data = []
    query = new_query(config['settings'])
    mkuid = get_uid_generator()
    # cycle through all nodes, querying
    # all sensors associated with each node.
    for sensor in sensors:
        if 'is-active' in sensor:
            if not sensor['is-active']: continue
        name,path = sensor['name'], sensor['path']
        node = sensor['node'] if 'node' in sensor else project
        unit = sensor['unit'] if 'unit' in sensor else 'undefined'
        code = mkuid((node,name,unit))
        after = nonce[code]
        result = query(path,after)
        lrow = lambda t,v: Row(node,name,unit,float(t//1000),float(v))
        rows = [lrow(r['t'],r['a']) for r in result if not '?' in r.values()]
        if not rows: continue
        fltr = lambda r: r.timestamp
        nonce[code] = max(rows,key=fltr).timestamp
        data += rows
    state['nonce'] = nonce
    return state,data

# do general housekeeping before data-acquisition
# begins.  Specifically, ensure that `config` is valid,
# and that we have all necessary data in `state`.
def setup(project,config,state):
    proto_config = {'sensor':list ,'settings': {
        'server':str,'login':{'name':str,'pass':str} } }
    msg = 'checking configuration values for project: ' + project
    mkerr = webctrl_error(msg)
    check_config('webctrl',proto_config,config,mkerr=mkerr)
    # if `config` checks out, we can assess `state`
    # and add/update any missing missing values.
    state = check_state(project,config,state)
    return state

# check on state, generating any missing values.
def check_state(project,config,state):
    # read & remove init section if found.
    if 'init' in state:
        delta = state['init']['start-from']
        del state['init']
    else: delta = None
    nonce = state['nonce'] if 'nonce' in state else {}
    state['nonce'] = check_nonce(project,config['sensor'],nonce,delta)
    return state


# Add a new nonce field for any sensors
# not found in the nonce.
def check_nonce(project,sensors,nonce,delta=None):
    mkuid = get_uid_generator()
    if not delta:
        delta = time.time() - 86400
    for sensor in sensors:
        node = sensor['node'] if 'node' in sensor else project
        unit = sensor['unit'] if 'unit' in sensor else 'undefined'
        name = sensor['name']
        code = mkuid((node,name,unit))
        if not code in nonce:
            nonce[code] = delta
    return nonce

# Generate a pre-configured query callable
# s.t. we don't all die of excess boilerplate.
def new_query(settings):
    tp = lambda t: time.strftime('%Y-%m-%d',time.gmtime(t))
    now = time.time()
    uri = settings['server']
    auth = ( settings['login']['name'], settings['login']['pass'] )
    # return a lambda requiring args querystring,nonce
    lam = lambda q,n : exec_query(uri,q,auth,tp(n),tp(now))
    return lam

# Actually execute the query of the webctrl server.
def exec_query(uri,sensor,auth,start,stop):
    print("querying: {}".format(sensor))
    params = {'id':sensor,'start':start,'end':stop,'format':'json'}
    req = requests.post(uri,params=params,auth=tuple(auth))
    if req.status_code != 200:
        print(req.text)
        raise Exception("Query Failed w/ Status Code {}".format(req.status_code))
    return req.json()[0]['s']
