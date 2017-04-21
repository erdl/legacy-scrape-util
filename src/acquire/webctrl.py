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
    # check that config is valid, and generate
    # start/stop times for all query points.
    starts,stops = setup(project,config,state)
    sensors = config['sensor']
    query = new_query(config['settings'])
    mkuid = get_uid_generator()
    # initialize the nonce as a copy of `starts`.
    nonce = {k:v for k,v in starts.items()}
    data = []
    for sensor in sensors:
        if not sensor.get('is-active',True): continue
        name,path = sensor['name'], sensor['path']
        node = sensor.get('node',project)
        unit = sensor.get('unit','undefined')
        code = mkuid((node,name,unit))
        start,stop = starts[code],stops[code]
        result = query(path,start,stop)
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
    nonce = state.get('nonce',{})
    start,stop = setup_times(project,config,nonce)
    return start,stop

# Add a new nonce field for any sensors
# not found in the nonce.
def setup_times(project,config,nonce):
    settings,sensors = config['settings'],config['sensor']
    mkuid = get_uid_generator()
    now = time.time()
    init = settings.get('init-time',now-86400)
    step = settings.get('step-time',31536000)
    mkstop = lambda s: min((s+step,now))
    start,stop = {},{}
    for sensor in sensors:
        node = sensor.get('node',project)
        unit = sensor.get('unit','undefined')
        name = sensor['name']
        code = mkuid((node,name,unit))
        start[code] = nonce.get(code,init)
        stop[code] = mkstop(start[code])
    return start,stop

# Generate a pre-configured query callable
# s.t. we don't all die of excess boilerplate.
def new_query(settings):
    tp = lambda t: time.strftime('%Y-%m-%d',time.gmtime(t))
    uri = settings['server']
    auth = ( settings['login']['name'], settings['login']['pass'] )
    # return a lambda fn that executes a query given
    # the args: query-string, start-time, end-time.
    lam = lambda q,s,e : exec_query(uri,q,auth,tp(s),tp(e))
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
