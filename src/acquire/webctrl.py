#!/usr/bin/env python3
from src.core.utils import Row, mkuid
import requests
import time

# Primary entry point for webctrl scrape.
# Returns data & updated state.
def scrape(project,config,state):
    state = check_state(config,state)
    nonce = state['nonce']
    sensor_map = config['sensor']
    data = []
    query = new_query(config['server'])
    # cycle through all nodes, querying
    # all sensors associated with each node.
    for node in sensor_map:
        sensors = sensor_map[node]
        for sensor in sensors:
            if 'actv' in sensor:
                if not sensor['actv']: continue
            name = sensor['name']
            path = sensor['path']
            unit = sensor['unit'] if 'unit' in sensor else 'undefined'
            code = mkuid(node,name,unit)
            after = nonce[code]
            result = query(path,after)
            lrow = lambda t,v: Row(node,name,unit,float(t//1000),float(v))
            rows = [lrow(r['t'],r['a']) for r in result if not '?' in r.values()]
            if not rows: continue
            fltr = lambda r: r.timestamp
            nonce[code] = max(rows,key=fltr).timestamp
            data += rows
    state['nonce'] = nonce
    if not 'nonce-file' in state:
        state['nonce-file'] = 'nonce'
    return data,state


# check on state, generating any missing values.
def check_state(config,state):
    # read & remove init section if found.
    if 'init' in state:
        delta = state['init']['start-from']
        del state['init']
    else: delta = None
    if not 'nonce' in state: state['nonce'] = {}
    state['nonce'] = check_nonce(state['nonce'],config['sensor'],delta)
    return state


# Add a new nonce field for any sensors
# not found in the nonce.
def check_nonce(nonce,sensor_map,delta=None):
    if not delta:
        delta = time.time() - 86400
    for node in sensor_map:
        for sensor in sensor_map[node]:
            name = sensor['name']
            unit = sensor['unit']
            code = mkuid(node,name,unit)
            if not code in nonce:
                nonce[code] = delta
    return nonce

# Generate a pre-configured query callable
# s.t. we don't all die of excess boilerplate.
def new_query(server):
    tp = lambda t: time.strftime('%Y-%m-%d',time.gmtime(t))
    now = time.time()
    uri = server['query']['uri']
    auth = ( server['login']['name'], server['login']['pass'] )
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
