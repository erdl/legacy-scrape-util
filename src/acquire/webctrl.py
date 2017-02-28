#!/usr/bin/env python3
from src.core.row import Row
import requests
import time


# Primary entry point for webctrl scrape.
# Returns data & nonce_new
def scrape(project,config,nonce):
    nonce = check_nonce(nonce,config['nodes'])
    nonce_new = {k:nonce[k] for k in nonce}
    data = []
    query = new_query(config['server'])
    for node in config['nodes']:
        sensors = config['nodes'][node]
        for sn in sensors:
            nn = nonce[sn]
            qs = sensors[sn]['query-string']
            rslt = query(qs,nn)
            unit = sensors[sn]['unit']
            lrow = lambda t,v : Row(node,sn,unit,float(t//1000),float(v))
            rows = [lrow(r['t'],r['a']) for r in rslt if not '?' in r.values()]
            if not rows: continue
            fltr = lambda r: r.timestamp
            nonce_new[sn] = max(rows,key=fltr).timestamp
            data += rows
    if data: nonce = nonce_new
    return data,nonce

# Add a new nonce field for any sensors
# not found in the nonce.
def check_nonce(nonce,nodes):
    delta = time.time() - 86400
    for n in nodes:
        sensors = nodes[n]
        for s in sensors:
            if s not in nonce:
                nonce[s] = delta
    return nonce

# Generate a pre-configured query callable
# s.t. we don't all die of excess boilerplate.
def new_query(server):
    tp = lambda t: time.strftime('%Y-%m-%d',time.gmtime(t))
    now = time.time()
    u,a = server['uri'],server['auth']
    # return a lambda requiring args querystring,nonce
    lam = lambda q,n : exec_query(u,q,a,tp(n),tp(now))
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
