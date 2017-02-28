#!/usr/bin/env python3
from importlib import import_module
import src.core.parameters as params
import src.core.errlog as errlog


# Primary entry point for runtime.
def run(project):
    config,nonce = params.get_parameters(project)
    data,nonce = acquire_data(config,nonce)
    data = reshape_data(config,data)
    export_data(data)
    params.update_nonce(nonce)

# Acquire the data via specifid method.
def acquire_data(config,nonce):
    daconf = config['data-acquisition']
    scraper = get_util('acquire',daconf['type'])
    data,nonce = scraper.scrape(daconf,nonce)
    return data,nonce

# Reshape data via specified mappings.
def reshape_data(config,data):
    if 'reshape' in config:
        rconf = config['reshape']
    else: return data
    rutils = {}
    rord = []
    for kind in rconf:
        rutils[kind] = get_util('reshape',kind)
        rord.append(kind)
    skey = lambda k: rutils[k].ORD
    rord = sorted(rord,key=skey)
    for rs in rord:
        data = rutils[rs].reshape(data)
    return data

# Save data via specified channels.
def export_data(config,data):
    expconf = config['export']
    for kind in expconf:
        exputil = get_util('export',kind)
        exputil.export(data,expconf[kind])


# Generate the scraper from 'type' field.
def get_util(category,kind):
    modname = 'src.{}.{}'.format(category,kind)
    try: mod = import_module(modname)
    except: raise Exception('no utility at: {}'.format(modname))
    return mod
