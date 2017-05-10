#!/usr/bin/env python3
from src.core.data_utils import Row, get_uid_generator, check_config, make_time_specs
from src.core.error_utils import error_template
import requests
import time

# an experimental shortcut to nicer errors :)
webctrl_error = error_template('`webctrl` data-acquisition step')

# Primary entry point for webctrl scrape.
# Returns data & updated state.
def acquire(project,config,state):
    # check that config is valid, parse sensor
    # parameters, and generate time specifications
    # for all active sensors.
    params,times = setup(project,config,state)
    # generate a wrapper around `exec_query` with
    # user-supplied configurations pre-applied.
    query = new_query(config['settings'])
    # initialize collectors for formatted
    # data and the new `nonce` values.
    nonce,data = {},[]
    # iteratively scrape all sensors.
    for uid,spec in params.items():
        # break out the webctrl-path and identity values from `spec`.
        path,*ident = (spec[k] for k in ('path','node','name','unit'))
        # pull start-time and maximum step from `times`.
        start,step = times[uid]['init'],times[uid]['step']
        # query webctrl at `path` from `start` to `start` + `step`.
        result = query(path,start,(start+step))
        # make a generator for the `Row` type based
        # on the supplied identity variables.
        mkrow = lambda t,v: Row(*ident,float(t//1000),float(v))
        # pass row generator and raw query-result to parser.
        rows = parse_rows(mkrow,result)
        # in the event of an empty query, set `start` value
        # as the new nonce, so we start from same place next time.
        if not rows:
            nonce[uid] = start
            continue
        # filter rows by timestamp.
        fltr = lambda r: r.timestamp
        # set most recent timestamp as the new `nonce` value.
        nonce[uid] = max(rows,key=fltr).timestamp
        data += rows # add our rows to `data`.
    # add newly generate `nonce` to `state`, overwriting
    # old value if it exists.
    state['nonce'] = nonce
    # fin ;)
    return state,data


# parse rows from raw webctrl data, removing erroneous data
# and duplicates (the webctrl bulk-trend server periodically
# returns duplicate values for a given timestamp...).
def parse_rows(mkrow,data):
    # extract timestamp & value, passing them to supplied row generator,
    # and filter out erroneous values (indicated by `'?'` in webctrl data).
    raw_rows = [mkrow(r['t'],r['a']) for r in data if not '?' in r.values()]
    rows,times,dups = [],[],[] # instantiate collectors.
    # iterate over rows, sorting out duplicates by timestamp.
    for row in raw_rows:
        t = row.timestamp
        if not t in times:
            times.append(t)
            rows.append(row)
        else: dups.append(row)
    # TODO: optionally save duplicates to archive.
    return rows


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
    params,times = setup_parameters(project,config,nonce)
    return params,times


# initialize sensor and time parameters from
# configuration and nonce values.  Returns two
# dicts, both organized by uid.  One containing
# sensor specifications, and the other containing
# `init` and `step` times for scraping the sensor.
def setup_parameters(project,config,nonce):
    settings,sensors = config['settings'],config['sensor']
    mkuid = get_uid_generator() # get a uid generator instance.
    params = {} # collector for parsed sensor parameters.
    # iteratively parse each sensor's configuration, generating
    # its uid and filling in default values as needed.
    for sensor in sensors:
        # skip sensors with optional `actv` field set to `false`.
        if not sensor.get('actv',True): continue
        # initialize `spec` with allowable defaults.
        spec = {'node':project,'unit':'undefined'}
        # populate spec with values from sensor.
        spec.update(sensor)
        # compile values for `node`, `name`, and `unit`.
        identity = (spec[k] for k in ('node','name','unit'))
        # pass identity to the `uid` generator.
        snid = mkuid(tuple(identity))
        # save `spec` to `params` with the sensor's `uid` as its key.
        params[snid] = spec
    # use tooling in `data-utils` to generate time specs.
    # returns a dict of form: `{'uid': {'init': .., 'step': ...},...}`
    times = make_time_specs(params,settings,nonce)
    return params,times


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
