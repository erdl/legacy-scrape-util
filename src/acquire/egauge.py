#!/usr/bin/env python3
from src.core.row import Row
import requests
import time

# primary entry point for scrape of egauges.
# Returns any acquired data & updated nonce.
def scrape(project,config,nonce):
    gauges = config["nodes"]
    nonce = check_nonce(nonce,gauges)
    data,nonce_new = [],{}
    fltr = lambda r : r.timestamp
    for gid in gauges:
        print('querying gauge: {}...'.format(gid))
        raw = query(gauges[gid],nonce[gid])
        rows = fmt_query(gid,raw)
        nonce_new[gid] = max(rows,key=fltr).timestamp if rows else nonce[gid]
        data += rows
    nonce.update(nonce_new)
    print('gauge queries complete...')
    return data,nonce

# Check if nonce exists for each node.
# If does not exist, set node's nonce
# to t minus 24 hours.
def check_nonce(nonce,gauges):
    delta = time.time() - 86400
    for gid in gauges:
        if not gid in nonce:
            nonce[gid] = delta
    return nonce

# Query a specified egauge.
# Returns a dictionary of all columns w/ headers as keys.
def query(gauge,after):
    uri = 'http://egauge{}.egaug.es/cgi-bin/egauge-show?c&C&m'
    params = {'w':after}
    r = requests.get(uri.format(gauge),params=params)
    # break up the recieved csv into two-dimensional list structure.
    rows = [[y for y in x.split(',')] for x in r.text.splitlines()]
    if not rows: return {}
    headers = [x.replace('"','') for x in rows.pop(0)]
    columns = list(zip(*rows))
    # reshape columns into form { header : [ values ] }
    return {h: list(map(float,columns[i])) for i,h in enumerate(headers)}

# Convert into row format.
# Returns list of named tuples.
def fmt_query(gauge_id,data):
    # separate times from readings.
    dtimes = data['Date & Time']
    values = {k: data[k] for k in data if k != 'Date & Time'}
    formatted = []
    # break out readings into form standard form:
    # ( node, sensor, unit, timestamp, value )
    for sn in values:
        unit,snid = parse_sntxt(sn)
        fmt = lambda t,v: Row(gauge_id,snid,unit,t,v)
        formatted += [fmt(t,v) for t,v in zip(dtimes,values[sn])]
    return formatted

# Split the egauge supplied sensor/column name
# into separate units and sensor id.
# Expects egauge column headers to be
# of the form `SensorName [unit]`.
def parse_sntxt(sensor):
    try:
        unit = sensor.split('[').pop().replace(']','').replace(' ','')
        snid = sensor.split(' [').pop(0).lower()
    except Exception as err:
        print('failed to parse: {}'.format(sensor))
        print('parse error: {}'.format(err))
        # if parse fails, lower case column name is used as
        # sensor id, and unit is listed as "undefined".
        unit = 'undefined'
        snid = sensor.lower()
    return unit,snid
