#!/usr/bin/env python3
from src.core.data_utils import Row,fmt_string
import time
import csv

# list required configuration fields
REQUIRE = []

def parse(project,config,state,filepath):
    rawdata = read_csv(filepath)
    rows = reformat_data(config,rawdata)
    return state,rows




def reformat_data(config,rawdata):
    title = rawdata.pop(0) # extract title row.
    hoboid = title[0].split(' ').pop() # extract id from title.
    node = 'hobo-{}'.format(fmt_string(hoboid))
    rows = [r[1:] for r in rawdata] # remove col 1 (row numbers).
    headers = rows.pop(0) # extract headers.
    # get timezone and a mapping of form [(name,unit),...]
    timezone,mapping = parse_headers(headers)
    timestrings = [r.pop(0) for r in rows] # extract time column.
    # convert list of time strings to a list of unix timestamps.
    timestamps = parse_times(timestrings,offset=timezone)
    formatted = []
    for row,ts in zip(rows,timestamps):
        for i,value in enumerate(row):
            name,unit = mapping[i]
            formatted.append(Row(node,name,unit,ts,value))
    return formatted



def parse_headers(headers):
    # split up headers to separate out names.
    split = [h.split(', ') for h in headers]
    # get column names from the split headers.
    names = [fmt_string(x.pop(0)) for x in split]
    # The `date-time` column name is a throwaway,
    # but it makes a nice unit test.
    assert names.pop(0) == fmt_string('Date Time')
    # consume date-time column section, and
    # extract timezone info.
    timezone = split.pop(0)[0].strip().lower()
    timezone = timezone.replace('gmt','').replace(':','')
    # re-split to separate out units.
    split = [x.pop(0).split(' ') for x in split]
    # get units from the new split.
    units = [x.pop(0) for x in split]
    # units should exist for all names & vice-versa.
    assert len(names) == len(units)
    # generate a list of form [(name,unit),...]
    mapping = list(zip(names,units))
    return timezone,mapping


def parse_times(times,offset=None):
    fmt = '%m/%d/%y %I:%M:%S %p'
    if offset:
        times = [t + ' ' + offset for t in times]
        fmt = fmt + ' %z'
    timestamps = []
    for t in times:
        extract = time.strptime(t,fmt)
        timestamps.append(time.mktime(extract))
    return timestamps







def read_csv(filename):
    with open(filename) as fp:
        reader = csv.reader(fp)
        title = next(reader) # get title row.
        header = next(reader)
        rows = list(reader)
    # extract id from title row.
    hoboid = title[0].split(' ').pop()
