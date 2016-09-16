#!/usr/bin/python
import os
import socket
import string
import time
import sys
import urllib
import re
import random
import subprocess
# TODO: logging
#import logging
#logging.basicConfig(level=logging.DEBUG)

# python 2.4 support:
#import requests
#import json
try: import simplejson as json
except ImportError: import json

debug=False

if len(sys.argv) > 2 or len(sys.argv) < 1:
    print >> sys.stderr, 'Usage: [--debug]'
    sys.exit(1)

if len(sys.argv) == 2:
    if sys.argv[1] == '--debug':
        print 'Debug on'
        debug=True

# Basic homebrew logging
loglevel=3 #Trace

dir="/var/core"
httpsrv="http://my-core-st1.i:8080/"
rsync_baseurl='my-core-st1.i::coredumps/'
if (debug): 
    httpsrv="http://localhost:8080/"
    rsync_baseurl='localhost::coredumps/'
    dir="testdir"

metadata_url=httpsrv+"metadata"
basic_url=httpsrv+"dump"

# TODO: switch to logger
def log(str, level=0):
    "This writes to log, loglevels: 0=error, 1=info, 2=debug, 3=trace"
    if (level>loglevel): 
        return
    if level == 0: prefix='ERROR:'
    elif level==1: prefix='INFO:'
    elif level==2: prefix='DEBUG:'
    elif level==3: prefix='TRACE:'
    else: prefix='UNKNOWN:'
    print time.strftime('%x %X'), prefix, str
    return
    
def outp(cmd):
    log(cmd, 2)
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    return '\n'.join(proc.communicate()[0].split('\n'))

def getmetadata(fname):
    basename=re.match('^([a-zA-Z_0-9-]+)\.[0-9]+\.[0-9]+\.core$',fname).group(1)
    binary=""
    gdbresult=""
    rpmpackage=""
    try:
        binary=outp(["which",basename]).strip()
    except subprocess.CalledProcessError:
        pass
    log('Binary: %s'%binary,3)
    if not binary: log('Binary not found',2)
    elif not os.path.isfile ("/usr/bin/gdb"): log('Gdb not found',1)
    else:   #if (binary) and os.path.isfile ("/usr/bin/gdb"):
        log(' '.join(("gdb", binary, dir+"/"+fname, "-ex", "bt", "-ex", "quit")),2)
        gdbresult=outp(["/usr/bin/gdb", binary, dir+"/"+fname, "-ex", "bt", "-ex", "quit"])
        rpmpackage=outp(["rpm", "-qf", binary])
    return {
        "basename": basename,
        "package": rpmpackage,
        "binary": binary,
        "hostname": socket.gethostname(),
        "gdbresult": gdbresult
    }

# Main
valid_chars='-_.%s%s'%(string.ascii_letters, string.digits)
rsync_list=[]
log ('Coredumps processor starting for dir: %s and server %s' % (dir, httpsrv), 1)


# Splay. Have to sleep because we are normally run with a cronjob on a multitude of servers at once 
if (not debug):
    sleeptime=random.uniform(0,20)
    log('Sleeping for %f secs'%sleeptime,1)
    time.sleep(sleeptime)

# Get and timesort list of core dumps 
filelist=os.listdir(dir)
log('Got list of %d files'%(len(filelist),),1)
if len(filelist)==0:
    log('No files found, terminating',1)
    sys.exit(0)
filelist.sort(key=lambda x: -os.path.getmtime(dir+'/'+x))
log('The newest is %s'%(filelist[0],),2)

# Examine each file from the newest to oldest, until we ecounter first obsolete file 
for fname in filelist:
    log('Found file ' + fname, 3)
    try:
        mdata=os.stat(dir +"/"+ fname)
    except OSError: # May be wiped out, no problem
        continue
    if (mdata.st_mtime < time.time()-4000):
        log('Old file: %s. Stopped file scan.'%(fname,),1)
        break
    if (mdata.st_mtime > time.time()-5):
        log('Too new file: %s. Maybe still incomplete. Skipping.'%(fname,),1)
        continue
    if (mdata.st_size > 3000000000): # Over 3 Tb. Sparse due to profiler or broken. Shall not burden rsync.
        log('File %s too large: %d bytes. Skipping.'%(fname,mdata.st_size),1)
        continue
    dump_basic={"name":fname, "size":str(mdata.st_size), "mtime":str(mdata.st_mtime)}
    log('Looks good: %s'%fname, 2)
    log('metadata: %s'%str(dump_basic),2)
    curstr = json.dumps(dump_basic)
    log('About to send json metadata: %s ' % (curstr), 3)
    r = urllib.urlopen(basic_url, data=curstr)
    res=r.read().strip()
    log("Result: %s"%res, 3)
    log("Second Result: %s"%r.read(), 3)
    if (res == "1"):
        log(json.dumps(getmetadata(fname)),3)
        metadata={}
        metadata.update(dump_basic)
        metadata.update(getmetadata(fname))
        r = urllib.urlopen(metadata_url, data=json.dumps(metadata))
        rsync_list.append(dir+'/'+fname)

log ('Created list: '+' '.join(rsync_list),2)
if rsync_list:
    args = ('rsync', '-v', '--sparse') + tuple(rsync_list) + ((rsync_baseurl + socket.gethostname() + '/'), )
    log('Now starting: %s'%' '.join(args), 2)
    if(not debug): 
        os.execv('/usr/bin/rsync', args)


