#!/usr/bin/python
import os
import socket
import string
import time
import json
import sys
import subprocess
import requests
import re
import random
from subprocess import PIPE,Popen


debug=1
loglevel=3 #Trace

dir="/var/core"
httpsrv="http://my-core-st1.i:8080/"
rsync_baseurl='my-core-st1.i::coredumps/'
if (debug): 
    #srv=(socket.gethostname(), 12345)
    httpsrv="http://localhost:8080/"
    rsync_baseurl='localhost::coredumps/'
    dir="testdir"

metadata_url=httpsrv+"metadata"
basic_url=httpsrv+"dump"

# TODO: add timing to log
def log(str, level=0):
    "This writes to log, loglevels: 0=error, 1=info, 2=debug, 3=trace"
    if (level>loglevel): return
    if level == 0: prefix='ERROR:'
    elif level==1: prefix='INFO:'
    elif level==2: prefix='DEBUG:'
    elif level==3: prefix='TRACE:'
    else: prefix='UNKNOWN:'
    print time.strftime('%x %X'), prefix, str
    return
    
def outp(cmd):
    print cmd
    proc = Popen(cmd, stdout=PIPE)
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
    if(binary):
        print "gdb", binary, dir+"/"+fname, "-ex", "bt", "-ex", "quit"
        gdbresult=outp(["/usr/bin/gdb", binary, dir+"/"+fname, "-ex", "bt", "-ex", "quit"])
        rpmpackage=outp(["rpm", "-qf", binary])
    return {
        "basename": basename,
        "package": rpmpackage,
        "binary": binary,
        "hostname": socket.gethostname(),
        "gdbresult": gdbresult
    }

# Constants
ERROR=0
INFO=1
DEBUG=2
TRACE=3

# Main
valid_chars='-_.%s%s'%(string.ascii_letters, string.digits)
files=[]
log ('Coredumps-processor starting for dir: %s and server %s' % (dir, httpsrv), 1)

if (not debug):
    sleeptime=random.uniform(0,10)
    log('Sleeping for %f secs'%sleeptime,1)
    time.sleep(sleeptime)

filelist=os.listdir(dir)
log('Got list of %d files'%(len(filelist),),1)
if len(filelist)==0:
    log('No files found, terminating',1)
    sys.exit(0)
filelist.sort(key=lambda x: -os.path.getmtime(dir+'/'+x))
log('The newest is %s'%(filelist[0],),2)

for fname in filelist:
    log('Found file ' + fname, 3)
    try:
        mdata=os.stat(dir +"/"+ fname)
    except OSError:
        break
    if (mdata.st_mtime < time.time()-4000):
        log('Encountered an old file: %s.'%(fname,),1)
        break
    if (mdata.st_mtime > time.time()-5):
        log('Too new file: %s. Maybe still incomplete. '%(fname,),1)
        break
    if (mdata.st_size > 10000000000):
        log('File %s too large: %d bytes. Skipping.'%(fname,mdata.st_size),1)
        break
    dump_basic={"name":fname, "size":str(mdata.st_size), "mtime":str(mdata.st_mtime)}
    curstr = json.dumps(dump_basic)
    # TODO: add hostname, gdb
    log('About to send this metadata: %s ' % (curstr), 3)
    r = requests.post(basic_url, data=curstr)
    if debug: print (r.text)
    if (r.text == "1"):
        if debug: print json.dumps(getmetadata(fname))
        metadata={}
        metadata.update(dump_basic)
        metadata.update(getmetadata(fname))
        r = requests.post(metadata_url, data=json.dumps(metadata))
        files.append(dir+'/'+fname)

log ('Created list: '+' '.join(files),2)
if files:
    args = ('rsync', '-vP') + tuple(files) + ((rsync_baseurl + socket.gethostname() + '/'), )
    if(not debug): 
        os.execv('/usr/bin/rsync', args) 
    else: 
        log("Would have executed: %s"%(' '.join(args)),2)

