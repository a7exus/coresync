#!/usr/bin/python

import sqlite3
import re
import time
import json
import sys
import subprocess
import BaseHTTPServer 
import codecs
import os

# Variables
sqlitedbase='/usr/local/bin/coresync-server.sqlite'
if (not os.path.isfile(sqlitedbase)): sqlitedbase='coresync-server.sqlite'
con=sqlite3.connect(sqlitedbase)
configfile='/usr/local/etc/onlineconf/coresync.conf'
if (not os.path.isfile(configfile)): configfile='coresync.conf'
listenport=8080
listenhost='0.0.0.0'
timeperiod=3600  ## We always consider dumps hourly
logfilename='/var/log/coresync-server.log'
loglevel=3 # Trace 
#ERROR, INFO, DEBUG, TRACE = 0, 1, 2, 3

logfile=codecs.open(logfilename,'a','utf-8')


def log( str, level=0 ):
    "This writes to log, loglevels: 0=error, 1=info, 2=debug, 3=trace"
    if (level>loglevel): return
    if level == 0: prefix='ERROR:'
    elif level==1: prefix='INFO:'
    elif level==2: prefix='DEBUG:'
    elif level==3: prefix='TRACE:'
    else: prefix='UNKNOWN:'
    print >>logfile, time.strftime('%x %X'), prefix, str
    logfile.flush()
    return
def getmailto(service):
    return ','.join(settings['mailing'].get(settings['servicegroups'].get(service,'default'),''))

def mail(service, subj, text=''):
    mailto=getmailto(service)
    log (u'Sending mail: %s to %s'%(subj, mailto),2)
    proc=subprocess.Popen('mail -s "%s" "%s"'%(subj, mailto), stdin=subprocess.PIPE, shell=True)
    proc.stdin.write(text.encode('utf-8'))
    proc.stdin.close()

def processfile(fmdata):
    "This checks if we need the file and requests it"
    (fname, fsize, fmtime) = (fmdata.get('name'), fmdata.get('size'), fmdata.get('mtime'))
    fsize=int(fsize)
    fmtime=int(float(fmtime))
    log('Processing name: %s, size: %s, mtime: %s'%(fname, fsize, fmtime), 3)
    if (fmtime<(int(time.time()) - timeperiod)):
        log('File is too old, ignoring: %s, size: %s, mtime: %s'%(fname, fsize, fmtime), 2)
        return 0
    if not re.match('^[A-Za-z0-9_-]+\.[0-9]+\.[0-9]+\.core$', fname):
        log("fname does not match against core_pattern: "+(fname))
        return 0
    (fentity,away) = fname.split('.', 1)
    log('Calculated entity name:%s'%(fentity,), 3)
    
    if not settings['dumpslimit'].has_key(fentity):
        log ('Dumpslimit not found for entity %s, using default %d' % (fentity, settings['dumpslimit']['default']), 1)
    calculated_threshold_time=max(
            int(time.time()) - timeperiod, 
            min(int(settings['dumpslimitreset'].get(fentity, 0)), int(time.time()))
    )
    log ('Current time is: %d, calculated_threshold_time is %d, timediff is %d' % (int(time.time()), calculated_threshold_time, int(time.time()) - calculated_threshold_time), 2)
    params = (
        int(settings['dumpslimit'].get(fentity, settings['dumpslimit']['default'])),
        fentity, 
        int(calculated_threshold_time)
    )
    log('Querying db for: '+ ', '.join(str(x) for x in params), 2)
    cur.execute('SELECT count(*)<=? FROM dumps WHERE entity=? AND added > ?', params)
    data=cur.fetchone()
    log('Got result from db: '+str(data), 2)
    log('Parsed result: %d'%(data[0],), 3)
    if not data[0]: 
        log('Already have enough dumps for %s'%(fentity,),1)
        return 0
    cur.execute('SELECT count(*)=0 FROM dumps where entity=? AND filename=? AND mtime=? AND size=?', (fentity, fname, int(fmtime), int(fsize)))
    data=cur.fetchone()
    log('Got result2 from db: '+str(data), 2)
    if not data[0]: 
        log('Already have this dump: %s'%(fmdata,),2)
        return 0
    log('Requesting: '+fname,1)
    #mail('Coredump %s'%fentity, 'Core dump is on the way: http://my-core-st1.s.smailru.net/%s'%fname)
    cur.execute('INSERT INTO dumps (entity, filename, mtime, size, added) VALUES (?,?,?,?,?)', (fentity, fname, int(fmtime), int(fsize), int(time.time())))
    log('== Done basic dump processing ==',2)
    return 1
def processmetadata(data):
    log(u'processing metadata %r'%data,3)
    log(u'email: ',3)
    log(u'Core dump is on the way: http://my-core-st1.s.smailru.net/%(hostname)s/%(name)s Host: %(hostname)s Version: %(package)s binary: %(binary)s %(gdbresult)s '%data, 3)
    data['alpha']=''
    if (data['hostname'].find('alpha') != -1):
        data['alpha']='Alpha '
    if (data['hostname'].find('beta') != -1):
        data['alpha']='Beta '
    if (data['hostname'].find('gamma') != -1):
        data['alpha']='Gamma '
    mail(data['basename'], u'%(alpha)sCoredump %(basename)s'%data, text=u'''Core dump is on the way: http://my-core-st1.s.smailru.net/%(hostname)s/%(name)s
Host: %(hostname)s
Version: %(package)s
binary: %(binary)s

Please contact a.loskutov@corp.mail.ru to unsubscribe.

GDB:
%(gdbresult)s
    '''%data)

def getSettings():
    global settings
    settings={}
    #global dumpslimit, dumpslimitreset, groups, mailing
    with open (configfile, 'r') as f:
        for l in f:
            l=l.lstrip()
            if l.find('dumpslimit:JSON')==0: settings['dumpslimit']=json.loads(l.replace('dumpslimit:JSON ',''))
            if l.find('dumpslimitreset:JSON')==0: settings['dumpslimitreset']=json.loads(l.replace('dumpslimitreset:JSON ',''))
            if l.find('groups:JSON')==0: settings['servicegroups']=json.loads(l.replace('groups:JSON ',''))
            if l.find('mailing:JSON')==0: settings['mailing']=json.loads(l.replace('mailing:JSON ',''))
    if not settings['dumpslimit'].has_key('default'):
        log('Dumpslimit default not found, this will possibly crash coresync-server', 0)
    return

class MyHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    def log_request(code,size):
        return
    def do_HEAD(s):
        s.send_response(403)
        s.end_headers()
    def do_GET(s):
        s.send_response(403)
        s.end_headers()    
    def do_POST(s):
        if (int(s.headers['Content-Length'])>1 and int(s.headers['Content-Length'])<1024000):
            if (s.path == '/dump'):
                try:
                    data = json.loads(s.rfile.read(int(s.headers['Content-Length'])))
                    #data[u'gdbresult'] = filter(lambda x: x in string.printable, data[u'gdbresult'])
                except ValueError:
                    log("JSON error")
                    s.send_response(400)
                    s.end_headers()
                    return
                s.send_response(200)
                s.send_header("Content-Type", "text/plain")
                s.end_headers()
                getSettings()
                s.wfile.write(processfile(data))
            elif (s.path == '/metadata'):
                try:
                    data = json.loads(s.rfile.read(int(s.headers['Content-Length'])))
                except ValueError:
                    log("JSON error")
                    s.send_response(400)
                    s.end_headers
                    return
                processmetadata(data)
                s.send_response(200)
                s.end_headers()
            else:
                s.send_response(404)
                s.end_headers
        return

# main()
getSettings()
log("Starting with settings: "+ str(settings['dumpslimit'])+' '+str(settings['dumpslimitreset']),1)
cur=con.cursor()

server_class = BaseHTTPServer.HTTPServer
httpd = server_class((listenhost, listenport), MyHandler)

log(str(time.asctime())+" Server Starts - %s:%s" % (listenhost, listenport),1)
try:
    httpd.serve_forever()
except KeyboardInterrupt:
    pass
httpd.server_close()
log(str(time.asctime())+" Server Stops - %s:%s" % (listenhost, listenport),1)


sys.exit(0)