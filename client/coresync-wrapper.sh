#!/bin/bash

export PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin:/root/bin
LOG=/var/log/coresync.log
MAILTO=a.loskutov@corp.mail.ru
/usr/local/bin/coresync.py >> $LOG 2>&1 \
 || tail $LOG | mail -s 'Coresync failed on '`hostname` $MAILTO

