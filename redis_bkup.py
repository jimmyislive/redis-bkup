#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
from datetime import datetime
import os
import sys
import shutil
from subprocess import Popen, PIPE
import time

import redis

__doc__ = '''

            Author: Jimmy John

            This script helps you to:

            1. take a backup of your redis db
            2. copy it over to a s3fs mounted s3 bucket
            3. encrypt the redis db file on the s3 bucket

            You need to have the following installed:

            1. S3FS (https://code.google.com/p/s3fs/wiki/FuseOverAmazon)
            2. openssl
            3. rsync

            The encrypted redis db file will be in your s3fs mount point with the
            name: <%Y-%m-%d:%H-%M-%S>_<redis dump filename in your redis.conf>

            put this into a cron job and run as frequently as you wish.

        '''

def backup_redis(args):
    '''backs up the redis db and returns the path of the backup file'''

    try:
        r = redis.StrictRedis(host=args.host,
                              port=int(args.port))

        #take the last save before doing the bgsave (which is an async op)
        #then take the new lastsave and keep checking until it's greater
        #see http://redis.io/commands/lastsave
        lastsave_before = r.lastsave()
        r.bgsave()
        lastsave_after = r.lastsave()

        attempt = 1
        while not (lastsave_after > lastsave_before):
            #exponential backoff...
            time.sleep(pow(2,attempt))
            lastsave_after = r.lastsave()
            attempt += 1

        backup_path = os.path.join(r.config_get('dir')['dir'], r.config_get('dbfilename')['dbfilename'])

        print 'backup complete...'

        return backup_path
    except Exception, e:
        print 'Error while doing the redis backup: {0}'.format(str(e))
        return None


def rsync(path, args):
    '''rsyncs the redis backup located at <path> to the s3fsmount dir.

    returns filename(success)|None(fail)
    '''

    filename = os.path.basename(path)
    dst_filename = '{0}_{1}'.format(datetime.now().strftime('%Y-%m-%d:%H-%M-%S'), filename)
    proc = Popen('rsync -avz {0} {1}'.format(path, os.path.join(args.s3fsmount, dst_filename)), shell=True)
    proc.wait()

    if proc.returncode:
        print 'An error ocurred while doing an rsync to {0}'.format(args.s3fsmount)
        return None
    else:
        print 'rsync complete...'
        return dst_filename

def encrypt_and_clean(rsynced_filename, args):
    '''

    btw, if yu want to decrypt it, just do:

    openssl enc -d -aes-256-cbc -in foo.enc -out foo.out -k <same passphrase>

    '''


    proc = Popen('openssl enc -aes-256-cbc -salt -in {0} -out {0}.enc -k {1}'.format(os.path.join(args.s3fsmount, rsynced_filename), args.password), shell=True)
    proc.wait()

    if proc.returncode:
        print 'An error ocurred while encrypting the redis dump on the s3fsmount'
        return 1
    else:
        #ok, so we have encrypted the redis dump, remove the old unencrypted one
        os.remove(os.path.join(args.s3fsmount, rsynced_filename))

        print 'encryption and clean complete...'
        return 0

def main():
    print '********************'
    print datetime.now()
    print '********************'
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', default='6379', help='redis port, default of 6379')
    parser.add_argument('--host', default='localhost', help='redis host, default of localhost')
    parser.add_argument('s3fsmount', help='the place where you have mounted your s3 bucket')
    parser.add_argument('password', help='the password to encrypt your db with after placing on S3')
    args = parser.parse_args()

    backup_path = backup_redis(args)

    if backup_path:
        rsynced_filename = rsync(backup_path, args)
        if rsynced_filename:
            return encrypt_and_clean(rsynced_filename, args)
        else:
            sys.exit(1)
    else:
        sys.exit(1)


if __name__ == '__main__':
    main()


