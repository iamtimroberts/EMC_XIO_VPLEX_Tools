# -*- coding: utf-8 -*-
'''
XtremIO/VPlex Epic Caché Refresh & Backup Script
'''
__author__ = "Tim Roberts"
__email__ = "troberts@example.com"
__version__ = "3.0"
__status__ = "Development"

import argparse
import logging
import subprocess
import sys
import time
import requests
import requests.packages.urllib3
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
requests.packages.urllib3.disable_warnings()

class Refresh(object):
    '''
    class docstring
    '''
    # EMC 
    xmsuser = ('xms_vplex_useranme', 'password')
    xmsipdict = {'1': '10.10.10.1', '2': '10.10.10.2'}
    xmsclusterdict = {'1': 'XIO-Cluster-1', '2': 'XIO-Cluster-2'}
    vplexurldict = {'1': 'vplex1.example.com', '2': 'vplex2.example.com'}

    # SMTP
    smtpserver = 'smtp.example.com'
    emailaddyfail = ['epicadmin@example.com', 'email@exampl.com']
    emailaddy = ['epicadmin@example.com']

    # Epic
    envs = {
        'sup' : {
            'name'     : 'epic environment',
            'srcHOST'  : 'cachserver.example.com',
            'srcENV'   : 'env name of refresh source',
            'srcCG'    : 'source XIO & Vplex consistency group',
            'srcRO'    : 'source XIO Read only SnapshotSet',
            'srcLV'    : 'source logical volume name',
            'tgtVG'    : 'target volume group name',
            'tgtLV'    : 'target logical volume name',
            'tgtDC'    : 'target datacenter #',
            'tgtCG'    : 'target XIO & Vplex consistency group',
            'xiotag'   : "/SnapshotSet/TagName",
            'naas'     : ['naaa of each disk in volume group',
                          '6000144000000000000000000000123',
                          '6000144000000000000000000000456',
                          '6000144000000000000000000000789',]
        }
    }

    def __init__(self, inst):
        # Setup initial vars
        self.env = Refresh.envs[inst]
        self.user = Refresh.xmsuser
        self.isfrozen = False
        # Setup File & Terminal Logging
        self.logdir = '/epic/backup_scripts/logs/'
        self.logfile = '%ssnap.log-%s' % (self.logdir, time.strftime("%Y%m%d-%H%M%S"))
        self.log = logging
        self.log.getLogger("requests").setLevel(self.log.WARNING)
        self.log.getLogger("urllib3").setLevel(self.log.WARNING)
        self.log.basicConfig(filename=self.logfile,
                             level=self.log.INFO,
                             format=' %(asctime)s %(levelname)s : %(message)s',
                             datefmt='%b %d %H:%M:%S')
        self.termlog = self.log.StreamHandler()
        self.termlog.setFormatter(self.log.Formatter(' %(asctime)s %(levelname)s : %(message)s',
                                                     datefmt='%b %d %H:%M:%S'))
        self.log.getLogger().addHandler(self.termlog)
        self.log.info('#-----------------------------------------------------------#')
        self.log.info('Beginning %s Snap/Refresh Process', self.env['name'].upper())
        self.log.info('')


    def refreshsnapset(self, dcenter, snapfrom, snapto, snaptype, suffix):
        '''
        Stuffs
        '''
        xmsip = Refresh.xmsipdict[dcenter]
        xmscluster = Refresh.xmsclusterdict[dcenter]
        addr = 'https://%s/api/json/v2/types/' % (xmsip)
        snapnew = snapto + '-NEW'
        try:
            self.log.info('Refreshing %s from %s in %s',
                          snapto, snapfrom, xmscluster)
            payload = {'cluster-id': xmscluster,
                       'to-snapshot-set-id': snapto,
                       'snapshot-set-name': snapnew,
                       'backup-snapshot-type': snaptype,
                       'backup-snap-suffix': '-' + suffix}
            if suffix == 'SSRW':
                payload['from-snapshot-set-id'] = snapfrom
            else:
                payload['from-consistency-group-id'] = snapfrom
            req = requests.post(addr + 'snapshots', json=payload,
                                auth=self.user, verify=False)
            req.raise_for_status()
        except requests.exceptions.HTTPError as err:
            errmessage = 'FAILED Refreshing %s from %s in %s' % (snapto, snapfrom, xmscluster)
            self.handle_error(err, errmessage)
        time.sleep(2)


    def renamesnapset(self, dcenter, snapfrom, snapto):
        '''
        Other stuffz
        '''
        xmsip = Refresh.xmsipdict[dcenter]
        xmscluster = Refresh.xmsclusterdict[dcenter]
        addr = 'https://%s/api/json/v2/types/' % (xmsip)
        try:
            self.log.info('Renaming %s to %s in %s', snapfrom, snapto, xmscluster)
            payload = {'new-name': snapto}
            req = requests.put(
                '%ssnapshot-sets?name=%s&cluster-name=%s' % (
                    addr, snapfrom, xmscluster),
                json=payload,
                auth=self.user,
                verify=False)
            req.raise_for_status()
        except requests.exceptions.HTTPError as err:
            errmessage = 'FAILED Renaming %s to %s in %s' % (snapfrom, snapto, xmscluster)
            self.handle_error(err, errmessage)
        time.sleep(2)


    def tagsnapset(self, dcenter, snapname, tagid):
        '''
        Other stuffz
        '''
        xmsip = Refresh.xmsipdict[dcenter]
        xmscluster = Refresh.xmsclusterdict[dcenter]
        addr = 'https://%s/api/json/v2/types/tags?name=%s' % (xmsip, tagid)
        try:
            self.log.info('Tagging %s in %s', snapname, xmscluster)
            payload = {'cluster-id' : xmscluster,
                       'entity' : 'SnapshotSet',
                       'entity-details' : snapname}
            req = requests.put(
                addr,
                json=payload,
                auth=self.user,
                verify=False)
            req.raise_for_status()
        except requests.exceptions.HTTPError as err:
            errmessage = 'FAILED tagging %s in %s' % (snapname, xmscluster)
            self.handle_error(err, errmessage)
        time.sleep(1)


    def rotatesnapset(self, dcenter, orig):
        '''
        Mo Stuffz
        '''
        old = orig + '-OLD'
        new = orig + '-NEW'
        self.renamesnapset(dcenter, orig, old)
        self.renamesnapset(dcenter, new, orig)


    def deleteoldsnapset(self, dcenter, snapname):
        '''
        Lotsa Stuffz
        '''
        xmsip = Refresh.xmsipdict[dcenter]
        xmscluster = Refresh.xmsclusterdict[dcenter]
        addr = 'https://%s/api/json/v2/types/' % (xmsip)
        snapname = snapname + '-OLD'
        try:
            self.log.info('Deleting %s from %s', snapname, xmscluster)
            req = requests.delete(
                '%ssnapshot-sets?name=%s&cluster-name=%s' % (
                    addr, snapname, xmscluster),
                auth=self.user,
                verify=False)
            req.raise_for_status()
        except requests.exceptions.HTTPError as err:
            errmessage = 'FAILED Deleting %s from %s' % (snapname, xmscluster)
            self.handle_error(err, errmessage)
        time.sleep(2)


    def refreshreadonly(self):
        '''
        readonly refresh
        '''
        self.cacheaction('freeze', self.env['srcHOST'], self.env['srcENV'])
        self.refreshsnapset('1', self.env['srcCG'], self.env['srcRO'], 'readonly', 'SSRO')
        self.refreshsnapset('2', self.env['srcCG'], self.env['srcRO'], 'readonly', 'SSRO')
        self.cacheaction('thaw', self.env['srcHOST'], self.env['srcENV'])
        self.rotatesnapset('1', self.env['srcRO'])
        self.rotatesnapset('2', self.env['srcRO'])
        self.deleteoldsnapset('1', self.env['srcRO'])
        self.deleteoldsnapset('2', self.env['srcRO'])
        self.tagsnapset('1', self.env['srcRO'], "/SnapshotSet/PRODUCTION")
        self.tagsnapset('2', self.env['srcRO'], "/SnapshotSet/PRODUCTION")


    def cacheaction(self, action, hostname, envt):
        '''
        cache stuffz
        '''
        try:
            self.log.info('Initiating %s of CACHE', action)
            cmd = [
                'ssh',
                hostname,
                '/epic/%s/bin/inst%s' % (envt, action)
                ]
            subprocess.check_output(cmd, stderr=subprocess.STDOUT)
            self.log.info('-> %s', ' '.join(cmd))
            self.isfrozen = True if action == 'freeze' else False
        except subprocess.CalledProcessError as err:
            self.handle_error(err.output, cmd)
        time.sleep(2)


    def getdevices(self):
        '''
        devices
        '''
        devices = []
        for naa in self.env['naas']:
            cmd = 'lsscsi -i | grep %s | awk \'{ print $6 }\'' % naa
            device = subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True)
            devices.append(device.strip())
        return devices


    def vgexport(self, tgtvg, tgtlv):
        '''
        vgexport stuffz
        '''
        lvpath = '/epic/%s' % (tgtlv)
        devices = self.getdevices()
        fusercmd = ['sudo', '/usr/sbin/fuser', '-kms', lvpath]
        cmds = (
            ['sudo', 'umount', '-f', lvpath],
            ['sudo', 'vgchange', '-an', tgtvg],
            ['sudo', 'vgremove', '-f', tgtvg],
            ['sudo', 'pvremove'] + devices
            )
        self.log.info('Exporting %s', tgtvg)
        subprocess.call(fusercmd, stderr=subprocess.STDOUT)
        self.log.info('-> %s', ' '.join(fusercmd))
        time.sleep(2)
        for cmd in cmds:
            try:
                subprocess.check_output(cmd, stderr=subprocess.STDOUT)
                self.log.info('-> %s', ' '.join(cmd))
                time.sleep(1)
            except subprocess.CalledProcessError as err:
                self.handle_error(err.output, cmd)


    def vgimport(self, tgtvg, srclv, tgtlv):
        '''
        import stuffz
        '''
        devices = self.getdevices()
        mountpoint = '/epic/%s' % (tgtlv)
        fsdirect = '/dev/%s/%s' % (tgtvg, tgtlv)
        cmds = (
            ['sudo', 'pvscan', '--cache'],
            ['sudo', 'vgimportclone', '-n', tgtvg] + devices,
            ['sudo', 'vgchange', '-ay', tgtvg],
            ['sudo', 'lvrename', tgtvg, srclv, tgtlv],
            ['sudo', 'mount', '-o', 'nouuid', mountpoint],
            ['sudo', 'umount', mountpoint],
            ['sudo', 'xfs_admin', '-U', 'generate', fsdirect],
            ['sudo', 'mount', mountpoint]
            )
        self.log.info('Importing %s', tgtvg)
        for cmd in cmds:
            try:
                subprocess.check_output(cmd, stderr=subprocess.STDOUT)
                self.log.info('-> %s', ' '.join(cmd))
                time.sleep(1)
            except subprocess.CalledProcessError as err:
                self.handle_error(err.output, cmd)


    def vplexinvalidate(self, congroup, dcenter):
        '''
        invalidate vplex cache
        '''
        vplexurl = Refresh.vplexurldict[dcenter]
        vplex = 'dc%s-vplex' % (dcenter)
        addr = 'https://%s/vplex/consistency-group+cache-invalidate' % (vplexurl)
        try:
            self.log.info('Invalidating %s cache on %s', congroup, vplex)
            payload = {'args' : '-g %s -f' % (congroup)}
            req = requests.post(
                addr,
                json=payload,
                auth=self.user,
                verify=False)
            req.raise_for_status()
        except requests.exceptions.HTTPError as err:
            errmessage = 'FAILED invalidating %s cache on %s' % (congroup, vplex)
            self.handle_error(err, errmessage)
        time.sleep(2)


    def deletelocks(self, tgtlv):
        '''
        remove cache locks
        '''
        mountpoint = '/epic/%s' % (tgtlv)
        try:
            self.log.info('Removing cache.lck files from %s', mountpoint)
            findcmd = ('sudo', 'find', mountpoint, '-type', 'f', '-name',
                       'cache.lck', '-exec', 'rm', '-f', '{}', ';')
            subprocess.check_call(findcmd, stderr=subprocess.STDOUT)
            self.log.info('-> %s', ' '.join(findcmd))
        except subprocess.CalledProcessError as err:
            self.handle_error(err.output, findcmd)


    def handle_error(self, err, cmd):
        '''
        handle errors cleanly
        '''
        if 'prd' in self.env['name']:
            title = '%s Backup Error' % self.env['name'].upper()
        else:
            title = '%s Snap/Refresh Error' % self.env['name'].upper()

        self.log.critical('FAILED to %s ', ' '.join(cmd))
        self.log.critical(err)

        if self.isfrozen:
            self.log.critical('ENV Marked as Frozen - Attempting to Thaw..')
            self.cacheaction('thaw', self.env['srcHOST'], self.env['srcENV'])
            self.isfrozen = False

        self.emailbackuplog(title, Refresh.emailaddyfail)
        sys.exit(1)


    def emailbackuplog(self, subj, recpt):
        '''
        email log results
        '''
        import smtplib
        import socket
        from email.mime.text import MIMEText

        with open(self.logfile, 'rb') as logreader:
            msg = MIMEText(logreader.read())

        msg['Subject'] = subj
        msg['From'] = socket.getfqdn()
        msg['To'] = ",".join(recpt)

        smtp = smtplib.SMTP(Refresh.smtpserver)
        smtp.sendmail(msg['From'], recpt, msg.as_string())
        smtp.quit()


def main():
    '''
    If not called as module the below is run
    '''
    # Set up arg parser
    parser = argparse.ArgumentParser()
    parser.add_argument('env', action='store',
                        help='Target Environment ex: sup, rel, val, prdbak')
    parser.add_argument('-r', '--rotate', action='store_true', default=False,
                        dest='isRotate',
                        help='Rotate the Parent RO Consistency Group?')
    envargs = parser.parse_args()

    # Instatiate Refresh class for this instance
    snap = Refresh(envargs.env)

    # Check if Readonly snapshot rotation is necessary
    if envargs.isRotate:
        snap.refreshreadonly()

    snap.vgexport(snap.env['tgtVG'], snap.env['tgtLV'])
    snap.refreshsnapset(snap.env['tgtDC'], snap.env['srcRO'], snap.env['tgtCG'], 'regular', 'SSRW')
    snap.rotatesnapset(snap.env['tgtDC'], snap.env['tgtCG'])
    snap.deleteoldsnapset(snap.env['tgtDC'], snap.env['tgtCG'])
    snap.tagsnapset(snap.env['tgtDC'], snap.env['tgtCG'], snap.env['xiotag'])
    snap.vplexinvalidate(snap.env['tgtCG'], snap.env['tgtDC'])
    snap.vgimport(snap.env['tgtVG'], snap.env['srcLV'], snap.env['tgtLV'])
    snap.deletelocks(snap.env['tgtLV'])

    # Wrap up the logging
    snap.log.info('')
    snap.log.info('Completed %s Snap/Refresh Process', snap.env['name'].upper())
    snap.log.info('#-----------------------------------------------------------#')

    snap.emailbackuplog('%s Snap/Refresh Complete' % (snap.env['name'].upper()),
                        snap.emailaddy)


if __name__ == '__main__':
    main()