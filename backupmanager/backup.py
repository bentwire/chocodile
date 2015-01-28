import logging
import os
import sys
import json
import ConfigParser

import cloudbackup.client.auth
import cloudbackup.client.agents
import cloudbackup.client.backup
import cloudbackup.client.rse
import cloudbackup.utils

class BackupManager(object):
    def __init__(self, userid, apikey, agentid, apihost):

        self.log = logging.getLogger(__name__)
        self.agentid = agentid
        self.authengine = cloudbackup.client.auth.Authentication(userid, apikey)
        self.backupengine = cloudbackup.client.backup.Backups(True, self.authengine, apihost)

        self.backupconfig = None

    def load_config(self, configid):

        try:
            self.backupconfig = backupengine.RetrieveBackupConfiguration(configid)
            return True
        except ValueError as e:
            self.log.debug("Config not found.")
            return False
    
    def create_config(self, config):
        self.log.debug("Generating backup config.")

        newconf = cloudbackup.client.backup.BackupConfiguration()

        newconf.ConfigurationId      = 0
        newconf.ConfigurationName    = config['ConfigurationName']
        newconf.MachineAgentId       = self.agentid
        newconf.VersionRetention     = int(config['VersionRetention'])
        newconf.MissedBackupActionId = 1
        newconf.Frequency            = 'Manually'
        newconf.NotifyRecipients     = config['NotifyEmail']
        newconf.NotifySuccess        = bool(config['NotifySuccess'])
        newconf.NotifyFailure        = bool(config['NotifyFailure'])

        newconf.AddFolders(config['BackupFolders'])
        newconf.AddFiles(config['BackupFiles'])

        self.log.debug("Backup config: {}".format(newconf.to_dict))

        ret = self.backupengine.CreateBackupConfiguration(newconf)

        if (ret == True):
            self.log.debug("Configuration generated.")
            self.backupconfig = newconf
            return newconf.ConfigurationId
        else:
            self.backupconfig = None
            return None

    def start_backup(self, configid, retry=20):
        try:
            sid = self.backupengine.StartBackup(configid, retry=retry)
            self.current_sid = sid
        except RuntimeError as e:
            self.log.debug("Failed to start backup: {}".format(e))
            self.current_sid = None
        return self.current_sid

    def watch_backup(self):
        if self.current_sid:
            self.backupengine.MonitorBackupProgress(self.current_sid, 30*1000)

if __name__ == '__main__':
    from config import Config

    logging.basicConfig()
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    configfile = Config.find_config("backup")

    config = Config(configfile)

    apikey = config.get('config:apikey')
    userid = config.get('config:userid')
    bootstrapfile = config.get('config:bootstrap')

    logger.debug("Found bootstrap file: {}".format(bootstrapfile))

    cfile = file(bootstrapfile, 'r')
    bootstrap = json.load(cfile)

    agentid = bootstrap['AgentId']
    apihost = bootstrap['ApiHostName']

    logger.debug("Our AgentID: {0}, Our APIHost: {1}".format(agentid, apihost))
    
    backupconf = config.get('backupconfig')

    logger.debug("Got backup config: {}".format(backupconf))

    backupmanager = BackupManager(userid, apikey, agentid, apihost)

    try:
        backupconfigid = config.get('backupconfigid:configid')
    except KeyError as e:
        logger.debug("Previous backup config not found, creating a new one.")
        backupconfigid = backupmanager.create_config(backupconf)
        config.set('backupconfigid:configid', backupconfigid)
        config.write()

    logger.debug("Got backup config id: {}".format(backupconfigid))

    logger.info("Starting backup using config: {}".format(backupconfigid))

    backupmanager.start_backup(backupconfigid)

    backupmanager.watch_backup()

    sys.exit(0)
