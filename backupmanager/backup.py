import logging
import types

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
        self.agentengine = cloudbackup.client.agents.Agents(True, self.authengine, apihost)
        self.rseengine = cloudbackup.client.rse.Rse('chocodile', '0.00', self.authengine, self.agentengine, None, '/tmp/chocodile-rse.log', apihost)

        self.backupconfig = None

    def load_config(self, configid):
        """
        Load a backup config from the API
        """

        try:
            self.backupconfig = self.backupengine.RetrieveBackupConfiguration(configid)
            self.log.debug("Found Config: {}".format(self.backupconfig.to_dict))
            return True
        except ValueError as e:
            self.log.debug("Config not found.")
            return False
    
    def create_config(self, config):
        """
        Create a backup config.

        The config dict must contain the following:
            ConfigurationName : The name of the configuration (This is what shows in CP)
            VersionRetention  : How long to keep the backup, must be one of 0, 30, 60
            NotifyEmail       : The email address for backup notifications to go to.
            NotifySuccess     : True if you want to send an email on success
            NotifyFailure     : True if you want to send an email on failure
        """

        self.log.debug("Generating backup config.")

        newconf = cloudbackup.client.backup.BackupConfiguration()

        newconf.ConfigurationId      = 0
        newconf.ConfigurationName    = config['ConfigurationName']
        newconf.MachineAgentId       = self.agentid
        newconf.VersionRetention     = int(config['VersionRetention'])
        newconf.MissedBackupActionId = 1
        newconf.Frequency            = 'Manually'
        newconf.NotifyRecipients     = config['NotifyEmail']
        newconf.NotifySuccess        = config['NotifySuccess'] in [ 'true', 'True', 'TRUE', '1' ]
        newconf.NotifyFailure        = config['NotifyFailure'] in [ 'true', 'True', 'TRUE', '1' ]

        folders = config['BackupFolders']
        self.log.debug("Adding folders: {}".format(folders))
        if isinstance(folders, types.ListType):
            if folders != []:
                self.log.debug("As list.")
                newconf.AddFolders(folders)
        elif isinstance(folders, types.StringTypes):
            if folders != '':
                folders=folders.split(' ')
                self.log.debug("As split string: {}".format(folders))
                newconf.AddFolders(folders)

        files = config['BackupFiles']
        self.log.debug("Adding files: {}".format(files))
        if isinstance(files, types.ListType):
            if files != []:
                self.log.debug("As list.")
                newconf.AddFiles(files)
        elif isinstance(files, types.StringTypes):
            if files != '':
                files=files.split(' ')
                self.log.debug("As split string: {}".format(files))
                newconf.AddFiles(files)

        self.log.debug("Backup config: {}".format(newconf.to_dict))

        ret = self.backupengine.CreateBackupConfiguration(newconf)

        if (ret == True):
            self.log.debug("Configuration generated.")
            self.backupconfig = newconf
            return newconf.ConfigurationId
        else:
            self.backupconfig = None
            return None

    def wake_agent(self):
        return self.agentengine.WakeSpecificAgent(self.agentid, self.rseengine, 30*1000, keep_agent_awake=False, wake_period=None)

    def start_backup(self, configid, backup_timeout=30*1000, monitor_period=5.0, retry=20):
        try:
            parameters = { 
                    'backupid': configid, 
                    'backup_timeout': backup_timeout, 
                    'monitor_period': monitor_period, 
                    'retry_attempts': retry
                    }

            ret = self.backupengine.StartBackupRetry(parameters)
            self.current_sid = ret['api_snapshotid']
            if ret['status'] == False:
                self.log.error("Backup failed!")
        except RuntimeError as e: # API throws RuntimeError when something breaks, catch it here.
            self.log.debug("Failed to start backup: {}".format(e))
            self.current_sid = None
        return self.current_sid

    def watch_backup(self):
        if self.current_sid:
            self.backupengine.MonitorBackupProgress(self.current_sid, 30*1000)
