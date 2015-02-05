import logging
import types
import time
from time import sleep

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

    def start_backup(self, configid, retries=20):

        for retry in range(retries):
            sid = None
            try:
                sid = self.backupengine.StartBackup(configid, retry = 0)
            except RuntimeError as e:
                self.log.error("Failed to start backup: {}".format(e))
                sid = None
                continue

            if sid == None or sid == -1:
                self.log.error("Invalid snapshot ID returned, retrying.")
                sid = None
                sleep(10)
                continue

            self.current_sid = sid
            return self.current_sid

        if sid == None:
            self.log.error("Failed to start backup in {} tries.".format(retries))

        self.current_sid = sid
        return self.current_sid

    def watch_backup(self, sid=None, timeout=3600):
        """
        Watch the backup progress waiting for it to complete
        Timeout is in seconds and defaults to an hour.

        Returns true if backup successful False otherwise.
        """

        ok_status = ['Completed', 'CompletedWithErrors']
        failed_status = ['Skipped', 'Missed', 'Stopped', 'Failed']

        if sid == None:
            sid = self.current_sid
        if sid:
            start_time = int(round(time.time()))
            finish_time = start_time + timeout
            while int(round(time.time())) < finish_time:
                # Poll every 10 seconds.
                sleep(10)
                ret = self.backupengine.GetBackupProgress(sid)
                """
                ret contains:
                    ret['BackupId']     Backup ID
                    ret['CurrentState'] Current state of the backup
                        One of: Queued, InProgress, Skipped, Missed, Stopped,
                                Completed, Failed, Prepairing, StartRequested,
                                StartScheduled, StopRequested, and CompletedWithErrors
                """

                self.log.debug("Got progress:")
                self.log.debug("BackupID:               {}".format(ret['BackupId']))
                self.log.debug("CurrentState:           {}".format(ret['CurrentState']))
                self.log.debug("BackupConfigurationID:  {}".format(ret['BackupConfigurationId']))

                if ret['CurrentState'] in ok_status:
                    return True
                elif ret['CurrentState'] in failed_status:
                    return False
            # Timeout occured...
            return False
        else:
            raise RuntimeError('Watch backup requires a sid to watch!')

    def get_report(self, sid=None, retries=20):
        ret = None
        if sid == None:
            sid = self.current_sid

        if sid:
            for retry in range(retries):
                try:
                    ret = self.backupengine.GetBackupReport(sid)
                except RuntimeError as e:
                    self.log.error("Failed to get status.  Retry.")
                    ret = None
                    continue
                if ret['SnapshotId'] == -1:
                    self.log.error("Got invalid snapshot ID in backup report.  Retrying.")
                    ret = None
                    continue

                return ret

            if ret == None:
                self.log.error("Failed to get status in {} tries.".format(retries))

        return ret
