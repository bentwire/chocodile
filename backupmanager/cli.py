import os
import logging
import json
import shlex
import sys
import subprocess32 as subprocess

from config import Config
from backup import BackupManager

logging.getLogger('requests').setLevel(logging.WARN)
logging.getLogger('cloudbackup').setLevel(logging.WARN)

def run():
    # Load config
    configfile = Config.find_config("backup")
    config = Config(configfile)

    # Set up logging
    try:
        logfile = config.get('config:logfile')
    except KeyError as e:
        logfile = None

    if logfile == '':
        logfile = None

    try:
        loglevel = config.get('config:loglevel')
    except KeyError as e:
        loglevel = 'WARN'
    loglevel = getattr(logging, loglevel.upper(), None)

    logging.basicConfig(filename=logfile, level=loglevel)
    logger = logging.getLogger()

    logger.info("Starting backup process.")

    # Pull the apikey and userid from config, also get the bootstrap file so we can get the agent ID.
    apikey = config.get('config:apikey')
    userid = config.get('config:userid')
    bootstrapfile = config.get('config:bootstrap')

    logger.debug("Found bootstrap file: {}".format(bootstrapfile))

    # Read the bootstrap file and get the AgentId and APIHost.  We need these to talk to the API.
    cfile = file(bootstrapfile, 'r')
    bootstrap = json.load(cfile)

    agentid = bootstrap['AgentId']
    apihost = bootstrap['ApiHostName']

    logger.debug("Our AgentID: {0}, Our APIHost: {1}".format(agentid, apihost))

    backupconf = config.get('backupconfig')

    logger.debug("Got backup config: {}".format(backupconf))

    backupmanager = BackupManager(userid, apikey, agentid, apihost)

    # Attempt to load the backup config specified in the config file
    try:
        backupconfigid = config.get('backupconfigid:configid')
        backupmanager.load_config(backupconfigid)
    # No backupid in config file, so lets create a backup config.
    except KeyError as e:
        logger.debug("Previous backup config not found, creating a new one.")
        backupconfigid = backupmanager.create_config(backupconf)
        if backupconfigid is not None:
            config.set('backupconfigid:configid', backupconfigid)
            config.write_empty_values = True
            config.write()
        else:
            logger.error("Failed to create backup config! Exiting.")
            exit (-1)

    logger.debug("Got backup config id: {}".format(backupconfigid))

    logger.info("Waking agent to start backup.")

    ret = backupmanager.wake_agent()
    if ret == False:
        logger.error("Failed to wake agent, exiting.")
        exit (-1)

    # Execute the pre-script, if it exists and is executable.
    if 'BackupPrescript' in backupconf:
        prescript = backupconf['BackupPrescript']
        preargs = shlex.split(prescript)
        if preargs != [] and os.path.isfile(preargs[0]) and os.access(preargs[0], os.X_OK):
            logger.info("Running prescript: {}".format(preargs))
            try:
                ret = subprocess.call(preargs)
            except OSError as e:
                logger.error("Failed to execute prescript: {}".format(e))
                exit (-1)
            logger.info("Prescript complete, returned: {}".format(ret))
        else:
            logger.error("Did not run Prescript {0}. Does it exist?".format(preargs))

    logger.info("Starting backup using config: {}".format(backupconfigid))
    ret = backupmanager.start_backup(backupconfigid)
    if ret == None:
        logger.error("Backup failed! Exiting.")
        exit (-1)

    logger.info("Waiting for backup to complete.")
    ret = backupmanager.watch_backup()
    if ret == False:
        logger.error("Backup failed! Exiting.")
        exit (-1)

    ret = backupmanager.get_report()

    if ret == None:
        logger.error("Backup failed! Exiting.")
        exit (-1)

    logger.info("Backup finished.")
    logger.info("Got backup report: {}".format(ret))

    # Execute the post-script, if it exists and is executable.
    if 'BackupPostscript' in backupconf:
        postscript = backupconf['BackupPostscript']
        postargs = shlex.split(postscript)
        if postargs != [] and os.path.isfile(postargs[0]) and os.access(postargs[0], os.X_OK):
            logger.info("Running postscript: {}".format(postscript))
            try:
                ret = subprocess.call(postargs)
            except OSError as e:
                logger.error("Failed to execute postscript: {}".format(e))
            logger.info("Postscript complete, returned: {}".format(ret))
        else:
            logger.error("Did not run Postscript {0}. Does it exist?".format(postargs))

    logger.info("Backup complete!")

    sys.exit(0)
