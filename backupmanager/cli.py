import os
import sys
import logging
import json

from config import Config
from backup import BackupManager

def run():
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
