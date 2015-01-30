import logging
import os
import sys
import string

from configobj import ConfigObj
from validate import Validator

CONFIG_FILENAME = 'config.ini'

CONFIG_DIRNAME = os.path.join(sys.prefix, 'config')

#log = logging.getLogger(__name__)

class Config(object):
    def __init__(self, configfile):
        self.log = logging.getLogger(__name__)
        self.log.debug("Loading config from file: {}".format(configfile))
        self.configfile = configfile
        self.configobj = ConfigObj(configfile, interpolation=False)

    @property
    def config(self):
        return self.configobj

    @config.setter
    def config(self, config):
        self.configobj = config

    def get(self, key):
        """
        Get a value from the config file.

        The key is a ':' delimited string.
        Each part of the key represents a level of depth in the config file
        For example:
        foo:bar:baz would be represented by
        [foo]
            [[bar]]
                baz = bat
        """

        sections = string.split(key, ':')
        self.log.debug("Found sections: {}".format(sections))
        root = self.configobj
        for section in sections:
            root = root[section]
        return root

    def set(self, key, value):
        """
        Set a value in the config file.

        The key is a ':' delimited string.
        Each part of the key represents a level of depth in the config file
        For example:
        foo:bar:baz would be represented by
        [foo]
            [[bar]]
                baz = bat

        Be aware, in order to persist the change you must run write()
        """

        sections = string.split(key, ':')
        self.log.debug("Found sections: {}".format(sections))
        root = self.configobj
        for section in sections[0:-1]:
            if section in root:
                root = root[section]
            else:
                self.log.debug("Creating new section: {}".format(section))
                root[section] = {}
                root = root[section]
        root[sections[-1]] = value

    def write(self):
        """
        Write config to disk. 
        
        Uses the same file as what was used to read the config.
        """

        self.configobj.write()

    @staticmethod
    def find_config(name):
        """
        Find the config file to use based on standard paths.

        This function is used to find a config file in the system.
        Config files can be in several standard locations
        """

        candidate_paths = [
                os.path.join('/etc', name, CONFIG_FILENAME),
                os.path.join(os.environ['HOME'], '.'+name, CONFIG_FILENAME),
                os.path.join(CONFIG_DIRNAME, CONFIG_FILENAME),
                os.path.join(os.path.abspath('ini'), CONFIG_FILENAME)
        ]

#        self.log.debug("Candidate paths: {}".format(candidate_paths))

        for candidate in candidate_paths:
            if os.path.exists(candidate):
                return candidate
        
        return None
