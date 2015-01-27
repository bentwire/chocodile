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
    """Builds conf on passing in a dict."""
    def __init__(self, configfile):
        self.log = logging.getLogger(__name__)
        self.log.debug("Loading config from file: {}".format(configfile))
        self.configfile = configfile
        self.configobj = ConfigObj(configfile, interpolation=False)
        self.log.debug("INIT COMPLETE")

    @property
    def config(self):
        return self.configobj

    @config.setter
    def config(self, config):
        self.configobj = config

    def get(self, key):
        sections = string.split(key, ':')
        self.log.debug("Found sections: {}".format(sections))
        root = self.configobj
        for section in sections:
            root = root[section]
        return root

    def set(self, key, value):
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
        self.configobj.write()

    @staticmethod
    def read(configfile):
        configobj = ConfigObj(configfile, interpolation=False)

        config = Config(configobj)
        config.config = configobj

        return config

    @staticmethod
    def find_config(name):
        candidate_paths = [
                os.path.join('/etc', name, CONFIG_FILENAME),
                os.path.join(os.environ['HOME'], '.'+name, CONFIG_FILENAME),
                os.path.join(CONFIG_DIRNAME, CONFIG_FILENAME),
                os.path.join(os.path.abspath('ini'), CONFIG_FILENAME)
        ]

        print("Candidate paths: {}".format(candidate_paths))

        for candidate in candidate_paths:
            if os.path.exists(candidate):
                return candidate
        
        return None
        
if __name__ == '__main__':
    logging.basicConfig()
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    configfile = Config.find_config("backup")
    config = Config(configfile)

    foo = config.config['foo']
    bar = foo['bar']
    baz = bar['baz']

    print("Got foo:bar, baz: {}".format(baz))

    baz = config.get('foo:bar:baz')
    print("Got foo:bar, baz: {}".format(baz))

    config.set('foo:baz:bat', 'bar')

    bat = config.get('foo:baz:bat')

    print("Got bat: {}".format(bat))

    config.write()
