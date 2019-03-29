# -- CromulentApp

from pyhocon import ConfigFactory
import logging, os

class CromulentApp(object):
    def __init__(self, config_fname):
        '''
        The Cromulent App with Cromwell's HOCON Config
        '''
        if config_fname is not None:
            logging.getLogger('root').info('Using config at {0}'.format(config_fname))
            self.config = ConfigFactory.parse_file(config_fname)


# -- CromulentApp (end)
