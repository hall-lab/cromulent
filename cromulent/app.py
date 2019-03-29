# -- CromulentApp

import pymysql.cursors
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

    # -- __init

    def __del__(self):
        if self.db is not None:
            self.db.close

    # -- __del__

    def connect(self):
        if self.db is not None: return self.db

        host = self.config.get("database.db.host", "localhost")
        port = self.config.get("database.db.port", "3306")
        url = self.config.get("database.db.url")
        if url is not None:
            #"jdbc:mysql://cromwell-mysql:3306/cromwell?rewriteBatchedStatements=true&useSSL=false"
            (host, port) = url.split("/")[2].split(":")

        self.db = pymysql.connect(
            host=host,
            port=port,
            user=self.config.get("database.db.user", "root"),
            password=self.config.get("database.db.password"), # only thing without a default
            db=self.config.get("database.db.password", "cromwell"),
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
        )
        return self.db

    # -- connect

# -- CromulentApp (end)
