# -- CromulentApp

import pymysql.cursors
from pyhocon import ConfigFactory
import logging, os, re
import sqlite3

class CromulentApp(object):
    def __init__(self, config_fname=None):
        '''
        The Cromulent App with Cromwell's HOCON Config
        '''
        self.config = None
        self.db = None
        if config_fname is not None:
            logging.getLogger('root').info('Using config at {0}'.format(config_fname))
            config_str = ''
            with open(config_fname, 'r') as f:
                for l in f.readlines():
                    if re.search("^\s*include required", l): continue
                    config_str += l
            self.config = ConfigFactory.parse_string(config_str)

    # -- __init

    def __del__(self):
        if self.db is not None:
            self.db.close

    # -- __del__

    def connect(self):
        if self.db is not None: return self.db

        assert self.config is not None, "No configuration found to connect to database!"

        if self.config.get("database.db.file", None):
            self._connect_sqlite()
        else:
            self._connect_mysql()
        return self.db

    def _connect_mysql(self):
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

    def _connect_sqlite(self):
        self.db = sqlite3.connect( self.config.get("database.db.file") )

    # -- connect

# -- CromulentApp (end)
