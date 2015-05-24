import unittest
from datetime import datetime

import odm

from sqlalchemy import Column, Integer, String, Boolean, DateTime

from pulsar.apps.test import TestPlugin


class PostgreSql(TestPlugin):
    name = 'postgresql'
    meta = "CONNECTION_STRING"
    default = 'postgresql+async://postgres@127.0.0.1:5432'
    desc = 'Default connection string for the PostgreSql server'


class Task(odm.Model):
    id = Column(Integer, primary_key=True)
    subject = Column(String(250))
    done = Column(Boolean, default=False)
    created = Column(DateTime, default=datetime.utcnow)


class TestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Create the application
        cls.dbs = {}
        logger.info('Create test databases')
        cls.setupdb()
        logger.info('Create test tables')
        cls.table_create()
