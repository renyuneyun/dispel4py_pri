#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#
#   Author  :   renyuneyun
#   E-mail  :   renyuneyun@gmail.com
#   Date    :   17/07/15 12:04:02
#   License :   Apache 2.0 (See LICENSE)
#

'''

'''

from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Boolean, Integer, Float, String, MetaData, ForeignKey, UniqueConstraint
from sqlalchemy.sql import expression

#engine = create_engine('sqlite:///:memory:', echo=True)
engine = create_engine('sqlite:///records.sqlite3', echo=True)

metadata = MetaData()
record = Table('record', metadata,
        Column('id', Integer, primary_key=True),
        Column('outlier', Boolean, server_default=expression.false()),
        Column('platform', String),
        Column('version', String),
        Column('run_id', String),
        Column('module', String),
        Column('num_iter', Integer),
        Column('np', Integer),
        Column('max_num_sieve', Integer),
        Column('max_prime', Integer),
        Column('time', Float),
        UniqueConstraint('platform', 'version', 'run_id', 'module', 'num_iter', 'np', 'max_num_sieve', 'max_prime', 'time'),
        )

metadata.create_all(engine)

