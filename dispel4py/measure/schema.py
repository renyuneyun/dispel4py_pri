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
from sqlalchemy import Table, Column, Integer, Float, String, MetaData, ForeignKey

#engine = create_engine('sqlite:///:memory:', echo=True)
engine = create_engine('sqlite:///records.sqlite3', echo=True)

metadata = MetaData()
record = Table('record', metadata,
        Column('id', Integer, primary_key=True),
        Column('platform', String),
        Column('num_iter', Integer),
        Column('np_mpi_inc', Integer),
        Column('max_num_sieve', Integer),
        Column('max_prime', Integer),
        Column('mpi_time', Float),
        Column('mpi_inc_time', Float),
        )

metadata.create_all(engine)

