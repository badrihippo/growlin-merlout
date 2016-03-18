#! /usr/bin/env python
#
# merlout.py
# Take the dumped Merlin Access Database files and import them to the
# new Growlin database

import os, sys

# Add paths for easy import
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)),'../growlin'))
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)),'../growlin/flask-admin-material'))

import csv
from datetime import datetime, timedelta
from growlin import app, models

if app.app.config['GROWLIN_USE_PEEWEE']:
    from peewee_writers import *
else:
    from mongo_writers import *

def process_one(function, filename):
    with open(filename, 'rb') as f:
        r = csv.DictReader(f)
        function(r)

def process_all():
    create_tables()
    print 'Importing UserGroups...'
    process_one(write_usergroups, 'List_of_Groups.csv')

    print 'Importing Users...'
    process_one(write_users, 'List_of_Users.csv')

    print 'Importing Currencies...'
    process_one(write_currencies, 'List_of_Currencies.csv')

    print 'Importing Publishers...'
    process_one(write_publishers, 'List_of_Publishers.csv')

    print 'Importing PublishPlaces...'
    process_one(write_publishplaces, 'List_of_Places_of_Publication.csv')

    print 'Importing CampusLocations...'
    process_one(write_locations, 'List_of_Locations.csv')

    print 'Importing BookItems...'
    process_one(write_bookitems, 'Accession_Register.csv')

    print 'Importing BorrowCurrent...'
    process_one(write_borrowcurrent, 'Current_Issues.csv')

if __name__ == '__main__':
    process_all()

