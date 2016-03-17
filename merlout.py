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

def process_all():
    print 'Importing UserGroups...'
    with open('List_of_Groups.csv', 'rb') as f:
        r = csv.reader(f)
        write_usergroups(r)

    print 'Importing Users...'
    with open('List_of_Users.csv', 'rb') as f:
        r = csv.DictReader(f)
        write_users(r)

    print 'Importing Currencies...'
    with open('List_of_Currencies.csv', 'rb') as f:
        r = csv.DictReader(f)
        write_currencies(r)

    print 'Importing Publishers...'
    with open('List_of_Publishers.csv', 'rb') as f:
        r = csv.DictReader(f)
        write_publishers(r)

    print 'Importing PublishPlaces...'
    with open('List_of_Places_of_Publication.csv', 'rb') as f:
        r = csv.DictReader(f)
        write_publishplaces(r)

    print 'Importing CampusLocations...'
    with open('List_of_Locations.csv', 'rb') as f:
        r = csv.DictReader(f)
        write_locations(r)

    print 'Importing BookItems...'
    with open('Accession_Register.csv', 'rb') as f:
        r = csv.DictReader(f)
        write_bookitems(r)

    print 'Importing BorrowCurrent...'
    with open('Current_Issues.csv', 'rb') as f:
        r = csv.DictReader(f)
        write_borrowcurrent(r)

if __name__ == '__main__':
    process_all()

