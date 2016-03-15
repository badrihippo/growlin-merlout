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
from growlin import models

def write_usergroups(reader):
    '''
    Import User Groups from a csv reader serving a file of format:
    ['GroupID', 'GroupName']
    '''

    if reader.next() != ['GroupID', 'GroupName']:
        raise ValueError("CSV format must be ['GroupID', 'GroupName']")

    for d in reader:
        models.UserGroup.objects(name=d[1]).update(set__position=d[0], upsert=True)

def write_users(reader):
    '''
    Import Users from a csv DictReader
    '''

    for d in reader:
        try:
            group = models.UserGroup.objects.get(name=d['GroupName'].strip())
        except models.db.DoesNotExist:
            group = models.UserGroup.objects.create(name=d['GroupName'].strip())

        email = d['Email'].strip() if d['Email'] else None
        name = d['UserName'].strip()
        username = name.lower()
        
        models.User.objects(name=name).update(set__group=group, set__email=email, set__username=username, upsert=True)

def write_currencies(reader):
    '''
    Import Currencies from a csv DictReader
    '''
    for d in reader:
        currency = d['Currency'].strip()
        models.Currency.objects(name=currency).update(set__symbol=currency[:4], upsert=True)

def write_publishers(reader):
    '''
    Import Publisher from a csv DictReader
    '''
    for d in reader:
        name = d['Publisher'].strip()
        models.Publisher.objects(name=name).update(set__name=name, upsert=True)

def write_publishplaces(reader):
    '''
    Import PublishPlaces from a csv DictReader
    '''
    for d in reader:
        name = d['Place of Publication'].strip()
        models.PublishPlace.objects(name=name).update(set__name=name, upsert=True)

def write_locations(reader):
    '''
    Import CampusLocations from a csv DictReader
    '''
    for d in reader:
        name = d['Location'].strip()
        try:
            prevent_borrowing = bool(int(d['PreventBorrow'].strip()))
        except ValueError:
            prevent_borrowing = False
        models.CampusLocation.objects(name=name).update(set__prevent_borrowing=prevent_borrowing, upsert=True)

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

if __name__ == '__main__':
    process_all()

