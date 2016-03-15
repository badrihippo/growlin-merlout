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

def process_all():
    print 'Importing UserGroups...'
    with open('List_of_Groups.csv', 'rb') as f:
        r = csv.reader(f)
        write_usergroups(r)

    print 'Import Users...'
    with open('List_of_Users.csv', 'rb') as f:
        r = csv.DictReader(f)
        write_users(r)

if __name__ == '__main__':
    process_all()

