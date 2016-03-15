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
from growlin import models

def _get_or_create(model, *args, **kwargs):
    try:
        model = model.objects.get(*args, **kwargs)
    except model.DoesNotExist:
        model = model.objects.create(*args, **kwargs)
    return model

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

def write_bookitems(reader):
    '''
    Import BookItems from a csv DictReader
    '''
    for d in reader:
        accession = 'b:' + d['Accession'].strip()
        try:
            b = models.BookItem.objects.get(accession=accession)
        except models.db.DoesNotExist:
            b = models.BookItem()
            b.accession = accession
        b.call_nos = [d['Call number'].strip()[:8]]
        b.title = d['Title'].strip()

        b.authors.append(_get_or_create(models.Creator, name=d['Author'].strip()))

        if b.publication is None: b.publication = models.BookPublicationDetails()
        b.publication.publisher = _get_or_create(models.Publisher, name=d['Publisher'])
        b.publication.place = _get_or_create(models.PublishPlace, name=d['Place of publication'])
        try:
            pub_year = int(d['Date of publication'].split(' ')[-1])
            if pub_year == 0: pub_year = None
        except:
            pub_year = None
        b.publication.pub_year = pub_year

        try:
            b.receipt_date = datetime.strptime(d['Date of receipt'], '%d %b %Y')
        except ValueError:
            b.receipt_date = None
        b.currency = _get_or_create(models.Currency, name=d['Currency'].strip())
        try:
            b.price = float(d['Price'])
        except ValueError:
            b.price = None
        b.source = d['Source'][:128]
        b.campus_location = _get_or_create(models.CampusLocation, name=d['Location'])

        b.save()

def write_borrowcurrent(reader):
    two_weeks = timedelta(days=14)
    for d in reader:
        accession = 'b:' + d['Accession']
        try:
            b = models.BookItem.objects.get(accession=accession)
        except models.BookItem.DoesNotExist:
            print 'Warning: Skipping %(accession)s - "%(title)s", borrowed by %(user)s: Record does note exist' % {
                'accession': accession,
                'title': d['Title'],
                'user': d['UserName']
                }
            continue

        user, group = d['UserName'].strip().split(',')
        try:
            g = models.UserGroup.objects.get(name=group)
            u = models.User.objects.get(name=user, group=g)
        except models.db.DoesNotExist:
            print 'Warning: Skipping %(accession)s - "%(title)s", borrowed by %(user)s: User does not exist!' % {
                'accession': b.accession,
                'title': b.title,
                'user': d['UserName'],
            }
            continue

        if b.borrow_current is not None:
            if b.borrow_current.user != u: # Borrowed by someone else
                print 'Warning: Skipping %(accession)s - "%(title)s", borrowed by %(user)s: Item already borrowed by %(borrower)s' % {
                    'accession': b.accession,
                    'title': b.title,
                    'user': d['UserName'],
                    'borrower': b.borrow_current.user.name
                }
                continue
        else:
            b.borrow_current = models.BorrowCurrent()

        b.borrow_current.user = u
        try:
            date_string = d['Date Borrowed'].strip()
            b.borrow_current.borrow_date = datetime.strptime(date_string, '%m/%d/%Y %H:%M:%S')
        except ValueError:
            try:
                b.borrow_current.borrow_date = datetime.strptime(date_string, '%m/%d/%y %H:%M:%S')
            except ValueError:
                print 'Warning: Unset borrow date for %(accession)s - "%(title)s": "%(date)s" is not a valid date' % {
                    'accession': b.accession,
                    'title': b.title,
                    'user': d['UserName'],
                    'date': date_string
                }
            if b.borrow_current.borrow_date is not None:
                b.borrow_current.due_date = b.borrow_current.borrow_date + two_weeks
        b.save()

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

