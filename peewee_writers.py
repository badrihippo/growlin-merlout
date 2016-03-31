import os, sys

# Add paths for easy import
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)),'../growlin'))
sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)),'../growlin/flask-admin-material'))

import csv
from datetime import datetime, timedelta
from growlin import models
from peewee import InsertQuery

def _get_or_create(model, *args, **kwargs):
    try:
        model = model.get(*args, **kwargs)
    except model.DoesNotExist:
        model = model.create(*args, **kwargs)
    return model

def _user_from_userid(userid):
    '''
    This functions takes in a string of the form
    'User, Group' and returns the user and group.
    Some users have commas in their names.
    '''

    usergroup = userid.strip().split(',')
    group = usergroup[-1].strip()
    user = ''.join(usergroup[:-1]).strip()

    return user, group

def create_tables():
    '''
    Create database tables for data insertion
    '''

    print 'Creating tables...'
    
    models.db.database.create_tables([
        models.CampusLocation,
        models.UserGroup,
        models.UserRole,
        models.Currency,
        models.User,
        models.UserRoles,
        models.Publisher,
        models.PublishPlace,
        models.Genre,
        models.ItemType,
        models.PeriodicalSubscription,
        models.Creator,
        models.FullItem,
        models.BorrowCurrent,
        models.BorrowPast
    ])
def write_usergroups(reader):
    '''
    Import User Groups from a csv DictReader serving a file of format:
    ['GroupID', 'GroupName']
    '''
    usergroups = [{'name': d['GroupName'], 'position': d['GroupID']} for d in reader]
    iq = InsertQuery(models.UserGroup, rows=usergroups)
    iq.execute()

def write_users(reader):
    '''
    Import Users from a csv DictReader
    '''
    users = []
    for d in reader:
        try:
            group = models.UserGroup.get(name=d['GroupName'].strip())
        except models.db.DoesNotExist:
            group = models.UserGroup.objects.create(name=d['GroupName'].strip())

        u = {}
        u['email'] = d['Email'].strip() if d['Email'] else None
        u['name'] = d['UserName'].strip()
        u['username'] = ''.join([u['name'], '.', group.name]).lower()[:32]
        u['group'] = group
        users.append(u)
    iq = InsertQuery(models.User, rows=users)
    iq.execute()

def write_currencies(reader):
    '''
    Import Currencies from a csv DictReader
    '''
    currencies = [{'name': d['Currency'], 'symbol': d['Currency'].strip()[:4]} for d in reader]
    iq = InsertQuery(models.Currency, rows=currencies)
    iq.execute()

def write_publishers(reader):
    '''
    Import Publisher from a csv DictReader
    '''
    publishers = [{'name': d['Publisher'].strip()} for d in reader]
    iq = InsertQuery(models.Publisher, rows=publishers)
    iq.execute()

def write_publishplaces(reader):
    '''
    Import PublishPlaces from a csv DictReader
    '''

    l = [{'name': d['Place of Publication'].strip()} for d in reader]
    iq = InsertQuery(models.PublishPlace, rows=l)
    iq.execute()

def write_locations(reader):
    '''
    Import CampusLocations from a csv DictReader
    '''
    l = []
    for d in reader:
        u = {}
        u['name'] = d['Location'].strip()
        try:
            u['prevent_borrowing'] = bool(int(d['PreventBorrow'].strip()))
        except ValueError:
            u['prevent_borrowing'] = False
        l.append(u)
    iq = InsertQuery(models.CampusLocation, rows=l)
    iq.execute()

def write_bookitems(reader):
    '''
    Import BookItems from a csv DictReader
    '''
    it, created = models.ItemType.get_or_create(name='book', defaults={'prefix':'b'})
    for d in reader:
        accession = 'b:' + d['Accession'].strip()
        b = models.BookItem()
        b.item_type = it
        b.accession = accession
        b.call_no = d['Call number'].strip()[:8]
        b.title = d['Title'].strip()

        b.author, created = models.Creator.get_or_create(name=d['Author'].strip())

        b.publication_publisher, created = models.Publisher.get_or_create(name=d['Publisher'])
        b.publication_place, created = models.PublishPlace.get_or_create(name=d['Place of publication'])
        try:
            pub_year = int(d['Date of publication'].split(' ')[-1])
            if pub_year == 0: pub_year = None
        except:
            pub_year = None
        b.publication_year = pub_year

        try:
            b.receipt_date = datetime.strptime(d['Date of receipt'], '%d %b %Y')
        except ValueError:
            b.receipt_date = None
        b.currency, created = models.Currency.get_or_create(name=d['Currency'].strip())
        try:
            b.price = float(d['Price'])
        except ValueError:
            b.price = None
        b.source = d['Source'][:128]
        b.campus_location, created = models.CampusLocation.get_or_create(name=d['Location'])
        b.save()

def write_borrowcurrent(reader):
    two_weeks = timedelta(days=14)
    for d in reader:
        if d['Category'] == '1':
            # It's a Book.
            accession = 'b:' + d['Accession']
        elif d['Category'] == '2':
            # It's a Periodical
            accession = 'p:' + d['Accession']
        else:
            accession = d['Accession']
        try:
            b = models.BookItem.get(accession=accession)
        except models.BookItem.DoesNotExist:
            print 'Warning: Skipping %(accession)s - "%(title)s", borrowed by %(user)s: Record does note exist' % {
                'accession': accession,
                'title': d['Title'],
                'user': d['UserName']
                }
            continue

        user, group = _user_from_userid(d['UserName'])
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
        bc, created = models.BorrowCurrent.get_or_create(user=u, item=b)
        if not created:
            if b.borrow_current.user != u: # Borrowed by someone else
                print 'Warning: Skipping %(accession)s - "%(title)s", borrowed by %(user)s: Item already borrowed by %(borrower)s' % {
                    'accession': b.accession,
                    'title': b.title,
                    'user': d['UserName'],
                    'borrower': b.borrow_current.user.name
                }
                continue

        bc.user = u
        try:
            date_string = d['Date Borrowed'].strip()
            bc.borrow_date = datetime.strptime(date_string, '%m/%d/%Y %H:%M:%S')
        except ValueError:
            try:
                bc.borrow_date = datetime.strptime(date_string, '%m/%d/%y %H:%M:%S')
            except ValueError:
                print 'Warning: Unset borrow date for %(accession)s - "%(title)s": "%(date)s" is not a valid date' % {
                    'accession': b.accession,
                    'title': b.title,
                    'user': d['UserName'],
                    'date': date_string
                }
            if bc.borrow_date is not None:
                bc.due_date = bc.borrow_date + two_weeks
        bc.save()

def set_admin():
    try:
        u = models.User.get(name='Amogh')
        print 'Set Amogh as admin (y/N)?',
        ans = raw_input()
        if ans[0] in ('y', 'Y'):
            r, created = models.UserRole.get_or_create(name='admin')
            ur, created = models.UserRoles.get_or_create(user=u, role=r)
            if not created:
                print 'Amogh was already admin!'
            else:
                print 'Administrator added successfully'
        else:
            print 'Abort.'
    except Exception, e:
        pass
