import csv
from datetime import datetime, timedelta
from growlin import models

def _get_or_create(model, *args, **kwargs):
    try:
        model = model.objects.get(*args, **kwargs)
    except model.DoesNotExist:
        model = model.objects.create(*args, **kwargs)
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

def create_tables(): pass # For peewee compat

def write_usergroups(reader):
    '''
    Import User Groups from a csv DictReader serving a file of format:
    ['GroupID', 'GroupName']
    '''

    for d in reader:
        models.UserGroup.objects(name=d['GroupName']).update(set__position=d['GroupID'], upsert=True)

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
        username = ''.join([name, '.', group.name]).lower()[:32]
        
        models.User.objects(name=name, group=group).update(set__email=email, set__username=username, upsert=True)

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
    try:
        it = models.ItemType.objects.get(name='book')
    except models.ItemType.DoesNotExist:
        print 'Creating ItemType: book'
        it = models.ItemType()
        it.name='book'
        it.prefix=''
        it.save()
    if it.prefix == '':
        prefix = ''
    else:
        prefix = '%s:' % it.prefix
    for d in reader:
        accession = prefix + d['Accession'].strip()
        try:
            b = models.BookItem.objects.get(accession=accession)
        except models.db.DoesNotExist:
            b = models.BookItem()
            b.accession = accession
        b.call_nos = [d['Call number'].strip()[:8]]
        b.title = d['Title'].strip()

        b.authors.append(_get_or_create(models.Creator, name=d['Author'].strip()))

        b.publication_publisher = _get_or_create(models.Publisher, name=d['Publisher'])
        b.publication_place = _get_or_create(models.PublishPlace, name=d['Place of publication'])
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

    # Generate dictionary of prefixes
    prefixes = {}
    for c in ('book', 'periodical'):
        try:
            it = models.ItemType.objects.get(name=c)
            if it.prefix:
                prefix = '%s:' % it.prefix
            else:
                prefix = ''
        except models.ItemType.DoesNotExist:
            prefix = ''
        prefixes[c] = prefix

    for d in reader:
        if d['Category'] == '1':
            # It's a Book.
            accession = prefixes['book'] + d['Accession']
        elif d['Category'] == '2':
            # It's a Periodical.
            accession = prefixes['periodical'] + d['Accession']
        else:
            accession = d['Accession']

        try:
            b = models.BookItem.objects.get(accession=accession)
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

def set_admin():
    try:
        u = models.User.objects.get(name='Amogh')
        print 'Set Amogh as admin (y/N)?',
        ans = raw_input()
        if ans[0] in ('y', 'Y'):
            r = _get_or_create(models.UserRole, name='admin')
            u.roles.append(r)
            u.save()
            print 'Administrator added'
        else:
            print 'Abort.'
    except Exception, e:
        print e
