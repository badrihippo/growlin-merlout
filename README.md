growlin-merlout
===============

Script to import data from the Merlin ILS and insert it into
[Growlin](http://github.com/badrihippo/growlin). Currently works
with the MongoDB version only.

To import data, you will need to have the `mdbtools` packange installed
on your system to run the first part of this script. Once installed,
copy the Merlin backup .mdb file, eg. `merlout.mdb`, and run the following
in a terminal:

    $ python AccessDump.py merlout.mdb
    $ python merlout.py

The commands may take a few minutes to return if your database is large.

Thanks to
[this Working with Data blog post](http://mazamascience.com/WorkingWithData/?p=168)
for the AccessDump.py script!
