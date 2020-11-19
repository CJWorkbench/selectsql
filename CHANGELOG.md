2020-11-19.01
-------------

* Add error for "you are only allowed one query at a time".

2020-06-17.01
-------------

* Add error for "query was commented out".
* Pass-through datetimes. (This isn't _full_ support for datetimes ...
  but it's better than before.)
* Use pandas from_records(), not read_sql(). Error handling is cleaner.
