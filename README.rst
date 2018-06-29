selectsql
---------

Workbench module that runs an SQL `SELECT` statement on its input.


Developing
----------

First, get up and running:

1. ``pip3 install pipenv``
2. ``pipenv sync`` # to download dependencies
3. ``pipenv run ./setup.py test`` # to test

To add a feature:

1. Write a test in ``tests/``
2. Run ``pipenv run ./setup.py test`` to prove it breaks
3. Edit ``scrapexpathlist.py`` to make the test pass
4. Run ``pipenv run ./setup.py test`` to prove it works
5. Commit and submit a pull request
