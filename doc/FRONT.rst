.. This file is a part of the AnyBlok / Bus project
..
..    Copyright (C) 2017 Jean-Sebastien SUZANNE <jssuzanne@anybox.fr>
..    Copyright (C) 2018 Jean-Sebastien SUZANNE <jssuzanne@anybox.fr>
..    Copyright (C) 2019 Jean-Sebastien SUZANNE <js.suzanne@gmail.com>
..
.. This Source Code Form is subject to the terms of the Mozilla Public License,
.. v. 2.0. If a copy of the MPL was not distributed with this file,You can
.. obtain one at http://mozilla.org/MPL/2.0/.

Front Matter
============

Information about the AnyBlok / Bus project.

Project Homepage
----------------

AnyBlok is hosted on `github <http://github.com>`_ - the main project
page is at https://github.com/AnyBlok/anyblok_bus. Source code is
tracked here using `GIT <https://git-scm.com>`_.

Releases and project status are available on Pypi at
http://pypi.python.org/pypi/anyblok_bus.

The most recent published version of this documentation should be at
http://doc.anyblok-bus.anyblok.org.

Installation
------------

Install released versions of AnyBlok from the Python package index with
`pip <http://pypi.python.org/pypi/pip>`_ or a similar tool::

    pip install anyblok_bus

Installation via source distribution is via the ``setup.py`` script::

    python setup.py install

Installation will add the ``anyblok`` commands to the environment.

Unit Test
---------

Run the test with ``nose``::

    pip install nose
    nosetests anyblok_bus/tests

Script
------

anyblok_bus add ``console_script`` to launch worker. A worker consume a queue defined
by the decorator **anyblok_bus.bus_consumer**::

    anyblok_bus -c anyblok_config_file.cfg

..note:: The profile name in the configuration is used to find the correct url to connect to rabbitmq

Dependencies
------------

AnyBlok / Bus works with **Python >= 3.4** and later and `pika >= 1.0.1 <https://pika.readthedocs.io>`_. The install process will
ensure that `AnyBlok <http://doc.anyblok.org>`_ is installed, in addition to other 
dependencies. The latest version of them is strongly recommended.

Author
------

Jean-Sébastien Suzanne

Contributors
------------

`Anybox <http://anybox.fr>`_ team:

* Jean-Sébastien Suzanne
* Florent Jouatte

`Sensee <http://sensee.com>`_ team:

* Julien SZKUDLAPSKI
* Jean-Sébastien Suzanne

Bugs
----

Bugs and feature enhancements to AnyBlok should be reported on the `Issue
tracker <https://github.com/AnyBlok/anyblok_bus/issues>`_.
