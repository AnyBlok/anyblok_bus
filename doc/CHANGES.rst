.. This file is a part of the AnyBlok / Bus project
..
..    Copyright (C) 2018 Jean-Sebastien SUZANNE <jssuzanne@anybox.fr>
..
.. This Source Code Form is subject to the terms of the Mozilla Public License,
.. v. 2.0. If a copy of the MPL was not distributed with this file,You can
.. obtain one at http://mozilla.org/MPL/2.0/.

.. contents::

CHANGELOG
=========

1.0.0 (2018-09-15)
------------------

* Improved logging: for helping to debug the messages
* Added create and update date columns
* fixed ``consume_all`` method. now the method does not stop when an exception is raised
* Used marsmallow version >= 3.0.0

0.1.0 (2018-06-05)
------------------

* add Worker to consume the message from rabbitmq
* add publish method to publish a message to rabbitmq
* add **anyblok_bus.bus_consumer** add decorator to défine the consumer
