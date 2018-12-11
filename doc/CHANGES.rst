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

1.2.0
-----

* Fixed Multiple consumer on the same model
* Refactored bus console script, Added processes parameter on bus_consumer.
  The goal is to define processes for one queue, by default all the queues 
  are in the same process
* Added better log when a queue is missing. Now if queues are missing, the 
  workers won't run
* Added adapter parameter to transform bus message, the schema attribute
  become now a simple kwargs argument give to adapter.

  The adapter is not required.

  .. note::
  
      To keep the compatibility, if no adapter is defined with a schema then
      the adapter is schema_adapter

1.1.0 (2018-09-15)
------------------

* Improved logging: for helping to debug the messages
* Added create and update date columns
* fixed ``consume_all`` method. now the method does not stop when an exception is raised
* Used marsmallow version >= 3.0.0

1.0.0 (2018-06-05)
------------------

* add Worker to consume the message from rabbitmq
* add publish method to publish a message to rabbitmq
* add **anyblok_bus.bus_consumer** add decorator to défine the consumer
