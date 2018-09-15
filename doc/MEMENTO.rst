.. This file is a part of the AnyBlok / Bus project
..
..    Copyright (C) 2018 Jean-Sebastien SUZANNE <jssuzanne@anybox.fr>
..
.. This Source Code Form is subject to the terms of the Mozilla Public License,
.. v. 2.0. If a copy of the MPL was not distributed with this file,You can
.. obtain one at http://mozilla.org/MPL/2.0/.

.. contents::

Usage
=====

Declare a new consumer
----------------------

In an AnyBlok Model you have to decorate a method with **bus_consumer**

::

    from  anyblok_bus import bus_consumer
    from anyblok import Declarations
    from .schema import MySchema

    @Declarations.register(Declarations.Model)
    class MyModel:

        @bus_consumer(queue_name='name of the queue', schema=MySchema())
        def my_consumer(cls, body):
            # do something


The schema must be an instance of marshmallow.Schema, `see the marshmallow documentation <http://marshmallow.readthedocs.io/en/latest/>`_

.. note:: 

    The decorated method become a classmethod with always the same prototype (cls, body)
    body is the desarialization of the message from the queue by the schema.


Publish a message through rabbitmq
----------------------------------

The publication is done by **registry.Bus.publish** method::

    registry.Bus.publish('exchange', 'routing_key', message, mimestype)

if the message have not be send, then an exception is raised

..warning::

    A profile must be defined and selected by the AnyBlok configuration **bus_profile**
