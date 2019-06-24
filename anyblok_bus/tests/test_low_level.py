# This file is a part of the AnyBlok / Bus api project
#
#    Copyright (C) 2018 Jean-Sebastien SUZANNE <jssuzanne@anybox.fr>
#
# This Source Code Form is subject to the terms of the Mozilla Public License,
# v. 2.0. If a copy of the MPL was not distributed with this file,You can
# obtain one at http://mozilla.org/MPL/2.0/.
import pytest
from .conftest import init_registry_with_bloks
from anyblok_bus import bus_consumer
from anyblok.column import Integer, String
from marshmallow import Schema, fields
from json import dumps, loads
from anyblok import Declarations
from anyblok_bus.status import MessageStatus
import pika
from time import sleep
from anyblok.config import Configuration
from contextlib import contextmanager
from pika.exceptions import (ConnectionClosedByBroker, ChannelClosedByBroker,
                             ProbableAccessDeniedError)
from anyblok_bus.worker import Worker
from threading import Thread
pika_url = 'amqp://guest:guest@127.0.0.1:5672/%2F'


@contextmanager
def get_channel():
    parameters = pika.URLParameters(pika_url)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.exchange_declare('unittest_exchange', durable=True)
    channel.queue_declare('unittest_queue', durable=True)
    channel.queue_bind('unittest_queue', 'unittest_exchange',
                       routing_key='unittest')
    channel.queue_purge('unittest_queue')  # case of the queue exist
    try:
        yield channel
    finally:
        channel.close()
        connection.close()


class OneSchema(Schema):
    label = fields.String(required=True)
    number = fields.Integer(required=True)


def add_in_registry():

    @Declarations.register(Declarations.Model)
    class Test:
        id = Integer(primary_key=True)
        label = String()
        number = Integer()

        @bus_consumer(queue_name='unittest_queue', schema=OneSchema())
        def decorated_method(cls, body=None):
            cls.insert(**body)
            return MessageStatus.ACK


@pytest.fixture(scope="class")
def registry(request, bloks_loaded):
    bus_profile = Configuration.get('bus_profile')
    registry = init_registry_with_bloks(('bus',), add_in_registry)
    registry.Bus.Profile.insert(name=bus_profile, url=pika_url)
    request.addfinalizer(registry.close)
    return registry


class TestPublish:

    @pytest.fixture(autouse=True)
    def transact(self, request, registry):
        transaction = registry.begin_nested()
        request.addfinalizer(transaction.rollback)

    def test_publish_ok(self, registry):
        with get_channel() as channel:
            registry.Bus.publish('unittest_exchange', 'unittest',
                                 dumps({'hello': 'world'}),
                                 'application/json')
            method_frame, header_frame, body = channel.basic_get(
                'unittest_queue')
            assert method_frame is not None

    def test_publish_wrong_url(self, registry):
        bus_profile = Configuration.get('bus_profile')
        registry.Bus.Profile.query().get(bus_profile).delete()
        registry.Bus.Profile.insert(
            name=bus_profile,
            url='amqp://guest:guest@localhost:5672/%2Fwrongvhost')
        with pytest.raises((ConnectionClosedByBroker,
                            ProbableAccessDeniedError)):
            registry.Bus.publish('unittest_exchange', 'unittest',
                                 dumps({'hello': 'world'}),
                                 'application/json')

    def test_publish_wrong_exchange(self, registry):
        with get_channel() as channel:
            with pytest.raises((ChannelClosedByBroker,
                                ProbableAccessDeniedError,
                                ConnectionClosedByBroker)):
                registry.Bus.publish('wrong_exchange', 'unittest',
                                     dumps({'hello': 'world'}),
                                     'application/json')

            method_frame, header_frame, body = channel.basic_get(
                'unittest_queue')


class AnyBlokWorker(Thread):
    def __init__(self, registry, profile):
        super(AnyBlokWorker, self).__init__()
        consumers = registry.Bus.get_consumers()
        if consumers:
            consumers = consumers[0][1]

        self.worker = Worker(registry, profile, consumers,
                             withautocommit=False)

    def run(self):
        self.worker.start()

    def is_consumer_ready(self):
        return self.worker.is_ready()

    def stop(self):
        self.worker.stop()


class TestConsumer:

    @pytest.fixture(autouse=True)
    def transact(self, request, registry):
        transaction = registry.begin_nested()
        request.addfinalizer(transaction.rollback)

    def test_consume_close_without_consumer(self, registry):
        with get_channel():
            bus_profile = Configuration.get('bus_profile')
            thread = AnyBlokWorker(registry, bus_profile)
            thread.start()
            while not thread.is_consumer_ready():
                pass

            thread.stop()
            thread.join()

    def test_consume_close_with_consumer(self, registry):
        with get_channel():
            bus_profile = Configuration.get('bus_profile')
            thread = AnyBlokWorker(registry, bus_profile)
            thread.start()
            while not thread.is_consumer_ready():
                pass

            thread.stop()
            thread.join()

    def test_consume_ok(self, registry):
        assert registry.Test.query().count() == 0
        assert registry.Bus.Message.query().count() == 0
        with get_channel():
            registry.Bus.publish('unittest_exchange', 'unittest',
                                 dumps({'label': 'label', 'number': 1}),
                                 'application/json')
            sleep(2)

        bus_profile = Configuration.get('bus_profile')
        thread = AnyBlokWorker(registry, bus_profile)
        thread.start()
        while not thread.is_consumer_ready():
            pass

        sleep(10)
        thread.stop()
        thread.join()

        assert registry.Test.query().count() == 1
        assert registry.Bus.Message.query().count() == 0

    def test_consume_ko(self, registry):
        assert registry.Test.query().count() == 0
        assert registry.Bus.Message.query().count() == 0
        with get_channel():
            registry.Bus.publish('unittest_exchange', 'unittest',
                                 dumps({'label': 'label'}),
                                 'application/json')
            sleep(2)

        bus_profile = Configuration.get('bus_profile')
        thread = AnyBlokWorker(registry, bus_profile)
        thread.start()
        while not thread.is_consumer_ready():
            pass

        sleep(10)
        thread.stop()
        thread.join()

        assert registry.Test.query().count() == 0
        assert registry.Bus.Message.query().count() == 1

    def test_get_unexisting_queues_ok(self, registry):
        with get_channel():
            assert registry.Bus.get_unexisting_queues() == []


class TestConsumer2:

    @pytest.fixture(autouse=True)
    def close_registry(self, request, bloks_loaded):

        def close():
            if hasattr(self, 'registry'):
                self.registry.close()

        request.addfinalizer(close)

    def init_registry_with_bloks(self, *args, **kwargs):
        self.registry = init_registry_with_bloks(('bus',), *args, **kwargs)
        return self.registry

    def test_consumer_without_adapter(self):

        def add_in_registry():

            @Declarations.register(Declarations.Model)
            class Test:
                id = Integer(primary_key=True)
                label = String()
                number = Integer()

                @bus_consumer(queue_name='unittest_queue')
                def decorated_method(cls, body=None):
                    cls.insert(**loads(body))
                    return MessageStatus.ACK

        with get_channel():
            bus_profile = Configuration.get('bus_profile')
            registry = self.init_registry_with_bloks(add_in_registry)
            registry.Bus.Profile.insert(name=bus_profile, url=pika_url)
            thread = AnyBlokWorker(registry, bus_profile)
            thread.start()
            while not thread.is_consumer_ready():
                pass

            assert registry.Test.query().count() == 0
            assert registry.Bus.Message.query().count() == 0
            registry.Bus.publish('unittest_exchange', 'unittest',
                                 dumps({'label': 'label', 'number': 1}),
                                 'application/json')
            sleep(2)

            assert registry.Test.query().count() == 1
            assert registry.Bus.Message.query().count() == 0
            thread.stop()
            thread.join()

    def test_get_unexisting_queues_ko(self):

        def add_in_registry():

            @Declarations.register(Declarations.Model)
            class Test:
                id = Integer(primary_key=True)
                label = String()
                number = Integer()

                @bus_consumer(queue_name='unexisting_unittest_queue',
                              schema=OneSchema())
                def decorated_method(cls, body=None):
                    cls.insert(**body)
                    return MessageStatus.ACK

        with get_channel():
            bus_profile = Configuration.get('bus_profile')
            registry = self.init_registry_with_bloks(add_in_registry)
            registry.Bus.Profile.insert(name=bus_profile, url=pika_url)
            assert (
                registry.Bus.get_unexisting_queues() ==
                ['unexisting_unittest_queue']
            )
