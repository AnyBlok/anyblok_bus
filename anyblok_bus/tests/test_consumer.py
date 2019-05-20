# This file is a part of the AnyBlok / Bus api project
#
#    Copyright (C) 2018 Jean-Sebastien SUZANNE <jssuzanne@anybox.fr>
#    Copyright (C) 2019 Jean-Sebastien SUZANNE <js.suzanne@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public License,
# v. 2.0. If a copy of the MPL was not distributed with this file,You can
# obtain one at http://mozilla.org/MPL/2.0/.
import pytest
from .conftest import init_registry_with_bloks, reload_registry
from anyblok.config import Configuration
from anyblok_bus.consumer import (bus_consumer, BusConfigurationException)
from anyblok_bus.bloks.bus.exceptions import TwiceQueueConsumptionException
from marshmallow import Schema, fields
from json import dumps
from anyblok import Declarations
from marshmallow.exceptions import ValidationError


class OneSchema(Schema):
    label = fields.String()
    number = fields.Integer()


def add_in_registry():

    @Declarations.register(Declarations.Model)
    class Test:

        @bus_consumer(queue_name='test', schema=OneSchema())
        def decorated_method(cls, body=None):
            return body


@pytest.fixture(scope="class")
def registry(request, bloks_loaded):
    registry = init_registry_with_bloks(('bus',), add_in_registry)
    request.addfinalizer(registry.close)
    return registry


class TestConsumer:

    @pytest.fixture(autouse=True)
    def transact(self, request, registry):
        transaction = registry.begin_nested()
        request.addfinalizer(transaction.rollback)

    def test_schema_ok(self, registry):
        assert (
            registry.Test.decorated_method(
                body=dumps({'label': 'test', 'number': '1'})) ==
            {'label': 'test', 'number': 1}
        )

    def test_schema_ko(self, registry):
        with pytest.raises(ValidationError):
            registry.Test.decorated_method(
                body=dumps({'label': 'test', 'number': 'other'}))

    def test_consumer_add_in_get_profile(self, registry):
        assert (
            registry.Bus.get_consumers() ==
            [(Configuration.get('bus_processes', 1),
             [('test', 'Model.Test', 'decorated_method')])])

    def test_reload(self, registry):
        reload_registry(registry, add_in_registry)


class TestValidator:

    @pytest.fixture(autouse=True)
    def close_registry(self, request, bloks_loaded):

        def close():
            if hasattr(self, 'registry'):
                self.registry.close()

        request.addfinalizer(close)

    def init_registry(self, *args, **kwargs):
        self.registry = init_registry_with_bloks(('bus',), *args, **kwargs)
        return self.registry

    def test_decorator_without_name(self):
        def add_in_registry():
            @Declarations.register(Declarations.Model)
            class Test:

                @bus_consumer(schema=OneSchema())
                def decorated_method(cls, body=None):
                    return body

        with pytest.raises(BusConfigurationException):
            self.init_registry(add_in_registry)

    def test_decorator_with_twice_the_same_name(self):

        def add_in_registry():
            @Declarations.register(Declarations.Model)
            class Test:

                @bus_consumer(queue_name='test', schema=OneSchema())
                def decorated_method1(cls, body=None):
                    return body

                @bus_consumer(queue_name='test', schema=OneSchema())
                def decorated_method2(cls, body=None):
                    return body

        registry = self.init_registry(add_in_registry)
        with pytest.raises(TwiceQueueConsumptionException):
            registry.Bus.get_consumers()

    def test_decorator_with_twice_the_same_name2(self):

        def add_in_registry():
            @Declarations.register(Declarations.Model)
            class Test:

                @bus_consumer(queue_name='test', schema=OneSchema())
                def decorated_method(cls, body=None):
                    return body

            @Declarations.register(Declarations.Model)
            class Test2:

                @bus_consumer(queue_name='test', schema=OneSchema())
                def decorated_method(cls, body=None):
                    return body

        registry = self.init_registry(add_in_registry)
        with pytest.raises(TwiceQueueConsumptionException):
            registry.Bus.get_consumers()

    def test_with_two_decorator(self):

        def add_in_registry():
            @Declarations.register(Declarations.Model)
            class Test:

                @bus_consumer(queue_name='test1', schema=OneSchema())
                def decorated_method1(cls, body=None):
                    return body

                @bus_consumer(queue_name='test2', schema=OneSchema())
                def decorated_method2(cls, body=None):
                    return body

        registry = self.init_registry(add_in_registry)
        # [(nb processes, [(queue, Model, method)])]
        assert len(registry.Bus.get_consumers()) == 1
        assert len(registry.Bus.get_consumers()[0]) == 2
        assert len(registry.Bus.get_consumers()[0][1]) == 2

    def test_with_two_decorator_in_different_processes(self):

        def add_in_registry():
            @Declarations.register(Declarations.Model)
            class Test:

                @bus_consumer(queue_name='test1', schema=OneSchema(),
                              processes=1)
                def decorated_method1(cls, body=None):
                    return body

                @bus_consumer(queue_name='test2', schema=OneSchema(),
                              processes=1)
                def decorated_method2(cls, body=None):
                    return body

        registry = self.init_registry(add_in_registry)
        # [(nb processes, [(queue, Model, method)])]
        assert len(registry.Bus.get_consumers()) == 2
        assert len(registry.Bus.get_consumers()[0]) == 2
        assert len(registry.Bus.get_consumers()[0][1]) == 1
        assert len(registry.Bus.get_consumers()[1]) == 2
        assert len(registry.Bus.get_consumers()[1][1]) == 1
