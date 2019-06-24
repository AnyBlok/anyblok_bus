# This file is a part of the AnyBlok / Bus api project
#
#    Copyright (C) 2018 Jean-Sebastien SUZANNE <jssuzanne@anybox.fr>
#    Copyright (C) 2019 Jean-Sebastien SUZANNE <js.suzanne@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public License,
# v. 2.0. If a copy of the MPL was not distributed with this file,You can
# obtain one at http://mozilla.org/MPL/2.0/.
import pytest
from anyblok_bus import bus_consumer
from anyblok.column import Integer, String
from marshmallow import Schema, fields
from json import dumps
from anyblok import Declarations
from anyblok_bus.status import MessageStatus
from .conftest import init_registry_with_bloks


class OneSchema(Schema):
    label = fields.String(required=True)
    number = fields.Integer(required=True)


def add_in_registry():

    @Declarations.register(Declarations.Model)
    class Test:
        id = Integer(primary_key=True)
        label = String()
        number = Integer()

        @bus_consumer(queue_name='test', schema=OneSchema())
        def decorated_method(cls, body=None):
            cls.insert(**body)
            return MessageStatus.ACK


@pytest.fixture(scope="class")
def registry(request, bloks_loaded):
    registry = init_registry_with_bloks(('bus',), add_in_registry)
    request.addfinalizer(registry.close)
    return registry


class TestMessage:

    @pytest.fixture(autouse=True)
    def transact(self, request, registry):
        transaction = registry.begin_nested()
        request.addfinalizer(transaction.rollback)

    def test_message_ok(self, registry):
        file_ = dumps({'label': 'label', 'number': 1})
        message = registry.Bus.Message.insert(
            message=file_.encode('utf-8'),
            queue='test',
            model='Model.Test',
            method='decorated_method')
        assert registry.Test.query().count() == 0
        message.consume()
        assert registry.Test.query().count() == 1
        assert registry.Bus.Message.query().count() == 0

    def test_message_ko(self, registry):
        file_ = dumps({'label': 'label'})
        message = registry.Bus.Message.insert(
            message=file_.encode('utf-8'),
            queue='test',
            model='Model.Test',
            method='decorated_method')
        assert registry.Test.query().count() == 0
        message.consume()
        assert registry.Test.query().count() == 0
        assert registry.Bus.Message.query().count() == 1

    def test_message_consume_all(self, registry):
        Test = registry.Test
        file_ = dumps({'label': 'label', 'number': 2})
        registry.Bus.Message.insert(
            message=file_.encode('utf-8'),
            sequence=2,
            queue='test',
            model='Model.Test',
            method='decorated_method')
        file_ = dumps({'label': 'label', 'number': 1})
        registry.Bus.Message.insert(
            message=file_.encode('utf-8'),
            sequence=1,
            queue='test',
            model='Model.Test',
            method='decorated_method')
        assert Test.query().count() == 0
        registry.Bus.Message.consume_all()
        assert Test.query().count() == 2
        assert Test.query().order_by(Test.id).all().number == [1, 2]
        assert registry.Bus.Message.query().count() == 0
