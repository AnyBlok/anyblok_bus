# This file is a part of the AnyBlok / Bus api project
#
#    Copyright (C) 2018 Jean-Sebastien SUZANNE <jssuzanne@anybox.fr>
#
# This Source Code Form is subject to the terms of the Mozilla Public License,
# v. 2.0. If a copy of the MPL was not distributed with this file,You can
# obtain one at http://mozilla.org/MPL/2.0/.
from anyblok_bus.status import MessageStatus
from logging import getLogger
from pika import SelectConnection, URLParameters

logger = getLogger(__name__)


class Worker:
    """Define consumers to consume the queue défined in the AnyBlok registry
    by the bus_consumer decorator

    ::

        worker = Worker(anyblokregistry, profilename)
        worker.start()  # blocking loop
        worker.is_ready()  # return True if all the consumer are started
        worker.stop()  # stop the loop and close the connection with rabbitmq

    :param registry: anyblok registry instance
    :param profile: the name of the profile which give the url of rabbitmq
    """

    def __init__(self, registry, profile, consumers, withautocommit=True):
        self.registry = registry
        self.profile = self.registry.Bus.Profile.query().filter_by(
            name=profile
        ).one()
        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tags = []
        self.ready = False
        self.consumers = consumers
        self.withautocommit = withautocommit

    def connect(self):
        """ Creating connection object """
        url = self.get_url()
        logger.info('Connecting to %s', url)
        return SelectConnection(
            URLParameters(url),
            self.on_connection_open,
            stop_ioloop_on_close=False
        )

    def reconnect(self):
        self._connection.ioloop.stop()
        if not self._closing:
            self.start()

    def get_url(self):
        """ Retrieve connection url """
        connection = self.profile
        if connection:
            return connection.url.url
        raise Exception("Unknown profile")

    def on_connection_open(self, *a):
        """ Called when we are fully connected to RabbitMQ """
        self.profile.state = 'connected'
        self.registry.commit()
        self._connection.add_on_close_callback(self.on_connection_closed)
        self._connection.channel(on_open_callback=self.on_channel_open)
        logger.info('Connexion opened')

    def on_channel_open(self, channel):
        """ Called when channel is opened """
        logger.info('Channel opened')
        self._channel = channel
        self._channel.add_on_close_callback(self.on_channel_closed)
        for queue, model, method in self.consumers:
            self.declare_consumer(queue, model, method)

        self.ready = True

    def is_ready(self):
        return self.ready

    def on_channel_closed(self, channel, reply_code, reply_text):
        """ Called when channel is closed """
        logger.warning(
            'Channel %i was closed: (%s) %s', channel, reply_code, reply_text
        )
        self._connection.close()

    def on_connection_closed(self, connection, reply_code, reply_text):
        """ Called when connection is closed by the server """
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            logger.warning(
                'Connection closed, reopening in 5 seconds: (%s) %s',
                reply_code, reply_text
            )
            self._connection.add_timeout(5, self.reconnect)

    def declare_consumer(self, queue, model, method):

        def on_message(unused_channel, basic_deliver, properties, body):
            logger.info(
                'received on %r tag %r', queue, basic_deliver.delivery_tag
            )
            self.registry.rollback()
            error = ""
            try:
                status = getattr(model, method)(body=body.decode('utf-8'))
            except Exception as e:
                logger.exception('Error during consumation of queue %r' % queue)
                self.registry.rollback()
                status = MessageStatus.ERROR
                error = str(e)

            if status is MessageStatus.ACK:
                self._channel.basic_ack(basic_deliver.delivery_tag)
                logger.info('ack queue %s tag %r',
                            queue, basic_deliver.delivery_tag)
            elif status is MessageStatus.NACK:
                self._channel.basic_nack(basic_deliver.delivery_tag)
                logger.info('nack queue %s tag %r',
                            queue, basic_deliver.delivery_tag)
            elif status is MessageStatus.REJECT:
                self._channel.basic_reject(basic_deliver.delivery_tag)
                logger.info('reject queue %s tag %r',
                            queue, basic_deliver.delivery_tag)
            elif status is MessageStatus.ERROR or status is None:
                self.registry.Bus.Message.insert(
                    content_type=properties.content_type,
                    message=body,
                    queue=queue,
                    model=model.__registry_name__,
                    method=method,
                    error=error,
                    sequence=basic_deliver.delivery_tag,
                )
                self._channel.basic_ack(basic_deliver.delivery_tag)
                logger.info('save message of the queue %s tag %r',
                            queue, basic_deliver.delivery_tag)

            if self.withautocommit:
                self.registry.commit()

        self._consumer_tags.append(
            self._channel.basic_consume(
                on_message,
                queue=queue,
                arguments=dict(model=model.__registry_name__, method=method)
            )
        )
        return True

    def stop_consuming(self):
        """ Set profile's state to 'disconnected' and cancels every related
            consumers
        """
        self.profile.state = 'disconnected'
        self.registry.commit()
        if self._channel:
            for consumer_tag in self._consumer_tags:
                self._channel.basic_cancel(self.on_cancelok, consumer_tag)
            else:
                self.close_channel_and_connection()

    def on_cancelok(self, unused_frame):
        logger.info('RabbitMQ acknowledged the cancellation of the consumer')
        self._consumer_tags.remove(unused_frame.method.consumer_tag)
        if not len(self._consumer_tags):
            self.close_channel_and_connection()

    def close_channel_and_connection(self):
        if (
            self._channel and
            not self._channel.is_closed and
            not self._channel.is_closing
        ):
            self._channel.close()

        if (
            self._connection and
            not self._connection.is_closed and
            not self._connection.is_closing
        ):
            self._connection.close()

    def start(self):
        """ Creating connection object and starting event loop """
        logger.info('start')
        self._connection = self.connect()
        self._connection.ioloop.start()

    def stop(self):
        logger.info('stop')
        self._closing = True
        self.ready = False
        self.stop_consuming()
