# This file is a part of the AnyBlok / Bus api project
#
#    Copyright (C) 2018 Jean-Sebastien SUZANNE <jssuzanne@anybox.fr>
#
# This Source Code Form is subject to the terms of the Mozilla Public License,
# v. 2.0. If a copy of the MPL was not distributed with this file,You can
# obtain one at http://mozilla.org/MPL/2.0/.
import functools
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

        self.should_reconnect = False
        self.was_consuming = False

        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tags = []

        self.consumers = consumers
        self.withautocommit = withautocommit

        self._consuming = False
        # In production, experiment with higher prefetch values
        # for higher consumer throughput
        self._prefetch_count = 1

    def get_url(self):
        """ Retrieve connection url """
        connection = self.profile
        if connection:
            return connection.url.url
        raise Exception("Unknown profile")

    def connect(self):
        """ Creating connection object """
        url = self.get_url()
        logger.info('Connecting to %s', url)
        return SelectConnection(
            parameters=URLParameters(url),
            on_open_callback=self.on_connection_open,
            on_open_error_callback=self.on_connection_open_error,
            on_close_callback=self.on_connection_closed,
        )

    def close_connection(self):
        self._consuming = False
        if self._connection.is_closing or self._connection.is_closed:
            logger.info('Connection is closing or already closed')
        else:
            logger.info('Closing connection')
            self._connection.close()

    def on_connection_open(self, *a):
        """ Called when we are fully connected to RabbitMQ """
        self.profile.state = 'connected'
        if self.withautocommit:
            self.registry.commit()

        self._connection.channel(on_open_callback=self.on_channel_open)
        logger.info('Connexion opened')

    def on_connection_open_error(self, _unused_connection, err):
        """This method is called by pika if the connection to RabbitMQ
        can't be established.
        :param pika.SelectConnection _unused_connection: The connection
        :param Exception err: The error
        """
        logger.error('Connection open failed: %s', err)
        self.reconnect()

    def on_connection_closed(self, _unused_connection, reason):
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.

        :param pika.connection.Connection connection: The closed connection obj
        :param Exception reason: exception representing reason for loss of
            connection.
        """
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            logger.warning('Connection closed, reconnect necessary: %s', reason)
            self.reconnect()

    def reconnect(self):
        """Will be invoked if the connection can't be opened or is
        closed. Indicates that a reconnect is necessary then stops the
        ioloop.
        """
        self.should_reconnect = True
        self.stop()

    def on_channel_open(self, channel):
        """This method is invoked by pika when the channel has been opened.
        The channel object is passed in so we can make use of it.

        Since the channel is now open, we'll declare the exchange to use.

        :param pika.channel.Channel channel: The channel object
        """
        logger.info('Channel opened')
        self._channel = channel
        self._channel.add_on_close_callback(self.on_channel_closed)
        for queue, model, method in self.consumers:
            self.declare_consumer(queue, model, method)

        self._consuming = True

    def on_channel_closed(self, channel, reason):
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters. In this case, we'll close the
        connection to shutdown the object.

        :param pika.channel.Channel: The closed channel
        :param Exception reason: why the channel was closed
        """
        logger.warning('Channel %i was closed: %s', channel, reason)
        self._consuming = False
        if self._connection.is_closing or self._connection.is_closed:
            logger.info('Connection is closing or already closed')
        else:
            logger.info('Closing connection')
            self._connection.close()

    def declare_consumer(self, queue, model, method):

        def on_message(unused_channel, basic_deliver, properties, body):
            print("==> from on_message")
            logger.info(
                'received on %r tag %r', queue, basic_deliver.delivery_tag
            )
            self.registry.rollback()
            error = ""
            try:
                Model = self.registry.get(model)
                status = getattr(Model, method)(body=body.decode('utf-8'))
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
                    content_type=properties.content_type, message=body,
                    queue=queue, model=model, method=method,
                    error=error, sequence=basic_deliver.delivery_tag,
                )
                self._channel.basic_ack(basic_deliver.delivery_tag)
                logger.info('save message of the queue %s tag %r',
                            queue, basic_deliver.delivery_tag)

            if self.withautocommit:
                self.registry.commit()

        self._consumer_tags.append(
            self._channel.basic_consume(
                queue,
                on_message,
                arguments=dict(model=model, method=method)
            )
        )
        return True

    def is_ready(self):
        return self._consuming

    def start(self):
        """ Creating connection object and starting event loop """
        logger.info('start')
        self._connection = self.connect()
        self._connection.ioloop.start()

    def stop(self):
        if not self._closing:
            self._closing = True
            logger.info('Stopping %r', self.consumers)
            if self._consuming:
                self.stop_consuming()
            else:
                self._connection.ioloop.stop()

        logger.info('Stopped %r', self.consumers)

    def stop_consuming(self):
        """ Set profile's state to 'disconnected' and cancels every related
            consumers
        """
        self.profile.state = 'disconnected'
        if self.withautocommit:
            self.registry.commit()

        if self._channel:
            logger.info('Sending a Basic.Cancel RPC command to RabbitMQ')
            for consumer_tag in self._consumer_tags:
                cb = functools.partial(self.on_cancelok, userdata=consumer_tag)
                self._channel.basic_cancel(consumer_tag, cb)
            else:
                self.close_channel()

    def on_cancelok(self, unused_frame, userdata):
        self._consuming = False
        logger.info(
            'RabbitMQ acknowledged the cancellation of the consumer: %s',
            userdata)
        self._consumer_tags.remove(unused_frame.method.consumer_tag)
        if not len(self._consumer_tags):
            self.close_channel()

    def close_channel(self):
        logger.info('Closing the channel')
        self._channel.close()
        if not self.should_reconnect:
            self._connection.close()
