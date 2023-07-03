import functools
import logging
import time
import threading

import casbin
import pika

LOGGER = logging.getLogger(__name__)


class RabbitWatcher:
    def __init__(
        self,
        host="localhost",
        port=5672,
        virtual_host="/",
        username="guest",
        password="guest",
        routing_key="casbin-policy-updated",
        **kwargs
    ):
        self.connection = None
        self.pub_channel = None
        self.routing_key = routing_key
        self.callback = None
        self.mutex = threading.Lock()
        self.subscribe_thread = threading.Thread(target=self.start_watch, daemon=True)
        credentials = pika.PlainCredentials(username, password)
        self.rabbit_config = pika.ConnectionParameters(
            host=host,
            port=port,
            virtual_host=virtual_host,
            credentials=credentials,
            **kwargs
        )

    def create_client(self):
        self.connection = pika.BlockingConnection(self.rabbit_config)
        self.pub_channel = self.connection.channel()
        self.pub_channel.queue_declare(queue=self.routing_key)

    def close(self):
        self.pub_channel.close()
        self.connection.close()

    def set_update_callback(self, callback):
        """
        sets the callback function to be called when the policy is updated
        :param callable callback: callback(event)
            - event: event received from the rabbitmq
        :return:
        """
        self.mutex.acquire()
        self.callback = callback
        self.mutex.release()

    def update(self):
        """
        update the policy
        """
        self.pub_channel.basic_publish(
            exchange="", routing_key=self.routing_key, body=str(time.time())
        )
        return True

    def update_for_add_policy(self, section, ptype, *params):
        """
        update for add policy
        :param section: section
        :param ptype:   policy type
        :param params:  other params
        :return:    True if updated
        """
        message = "Update for add policy: " + section + " " + ptype + " " + str(params)
        LOGGER.info(message)
        return self.update()

    def update_for_remove_policy(self, section, ptype, *params):
        """
        update for remove policy
        :param section: section
        :param ptype:   policy type
        :param params:  other params
        :return:    True if updated
        """
        message = (
            "Update for remove policy: " + section + " " + ptype + " " + str(params)
        )
        LOGGER.info(message)
        return self.update()

    def update_for_remove_filtered_policy(self, section, ptype, field_index, *params):
        """
        update for remove filtered policy
        :param section: section
        :param ptype:   policy type
        :param field_index: field index
        :param params: other params
        :return:
        """
        message = (
            "Update for remove filtered policy: "
            + section
            + " "
            + ptype
            + " "
            + str(field_index)
            + " "
            + str(params)
        )
        LOGGER.info(message)
        return self.update()

    def update_for_save_policy(self, model: casbin.Model):
        """
        update for save policy
        :param model: casbin model
        :return:
        """
        message = "Update for save policy: " + model.to_text()
        LOGGER.info(message)
        return self.update()

    def update_for_add_policies(self, section, ptype, *params):
        """
        update for add policies
        :param section: section
        :param ptype:   policy type
        :param params:  other params
        :return:
        """
        message = (
            "Update for add policies: " + section + " " + ptype + " " + str(params)
        )
        LOGGER.info(message)
        return self.update()

    def update_for_remove_policies(self, section, ptype, *params):
        """
        update for remove policies
        :param section: section
        :param ptype:   policy type
        :param params:  other params
        :return:
        """
        message = (
            "Update for remove policies: " + section + " " + ptype + " " + str(params)
        )
        LOGGER.info(message)
        return self.update()

    def start_watch(self):
        """
        starts the watch thread
        :return:
        """

        def _ack_message(ch, delivery_tag):
            if ch.is_open:
                ch.basic_ack(delivery_tag)
            else:
                LOGGER.warning("sub channel has closed.")

        def _watch_callback(ch, method, properties, body):
            self.mutex.acquire()
            if self.callback is not None:
                self.callback(body)
            self.mutex.release()
            cb = functools.partial(_ack_message, ch, method.delivery_tag)
            ch.connection.add_callback_threadsafe(cb)

        while True:
            try:
                sub_connection = pika.BlockingConnection(self.rabbit_config)
                sub_channel = sub_connection.channel()
                sub_channel.queue_declare(queue=self.routing_key)
                sub_channel.basic_consume(
                    queue=self.routing_key, on_message_callback=_watch_callback
                )
                try:
                    sub_channel.start_consuming()
                except KeyboardInterrupt:
                    sub_channel.stop_consuming()

                sub_connection.close()
                break
                # Do not recover if connection was closed by broker
            except pika.exceptions.ConnectionClosedByBroker:
                break
                # Do not recover on channel errors
            except pika.exceptions.AMQPChannelError:
                break
                # Recover on all other connection errors
            except pika.exceptions.AMQPConnectionError:
                continue


def new_watcher(
    host="localhost",
    port=5672,
    virtual_host="/",
    username="guest",
    password="guest",
    routing_key="casbin-policy-updated",
    **kwargs
):
    """
    creates a new watcher
    :param host:
    :param port:
    :param virtual_host:
    :param username:
    :param password:
    :param routing_key:
    :return: a new watcher
    """
    rabbit = RabbitWatcher(
        host=host,
        port=port,
        virtual_host=virtual_host,
        username=username,
        password=password,
        routing_key=routing_key,
        **kwargs
    )
    rabbit.create_client()
    rabbit.subscribe_thread.start()
    LOGGER.info("Rabbitmq watcher started")
    return rabbit
