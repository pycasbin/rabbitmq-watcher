import logging
import time
import threading

import casbin
import pika

LOGGER = logging.getLogger(__name__)


class RabbitWatcher(object):
    def __init__(
        self,
        host='localhost',
        port=5672,
        virtual_host='/',
        username='guest',
        password='guest',
        routing_key='casbin-policy-updated',
        **kwargs
    ):
        self.connection = None
        self.channel = None
        self.routing_key = routing_key
        self.callback = None
        self.mutex = threading.Lock()
        self.watch_thread = threading.Thread(target=self.start_watch, daemon=True)
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
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.routing_key)

    def close(self):
        self.channel.close()
        self.connection.close()

    def set_update_callback(self, callback):
        """
        sets the callback function to be called when the policy is updated
        :param callback:
        :return:
        """
        self.mutex.acquire()
        self.callback = callback
        self.mutex.release()

    def update(self):
        """
        update the policy
        """
        self.channel.basic_publish(routing_key=self.routing_key, body=str(time.time()))
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

        def _watch_callback(ch, method, properties, body):
            self.mutex.acquire()
            if self.callback is not None:
                self.callback(body)
            self.mutex.release()
            self.channel.basic_ack(method.delivery_tag)

        self.channel.basic_consume(queue=self.routing_key, on_message_callback=_watch_callback)


def new_watcher(
    host='localhost',
    port=5672,
    virtual_host='/',
    username='guest',
    password='guest',
    routing_key='casbin-policy-updated',
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
    rabbit.watch_thread.start()
    LOGGER.info("Rabbitmq watcher started")
    return rabbit
