import logging
import threading
import time

import casbin
import pika


class RabbitWatcher:
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
        self.client = None
        credentials = pika.PlainCredentials(username, password)
        self.rabbit_config = pika.ConnectionParameters(
            host=host,
            port=port,
            virtual_host=virtual_host,
            credentials=credentials,
            **kwargs
        )
        self.routing_key = routing_key

    def create_client(self):
        self.client = pika.BlockingConnection()

    def close(self):
        self.running = False
        self.logger.info("ETCD watcher closed")

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
        self.client.put(self.keyName, str(time.time()))
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
        self.logger.info(message)
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
        self.logger.info(message)
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
        self.logger.info(message)
        return self.update()

    def update_for_save_policy(self, model: casbin.Model):
        """
        update for save policy
        :param model: casbin model
        :return:
        """
        message = "Update for save policy: " + model.to_text()
        self.logger.info(message)
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
        self.logger.info(message)
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
        self.logger.info(message)
        return self.update()

    def start_watch(self):
        """
        starts the watch thread
        :return:
        """
        events_iterator, cancel = self.client.watch(self.keyName)
        for event in events_iterator:
            if isinstance(event, etcd3.events.PutEvent) or isinstance(
                    event, etcd3.events.DeleteEvent
            ):
                self.mutex.acquire()
                if self.callback is not None:
                    self.callback(event)
                self.mutex.release()


def new_watcher(endpoints, keyname):
    """
    creates a new watcher
    :param endpoints:
    :param keyname:
    :return: a new watcher
    """
    etcd = ETCDWatcher(
        endpoints=endpoints, running=True, callback=None, key_name=keyname
    )
    etcd.create_client()
    etcd.watch_thread.start()
    etcd.logger.info("ETCD watcher started")
    return etcd
