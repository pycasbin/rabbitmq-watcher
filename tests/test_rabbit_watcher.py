import os
import time
from unittest import TestCase

import casbin
import pika

from casbin_rabbitmq_watcher import new_watcher


def get_examples(path):
    examples_path = os.path.split(os.path.realpath(__file__))[0] + "/../examples/"
    return os.path.abspath(examples_path + path)


class TestConfig(TestCase):
    def test_watcher_init(self):
        watcher = new_watcher()
        assert isinstance(watcher.connection, pika.BlockingConnection)
        watcher.close()

    def test_with_enforcer(self):
        callback_flag = False

        def _test_update_callback(event):
            nonlocal callback_flag
            callback_flag = True
            print("update callback, event: {}".format(event))

        watcher = new_watcher()
        watcher.set_update_callback(_test_update_callback)

        e = casbin.Enforcer(
            get_examples("rbac_model.conf"), get_examples("rbac_policy.csv")
        )
        e.set_watcher(watcher)
        assert callback_flag is False
        e.save_policy()
        time.sleep(0.5)
        assert callback_flag is True
        # related update function not be called in py-casbin yet
        e.add_policy("eve", "data3", "read")
        e.remove_policy("eve", "data3", "read")
        rules = [
            ["jack", "data4", "read"],
            ["katy", "data4", "write"],
            ["leyo", "data4", "read"],
            ["ham", "data4", "write"],
        ]
        e.add_policies(rules)
        e.remove_policies(rules)
        watcher.close()
