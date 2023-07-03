import os
import time
from unittest import TestCase

import casbin
from rabbitmq_watcher import new_watcher


def get_examples(path):
    examples_path = os.path.split(os.path.realpath(__file__))[0] + "/../examples/"
    return os.path.abspath(examples_path + path)


class TestConfig(TestCase):
    def test_update_rabbitmq_watcher(self):
        watcher = new_watcher()
        watcher.channel.basic_publish(exchange='', routing_key="casbin-policy-updated", body=str(time.time()))
        assert watcher.update() is True

    def test_with_enforcer(self):
        def _test_update_callback(msg):
            print("update callback, msg: {}".format(msg))

        watcher = new_watcher()
        watcher.set_update_callback(_test_update_callback)

        e = casbin.Enforcer(
            get_examples("rbac_model.conf"), get_examples("rbac_policy.csv")
        )
        e.set_watcher(watcher)
        e.save_policy()
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
