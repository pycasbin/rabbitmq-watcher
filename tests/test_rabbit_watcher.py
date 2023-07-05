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

    def test_update(self):
        callback_flag = False

        def callback_function(event):
            nonlocal callback_flag
            callback_flag = True
            print("update callback, event: {}".format(event))

        w = new_watcher()
        w.set_update_callback(callback_function)
        assert callback_flag is False
        w.update()
        time.sleep(0.5)
        assert callback_flag is True

    def test_update_for_add_policy(self):
        callback_flag = False

        def callback_function(event):
            nonlocal callback_flag
            callback_flag = True
            print("update for add policy, event: {}".format(event))

        w = new_watcher()
        w.set_update_callback(callback_function)
        assert callback_flag is False
        w.update_for_add_policy("test1", "test2", "test3")
        time.sleep(0.5)
        assert callback_flag is True

    def test_update_for_remove_policy(self):
        callback_flag = False

        def callback_function(event):
            nonlocal callback_flag
            callback_flag = True
            print("update for remove policy, event: {}".format(event))

        w = new_watcher()
        w.set_update_callback(callback_function)
        assert callback_flag is False
        w.update_for_remove_policy("test1", "test2", "test3")
        time.sleep(0.5)
        assert callback_flag is True

    def test_update_for_remove_filtered_policy(self):
        callback_flag = False

        def callback_function(event):
            nonlocal callback_flag
            callback_flag = True
            print("update for remove filtered policy, event: {}".format(event))

        w = new_watcher()
        w.set_update_callback(callback_function)
        assert callback_flag is False
        w.update_for_remove_filtered_policy("test1", "test2", "test3")
        time.sleep(0.5)
        assert callback_flag is True

    def test_update_for_add_policies(self):
        callback_flag = False

        def callback_function(event):
            nonlocal callback_flag
            callback_flag = True
            print("update for add policies, event: {}".format(event))

        w = new_watcher()
        w.set_update_callback(callback_function)
        assert callback_flag is False
        w.update_for_add_policies("test1", "test2", "test3")
        time.sleep(0.5)
        assert callback_flag is True

    def test_update_for_remove_policies(self):
        callback_flag = False

        def callback_function(event):
            nonlocal callback_flag
            callback_flag = True
            print("update for remove policies, event: {}".format(event))

        w = new_watcher()
        w.set_update_callback(callback_function)
        assert callback_flag is False
        w.update_for_remove_policies("test1", "test2", "test3")
        time.sleep(0.5)
        assert callback_flag is True

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
