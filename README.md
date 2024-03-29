# rabbitmq-watcher

[![Build Status](https://github.com/pycasbin/rabbitmq-watcher/actions/workflows/build.yml/badge.svg)](https://github.com/pycasbin/rabbitmq-watcher/actions/workflows/build.yml)
[![Coverage Status](https://coveralls.io/repos/github/pycasbin/rabbitmq-watcher/badge.svg)](https://coveralls.io/github/pycasbin/rabbitmq-watcher)
[![Version](https://img.shields.io/pypi/v/casbin-rabbitmq-watcher.svg)](https://pypi.org/project/casbin-rabbitmq-watcher/)
[![PyPI - Wheel](https://img.shields.io/pypi/wheel/casbin-rabbitmq-watcher.svg)](https://pypi.org/project/casbin-rabbitmq-watcher/)
[![Pyversions](https://img.shields.io/pypi/pyversions/casbin-rabbitmq-watcher.svg)](https://pypi.org/project/casbin-rabbitmq-watcher/)
[![Download](https://img.shields.io/pypi/dm/casbin-rabbitmq-watcher.svg)](https://pypi.org/project/casbin-rabbitmq-watcher/)
[![Discord](https://img.shields.io/discord/1022748306096537660?logo=discord&label=discord&color=5865F2)](https://discord.gg/S5UjpzGZjN)

Rabbitmq Watcher is the rabbitmq watcher for pycasbin. With this library, Casbin can synchronize the policy with the database in multiple enforcer instances.
## Installation
```bash
pip install casbin-rabbitmq-watcher
```

## Simple Example

```python
import os

import casbin
from casbin_rabbitmq_watcher import new_watcher


def get_examples(path):
    examples_path = os.path.split(os.path.realpath(__file__))[0] + "/../examples/"
    return os.path.abspath(examples_path + path)


def update_callback_func(msg):
    ...


watcher = new_watcher()
watcher.set_update_callback(update_callback_func)

e = casbin.Enforcer(
    get_examples("rbac_model.conf"), get_examples("rbac_policy.csv")
)

e.set_watcher(watcher)
# update_callback_func will be called
e.save_policy()
```

## Getting Help

- [Casbin](https://github.com/casbin/pycasbin)

## License

This project is under Apache 2.0 License. See the [LICENSE](LICENSE) file for the full license text.
