from os import environ

from environment import Environment

timezone = 'Poland'
environment = environ.get("envname", Environment.LOCAL)
