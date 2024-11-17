from os import environ

from environment import Environment

timezone = 'Europe/Warsaw'
environment = environ.get("envname", Environment.LOCAL)
