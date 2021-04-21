import os
from setuptools import setup, find_packages

lib = os.path.dirname(os.path.realpath(__file__))
requirements = lib + "/requirements.txt"

if os.path.isfile(requirements):
    with open(requirements) as f:
        install_requires = [
            line for line
            in f.read().splitlines()
            if line and not line.startswith("#")
        ]
# Sets up and installs package, installs requirements
setup(
    name="pgimport", 
    packages=find_packages(), 
    install_requires=install_requires,
    version = "0.1.0",
    description="contains code to import small datasets into PingThings' PredictiveGrid",
    author="PingThingsIO"
)