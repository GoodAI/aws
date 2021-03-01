import os
import time
from dataclasses import dataclass
from os.path import expanduser
from typing import List, Dict, Any

import yaml

CONFIG_LOC = "~/.aws_config.yaml"


@dataclass
class Params:
    # access
    pem_file = "~/.ssh/key.pem"

    # machines
    instance_type = "p2.xlarge"   # e.g. "p2.xlarge"
    ami_id = "FILL_IN"            # e.g. "ami-123456"
    security_group = "FILL_IN"    # e.g. "sg-123abc"
    region = "us-east-1"          # e.g. "us-east-1"

    # filtering & other
    owner = "AUTO_DETECT"         # detected automatically
    repo_name = "AUTO_DETECT"
    group = "MY_GROUP"            # optionally setup by user


def _get_members(params: Params) -> List[str]:
    """ Get members of the Params dataclass (with default values)
    https://stackoverflow.com/questions/1398022/looping-over-all-member-variables-of-a-class-in-python
    """
    members = [
        attr
        for attr in dir(params)
        if not callable(getattr(params, attr)) and not attr.startswith("__")
    ]
    return members


def from_dict(dictionary: Dict[str, Any]) -> Params:
    params = Params()
    for key, val in dictionary.items():
        if not hasattr(params, key):
            raise Exception(f"Invalid contents of the yaml file! key: {key}")
        params.__setattr__(key, val)
    return params


def to_dict(params: Params) -> Dict[str, Any]:
    result = {}
    # https://stackoverflow.com/questions/1398022/looping-over-all-member-variables-of-a-class-in-python
    members = [
        attr
        for attr in dir(params)
        if not callable(getattr(params, attr)) and not attr.startswith("__")
    ]

    for member in members:
        result[member] = getattr(params, member)
    return result


def loadconfig() -> Params:
    """Define the default config, make config.yaml if not found, load it"""

    path, repo_name = os.path.split(os.getcwd())

    # generate new yaml if not found
    config_name = CONFIG_LOC.replace("~", expanduser("~"))
    if not os.path.isfile(config_name):
        def_params = Params()
        def_params.repo_name = repo_name
        owner = input(f"\n\n\tHello new user, what's your name?\n\t").strip()
        print(
            f"\tOK, setting the owner={owner} \t\t, set path to your *.pem file at: {config_name}"
        )
        def_params.owner = owner
        with open(config_name, "w") as file:
            yaml.dump(to_dict(def_params), file)
        time.sleep(15)

    with open(config_name, "r") as file:
        config = from_dict(yaml.load(file, Loader=yaml.FullLoader))

    # potentially sanitize the home dir
    config.pem_file = config.pem_file.replace("~", expanduser("~"))
    return config
