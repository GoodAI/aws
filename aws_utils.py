from typing import List, Dict

import boto3
import click

from aws_config import Params


@click.group()
def cli():
    pass


def _collect_instances(config: Params):
    """Collect instances, filter by the params"""

    client = boto3.client("ec2", region_name=config.region)
    instances = [
        x["Instances"][0]
        for x in client.describe_instances(
            Filters=[
                {"Name": "instance-state-name", "Values": ["running"]},
                {"Name": "tag:Owner", "Values": [config.owner]},
                {"Name": "tag:Group", "Values": [config.group]},
            ]
        )["Reservations"]
    ]
    return instances


def _get_tag_val(tags: List[Dict], key: str):
    """Get value of a tag of a given key"""

    for one_tag in tags:
        if one_tag["Key"] == key:
            return one_tag["Value"]
    return ""


def _get_instance_by_name(name: str, config: Params):
    """Collect running instances and return instance of a given name or None"""

    instances = _collect_instances(config)
    instances_by_name = []
    for instance in instances:
        if _get_tag_val(instance["Tags"], "Name") == name:
            instances_by_name.append(instance)

    if len(instances_by_name) == 0:
        print(f"ERROR: no machine with this name found: {name}")
        return None
    assert len(instances_by_name) == 1, "multiple machines with this name found!"
    return instances_by_name[0]
