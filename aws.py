import os
import sys
import time
from datetime import datetime
from signal import signal, SIGINT
from subprocess import call
from sys import exit
from typing import List, Optional, Dict, Tuple

import boto3
import click
import paramiko
from paramiko import SSHException
from paramiko.ssh_exception import NoValidConnectionsError
from prettytable import PrettyTable

from aws_config import loadconfig, Params
from aws_utils import _get_instance_by_name, _collect_instances, _get_tag_val
from launch import (
    upload_and_run,
    start,
    make_script,
    make_parallel_commands,
    cleanup_instance, _compress_folder,
)
from machine_monitor import MachineMonitor


@click.group()
def cli():
    pass


@cli.command()
@click.argument("name")
@click.option("--owner", default=None, help="Owner of the machine or default one")
def kill(name: str, owner: Optional[str]):
    config = loadconfig()
    if owner is not None:
        config.owner = owner

    instance = _get_instance_by_name(name, config)
    if instance is None:
        print(f"Could not find instance called {name}")
        return

    client = boto3.client("ec2", region_name=config.region)
    response = client.terminate_instances(InstanceIds=[instance["InstanceId"]])
    print(f"Terminate response: {response}")
    print(f"\n Instance terminated")


@cli.command()
@click.option("--owner", default=None, help="Custom owner of the machines")
@click.option("--sleep", default=0.1, help="How long to sleep between updates")
@click.option("--debug/--no_debug", help="print all lines?")
def monitor(owner: Optional[str], sleep: float, debug: bool):
    """Monitor the state of running instances of a given owner"""
    config = loadconfig()
    if owner is not None:
        config.owner = owner

    def clear_console():
        _ = call("clear" if os.name == "posix" else "cls", shell=True)

    def handler(signal_received, frame):
        # https://www.devdungeon.com/content/python-catch-sigint-ctrl-c
        print(
            "\n\nSIGINT or CTRL-C detected, joining threads.. "
            + "\n\t..might take time if some monitor is trying to connect\n"
        )

        # join the threads and exit
        for name, thread in threads.items():
            thread.comm["should_stop"] = True
        for name, thread in threads.items():
            if thread.is_alive():
                thread.join()
            else:
                print(f"WARNING, a dead thread named {name} found!")

        print(f"\nAll threads ended, exiting, bye\n")
        exit(0)

    def get_machines(config: Params) -> Dict[str, str]:
        instances = _collect_instances(config)
        machines = {
            _get_tag_val(ins["Tags"], "Name"): ins["PrivateIpAddress"]
            for ins in instances
        }
        return machines

    def _all_gpus_same_memory(received: List[Dict[str, str]]) -> bool:
        first_mem = received[0]["total_mem"]
        for rec in received:
            if first_mem != rec["total_mem"]:
                return False
        return True

    def make_gpu_fields(received: List[Dict[str, str]]) -> Tuple[str, str]:
        loads = []
        mems = []
        all_same = _all_gpus_same_memory(received)

        for gpu in received:
            loads.append(gpu["util"])
            if all_same:
                mems.append(gpu["used_mem"])
            else:
                mems.append(gpu["used_mem"] + " /" + gpu["total_mem"] + "G")

        # if all memories are the same, append total memory at the end
        memory = ",".join(mems)
        if all_same:
            memory += " /" + received[0]["total_mem"] + "G"

        return ",".join(loads), memory

    # Tell Python to run the handler() function when SIGINT is recieved
    signal(SIGINT, handler)

    threads: Dict[str, MachineMonitor] = {}

    while True:
        # go through the machines, launch new monitors
        machines = get_machines(config)
        for name, ip in machines.items():
            if not name in threads:
                threads[name] = MachineMonitor(name, sleep, config)
                threads[name].start()
        # go through existing monitors, delete the old ones (note monitors end themselves)
        to_remove = []
        for name, _ in threads.items():
            if name not in machines:
                to_remove.append(name)
        for remove in to_remove:
            threads.pop(remove)

        t = PrettyTable()
        t.field_names = [
            "Name",
            "Ip",
            "Load",
            "GPU",
            "GPU mem",
            "EXPID",
            "stdout",
        ]  # "Heartbeat"
        for name, ip in machines.items():
            comm = threads[name].comm
            load = comm["load"]
            gpu_utils, gpu_mems = make_gpu_fields(comm["gpu"])
            last_lines, heartbeat = comm["result"]
            if len(last_lines) == 0:
                t.add_row(
                    [name, ip, load, gpu_utils, gpu_mems, "-", "WARNING: NO LINE READ"]
                )
            else:
                for line_id, (exp_id, last_line) in enumerate(last_lines):
                    if line_id == 0:
                        t.add_row(
                            [name, ip, load, gpu_utils, gpu_mems, exp_id, last_line]
                        )
                    else:
                        t.add_row(["", "", "", "", "", exp_id, last_line])

        t.align = "l"

        if not debug:
            clear_console()
        print(f"Found {len(machines)} machines owned by {config.owner}:\n")
        print(t)
        time.sleep(sleep)


@cli.command()
@click.option("--owner", default=None, help="Custom owner of the machines")
def list(owner: Optional[str] = None):
    """List currently running machines (of the owner, subgroup, group..)"""
    config = loadconfig()
    if owner is not None:
        config.owner = owner
    instances = _collect_instances(config)
    addresses = [ins["PrivateIpAddress"] for ins in instances]
    names = [_get_tag_val(ins["Tags"], "Name") for ins in instances]

    machines = [{name: address} for name, address in zip(names, addresses)]
    print(f"running machines are: , names {machines}")


@cli.command()
@click.argument("name")
@click.option("--owner", default=None, help="Custom owner of the machines")
@click.option("--setup/--no_setup", help="show the setup log?")
@click.option("--debug/--no_debug", help="print all lines?")
@click.option("--sleep", default=0.2, help="how long to sleep between updates")
@click.option(
    "--group",
    default=0,
    help="experiment group (when more experiments ran in parallel)",
)
def lastline(
    name: str, owner: Optional[str], setup: bool, debug: bool, sleep: float, group: int
):
    """Show the last line of the experiment_[group].log or setup.log of a given machine"""
    config = loadconfig()
    if owner is not None:
        config.owner = owner

    instance = _get_instance_by_name(name, config)
    if instance is None:
        return
    instance_ip = instance["PrivateIpAddress"]
    print(f"Connecting to {name} with IP: {instance_ip}")

    # prepare the connection
    key = paramiko.RSAKey.from_private_key_file(config.pem_file)
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    file = "setup.log" if setup else f"experiment_{group}.log"

    try:
        client.connect(hostname=instance_ip, username="ubuntu", pkey=key)
        print(f"DONE, connected")
        while True:
            stdin, stdout, stderr = client.exec_command(f"tail -n 2 {file}")
            read_bytes = stdout.read()
            last_line = MachineMonitor.extract_last_line(read_bytes)
            print(last_line)
            if not debug:
                sys.stdout.write("\033[F")  # Cursor up one line

            time.sleep(sleep)

    except SSHException or NoValidConnectionsError:
        print(f"Could not connect to: {name} with IP {instance_ip}")


@cli.command()
@click.argument("name")
@click.option("--owner", default=None, help="Custom owner of the machines")
@click.option("--setup/--no_setup", help="show the setup log?")
@click.option(
    "--group",
    default=0,
    help="experiment group (when more experiments ran in parallel)",
)
def tail(name: str, setup: bool, owner: Optional[str], group: int):
    """Tail the contents of experiment.log / setup.log of a given machine"""

    config = loadconfig()
    if owner is not None:
        config.owner = owner

    instance = _get_instance_by_name(name, config)
    if instance is None:
        return
    instance_ip = instance["PrivateIpAddress"]
    print(f"Connecting to {name} with IP: {instance_ip}")
    file = "setup.log" if setup else f"experiment_{group}.log"

    command = " ".join(
        [
            "ssh",
            f"-i {config.pem_file} " "ubuntu@" + instance_ip,
            f"'tail -f -n 2000 {file} && exec bash -l'",
        ]
    )
    print(command)
    os.system(command)
    return


@cli.command()
@click.argument("commands")
@click.option("--repeats", default=1, help="How many times to repeat given command (s)")
@click.option("--parallel", default=1, help="How many experiments to run in parallel?")
@click.option(
    "--instance", default=None, help="instance type to be used (e.g. p2.xlarge)"
)
@click.option("--ami", default=None, help="ami_id to be used")
@click.option("--owner", default=None, help="custom owner of the machine")
def launch(
    commands: str,
    repeats: int,
    parallel: int,
    instance: Optional[str],
    owner: Optional[str],
    ami: Optional[str],
):
    """Launch machine which will execute given commands (splitted by |) and auto-terminate after"""
    config = loadconfig()
    if instance is not None:
        config.instance_type = instance
    if ami is not None:
        config.ami_id = ami
    if owner is not None:
        config.owner = owner

    parallel_commands = make_parallel_commands(commands, repeats, parallel)
    print(f"Will launch the following commands:")
    for id, coms in enumerate(parallel_commands):
        print(f"Iteration {id}:")
        formatted = "\n\t\t".join(coms)
        print(f"\t\t{formatted}")

    instance, name = start(config)
    upload_and_run(instance, parallel_commands, config)
    print(f"ALL DONE, instance name: {name}")


@cli.command()
@click.argument("name")
def makeimage(name: str):
    """Make the AMI - image from the currently running instance of a given name"""
    config = loadconfig()

    instance = _get_instance_by_name(name, config)
    if instance is None:
        print(f"ERROR: running instance named {name} not found!")
        return

    choice = input(
        "This will cleanup currently running machine and will break logs of any currently running "
        "experiments.\n\n\t PRESS y and enter for continue..."
    )
    if choice == "y":
        cleanup_instance(instance, name, config)
    else:
        print(f"Aborting")
        return

    date_time = datetime.utcnow().strftime("%Y-%m-%d--%H-%M")
    image_name = f"i-{config.repo_name}-{date_time}"
    client = boto3.client("ec2", region_name=config.region)
    result = client.create_image(InstanceId=instance["InstanceId"], Name=image_name)
    print(f"result: {result}")
    image_id = result["ImageId"]
    print(
        f"\n\nDone, resulting ImageId: {image_id}\t Change this in the config.yaml when ready."
    )


@cli.command()
@click.argument("commands")
@click.option("--repeats", default=1, help="How many times to repeat given command (s)")
@click.option("--parallel", default=1, help="How many experiments to run in parallel?")
def debugscript(commands: str, repeats: int, parallel: int):
    """Debug purposes, prints out the whole script"""
    config = loadconfig()

    _compress_folder()

    parallel_commands = make_parallel_commands(commands, repeats, parallel)

    print(f"Will launch the following commands:")
    for id, coms in enumerate(parallel_commands):
        print(f"Iteration {id}:")
        formatted = "\n\t\t".join(coms)
        print(f"\t\t{formatted}")

    script = make_script(parallel_commands, config.repo_name)
    print(f"XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
    print(script)


if __name__ == "__main__":
    cli()
