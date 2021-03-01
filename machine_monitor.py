import os
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Union, List, Optional, Dict, Tuple

import paramiko
from paramiko import SSHClient, SSHException

from aws_config import Params
from aws_utils import _get_instance_by_name
from launch import SHUTDOWN_MESSAGE


class MachineMonitor(threading.Thread):
    """ SSH to the instance and periodically update info from the machine, namely:
        -last line of stdout
        -CPU load
        -GPU info
    """

    def __init__(self, name: str, sleep: float, config: Params):
        threading.Thread.__init__(self)
        self.name = name
        self.daemon = True
        # python dicts should be thread safe
        self.comm = {
            "should_stop": False,
            "result": ([("?", "Connecting..")], self.now()),
            "load": "?",
            "gpu": [{"util": "?", "used_mem": "?", "total_mem": "?"}],
        }
        self.sleep = sleep
        self.config = config
        self.exp_ids: Dict[int:int] = {}  # maps experiment_group_id => Sacred EXPID
        self.is_exp_ids_detected = False
        self.is_final_logs_downloaded = False

    @staticmethod
    def now() -> str:
        return datetime.now().strftime("%H:%M:%S.%f")[:-3]

    def _write(self, result: Union[str, List[Tuple[str, str]]]):
        if isinstance(result, str):
            result = [("??", result)]
        self.comm[f"result"] = (result, self.now())

    @staticmethod
    def _extract_exp_id(line: str) -> Optional[int]:
        """At some point, the Sacred-monitored run says: Started run with ID \"7160\""""

        id_announcement = "Started run with ID "
        if id_announcement in line:
            number = line.split(id_announcement)[1].split('"')[1]
            try:
                value = int(number)
                return value
            except:
                return None

    def monitor_exp_id(self, line: str, group_id: int) -> str:
        """Update EXPID of a current group from the log.
        Return the current most up-to-date EXPID"""
        parsed = self._extract_exp_id(line)
        if parsed is not None:
            self.exp_ids[group_id] = parsed
        return self.exp_ids[group_id] if group_id in self.exp_ids is not None else "?"

    def download_logs(self, group_ids: List[int], client: SSHClient):
        """Download all the experiment_{group_id}.log files locally"""

        Path(f"remote/logs/{self.name}").mkdir(parents=True, exist_ok=True)

        ftp_client = client.open_sftp()
        # print(f"Client open, downloading the logs...")

        for group_id in group_ids:
            remote_name = f"/home/ubuntu/experiment_{group_id}.log"
            local_name = f"remote/logs/{self.name}/experiment_{group_id}.log"

            if os.path.exists(local_name):
                os.remove(local_name)
            try:
                ftp_client.get(remote_name, local_name)
                print(f"Downloaded the: {local_name}")
            except SSHException:
                print(f"SFTP failed")

        ftp_client.close()

    def download_logs_before_shutdown(
        self, group_ids: List[int], group_zero_line: str, client: SSHClient
    ):
        """Check for the SHUTDOWN announcement, if detected, fetch all logs locally."""

        if SHUTDOWN_MESSAGE in group_zero_line and not self.is_final_logs_downloaded:
            self.is_final_logs_downloaded = True
            print(f"SHUTDOWN_MESSAGE detected, downloading the final logs locally...")
            self.download_logs(group_ids, client)
            print(f"Logs downloaded!")

    def _initial_detect_ids(self, group_ids: List[int], client: SSHClient):
        """Initial attempt to determine EXPID
            -for each group_id do:
                -download the log,
                -go backwards through the log and locate EXPID announcement
                -remember it
        """

        self.download_logs(group_ids, client)

        for group_id in group_ids:
            local_name = f"remote/logs/{self.name}/experiment_{group_id}.log"

            try:
                fp = open(local_name)
                lines = fp.readlines()
                for line in reversed(lines):
                    expid = self._extract_exp_id(line)
                    if expid is not None:
                        # print(f'DETECTED for {group_id} EXPID={expid} ')
                        self.exp_ids[group_id] = expid
                        fp.close()
                        break
                fp.close()
            except:
                continue

    @staticmethod
    def extract_last_line(stdout_result: bytes) -> str:
        """Gets the output of stdout.read() and extracts the last line.

        It handles the tqdm's newlines, carriage returns, arrows.. in the string.
        """
        # split by carriage returns (moves cursor back to update the progress bar)
        last_line = stdout_result.decode("utf-8").split("\r")[-1].strip()
        # extract the last line
        last_line = last_line.split("\n")[-1]
        # remove the up arrow used by the validation
        last_line = last_line.replace("\x1b[A", "")
        return last_line

    def run(self):
        instance = _get_instance_by_name(self.name, self.config)
        if instance is None:
            self._write(f"ERROR: could not find the instance")
            return
        instance_ip = instance["PrivateIpAddress"]
        # self.write(f'Connecting to {self.name} with IP: {instance_ip}')
        self._write(f"Connecting...")

        key = paramiko.RSAKey.from_private_key_file(self.config.pem_file)
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        while not self.comm["should_stop"]:

            if _get_instance_by_name(self.name, self.config) is None:
                self._write(f"My instance terminated, exiting!")
                return

            try:
                client.connect(hostname=instance_ip, username="ubuntu", pkey=key)
                # self.write(f'DONE, connected to {self.name} with IP: {instance_ip}')
                self._write(f"DONE, connected!")

                while not self.comm["should_stop"]:

                    if _get_instance_by_name(self.name, self.config) is None:
                        self._write(f"My instance terminated, exiting!")
                        return

                    # read the CPU load
                    stdin, stdout, stderr = client.exec_command(f"cat /proc/loadavg")
                    last_min_load = stdout.read().decode("utf-8").split(" ")[0].strip()
                    self.comm["load"] = last_min_load

                    # read the GPU utilization, memory, total memory and format it
                    stdin, stdout, stderr = client.exec_command(
                        "nvidia-smi --query-gpu=utilization.gpu,utilization.memory,memory.total "
                        "--format=csv "
                    )
                    line_per_gpu = stdout.read().decode("utf-8").split("\n")[1:]
                    # expected format is: 2 %, 2 %, 11178 MiB
                    result_per_gpu = []
                    for line in line_per_gpu:
                        values = line.split(",")
                        if len(values) == 1:
                            continue
                        res = {
                            "util": values[0].replace(" ", ""),
                            "used_mem": values[1].replace(" ", ""),
                            "total_mem": str(
                                int(round(float(values[2].split(" ")[1]) / 1024, 0))
                            ),
                        }
                        result_per_gpu.append(res)
                    self.comm["gpu"] = result_per_gpu

                    # get experiment_{group_id}.log files:
                    stdin, stdout, stderr = client.exec_command(f"ls experiment_*.log")
                    files = stdout.read().decode("utf-8").split("\n")
                    file_ids = []
                    for file in files:
                        if len(file) > 0:
                            f = file.split("_")[1].split(".log")[0]
                            file_ids.append(int(f))
                    file_ids = sorted(file_ids)

                    # detect the EXPID during setup (might have been announced before in the log)
                    if not self.is_exp_ids_detected:
                        self.is_exp_ids_detected = True
                        self._initial_detect_ids(file_ids, client)

                    # read the last line per experiment group
                    results = []
                    for group_id in file_ids:
                        filename = f"/home/ubuntu/experiment_{group_id}.log"
                        stdin, stdout, stderr = client.exec_command(
                            f"tail -n 2 {filename}"
                        )
                        last_line = self.extract_last_line(stdout.read())
                        id = self.monitor_exp_id(
                            last_line, group_id
                        )  # update the EXPID
                        results.append((id, last_line))

                        if group_id == 0:
                            self.download_logs_before_shutdown(
                                file_ids, last_line, client
                            )

                    # write all lines including the heartbeat
                    self._write(results)
                    time.sleep(self.sleep)

            # except SSHException or NoValidConnectionsError:
            except:
                self._write(f"Could not connect!")

        print(f"\t{self.name} - monitor: exiting, bye")
