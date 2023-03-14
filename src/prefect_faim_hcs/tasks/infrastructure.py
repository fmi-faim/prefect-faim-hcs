import os
import subprocess
from os.path import join

from prefect import task
from prefect.tasks import task_input_hash


@task(cache_key_fn=task_input_hash)
def log_infrastructure(run_dir: str):
    env = {}
    env.update(os.environ)

    env_prefix = os.environ["CONDA_PREFIX"]
    cmd = (
        f"micromamba env export -p {env_prefix} >"
        f" {join(run_dir, 'environment.yaml')}"
    )
    result = subprocess.run(cmd, shell=True, check=True, env=env)
    result.check_returncode()

    cmd = (
        f"micromamba run -p {env_prefix} pip list --format=freeze >"
        f" {join(run_dir, 'requirements.txt')}"
    )
    result = subprocess.run(cmd, shell=True, check=True)
    result.check_returncode()

    cmd = f"hostnamectl > {run_dir}/host.txt"
    result = subprocess.run(cmd, shell=True, check=True)
    result.check_returncode()
