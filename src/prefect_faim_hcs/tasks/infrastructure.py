import subprocess
from os.path import join

from prefect import task
from prefect.tasks import task_input_hash


@task(cache_key_fn=task_input_hash)
def log_infrastructure(run_dir: str):
    cmd = f"micromamba env export > {join(run_dir, 'environment.yaml')}"
    result = subprocess.run(cmd, shell=True, check=True)
    result.check_returncode()

    cmd = f"pip list --format=freeze > {join(run_dir, 'requirements.txt')}"
    result = subprocess.run(cmd, shell=True, check=True)
    result.check_returncode()

    cmd = f"hostnamectl > {run_dir}/host.txt"
    result = subprocess.run(cmd, shell=True, check=True)
    result.check_returncode()
