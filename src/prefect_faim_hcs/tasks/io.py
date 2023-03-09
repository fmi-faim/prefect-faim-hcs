from os.path import basename, join

from cpr.csv.CSVTarget import CSVTarget
from cpr.utilities.utilities import task_input_hash
from faim_hcs.io.MolecularDevicesImageXpress import parse_files
from prefect import task


@task(cache_key_fn=task_input_hash, refresh_cache=True)
def get_file_list(acquisition_dir: str, run_dir: str):
    files = CSVTarget.from_path(
        path=join(run_dir, basename(acquisition_dir) + "_files.csv")
    )

    files.set_data(parse_files(acquisition_dir=acquisition_dir))

    return files
