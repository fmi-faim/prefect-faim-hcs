from os.path import basename, join

from cpr.csv.CSVTarget import CSVTarget
from cpr.utilities.utilities import task_input_hash
from faim_hcs.io.MolecularDevicesImageXpress import parse_files
from prefect import get_run_logger, task


@task(cache_key_fn=task_input_hash, refresh_cache=True)
def get_file_list(acquisition_dir: str, run_dir: str):
    files = CSVTarget.from_path(
        path=join(run_dir, basename(acquisition_dir) + "_files.csv")
    )
    logger = get_run_logger()
    logger.info(files.data_hash)

    df = parse_files(acquisition_dir=acquisition_dir)
    files.set_data(df.sort_values(by=df.columns.values.tolist()))
    logger.info(files.data_hash)
    files.compute_data_hash()
    logger.info(files.data_hash)
    return files
