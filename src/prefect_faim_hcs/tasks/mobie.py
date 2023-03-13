from os.path import exists, join

import mobie.metadata as mom
from cpr.utilities.utilities import task_input_hash
from cpr.zarr.ZarrSource import ZarrSource
from faim_hcs.mobie import add_wells_to_project
from mobie.validation import validate_project
from prefect import get_run_logger, task


@task(cache_key_fn=task_input_hash, refresh_cache=True)
def create_mobie_project(
    project_folder: str,
):
    logger = get_run_logger()
    if exists(project_folder):
        logger.info(f"MoBIE project at {project_folder} already exists.")
    else:
        mom.project_metadata.create_project_metadata(root=project_folder)
        logger.info(f"Created new MoBIE project at {project_folder}.")


@task(cache_key_fn=task_input_hash, refresh_cache=True)
def add_mobie_dataset(
    project_folder: str,
    dataset_name: str,
    description: str,
    plate: ZarrSource,
    is2d: bool,
):
    logger = get_run_logger()
    mom.dataset_metadata.create_dataset_structure(
        root=project_folder,
        dataset_name=dataset_name,
        file_formats=["ome.zarr"],
    )
    mom.dataset_metadata.create_dataset_metadata(
        dataset_folder=join(project_folder, dataset_name),
        description=description,
        is2d=is2d,
    )
    mom.project_metadata.add_dataset(
        root=project_folder,
        dataset_name=dataset_name,
        is_default=False,
    )

    add_wells_to_project(
        plate=plate.get_data(),
        dataset_folder=join(project_folder, dataset_name),
        well_group="0",
        view_name="default",
    )

    add_wells_to_project(
        plate=plate.get_data(),
        dataset_folder=join(project_folder, dataset_name),
        well_group="0/projections",
        view_name="Projections",
        label_suffix="_projection",
    )

    validate_project(root=project_folder)

    logger.info(f"Added {dataset_name} to MoBIE project {project_folder}.")
