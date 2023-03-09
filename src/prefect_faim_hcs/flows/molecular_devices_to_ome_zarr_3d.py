import json
from os.path import dirname, exists, join

from cpr.Serializer import cpr_serializer
from faim_hcs.Zarr import PlateLayout
from faim_prefect.block.choices import Choices
from faim_prefect.parallelization.utils import wait_for_task_run
from prefect import flow, get_run_logger
from prefect.filesystems import LocalFileSystem
from prefect_shell import ShellOperation
from pydantic import BaseModel

from src.prefect_faim_hcs.tasks.io import get_file_list
from src.prefect_faim_hcs.tasks.mobie import add_mobie_dataset, create_mobie_project
from src.prefect_faim_hcs.tasks.zarr import (
    add_well_to_plate_task,
    build_zarr_scaffold_task,
)

groups = Choices.load("fmi-groups")


class User(BaseModel):
    name: str
    group: groups.get()
    run_name: str


class OMEZarr(BaseModel):
    output_dir: str
    order_name: str
    barcode: str
    n_channels: int
    plate_layout: PlateLayout = PlateLayout.I384
    write_empty_chunks: bool = True


class MoBIE(BaseModel):
    project_folder: str
    dataset_name: str
    description: str


with open(
    join("src/prefect_faim_hcs/flows/molecular_devices_to_ome_zarr_3d.md"),
    encoding="UTF-8",
) as f:
    description = f.read()


def validate_parameters(
    user: User,
    acquisition_dir: str,
    ome_zarr: OMEZarr,
    mobie: MoBIE,
    parallelization: int,
):
    logger = get_run_logger()
    base_dir = LocalFileSystem.load("base-output-directory").basepath
    if not exists(join(base_dir, user.group)):
        logger.error(f"Group '{user.group}' does not exist in '{base_dir}'.")

    if not exists(acquisition_dir):
        logger.error(f"Acquisition directory '{acquisition_dir}' does not " f"exist.")

    if not exists(ome_zarr.output_dir):
        logger.error(f"Output directory '{ome_zarr.output_dir}' does not " f"exist.")

    mobie_parent = dirname(mobie.project_folder.removesuffix("/"))
    if not exists(mobie_parent):
        logger.error(f"Output dir for MoBIE project does not exist: {mobie_parent}")

    if parallelization < 1:
        logger.error(f"parallelization = {parallelization}. Must be >= 1.")

    run_dir = join(base_dir, user.group, user.name, "prefect-runs", user.run_name)

    parameters = {
        "user": user.dict(),
        "acquisition_dir": acquisition_dir,
        "ome_zarr": ome_zarr.dict(),
        "mobie": mobie.dict(),
        "parallelization": parallelization,
    }

    with open(join(run_dir, "parameters.json"), "w") as f:
        f.write(json.dumps(parameters, indent=4))

    return run_dir


@flow(
    name="MolecularDevices to OME-Zarr [3D]",
    description=description,
    cache_result_in_memory=False,
    persist_result=True,
    result_serializer=cpr_serializer(),
    result_storage=LocalFileSystem.load("prefect-faim-hcs"),
)
def molecular_devices_to_ome_zarr_3d(
    user: User,
    acquisition_dir: str,
    ome_zarr: OMEZarr,
    mobie: MoBIE,
    parallelization: int = 24,
):
    run_dir = validate_parameters(
        user=user,
        acquisition_dir=acquisition_dir,
        ome_zarr=ome_zarr,
        mobie=mobie,
        parallelization=parallelization,
    )

    logger = get_run_logger()

    logger.info(f"Run logs are written to: {run_dir}")
    logger.info(f"OME-Zarr output-dir: {ome_zarr.output_dir}")
    logger.info(f"MoBIE output-dir: {mobie.project_folder}")

    files = get_file_list(acquisition_dir=acquisition_dir, run_dir=run_dir)

    plate = build_zarr_scaffold_task(
        root_dir=ome_zarr.output_dir,
        files=files,
        layout=ome_zarr.plate_layout,
        order_name=ome_zarr.order_name,
        barcode=ome_zarr.barcode,
    )

    buffer = []
    wells = []
    for well_id in files["well"].unique():
        buffer.append(
            add_well_to_plate_task.submit(
                zarr_source=plate,
                files=files,
                well=well_id,
                channels=[f"w{i}" for i in range(ome_zarr.n_channels)],
                write_empty_chunks=ome_zarr.write_empty_chunks,
            )
        )

        wait_for_task_run(
            results=wells,
            buffer=buffer,
            max_buffer_length=parallelization,
            result_insert_fn=lambda r: r.result(),
        )

    wait_for_task_run(
        results=wells,
        buffer=buffer,
        max_buffer_length=0,
        result_insert_fn=lambda r: r.result(),
    )

    create_mobie_project(project_folder=mobie.project_folder)

    add_mobie_dataset(
        project_folder=mobie.project_folder,
        dataset_name=mobie.dataset_name,
        description=mobie.description,
        plate=plate,
        is2d=False,
    )

    ShellOperation(commands=[f"micromamba list > {run_dir}/environment.yaml"]).run()

    ShellOperation(commands=[f"pip list > {run_dir}/requirements.txt"]).run()

    ShellOperation(commands=[f"hostnamectl > {run_dir}/host.txt"]).run()

    return plate, join(mobie.project_folder, mobie.dataset_name)


if __name__ == "__main__":
    molecular_devices_to_ome_zarr_3d()
