from os.path import join

from faim_hcs.io.MolecularDevicesImageXpress import parse_files
from faim_hcs.Zarr import PlateLayout
from faim_prefect.parallelization.utils import wait_for_task_run
from prefect import flow, get_run_logger
from pydantic import BaseModel

from src.prefect_faim_hcs._version import version
from src.prefect_faim_hcs.tasks.mobie import add_mobie_dataset, create_mobie_project
from src.prefect_faim_hcs.tasks.zarr import (
    add_well_to_plate_task,
    build_zarr_scaffold_task,
)


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


@flow(
    name="MolecularDevices to OME-Zarr [3D]",
    description=description,
    version=version,
)
def molecular_devices_to_ome_zarr_3d(
    acquisition_dir: str,
    ome_zarr: OMEZarr,
    mobie: MoBIE,
    parallelization: int = 24,
):
    logger = get_run_logger()

    logger.info(f"OME-Zarr output-dir: {ome_zarr.output_dir}")
    logger.info(f"MoBIE output-dir: {mobie.project_folder}")

    files = parse_files(acquisition_dir=acquisition_dir)
    logger.info(f"Found {len(files)} in {acquisition_dir}")

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

    return plate, join(mobie.project_folder, mobie.dataset_name)


if __name__ == "__main__":
    molecular_devices_to_ome_zarr_3d()
