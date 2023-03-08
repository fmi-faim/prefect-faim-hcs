from faim_hcs.io.MolecularDevicesImageXpress import parse_files
from faim_hcs.Zarr import PlateLayout
from prefect import flow

from src.prefect_faim_hcs._version import version
from src.prefect_faim_hcs.tasks.zarr import (
    add_well_to_plate_task,
    build_zarr_scaffold_task,
)


@flow(
    name="Build OME-Zarr Plate [3D]",
    version=version,
)
def build_ome_zarr_plate_3d(
    acquisition_dir: str,
    output_dir: str,
    order_name: str,
    barcode: str,
    plate_layout: PlateLayout = PlateLayout.I96,
    channels: list[str] = ["w1", "w2", "w3", "w4"],
):
    files = parse_files(acquisition_dir=acquisition_dir)

    plate = build_zarr_scaffold_task(
        root_dir=output_dir,
        files=files,
        layout=plate_layout,
        order_name=order_name,
        barcode=barcode,
    )

    for well in files["well"].unique():
        add_well_to_plate_task(
            zarr_source=plate,
            files=files,
            well=well,
            channels=channels,
        )

    return plate
