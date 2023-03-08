import pandas as pd
from cpr.zarr.ZarrSource import ZarrSource
from faim_hcs.MetaSeriesUtils import (
    get_well_image_CYX,
    get_well_image_CZYX,
    montage_grid_image_YX,
)
from faim_hcs.Zarr import (
    PlateLayout,
    build_zarr_scaffold,
    write_cyx_image_to_well,
    write_czyx_image_to_well,
)
from pandas import DataFrame
from prefect import task
from zarr import Group


def add_CYX_image_to_zarr_group(
    group: Group,
    files: DataFrame,
    channels: list[str],
):
    image, ch_hists, ch_metadata, metadata = get_well_image_CYX(
        well_files=files,
        channels=channels,
        assemble_fn=montage_grid_image_YX,
    )
    write_cyx_image_to_well(image, ch_hists, ch_metadata, metadata, group, True)


def add_CZYX_image_to_zarr_group(
    group: Group,
    files: DataFrame,
    channels: list[str],
):
    stack, ch_hists, ch_metadata, metadata = get_well_image_CZYX(
        well_files=files,
        channels=channels,
        assemble_fn=montage_grid_image_YX,
    )
    write_czyx_image_to_well(stack, ch_hists, ch_metadata, metadata, group, True)


@task()
def build_zarr_scaffold_task(
    root_dir: str,
    files: pd.DataFrame,
    layout: PlateLayout,
    order_name: str,
    barcode: str,
):
    plate = build_zarr_scaffold(
        root_dir=root_dir,
        files=files,
        layout=layout,
        order_name=order_name,
        barcode=barcode,
    )

    return ZarrSource.from_path(
        path=plate.store.path,
        group="/",
        mode="w",
    )


@task()
def add_well_to_plate_task(
    zarr_source: ZarrSource,
    files: DataFrame,
    well: str,
    channels: list[str],
):
    plate = zarr_source.get_data()
    field: Group = plate[well[0]][str(int(well[1:]))][0]
    projections = field.create_group("projections")

    well_files = files[files["well"] == well]

    projection_files = well_files[well_files["z"].isnull()]
    if len(projection_files) > 0:
        add_CYX_image_to_zarr_group(
            group=projections,
            files=projection_files,
            channels=channels,
        )

    stack_files = well_files[~well_files["z"].isnull()]
    if len(stack_files) > 0:
        add_CZYX_image_to_zarr_group(
            group=field,
            files=stack_files,
            channels=channels,
        )

    return zarr_source
