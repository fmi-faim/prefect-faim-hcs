from cpr.csv.CSVTarget import CSVTarget
from cpr.utilities.utilities import task_input_hash
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
from prefect import get_run_logger, task
from zarr import Group


def add_CYX_image_to_zarr_group(
    group: Group,
    files: DataFrame,
    channels: list[str],
    write_empty_chunks: bool = True,
):
    image, ch_hists, ch_metadata, metadata = get_well_image_CYX(
        well_files=files,
        channels=channels,
        assemble_fn=montage_grid_image_YX,
    )
    write_cyx_image_to_well(
        img=image,
        histograms=ch_hists,
        ch_metadata=ch_metadata,
        general_metadata=metadata,
        group=group,
        write_empty_chunks=write_empty_chunks,
    )


def add_CZYX_image_to_zarr_group(
    group: Group,
    files: DataFrame,
    channels: list[str],
    write_empty_chunks: bool = True,
):
    stack, ch_hists, ch_metadata, metadata = get_well_image_CZYX(
        well_files=files,
        channels=channels,
        assemble_fn=montage_grid_image_YX,
    )
    write_czyx_image_to_well(
        img=stack,
        histograms=ch_hists,
        ch_metadata=ch_metadata,
        general_metadata=metadata,
        group=group,
        write_empty_chunks=write_empty_chunks,
    )


@task(cache_key_fn=task_input_hash)
def build_zarr_scaffold_task(
    root_dir: str,
    files: CSVTarget,
    layout: PlateLayout,
    order_name: str,
    barcode: str,
):
    plate = build_zarr_scaffold(
        root_dir=root_dir,
        files=files.get_data(),
        layout=layout,
        order_name=order_name,
        barcode=barcode,
    )

    return ZarrSource.from_path(
        path=plate.store.path,
        group="/",
        mode="w",
    )


@task(cache_key_fn=task_input_hash)
def add_well_to_plate_task(
    zarr_source: ZarrSource,
    files_proxy: CSVTarget,
    well: str,
    channels: list[str],
    write_empty_chunks: bool = True,
) -> ZarrSource:
    files = files_proxy.get_data()

    logger = get_run_logger()
    logger.info(f"Start processing well {well}.")

    plate = zarr_source.get_data()
    row = well[0]
    col = str(int(well[1:]))
    field: Group = plate[row][col][0]
    projections = field.create_group("projections")

    well_files = files[files["well"] == well]

    projection_files = well_files[well_files["z"].isnull()]
    if len(projection_files) > 0:
        add_CYX_image_to_zarr_group(
            group=projections,
            files=projection_files,
            channels=channels,
            write_empty_chunks=write_empty_chunks,
        )

    stack_files = well_files[~well_files["z"].isnull()]
    if len(stack_files) > 0:
        add_CZYX_image_to_zarr_group(
            group=field,
            files=stack_files,
            channels=channels,
            write_empty_chunks=write_empty_chunks,
        )

    zarr_well = ZarrSource.from_path(
        path=zarr_source.get_path(),
        group=f"{row}/{col}/0",
        mode="r",
    )
    return zarr_well
