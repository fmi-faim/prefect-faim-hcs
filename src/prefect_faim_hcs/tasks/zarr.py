import pandas as pd
from cpr.zarr.ZarrSource import ZarrSource
from faim_hcs.Zarr import PlateLayout, build_zarr_scaffold
from prefect import task


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
