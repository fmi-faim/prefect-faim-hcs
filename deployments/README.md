# Build
```shell
prefect deployment build src/prefect_faim_hcs/flows/molecular_devices_to_ome_zarr_3d.py:molecular_devices_to_ome_zarr_3d -n "default" -q slurm -sb github/prefect-faim-hcs --skip-upload -o deployments/molecular_devices_to_ome_zarr_3d.yaml -ib process/prefect-faim-hcs -t hcs -t MD
```

# Apply
```shell
prefect deployment apply deployments/molecular_devices_to_ome_zarr_3d.yaml
```
