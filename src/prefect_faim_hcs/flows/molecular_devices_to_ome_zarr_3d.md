# Molecular Devices ImageXpress to OME-Zarr - 3D
Converts a 3D multi-channel multi-well plate acquisition into an OME-Zarr.

## Input Format
Standard Molecular Devices ImageXpress acquistions can be converted. Such an acquisition can contain z-projections, single z-planes and z-stacks.

## Flow Parameters
* `user`:
    * `name`: Name of the user.
    * `group`: Group name of the user.
    * `run_name`: Name of processing run.
* `acquisition_dir`: Path to the MD ImageXpress acquisition directory.
* `ome_zarr`:
    * `output_dir`: Path to where the OME-Zarr is written to.
    * `order_name`: Name of the plate order.
    * `barcode`: Plate barcode.
    * `n_channels`: List of integers indicating the channels.
    * `plate_layout`: Either 96-well-plate or 384-well-plate layout.
    * `write_empty_chunks`: Set this to `False` if you have acquired single planes alongside full z-stacks.
* `mobie`:
    * `project_folder`: MoBIE project folder.
    * `dataset_name`: Name of this dataset.
    * `description`: Description of the dataset.
* `parallelization`: How many wells are written in parallel. This number if optimized for our setup. __Do not change this.__

## Output Format
The output is an OME-Zarr which extends the [NGFF spec](https://ngff.openmicroscopy.org/latest/#hcs-layout).

All acquired fields of a well are montaged into a `CZYX` stack and saved in the zeroth field of the corresponding well. The respective projections are saved as `CYX` in the sub-group `projecitons` of the well-group.

### Metadata
Multiple metadata fields are added to the OME-Zarr `.zattrs` files.

`{plate_name}/.zattrs`:
* `barcode`: The barcode of the imaged plate
* `order_name`: Name of the plate order

`{plate_name}/{row}/{col}/0/.zattrs`:
* `acquisition_metadata`: A dictionary with key `channels`.
    * `channels`: A list of dicitionaries for each acquired channel, with the following keys:
        * `channel-name`: Name of the channel during acquisition
        * `display-color`: RGB hex-code of the display color
        * `exposure-time`
        * `exposure-time-unit`
        * `objective`: Objective description
        * `objective-numerical-aperture`
        * `power`: Illumination power used for this channel
        * `shading-correction`: Set to `On` if a shading correction was applied automatically.
        * `wavelength`: Name of the wavelength as provided by the microscope.
* `histograms`: A list of relative paths to the histograms of each channel.

## Packages
* [faim-hcs](https://github.com/fmi-faim/faim-hcs)
* [mobie-utils-python](https://github.com/mobie/mobie-utils-python)
* [custom-prefect-result](https://github.com/fmi-faim/custom-prefect-result)
* [faim-prefect](https://github.com/fmi-faim/faim-prefect)
* [prefect](https://github.com/PrefectHQ/prefect)
* [prefect-shell](https://github.com/PrefectHQ/prefect-shell)
