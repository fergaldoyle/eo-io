#  Copyright (c) 2022.
#  The ECHOES Project (https://echoesproj.eu/) / Compass Informatics

import abc
import datetime
import functools
import json
import numpy as np
import os
import rioxarray
import tempfile
from satpy.scene import Scene
import xarray as xr
from os import makedirs
from os.path import dirname, join

from .s3_interface import ReadWriteData
from ..settings import configuration
from ..utils.resample import Resample


class BaseWriter(abc.ABC):
    """
    Upload files to S3
    """

    def __init__(self, store, data, top_level_directory, product_identifier, extension=''):
        self.store = store
        self.data = data
        self.top_level_directory = top_level_directory
        self.file_path = self._product_path(product_identifier, extension)

    def _product_path(self, info, extension):
        dt = datetime.datetime.strptime(info['startTimeFromAscendingNode'], '%Y-%m-%dT%H:%M:%S.%fZ')
        return join(self.top_level_directory, info['platform'], info['instrument'], info['processingLevel'],
                    str(dt.year), "{:02d}".format(dt.month), info['id']) + extension

    @abc.abstractmethod
    def write(self, full_path):
        pass

    def to_store(self):
        with tempfile.TemporaryDirectory() as tempdir:
            full_path = join(tempdir, self.file_path)
            makedirs(dirname(full_path))
            self.write(full_path)
            self.store.upload_file(full_path, self.file_path)
            print(f"s3-location: {self.store.bucketname} {self.file_path}")


class GeoTiffWriter(BaseWriter):
    """
    Create GeoTIFF using Rasterio writer and upload to S3
    """

    def write(self, full_path):
        self.data.rio.to_raster(full_path)


class SceneGeoTiffWriter(BaseWriter):
    """
    Create GeoTIFF using Satpy and upload to S3
    """

    def write(self, full_path):
        name = self.data.keys()[0]
        self.data.save_datasets(datasets=[name, ], filename=full_path, writer='geotiff', include_scale_offset=True,
                                dtype=np.float32)


class MetaDataWriter(BaseWriter):
    """
    Upload metadata to S3
    """

    def write(self, full_path):
        with open(full_path, 'w') as f:
            json.dump(self.data, f)


class ZarrWriter(BaseWriter):
    """
    Save data to Zarr on S3
    """

    def _product_path(self, info, extension):
        return join(self.top_level_directory, info['platform'], info['instrument'], info['processingLevel'], 'zarr')

    def write(self, full_path):
        # ds_store = self.store.read_zarr()
        self.data.attrs = {k: v for k, v in self.data.attrs.items() if
                           isinstance(v, (str, int, float, np.ndarray, list, tuple))}
        ds = self.data.to_dataset(name=self.data.attrs['name'])
        self.store.to_zarr(ds, full_path)
        return self

    def to_store(self):
        self.write(self.file_path)
        print(f"s3-location: {self.store.bucketname} {self.file_path}")


class Store:
    """
    Resample dataset, store both metadata and data on S3
    """

    def __init__(self, config_dict, store, product_directory):
        self.config_dict = config_dict
        self.top_level_directory = product_directory
        self.store = store
        self._dataarray = None
        self._info = None
        self.area_id = None
        self.proj_string = None
        self.shape = None
        self.area_extent = None
        self.scene = None
        self.file_path = None

    @staticmethod
    def _expand_and_add_coord(ds, value, dim):
        ds = ds.expand_dims(dim=dim)
        ds[dim] = [value]
        ds = ds.assign_coords({dim: [value]})
        return ds

    def _set_area_info(self):
        if not (self.area_id or self.proj_string):
            datacube = self.read_zarr()
            if datacube:
                self.area_id = datacube.attrs['area_id']
                self.proj_string = datacube.attrs['proj_string']
                self.shape = datacube.attrs['shape']
                self.area_extent = datacube.attrs['area_extent']
            else:
                raise ValueError('No dataset stored')

    def resample(self):
        self._set_area_info()
        self._dataarray = Resample(self._dataarray, self.area_id, self.proj_string, self.shape,
                                   self.area_extent).dataset
        return self

    def add_attributes_to_dataset(self):
        # start_time = datetime.datetime.strptime(self._dataarray.start_time, '%d-%b-%Y %H:%M:%S.%f')
        try:
            self._dataarray = self._expand_and_add_coord(self._dataarray, self._dataarray.start_time, 'time')
        except ValueError:
            pass  # Where dimension time already exists.
        self._dataarray[r'relativeOrbitNumber'] = xr.DataArray(data=[self._info[r'relativeOrbitNumber']], dims=['time'])
        self._dataarray['platformSerialIdentifier'] = xr.DataArray(data=[self._info['platformSerialIdentifier']],
                                                                   dims=['time'])
        self._dataarray['title'] = xr.DataArray(data=[self._info['title']], dims=['time'])
        return self

    def read_zarr(self):
        writer = ZarrWriter(self.store, self._dataarray, self.top_level_directory, self._info)
        return self.store.read_zarr(writer.file_path)

    def metadata_to_json(self):
        writer = MetaDataWriter(self.store, self._info, self.top_level_directory, self._info, '.json')
        writer.to_store()
        self.file_path = writer.file_path
        return self

    def to_tiff(self):
        writer = GeoTiffWriter(self.store, self._dataarray, self.top_level_directory, self._info, '.tif')
        writer.to_store()
        self.file_path = writer.file_path
        return self

    def to_zarr(self):
        self.add_attributes_to_dataset()
        try:
            self.resample()
        except ValueError:
            pass  # dataset does not exist
        writer = ZarrWriter(self.store, self._dataarray, self.top_level_directory, self._info)
        writer.to_store()
        self.file_path = writer.file_path
        return self

    @property
    def dataset(self):
        return self._dataarray

    @dataset.setter
    def dataset(self, dataset):
        self._dataarray = dataset

    @property
    def info(self):
        return self._info

    @info.setter
    def info(self, info):
        self._info = info

    def __repr__(self):
        return repr(self.store)


class StoreScene(Store):

    @Store.dataset.setter
    def dataset(self, dataset):
        self.scene = dataset
        self._dataarray = dataset[dataset.keys()[0]]
        self._dataarray.attrs = {**self.scene.attrs, **self._dataarray.attrs}

    def to_tiff(self):
        SceneGeoTiffWriter(self.store, self.scene, self.top_level_directory, self._info, '.tif')
        return self


def store(dataset, product_name, info):

    config = configuration()
    if isinstance(dataset, Scene):
        store_cls = StoreScene  # A Satpy Scene
    else:
        store_cls = Store

    store_ = store_cls(config, ReadWriteData(config), product_name)
    store_.dataset = dataset
    store_.info = info
    return store_
