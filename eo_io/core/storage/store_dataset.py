#  Copyright (c) 2022.
#  The ECHOES Project (https://echoesproj.eu/) / Compass Informatics

"""
Post process and insert data from eoain processing chain into S3 storage
"""

import abc
import datetime
import json
import numpy as np
import rioxarray
import tempfile
from satpy.scene import Scene
import xarray as xr
from os import makedirs
from os.path import dirname, join
from dataclasses import dataclass

from .s3_interface import ReadWriteData
from ..settings import configuration
from ..utils.resample import Resample


class BaseWriter(abc.ABC):
    """
    Upload files to S3
    """

    def __init__(self, store: object, data: 'xr.Dataset or satpy.Dataset', path):
        self.store = store
        self.data = data
        self.path = path
        self.key = None
        self.keys = self.get_keys()
        self.key_idx = 0


    @abc.abstractmethod
    def write(self, full_path):
        ...

    def get_keys(self):
        return list(self.data.keys())

    def to_store(self):
        with tempfile.TemporaryDirectory() as tempdir:
            full_path = join(tempdir, self.path)
            makedirs(dirname(full_path))
            self.write(full_path)
            self.store.upload_file(full_path, self.path)
            print(f"s3-location: {self.store.bucketname} {self.path}")
        return self.path


class IterWrite(abc.ABC):

    @abc.abstractmethod
    def __init__(self):
        self.keys, self.key, self.key_idx = [..., ], ..., 0
        self.product_path = ...

    def __iter__(self):
        return self

    def __next__(self):
        try:
            self.key = self.keys[self.key_idx]
            print(f'Writing variable {self.key} to store')
            product_path = self.to_store()
        except IndexError:
            raise StopIteration()
        self.key_idx += 1
        return product_path

    @abc.abstractmethod
    def to_store(self):
        ...


class GeoTiffWriter(BaseWriter, IterWrite):
    """
    Create GeoTIFF using Rasterio writer and upload to S3
    """

    def write(self, full_path):
        try:
            data = self.data.rename({'lat': 'y', 'lon': 'x'})
        except ValueError:
            pass
        data.rio.to_raster(full_path)


class SceneGeoTiffWriter(BaseWriter, IterWrite):
    """
    Create GeoTIFF using Satpy and upload to S3
    """

    def write(self, full_path):
        self.data.save_datasets(datasets=[self.key['name'], ], filename=full_path, writer='geotiff',
                                include_scale_offset=True, dtype=np.float32)


class MetaDataWriter(BaseWriter):
    """
    Upload metadata to S3
    """

    def write(self, full_path):
        with open(full_path, 'w') as f:
            json.dump(self.data, f)

    def get_keys(self):
        return None


class ZarrWriter(BaseWriter):
    """
    Save data to Zarr on S3
    """

    def get_product_path(self, info, extension):
        return join(self.top_level_directory, info['platform'], info['instrument'], info['processingLevel'], 'zarr')

    def write(self, full_path):
        # ds_store = self.store.read_zarr()
        self.data.attrs = {k: v for k, v in self.data.attrs.items() if
                           isinstance(v, (str, int, float, np.ndarray, list, tuple))}
        ds = self.data.to_dataset(name=self.data.attrs['name'])
        self.store.to_zarr(ds, full_path)
        return self

    def to_store(self):
        product_path = self.get_product_path(self.top_level_directory, self.product_identifier,
                                             self.extension, self.key)
        self.write(product_path)
        print(f"s3-location: {self.store.bucketname} {self.product_path}")
        return product_path



@dataclass
class BaseMetadata(abc.ABC):

    @abc.abstractmethod
    def get_path(self):
        ...


@dataclass
class Metadata(BaseMetadata):
    top_level_directory: str
    platform: str
    instrument: str
    processingLevel: str
    startTimeFromAscendingNode: str
    id: str
    relativeOrbitNumber: int
    platformSerialIdentifier: str

    def get_path(self):
        dt = datetime.datetime.strptime(self.startTimeFromAscendingNode, '%Y-%m-%dT%H:%M:%S.%fZ')
        return join(self.top_level_directory, self.platform, self.instrument, self.processingLevel,
                    str(dt.year), "{:02d}".format(dt.month), self.id, dt.strftime('%Y%m%d'))


class Store:
    """
    Resample dataset, store both metadata and data on S3
    """

    def __init__(self, config_dict, store):
        self.config_dict = config_dict
        self.store = store
        self._metadata = None
        self._dataset = None
        self.path = None
        self.area_id = None
        self.proj_string = None
        self.shape = None
        self.area_extent = None
        self.scene = None

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
        self._dataset = Resample(self._dataset, self.area_id, self.proj_string, self.shape,
                                 self.area_extent).dataset
        return self

    def add_attributes_to_dataset(self):
        # start_time = datetime.datetime.strptime(self._dataarray.start_time, '%d-%b-%Y %H:%M:%S.%f')
        try:
            self._dataset = self._expand_and_add_coord(self._dataset, self._dataset.start_time, 'time')
        except ValueError:
            pass  # Where dimension time already exists.
        self._dataset[r'relativeOrbitNumber'] = xr.DataArray(data=[self._metadata[r'relativeOrbitNumber']], dims=['time'])
        self._dataset['platformSerialIdentifier'] = xr.DataArray(data=[self._metadata['platformSerialIdentifier']],
                                                                 dims=['time'])
        self._dataset['title'] = xr.DataArray(data=[self._metadata['title']], dims=['time'])
        return self

    def read_zarr(self):
        writer = ZarrWriter(self.store, self._dataset, self.path_strs)
        return self.store.read_zarr(writer.product_path)

    def metadata_to_json(self):
        return MetaDataWriter(self.store, self._metadata, self.path + '.json')

    def to_tiff(self):
        return list(GeoTiffWriter(self.store, self._dataset, self.path + '.tif'))

    def to_zarr(self):
        self.add_attributes_to_dataset()
        try:
            self.resample()
        except ValueError:
            pass  # dataset does not exist
        writer = ZarrWriter(self.store, self._dataset, self.path)
        return writer.to_store()

    @property
    def dataset(self):
        return self._dataset

    @dataset.setter
    def dataset(self, dataset):
        self._dataset = dataset

    @property
    def metadata(self):
        return self._metadata

    @metadata.setter
    def metadata(self, metadata):
        self._metadata = metadata
        self.path = self._metadata.get_path()

    def __repr__(self):
        return "Storage object, containing: " + repr(self.dataset)


class StoreScene(Store):
    """
    Store a Satpy scene
    """

    @Store.dataset.setter
    def dataset(self, dataset):
        self.scene = dataset
        self._dataset = dataset
        self._dataset.attrs = {**self.scene.attrs, **self._dataset.attrs}

    def to_tiff(self):
        return list(SceneGeoTiffWriter(self.store, self.scene, self.path + '.tif'))


def store(dataset: "xr.Dataset OR satpy.Dataset", metadata: object):
    config = configuration()
    if isinstance(dataset, Scene):
        store_cls = StoreScene  # A Satpy Scene
    else:
        store_cls = Store

    store_ = store_cls(config, ReadWriteData(config))
    store_.dataset = dataset
    store_.metadata = metadata
    return store_

