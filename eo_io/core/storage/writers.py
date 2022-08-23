import json
import numpy as np
import tempfile
from os import makedirs
from os.path import dirname, join
import abc

from os.path import join
import xarray as xr
from ..utils.resample import Resample


class BaseWriter(abc.ABC):
    """
    Upload files to S3
    """

    def __init__(self, store: object, data: 'xr.Dataset or satpy.Dataset', path):
        self.store = store
        self.data = data
        self.product_path = path
        self.key = None
        self.keys = self.get_keys()
        self.key_idx = 0
        self._validate_dataset()

    def _validate_dataset(self):
        if not self.data:
            raise ValueError('The dataset is empty. Set the dataset and metadata first.')

    @abc.abstractmethod
    def write(self, full_path):
        ...

    def get_keys(self):
        return list(self.data.keys())

    def to_store(self):
        with tempfile.TemporaryDirectory() as tempdir:
            full_path = join(tempdir, self.product_path)
            makedirs(dirname(full_path))
            self.write(full_path)
            self.store.upload_file(full_path, self.product_path)
            print(f"s3-location: {self.store.bucketname} {self.product_path}")
        return self.product_path


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


class ZarrWriterSimple(BaseWriter):
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


class ZarrWriter:

    def __init__(self, dataset, metadata, product_path):
        self._metadata = metadata
        self.dataset = dataset
        self.product_path = product_path
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

    def resample_dataset(self):
        self._set_area_info()
        self.dataset = Resample(self.dataset, self.area_id, self.proj_string, self.shape,
                                self.area_extent).dataset
        return self

    def add_attributes_to_dataset(self):
        # start_time = datetime.datetime.strptime(self._dataarray.start_time, '%d-%b-%Y %H:%M:%S.%f')
        try:
            self.dataset = self._expand_and_add_coord(self.dataset, self.dataset.start_time, 'time')
        except ValueError:
            pass  # Where dimension time already exists.
        self.dataset[r'relativeOrbitNumber'] = xr.DataArray(data=[self._metadata[r'relativeOrbitNumber']], dims=['time'])
        self.dataset['platformSerialIdentifier'] = xr.DataArray(data=[self._metadata['platformSerialIdentifier']],
                                                                dims=['time'])
        self.dataset['title'] = xr.DataArray(data=[self._metadata['title']], dims=['time'])
        return self

    def read_zarr(self):
        writer = ZarrWriter(self.store, self.dataset, self.product_path_strs)
        return self.store.read_zarr(writer.product_path)

    def to_store(self):
        self.add_attributes_to_dataset()
        try:
            self.resample_dataset()
        except ValueError:
            pass  # dataset does not exist
        writer = ZarrWriterSimple(self.store, self.dataset, self.product_path)
        return writer.to_store()
