#  Copyright (c) 2022.
#  The ECHOES Project (https://echoesproj.eu/) / Compass Informatics

"""
Post process and insert data from eoain processing chain into S3 storage
"""


from satpy.scene import Scene
from .s3_interface import ReadWriteData
from ..settings import configuration
from .writers import MetaDataWriter, GeoTiffWriter, SceneGeoTiffWriter, ZarrWriter


class Store:
    """
    Resample dataset, store both metadata and data on S3
    """

    def __init__(self, config_dict, store):
        self.config_dict = config_dict
        self.store = store
        self._metadata = None
        self._dataset = None
        self.product_path = None
        self.scene = None
        self.datacube = None

    def metadata_to_json(self):
        return MetaDataWriter(self.store, self._metadata, self.product_path + '.json')

    def to_tiff(self):
        return list(GeoTiffWriter(self.store, self._dataset, self.product_path + '.tif'))

    def to_zarr(self):
        self.datacube = ZarrWriter(self._dataset, self._metadata, self.product_pat)
        return self.datacube.to_store()

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
        self.product_path = self._metadata.get_path()

    @property
    def source_product_path(self):
        return self._metadata.source_product.product_path

    def __repr__(self):
        return repr(self.metadata)
        # return "Storage object, containing: " + repr(self.dataset)


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
        return list(SceneGeoTiffWriter(self.store, self.scene, self.product_path + '.tif'))


def store(dataset: "Union[xr.Dataset, satpy.Dataset, str]", metadata: object):
    config = configuration()
    if isinstance(dataset, Scene):
        store_cls = StoreScene  # A Satpy Scene
    else:
        store_cls = Store

    store_ = store_cls(config, ReadWriteData(config))
    store_.dataset = dataset
    store_.metadata = metadata
    return store_
