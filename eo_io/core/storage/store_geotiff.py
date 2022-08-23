#  Copyright (c) 2022.
#  The ECHOES Project (https://echoesproj.eu/) / Compass Informatics

"""
Post process and insert data from Sentinel-Hub API into S3 storage
"""


import os
import tempfile
from datetime import datetime
from glob import glob
from os.path import join, basename
import logging
import eo_io

from osgeo import gdal


class ToS3:

    def __init__(self, processing_module, frequency, request_func, testing=False):
        config_s3 = eo_io.configuration()
        self.storage = eo_io.ReadWriteData(config_s3)
        self.processing_module = processing_module
        self.frequency = frequency
        self.request_func = request_func
        self.info = None
        self.testing = testing

    @staticmethod
    def _product_path(product_identifier, extension):
        product_path = '/'.join(product_identifier.split('/')[2:])
        return os.path.splitext(product_path)[0] + extension

    def get_data(self, tempdir):
        return self.request_func(tempdir)

    def object_name(self, request, local_fname):
        input = request.payload['input']['data'][0]
        instrument = input['type']
        timerange = input['dataFilter']['timeRange']
        bbox = '_'.join(str(i) for i in request.payload['input']['bounds']['bbox'])
        start = datetime.strptime(timerange['from'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y%m%d')
        end = datetime.strptime(timerange['to'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y%m%d')
        fname = join(basename(local_fname))
        try:
            mosaicking = input['dataFilter']['mosaickingOrder']
        except KeyError:
            mosaicking = 'nomosaicking'
        fname = fname.replace('response', mosaicking)
        fname = fname.replace('request', mosaicking)
        name_parts = [f'BBOX({bbox})', self.processing_module, instrument, self.frequency, f'{start}-{end}', fname]
        if self.testing:
            name_parts = ['_test'] + name_parts
        return join(*name_parts)

    @staticmethod
    def compress_geotiff(dataset, local_fname):
        creationOptions = ['COMPRESS=JPEG']
        if dataset.RasterCount >= 3:
            bandList = [1, 2, 3]
            creationOptions.append('PHOTOMETRIC=YCBCR')
            # when YCBCR is enabled, filesize will be much smaller, bump up the quality
            jpeg_quality_option = 'JPEG_QUALITY=95'
        else:
            bandList = None
            jpeg_quality_option = 'JPEG_QUALITY=85'

        creationOptions.append(jpeg_quality_option)
        gdal.Translate(f'{local_fname}.gdal', local_fname, format='GTiff', creationOptions=creationOptions,
                       bandList=bandList)
        os.remove(local_fname)
        os.rename(f'{local_fname}.gdal', local_fname)

    @staticmethod
    def validate_geotiff(dataset):
        r, g, b, *_ = dataset.ReadAsArray()
        a = _[0] if len(_) else None
        try:
            # Check that all the values are not all the same for each band
            assert not all([(i[0] == i).all() for i in (r, g, b, a) if i is not None])
        except AssertionError:
            raise AssertionError("All the bands 0 or 255")

    def to_storage(self):
        with tempfile.TemporaryDirectory() as tempdir:
            request = self.get_data(tempdir)
            object_names = []
            dirname = glob(join(tempdir, '*'))[0]
            fn_tiff = join(dirname, 'response.tiff')
            fn_json = join(dirname, 'request.json')
            try:
                dataset = gdal.Open(fn_tiff)
                self.validate_geotiff(dataset)
                self.compress_geotiff(dataset, fn_tiff)
                store_name_tiff = self.storage.upload_file(fn_tiff, self.object_name(request, fn_tiff))
                store_name_json = self.storage.upload_file(fn_json, self.object_name(request, fn_json))
                object_names.append(store_name_tiff)
                object_names.append(store_name_json)
                print('s3-location: ' + ' '.join(store_name_tiff))
            except AssertionError:
                logging.info(f"Skipping {fn_tiff} as all the bands values are the same value")
                if self.testing:
                    raise
            return object_names
