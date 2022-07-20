#  Copyright (c) 2022.
#  The ECHOES Project (https://echoesproj.eu/) / Compass Informatics

from os.path import join
import boto3
import zarr
import xarray as xr
from botocore.exceptions import ClientError
from zarr.errors import GroupNotFoundError
import s3fs


class ObjectStoreInterface:
    """S3 setup"""

    def __init__(self, platform):
        self.platform = platform

        self.bucketname = platform.bucket
        self.endpoints = {'external': self.platform.endpoint_url_local,
                          'local': self.platform.endpoint_url_ext}
        self.credentials = dict(region_name=self.platform.region_name,
                                aws_access_key_id=self.platform.aws_access_key_id,
                                aws_secret_access_key=self.platform.aws_secret_access_key,
                                config=self.platform.config)
        self.resource_loc = self._resource(loc_ext='local')
        self.resource_ext = self._resource(loc_ext='external')
        self.client_loc = self._client(loc_ext='local')
        self.client_ext = self._client(loc_ext='external')
        self.client_loc.create_bucket(Bucket=self.bucketname)

    def _resource(self, loc_ext='local'):
        return boto3.resource('s3', endpoint_url=self.endpoints[loc_ext], **self.credentials)

    def _client(self, loc_ext='local'):
        return boto3.client('s3', endpoint_url=self.endpoints[loc_ext], **self.credentials)


class ReadWriteFiles(ObjectStoreInterface):
    """"Read and write files to S3"""

    def upload_file(self, local_fname, store_name):
        try:
            try:
                self.client_loc.upload_file(local_fname, self.bucketname, store_name)
            except TypeError:
                self.client_loc.upload_fileobj(local_fname, self.bucketname, store_name)
            return self.bucketname, store_name
        except ClientError as e:
            print(e)

    def check_exists(self, object_name):
        return self.client_loc.list_objects_v2(Bucket=self.bucketname, Prefix=object_name)['Contents'][0][
                   'Key'] == object_name

    def remove_temp(self):
        """Remove temporary object directory"""
        bucket = self.resource_loc.Bucket(self.bucketname)
        bucket.objects.filter(Prefix="_test").delete()


class ReadWriteZarr(ObjectStoreInterface):
    """Read & write DataSets to the zarr format functionality"""

    def _s3_file_system(self, obj_name, loc_ext='local'):
        s3 = s3fs.S3FileSystem(anon=False,
                               key=self.platform.aws_access_key_id,
                               secret=self.platform.aws_secret_access_key,
                               client_kwargs={'endpoint_url': self.endpoints[loc_ext]})
        return s3fs.S3Map(root=obj_name, s3=s3, check=False)

    def to_zarr(self, dataset, key_name_zarr, append_dim='time'):
        obj_name = join(self.bucketname, key_name_zarr)
        file_sys_io = self._s3_file_system(obj_name)

        dataset['time'] = dataset['time'].astype(int)
        try:
            del dataset['crs']
        except KeyError:
            pass
        if not any(self.resource_loc.Bucket(self.bucketname).objects.filter(
                Prefix=key_name_zarr)):
            compressor = zarr.Blosc(cname='zstd', clevel=3, shuffle=2)
            encodings = {v: {'compressor': compressor} for v in
                         list(set(dataset.data_vars.keys())) + list(dataset._coord_names)}
            dataset.to_zarr(store=file_sys_io, encoding=encodings, consolidated=True)
        else:
            dataset.to_zarr(store=file_sys_io, mode='a', append_dim=append_dim,
                            consolidated=True, compute=False)
        return obj_name

    def read_zarr(self, key_name_zarr):
        obj_name = join(self.bucketname, key_name_zarr)
        file_sys_io = self._s3_file_system(obj_name)

        try:
            with xr.open_zarr(file_sys_io) as ds:
                return ds
        except (TypeError, GroupNotFoundError):
            ds = None  # Store does not exist
        return ds


class ReadWriteData(ReadWriteZarr, ReadWriteFiles):
    pass
