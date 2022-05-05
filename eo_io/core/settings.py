#  Copyright (c) 2022.
#  The ECHOES Project (https://echoesproj.eu/) / Compass Informatics

from dataclasses import dataclass
import urllib
from socket import timeout
from botocore.client import Config
from eo_io.core.tools import read_yaml
from glob import glob
from os.path import join
from pathlib import Path
import os


@dataclass
class Configuration:
    platform: str
    priority: int
    filename: str
    region_name: str
    endpoint_url_local: str
    endpoint_url_ext: str
    aws_access_key_id: str
    aws_secret_access_key: str
    config: object
    output_directory: str
    bucket: str
    sh_instance_id: str
    sh_client_id: str
    sh_client_secret: str

    def is_storage_accessible(self) -> bool:
        """
        Call from am_i_on_this_platform() to check that url is accessible on this platform
        If we get some response from the endpoint_url_local then we know we are on that
        platform.
        """
        try:
            status = urllib.request.urlopen(self.endpoint_url_local, timeout=2).status == 200
        except urllib.error.HTTPError:
            status = True
        except urllib.error.URLError:
            status = False
        except timeout:
            status = False
        return status

    def am_i_on_this_platform(self) -> bool:
        try:
            # The fast check
            on_platform = (self.platform == os.environ['DATA_SINK'])
        except KeyError:
            # The slow check
            on_platform = self.is_storage_accessible()
            if on_platform:
                # Set the environment variable so that next time it will be fast
                os.environ['DATA_SINK'] = self.platform
        return on_platform


def configs():
    home_dir = os.path.expanduser("~")
    for file_name in glob(join(home_dir, 'eoconfig', '*.yaml')):
        config_yaml = read_yaml(file_name)
        platform_name = list(config_yaml.keys())[0]
        platform_settings = config_yaml[platform_name]['storage']
        platform_settings['priority'] = config_yaml[platform_name]['priority']
        platform_settings['platform'] = platform_name
        platform_settings['filename'] = file_name
        platform_settings['config'] = eval(platform_settings['config']) if platform_settings['config'] else None
        platform_settings['sh_instance_id'] = config_yaml['sentinel-hub']['instance_id']
        platform_settings['sh_client_id'] = config_yaml['sentinel-hub']['sh_client_id']
        platform_settings['sh_client_secret'] = config_yaml['sentinel-hub']['sh_client_secret']
        yield platform_settings


def configuration():
    # sort so the highest priority comes first
    configs_list = sorted(configs(), key=lambda c: c['priority'], reverse=True)
    config_dict = {platform['platform']: platform for platform in configs_list}
    try:
        # The case where the data source is given by the environment variable
        data_source_platform = config_dict[os.environ['DATA_SOURCE']]
    except KeyError:
        # Otherwise get the highest priority data source
        data_source_platform = configs_list[0]
        os.environ['DATA_SOURCE'] = data_source_platform['platform']

    # If we are not on the data_source_platform we cannot store there, so find where we can
    for platform_settings in configs_list:
        platform = Configuration(**platform_settings)
        if platform.am_i_on_this_platform():
            break
    else:
        raise IOError('Could not determine the platform')

    config = Configuration(**{**platform_settings, **data_source_platform})

    # try:
    #     os.environ['SH_CLIENT_ID'] = config.sh_client_id
    #     os.environ['SH_CLIENT_SECRET'] = config.sh_client_secret
    # except AttributeError:
    #     print('Sentinel-Hub credentials missing from configuration file')

    return config


if __name__ == '__main__':
    print(configuration())
