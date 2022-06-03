#  Copyright (c) 2022.
#  The ECHOES Project (https://echoesproj.eu/) / Compass Informatics

from dataclasses import dataclass
from botocore.client import Config  # Do not remove: used in eval
from eo_io.core.tools import read_yaml
from os.path import join
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


def configuration():
    home_dir = os.path.expanduser("~")
    file_name = join(home_dir, 'eoconfig', 'config_eo_service.yml')
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
    return Configuration(**platform_settings)


if __name__ == '__main__':
    print(configuration())
