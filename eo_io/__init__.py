#  Copyright (c) 2022.
#  The ECHOES Project (https://echoesproj.eu/) / Compass Informatics

from eoian.core.settings import configuration
from eoian.core.storage.s3_interface import ReadWriteData
from eoian.core.tools import read_yaml
from eoian.core.storage import store_dataset, store_geotiff