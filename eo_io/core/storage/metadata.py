import abc
import datetime
from os.path import join
from dataclasses import dataclass


@dataclass
class BaseMetadata(abc.ABC):

    @abc.abstractmethod
    def get_path(self):
        ...


@dataclass
class Metadata(BaseMetadata):
    source_product: str
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