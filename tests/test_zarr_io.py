import pytest
from eo_io import ReadWriteZarr

import eo_io
import xarray as xr
import numpy as np

print(ReadWriteZarr)
config_s3 = eo_io.configuration()


@pytest.fixture
def remove_objects():
    """
    Delete _tests/ in the bucket, both before and after the test is done
    """
    # pass
    store_temp = eo_io.ReadWriteData(config_s3)
    store_temp.remove_temp()
    yield
    store_temp.remove_temp()  # To see the output, comment out this line and look in  <bucket-name>/_tests/


def test_write(remove_objects):
    da = xr.DataArray(np.random.randn(3, 3, 2),
                      dims=("x", "y", 'time'),
                      coords={"x": [1, 2, 3], "y": [1, 2, 3], "time": [1, 2]})
    ds = xr.Dataset({'test': da})
    store = eo_io.ReadWriteZarr(config_s3)
    store.to_zarr(ds, '_tests')
    assert ds == store.read_zarr('_tests')
