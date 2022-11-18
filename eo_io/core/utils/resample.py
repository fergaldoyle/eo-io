import numpy as np
import xarray as xr
from pyresample import kd_tree, geometry


class Resample:

    def __init__(self, dataset, area_id, proj_string, shape, area_extent):
        self.ds = dataset
        self.dataset_swath_def = self.swath_def(dataset)

        if area_id and proj_string and shape and area_extent:
            self.target_area_def = geometry.create_area_def(area_id, proj_string, shape=shape,
                                                            area_extent=area_extent)
        else:
            self.target_area_def = self.swath_def(self.ds).compute_optimal_bb_area()

    @staticmethod
    def swath_def(dataset):
        dl = len(dataset.lon.shape)
        if dl == 1:
            lons, lats = np.meshgrid(dataset.lon.values, dataset.lat.values)
        elif dl == 2:
            lons, lats = dataset.lon.values, dataset.lat.values
        else:
            raise ValueError('The dimensions the lats/lons are excepted to be 1D or 2D')
        return geometry.SwathDefinition(lons=lons, lats=lats)

    def resample(self, values):
        res = kd_tree.resample_gauss(self.dataset_swath_def,
                                     values.ravel(),
                                     self.target_area_def,
                                     radius_of_influence=40,
                                     sigmas=20,
                                     reduce_data=False)
        area = self.target_area_def
        lons, lats = area.get_lonlats()
        return xr.DataArray(
            res,
            dims=('y', 'x'),
            coords={
                'y': area.projection_y_coords,
                'x': area.projection_x_coords,
                'lon': (['y', 'x'], lons),
                'lat': (['y', 'x'], lats),
            },
        ).chunk(*self.target_area_def.shape)

    @property
    def dataset(self):
        ds_attrs = self.ds.attrs.copy()
        self.ds = xr.Dataset({k: self.resample(ds.values) for k, ds in self.ds.items()})
        self.ds.attrs = ds_attrs
        self.ds.attrs.update({'area_id': self.target_area_def.area_id,
                              'proj_string': self.target_area_def.proj_str,
                              'shape': self.target_area_def.shape,
                              'area_extent': self.target_area_def.area_extent})
        return self.ds


