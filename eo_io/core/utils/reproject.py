import numpy as np
from pyproj import Proj, itransform


def reproject(in_proj, out_proj, xs, ys):
    proj_in = Proj(init="epsg:" + str(in_proj))
    proj_out = Proj(init="epsg:" + str(out_proj))
    new_x, new_y = list(zip(*itransform(proj_in, proj_out, zip(*[xs, ys]), always_xy=True)))
    return np.array(new_x), np.array(new_y)
