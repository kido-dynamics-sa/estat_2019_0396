import numpy as np


def lonlat_to_tile0(lng, lat):
    """Transform each of lng and lat into its mercator float in [0-1].

    The returned xtile, ytile can be used to obtain the x,y at any arbitratry
    zoom level (up to the precision provided by the float).
    """
    lat = np.radians(lat)
    xtile = (lng + 180.0) / 360.0
    ytile = (1.0 - np.log(np.tan(lat) + (1.0 / np.cos(lat))) / np.pi) / 2.0
    return xtile, ytile


def lonlat_to_tile(lng, lat, zoom):
    """Transform lng and lat to Mercator X, Y tiles at zoom level *zoom*."""
    xtile, ytile = lonlat_to_tile0(lng, lat)
    n = 2**zoom
    return (xtile * n).astype("uint64"), (ytile * n).astype("uint64")


def tile_to_lonlat(xtile, ytile, zoom, center=False):
    """Return the lon, lat corresponding to a Mercator tile.

    If *center=False*, the lon, lat of the "bottom left" corner (min lon, lat)
    is returned. If *center=True* the lon, lat of the center of the tile is.
    """
    if center:
        xtile += 0.5
        ytile += 0.5
    n = 2.0**zoom
    lon_deg = xtile / n * 360.0 - 180.0
    lat_rad = np.arctan(np.sinh(np.pi * (1.0 - 2.0 * ytile / n)))
    lat_deg = np.degrees(lat_rad)
    return lon_deg, lat_deg


def encode(lng, lat, z=32):
    """Encode a (lng, lat) to a single integer to precision of zoom level *z*.

    The value returned is the values of the xtile and ytile padded together.
    The default zoom is set to z=32 as it is the maximum that can fit the
    encoding in an uint64.
    """
    x, y = lonlat_to_tile(lng, lat, z)
    return x * 2**z + y


def decode(geocode, z=32, center=True):
    """Transform a *geocode* obtained with *encode* to an approximate lon, lat.

    The returned lon, lat is equal to the original encoded values up to the
    precision of the z level. Note that the value of z _must_ be the same
    used with the function *encode* for results to be consistent.
    """
    x = geocode // 2**z
    y = geocode % (2**z)
    return tile_to_lonlat(x, y, z, center=center)


def average_tiles(xs, ys, xz, weights=None, grouping=None):
    """Return the average lon/lat of tiles with optional weighting and grouping.

    The average is computed as arithmetic mean of the lon/lats of the tiles
    defined by xs, ys, zs. If a grouping is given, the xs, ys, zs must be
    pandas.Series, otherwise they can be pandas.Series or numpy arrays.
    """
    lons, lats = tile_to_lonlat(xs, ys, xz, center=True)
    if weights is not None:
        lons *= weights
        lats *= weights
        if grouping is not None:
            norm = weights.groupby(grouping).mean()
        else:
            norm = weights.mean()
    else:
        norm = 1

    if grouping is not None:
        mean_lons = lons.groupby(grouping).mean() / norm
        mean_lats = lats.groupby(grouping).mean() / norm
    else:
        mean_lons = lons.mean() / norm
        mean_lats = lats.mean() / norm
    return mean_lons, mean_lats


def recursive_count_level(df, zoom_level, threshold, max_zoom=16):
    """UNTESTED. Recursively build a tree by Mercator tiles.

    The *df* needs to have df['X0'] and df['Y0'] corresponding to the
    lonlat_to_tile0.
    """
    if df.shape[0] < threshold or zoom_level >= max_zoom:
        return df.shape[0]
    else:
        return (
            df.groupby(
                [
                    (df["X0"] * 2**zoom_level).astype(int),
                    (df["Y0"] * 2**zoom_level).astype(int),
                ]
            )
            .apply(recursive_count_level, zoom_level + 1, threshold)
            .to_dict()
        )


def haversine(lonlat1, lonlat2):
    """Calculate the great circle distance in m between two points on the
    earth (specified in decimal degrees).
    """
    lonlat1 = np.radians(lonlat1)
    lonlat2 = np.radians(lonlat2)
    delta = lonlat2 - lonlat1
    a = (
        np.sin(delta[:, 1] / 2.0) ** 2
        + np.cos(lonlat1[:, 1]) * np.cos(lonlat2[:, 1]) * np.sin(delta[:, 0] / 2.0) ** 2
    )
    c = 2 * np.arcsin(np.sqrt(a))
    return 6367000 * c


def distance_codes(codes1, codes2, z=32):
    x1, y1 = codes1 // 2**z, codes1 % (2**z)
    x2, y2 = codes2 // 2**z, codes2 % (2**z)
    dxs = np.sign(x1 - x2)
    dys = np.sign(y1 - y2)
    lon1, lat1 = tile_to_lonlat(
        x1 - np.minimum(dxs, 0), y1 - np.maximum(dys, 0), zoom=z
    )
    lon2, lat2 = tile_to_lonlat(
        x2 + np.maximum(dxs, 0), y2 + np.minimum(dys, 0), zoom=z
    )
    return haversine(np.stack([lon1, lat1]).T, np.stack([lon2, lat2]).T)
