"""Utility fonctions for osm-deep-labels
"""
from osgeo import gdal, osr
import os
import requests


def get_image_coordinates(filename):
    """Retrieve geotiff image coordinates with GDAL

    Use the `GetGeoTransform` method, that provides the following values:
      + East/West location of Upper Left corner
      + East/West pixel resolution
      + 0.0
      + North/South location of Upper Left corner
      + 0.0
      + North/South pixel resolution

    See GDAL documentation (https://www.gdal.org/gdal_tutorial.html)

    Parameters
    ----------
    filename : str
        Name of the image file from which coordinates are extracted

    Returns
    -------
    dict
        Bounding box of the image (west, south, east, north coordinates)
    
    """
    ds = gdal.Open(filename)
    width = ds.RasterXSize
    height = ds.RasterYSize
    gt = ds.GetGeoTransform()
    minx = gt[0]
    miny = gt[3] + height * gt[5]
    maxx = gt[0] + width * gt[1]
    maxy = gt[3]
    minx, miny, maxx, maxy = set_coordinates_as_x_y(minx, miny, maxx, maxy, ds)
    return {"west": minx, "south": miny, "east": maxx, "north": maxy}


def set_coordinates_as_x_y(minx, miny, maxx, maxy, ds):
    """Transform coordinates into a (x,y)-compatible projection

    Parameters
    ----------
    minx : float
        Min-x coordinates (west)
    miny : float
        Min-y coordinates (south)
    minx : float
        Max-x coordinates (east)
    miny : float
        Max-y coordinates (north)
    ds : osgeo.gdal.Dataset
        Image geographical information provided by GDAL

    Returns
    -------
    tuple
        Modified coordinates
    """
    source = osr.SpatialReference()
    source.ImportFromWkt(ds.GetProjection())
    target = osr.SpatialReference()
    target.ImportFromEPSG(4326)
    transform = osr.CoordinateTransformation(source, target)
    minx, miny = transform.TransformPoint(minx, miny)[:2]
    maxx, maxy = transform.TransformPoint(maxx, maxy)[:2]
    return minx, miny, maxx, maxy


def get_osm_buildings_from_coordinates(coordinates, file_extension='xml',
                                       timeout=10000):
    """Request overpass API to get OSM buildings within an area defined by a
    given bounding box

    Parameters
    ----------
    coordinates : dict
        Bounding box where to look for buildings
    file_extension : str
        Overpass request output file extension (by default, 'xml')
    timeout : int
        Request timeout

    Returns
    -------
    tuple
        Content of the request response, as a tuple with one single byte value

    """
    request = ('[out:{ext}][timeout:{timeout}];'
               '{structure}["{criteria}"]'
               '({south:.8f},{west:.8f},{north:.8f},{east:.8f});'
               '(._;>;);out;')
    request_complete = request.format(ext=file_extension,
                                      structure='way',
                                      criteria='building',
                                      south=coordinates.get("south"),
                                      west=coordinates.get("west"),
                                      north=coordinates.get("north"),
                                      east=coordinates.get("east"),
                                      timeout=timeout)
    overpass_url = "http://overpass-api.de/api/interpreter"
    response = requests.get(overpass_url, params={'data': request_complete})
    return response.content


def filename_sanity_check(filename):
    """Check a filename by replacing '-' characters with '_'.

    Parameters
    ----------
    filename : str
        Filename to clean

    Returns
    -------
    str
        Cleaned filename
    """
    return filename.replace("-", "_")
