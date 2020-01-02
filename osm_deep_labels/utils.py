"""Utility fonctions for osm-deep-labels
"""

from configparser import ConfigParser
import os
import requests
import sys

import mapnik
from osgeo import gdal, osr


def generate_db_string(config_filename):
    """Build a database connexion string starting from a config file

    Parameters
    ----------
    config_filename : str
        Path to the database connexion configuration file

    Returns
    -------
    str
        Database connexion string
    """
    config = ConfigParser()
    if os.path.isfile(config_filename):
        config.read(config_filename)
    else:
        logger.error(
            "The specified configuration file does not exist ().",
            config_filename
        )
        sys.exit(1)
    user = config.get("database", "user")
    if config.has_option("database", "password"):
        user = user + ":" + config.get("bdlhes", "password")
    db_string = "postgresql://{}@{}:{}/{}".format(
        user,
        config.get("database", "host"),
        config.get("database", "port"),
        config.get("database", "dbname")
        )
    return db_string


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
    srid = int(ds.GetProjection().split('"')[-2])
    return {"west": minx, "south": miny, "east": maxx, "north": maxy, "srid": srid}


def set_coordinates_as_x_y(coordinates, image_filename):
    """Transform coordinates into a (x,y)-compatible projection

    Parameters
    ----------
    coordinates : dict of 4 float values
        Min-x, min-y, max-x and max-y coordinates with keys 'west', 'south',
    'east', 'north'
    image_filename : str
        Image path on the file system (will be used to get the original image
    projection)
    srid : int
        Geographical projection

    Returns
    -------
    dict
        Bounding box of the image (west, south, east, north coordinates)
    """
    SRID = 4326
    minx, miny = coordinates["west"], coordinates["south"]
    maxx, maxy = coordinates["east"], coordinates["north"]
    source = osr.SpatialReference()
    ds = gdal.Open(image_filename)
    source.ImportFromWkt(ds.GetProjection())
    target = osr.SpatialReference()
    target.ImportFromEPSG(SRID)
    transform = osr.CoordinateTransformation(source, target)
    minx, miny = transform.TransformPoint(minx, miny)[:2]
    maxx, maxy = transform.TransformPoint(maxx, maxy)[:2]
    return {"west": minx, "south": miny, "east": maxx, "north": maxy, "srid": SRID}


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


def get_mapnik_projection(srid):
    """
    """
    url = "http://spatialreference.org/ref/epsg/{srid}/mapnikpython/"
    response = requests.get(url.format(srid=srid))
    response_str = response.content.decode()
    mapnik_projection = response_str.split("\n")[1].split(" = ")[-1]
    return mapnik_projection[1:-1]


def RGBToHTMLColor(rgb_tuple):
    """Convert an (R, G, B) tuple to #RRGGBB

    Parameters
    ----------
    rgb_list : list
        List of red, green, blue pixel values

    Returns
    -------
    str
        HTML-version of color
    """
    return '#%02x%02x%02x' % tuple(rgb_tuple)


def generate_raster(filename, img_size, coordinates,
                    background_color, building_color):
    """Generate a raster through requesting a PostGIS database with Mapnik

    Parameters
    ----------
    filename : str
        Output raster path on the file system
    img_size : int
        Size of the desired output image, in pixel (height=width)
    coordinates : dict
        Geographical coordinates of area of interest (west, south, east, north)
    and corresponding geographical projection (SRID)
    background_color : list of 3 ints
        Color of background pixels in [R,G,B]-format
    building_color : list of 3 ints
        Color of building pixels in [R,G,B]-format

    """
    mapnik_projection = get_mapnik_projection(coordinates["srid"])
    m = mapnik.Map(img_size, img_size, mapnik_projection)
    m.background = mapnik.Color(RGBToHTMLColor(background_color))
    s = mapnik.Style()
    r = mapnik.Rule()
    polygon_symbolizer = mapnik.PolygonSymbolizer()
    polygon_symbolizer.fill = mapnik.Color(RGBToHTMLColor(building_color))
    r.symbols.append(polygon_symbolizer)
    s.rules.append(r)
    m.append_style('building_style', s)

    place_name = filename_sanity_check(filename.split("/")[-1].split(".")[0])
    subquery = ("(SELECT way "
                "FROM {place}_polygon WHERE building='yes') AS building"
                "").format(place=place_name)
    postgis_params = {'host': "localhost", 'port': "5432", 'user': "rdelhome",
                      'dbname': "osm", 'table': subquery,
                      'geometry_field': "way", 'srid': coordinates["srid"],
                      'extent_from_subquery': True}
    ds = mapnik.PostGIS(**postgis_params)

    layer = mapnik.Layer('buildings')
    layer.datasource = ds
    layer.srs = mapnik_projection
    layer.styles.append('building_style')
    m.layers.append(layer)

    m.zoom_to_box(mapnik.Box2d(coordinates['west'], coordinates['south'],
                               coordinates['east'], coordinates['north']))
    mapnik.render_to_file(m, filename, 'tif')
