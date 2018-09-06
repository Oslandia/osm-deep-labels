# OSM deep labels

## Motivation

This repository aims at producing deep learning labels starting from
OpenStreetMap (OSM) data for semantic segmentation algorithms applied to aerial
images.

The project is strongly related
to [Deeposlandia](http://github.com/Oslandia/deeposlandia), another Oslandia
project dedicated to deep learning algorithm design.

## Installation

This project runs under Python 3.

Within a virtual environment , by downloading the repo and `pip`-ing the `setup.py` file:

```
git clone http://github.com/Oslandia/osm-deep-labels
cd osm-deep-labels
pip install .
```

Additionally the project requires `GDAL`. For ̀Ubuntu` distributions, the following operations are needed to install this program:
```
sudo apt-get install libgdal-dev
sudo apt-get install python3-gdal
```

The `GDAL` version can be verified by:
```
gdal-config --version
```

Now let's retrieve a `GDAL` for Python that corresponds to the `GDAL` of system:
```
pip install --global-option=build_ext --global-option="-I/usr/include/gdal" GDAL==`gdal-config --version`
python3 -c "import osgeo; print(osgeo.__version__)"
```

For other OS, please visit the `GDAL` installation documentation.
