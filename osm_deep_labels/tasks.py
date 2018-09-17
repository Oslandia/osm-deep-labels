"""Main Luigi module, where pipeline tasks are defined
"""

import json
import luigi
from luigi.contrib.postgres import PostgresQuery
import os
import sh
import psycopg2
import numpy as np
from PIL import Image

from osm_deep_labels import utils


class GetCoordinates(luigi.Task):
    """
    """
    datapath = luigi.Parameter(default="./data/aerial/input/training/images")
    filename = luigi.Parameter()

    def output(self):
        path_items = self.datapath.split("/")[:-1]
        output_path = os.path.join(*path_items, "coordinates")
        os.makedirs(output_path, exist_ok=True)
        output_filename = os.path.join(output_path, self.filename + ".json")
        return luigi.LocalTarget(output_filename)

    def run(self):
        image_filename = os.path.join(self.datapath, self.filename + ".tif")
        coordinates = utils.get_image_coordinates(image_filename)
        with self.output().open('w') as fobj:
            json.dump(coordinates, fobj)


class GetReprojectedCoordinates(luigi.Task):
    """
    """
    datapath = luigi.Parameter(default="./data/aerial/input/training/images")
    filename = luigi.Parameter()

    def requires(self):
        return GetCoordinates(self.datapath, self.filename)

    def output(self):
        path_items = self.datapath.split("/")[:-1]
        output_path = os.path.join(*path_items, "coordinates")
        os.makedirs(output_path, exist_ok=True)
        output_filename = os.path.join(output_path,
                                       self.filename + "_x_y.json")
        return luigi.LocalTarget(output_filename)

    def run(self):
        with self.input().open('r') as fobj:
            coordinates = json.load(fobj)
        print(coordinates)
        image_filename = os.path.join(self.datapath, self.filename + ".tif")
        coordinates = utils.set_coordinates_as_x_y(coordinates, image_filename)
        print(coordinates)
        with self.output().open('w') as fobj:
            json.dump(coordinates, fobj)


class GetOSMBuildings(luigi.Task):
    """
    """
    datapath = luigi.Parameter(default="./data/aerial/input/training/images")
    filename = luigi.Parameter()
    extension = luigi.Parameter(default="json")

    def requires(self):
        return GetReprojectedCoordinates(self.datapath, self.filename)

    def output(self):
        path_items = self.datapath.split("/")[:-1]
        output_path = os.path.join(*path_items, "osm")
        os.makedirs(output_path, exist_ok=True)
        output_filename = os.path.join(output_path,
                                       self.filename + "." + self.extension)
        return luigi.LocalTarget(output_filename)

    def run(self):
        with self.input().open('r') as fobj:
            coordinates = json.load(fobj)
        buildings = utils.get_osm_buildings_from_coordinates(coordinates,
                                                             self.extension)
        with open(self.output().path, 'wb') as fobj:
            fobj.write(buildings)


class GenerateAllOSMBuildings(luigi.Task):
    """
    """
    datapath = luigi.Parameter(default="./data/aerial/input/training/images")
    extension = luigi.Parameter(default="xml")

    def requires(self):
        filenames = [filename.split('.')[0]
                     for filename in os.listdir(self.datapath)]
        return {f: GetOSMBuildings(self.datapath, f, self.extension)
                for f in filenames}

    def complete(self):
        return False


class StoreOSMBuildingsToDatabase(luigi.Task):
    """
    """
    datapath = luigi.Parameter(default="./data/aerial/input/training/images")
    filename = luigi.Parameter()

    def requires(self):
        return {"coordinates": GetCoordinates(self.datapath, self.filename),
                "buildings": GetOSMBuildings(self.datapath, self.filename,
                                             "xml")}

    def output(self):
        path_items = self.datapath.split("/")[:-1]
        output_path = os.path.join(*path_items, "osm")
        os.makedirs(output_path, exist_ok=True)
        filename = self.filename + "-task-osm2pgsql.txt"
        output_filename = os.path.join(output_path, filename)
        return luigi.LocalTarget(output_filename)

    def run(self):
        with self.input()["coordinates"].open('r') as fobj:
            coordinates = json.load(fobj)
        print(coordinates)
        safe_filename = utils.filename_sanity_check(self.filename)
        osm2pgsql_args = ['-H', "localhost",
                          '-P', "5432",
                          '-d', "osm",
                          '-U', "rde",
                          '-l',
                          '-E', coordinates["srid"],
                          '-p', safe_filename,
                          self.input()["buildings"].path]
        with self.output().open("w") as fobj:
            sh.osm2pgsql(osm2pgsql_args)
            fobj.write(("osm2pgsql used file {} to insert OSM data"
                        " into {} database"
                        "").format(self.input()["buildings"].path, "osm"))


class GenerateRaster(luigi.Task):
    """
    """
    datapath = luigi.Parameter(default="./data/aerial/input/training/images")
    filename = luigi.Parameter()
    building_color = luigi.ListParameter(default=[255, 255, 255])
    background_color = luigi.ListParameter(default=[0, 0, 0])
    image_size = luigi.IntParameter(default=5000)

    def requires(self):
        return {"coordinates": GetCoordinates(self.datapath, self.filename),
                "buildings": StoreOSMBuildingsToDatabase(self.datapath,
                                                         self.filename)}

    def output(self):
        path_items = self.datapath.split("/")[:-1]
        output_path = os.path.join(*path_items, "osm_labels")
        os.makedirs(output_path, exist_ok=True)
        filename = self.filename + ".tif"
        output_filename = os.path.join(output_path, filename)
        return luigi.LocalTarget(output_filename)

    def run(self):
        with self.input()["coordinates"].open('r') as fobj:
            coordinates = json.load(fobj)
        utils.generate_raster(self.output().path, self.image_size, coordinates,
                              self.background_color, self.building_color)


class GenerateAllOSMRasters(luigi.Task):
    """
    """
    datapath = luigi.Parameter(default="./data/aerial/input/training/images")
    building_color = luigi.ListParameter(default=[255, 255, 255])
    background_color = luigi.ListParameter(default=[0, 0, 0])
    image_size = luigi.IntParameter(default=5000)

    def requires(self):
        filenames = [filename.split('.')[0]
                     for filename in os.listdir(self.datapath)]
        return {f: GenerateRaster(self.datapath, f,
                                  self.building_color, self.background_color,
                                  self.image_size)
                for f in filenames}

    def complete(self):
        return False
