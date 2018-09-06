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

from osmrendering import controller


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
        coordinates = controller.get_image_coordinates(image_filename)
        with self.output().open('w') as fobj:
            json.dump(coordinates, fobj)


class GetOSMBuildings(luigi.Task):
    """
    """
    datapath = luigi.Parameter(default="./data/aerial/input/training/images")
    filename = luigi.Parameter()

    def requires(self):
        return GetCoordinates(self.datapath, self.filename)

    def output(self):
        path_items = self.datapath.split("/")[:-1]
        output_path = os.path.join(*path_items, "osm")
        os.makedirs(output_path, exist_ok=True)
        output_filename = os.path.join(output_path, self.filename + ".xml")
        return luigi.LocalTarget(output_filename)

    def run(self):
        with self.input().open('r') as fobj:
            coordinates = json.load(fobj)
        buildings = controller.get_osm_buildings_from_coordinates(coordinates, "xml")
        with open(self.output().path, 'wb') as fobj:
            fobj.write(buildings)


class StoreOSMBuildingsToDatabase(luigi.Task):
    """
    """
    datapath = luigi.Parameter(default="./data/aerial/input/training/images")
    schema = luigi.Parameter(default="aerial")
    filename = luigi.Parameter()

    def requires(self):
        return GetOSMBuildings(self.datapath, self.filename)

    def output(self):
        path_items = self.datapath.split("/")[:-1]
        output_path = os.path.join(*path_items, "osm")
        os.makedirs(output_path, exist_ok=True)
        filename = self.filename + "-task-osm2pgsql.txt"
        output_filename = os.path.join(output_path, filename)
        return luigi.LocalTarget(output_filename)

    def run(self):
        safe_filename = controller.filename_sanity_check(self.filename)
        osm2pgsql_args = ['-H', "localhost",
                          '-P', "5432",
                          '-d', "osm",
                          '-U', "rde",
                          '-l',
                          '-p', safe_filename,
                          self.input().path]
        with self.output().open("w") as fobj:
            sh.osm2pgsql(osm2pgsql_args)
            fobj.write(("osm2pgsql used file {} to insert OSM data into {} "
                        "database").format(self.input().path, "osm"))


class GenerateRaster(luigi.Task):
    """
    """
    datapath = luigi.Parameter(default="./data/aerial/input/training/images")
    filename = luigi.Parameter()
    schema = luigi.Parameter(default="aerial")
    building_color = luigi.ListParameter(default=[255, 255, 255])
    background_color = luigi.ListParameter(default=[0, 0, 0])
    image_size = luigi.IntParameter(default=5000)

    def requires(self):
        return {"coordinates": GetCoordinates(self.datapath, self.filename),
                "buildings": StoreOSMBuildingsToDatabase(self.datapath,
                                                         self.schema,
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
        safe_filename = controller.filename_sanity_check(self.filename)
        query2 = ("WITH all_buildings AS ("
                  "SELECT st_setsrid(way, 4326) as building "
                  "FROM {filename}_polygon "
                  "WHERE building='yes'"
                  "), "
                  "ref_raster AS ("
                  "SELECT ST_AsRaster("
                  "ST_SetSRID(ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax}),"
                  " 4326), "
                  "width:={img_size}, height:={img_size}) as envelop"
                  ") "
                  "SELECT st_astiff(st_asraster(geom:=st_collect(b.building), "
                  "ref:=r.envelop, "
                  "pixeltype:=ARRAY['8BUI', '8BUI', '8BUI'], "
                  "value:=ARRAY{building}, "
                  "nodataval:=ARRAY{background})) "
                  "FROM all_buildings AS b "
                  "JOIN ref_raster AS r "
                  "ON ST_Intersects(b.building, r.envelop)"
                  "GROUP BY r.envelop"
        # query = ("WITH all_buildings AS ("
        #          "SELECT way as buildings FROM {filename}_polygon "
        #          "WHERE building='yes'"
        #          ")"
        #          ", ref_raster AS ("
        #          "SELECT ST_AsRaster("
        #          "ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax}), "
        #          "width:={img_size}, height:={img_size})"
        #          ")"
        #          "SELECT st_astiff(st_asraster(geom:=st_union(buildings), "
        #          "ref:=ref_raster, "
        #          "pixeltype:=ARRAY['8BUI', '8BUI', '8BUI'], "
        #          "value:=ARRAY{building}, "
        #          "nodataval:=ARRAY{background})) "
        #          "FROM all_buildings"
                 ";").format(filename=safe_filename,
                             xmin=coordinates["west"],
                             ymin=coordinates["south"],
                             xmax=coordinates["east"],
                             ymax=coordinates["north"],
                             img_size=self.image_size,
                             building=list(self.building_color),
                             background=list(self.background_color))
        conn = psycopg2.connect(database="osm", user="rde",
                                host="localhost", port="5432")
        cur = conn.cursor()
        cur.execute(query)
        rset = cur.fetchall()
        for data in rset:
            building = data[0]
            if not building is None:
                buf = building.tobytes()
                with open(self.output().path, 'wb') as fobj:
                    fobj.write(buf)
            else:
                empty_image = np.zeros([self.image_size, self.image_size, 3],
                                       dtype=np.uint8)
                Image.fromarray(empty_image).save(self.output().path)

class GenerateAllOSMRasters(luigi.Task):
    """
    """
    datapath = luigi.Parameter(default="./data/aerial/input/training/images")
    schema = luigi.Parameter(default="aerial")
    building_color = luigi.ListParameter(default=[255, 255, 255])
    background_color = luigi.ListParameter(default=[0, 0, 0])
    image_size = luigi.IntParameter(default=5000)

    def requires(self):
        filenames = [filename.split('.')[0]
                     for filename in os.listdir(self.datapath)]
        return {f: GenerateRaster(self.datapath, f, self.schema,
                                  self.building_color, self.background_color,
                                  self.image_size) for f in filenames}

    def complete(self):
        return False
