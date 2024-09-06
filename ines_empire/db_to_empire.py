import spinedb_api as api
from spinedb_api import DatabaseMapping
import sys
import yaml
import csv
from collections import defaultdict


def write_sets(source_db, set_list):
    for set_name, set_dimens in set_list.items():
        entities = source_db.get_entity_items(entity_class_name='__'.join(set_dimens))
        tab_file = "Sets_" + set_name + ".tab"
        with open(tab_files_path + tab_file, 'w+', newline='') as csv_file:
            csv_writer = csv.writer(csv_file, dialect='excel-tab')
            csv_writer.writerow(set_dimens)
            for entity in entities:
                csv_writer.writerow(entity["entity_byname"])


def write_params(source_db, param_listing):
    for type_name, type_params in param_listing.items():
        for param_name, param_dimens in type_params.items():
            tab_file = type_name + "_" + param_name + ".tab"
            with open(tab_files_path + tab_file, 'w+', newline="") as csv_file:
                if len(param_dimens) > 1:
                    header = param_dimens[0] + param_dimens[1:]
                else:
                    header = param_dimens[0]
                csv_writer = csv.writer(csv_file, dialect='excel-tab')
                csv_writer.writerow(header)

                entities = source_db.get_entity_items(entity_class_name='__'.join(param_dimens[0]))
                for entity in entities:
                    for param in source_db.get_parameter_value_items(entity_class_name='__'.join(param_dimens[0]),
                                                                     entity_byname=entity["entity_byname"],
                                                                     parameter_definition_name=param_name):
                        if param:
                            param_value = api.from_database(param["value"], param["type"])
                            if len(param_dimens) > 1:
                                for i, index in enumerate(param_value.indexes):
                                    csv_writer.writerow(entity["entity_byname"] + (index,) + (str(param_value.values[i]),))
                            else:
                                csv_writer.writerow(entity["entity_byname"] + (str(param_value),))


if len(sys.argv) > 1:
    url_db = sys.argv[1]
else:
    exit("Please give source database url as the first argument and the path to output folder as second argument")
if len(sys.argv) > 2:
    tab_files_path = sys.argv[2]
else:
    exit("Please give source database url as the first argument and the path to output folder as second argument")

with open('param_dimens.yaml', 'r') as yaml_file:
    param_listing = yaml.safe_load(yaml_file)
with open('sets.yaml', 'r') as yaml_file:
    set_list = yaml.safe_load(yaml_file)

with DatabaseMapping(url_db) as source_db:
    write_sets(source_db, set_list)
    write_params(source_db, param_listing)
