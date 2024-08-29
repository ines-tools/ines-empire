import spinedb_api as api
from spinedb_api import DatabaseMapping
import sys
import pyarrow
import numpy
import spinetoolbox as toolbox
import yaml
#import cProfile
import copy
import csv
from collections import defaultdict


def add_node_technology(target_db):
    with open(path + "Sets_Node.tab") as csv_file:
        csv_reader = csv.reader(csv_file, dialect='excel-tab')
        first_line = True
        nodes = []
        for row in csv_reader:
            if not first_line:
                nodes.append(row[0])
            first_line = False
    with open(path + "Sets_Technology.tab") as csv_file:
        csv_reader = csv.reader(csv_file, dialect='excel-tab')
        first_line = True
        technologies = []
        for row in csv_reader:
            if not first_line:
                technologies.append(row[0])
            first_line = False

    added, error = target_db.add_entity_class_item(name="Node__Technology",
                                                   dimension_name_list=tuple(["Node", "Technology"]))
    if error:
        print("Failed to add Node__Technology entity_class: " + error)

    for node in nodes:
        for technology in technologies:
            entity_byname = (node, technology)
            added, error = target_db.add_entity_item(entity_class_name="Node__Technology",
                                                     entity_byname=entity_byname)
            if error:
                print("error adding node_technology entities: " + error)
    return target_db


def add_seasons(target_db):
    seasons = ["winter", "spring", "summer", "fall", "peak", "peak1", "peak2"]
    added, error = target_db.add_entity_class_item(name="Season")
    if error:
        print("Failed to add Season entity_class")
    for season in seasons:
        added, error = target_db.add_entity_item(entity_class_name="Season",
                                                 entity_byname=(season, ))
        if error:
            print("Failed to add season entity " + season + " due to error " + error)
    target_db.commit_session("Added seasons")
    return target_db


def add_sets(target_db, set_list):
    for set_name, set_dimens in set_list.items():
        tab_file = "Sets_" + set_name + ".tab"
        with open(path + tab_file) as csv_file:
            csv_reader = csv.reader(csv_file, dialect='excel-tab')
            first_line = True
            if len(set_dimens) == 1:
                if set_name == set_dimens[0]:
                    added, error = target_db.add_entity_class_item(name='__'.join(set_dimens))
                else:
                    added, error = target_db.add_entity_class_item(name=set_name)
            else:
                added, error = target_db.add_entity_class_item(name='__'.join(set_dimens),
                                                               dimension_name_list=tuple(set_dimens))
            if error:
                print("error adding entity_classes (set): " + error)
            for row in csv_reader:
                if not first_line:
                    entity_byname = tuple(row)
                    if len(set_dimens) == 1 and set_name is not set_dimens[0]:
                        added, error = target_db.add_entity_item(entity_class_name=set_name,
                                                                 entity_byname=entity_byname)
                    else:
                        added, error = target_db.add_entity_item(entity_class_name='__'.join(set_dimens),
                                                                 entity_byname=entity_byname)
                    if error:
                        print("error adding entity (set members): " + error)
                    if len(entity_byname) == 1 and set_name is set_dimens[0]:
                        added, error = target_db.add_entity_alternative_item(entity_class_name='__'.join(set_dimens),
                                                                             entity_byname=entity_byname,
                                                                             alternative_name=alternative_name)
                        if error:
                            print("error adding entity_alternative: " + error)

                first_line = False
    target_db.commit_session("Added sets")
    return target_db


def add_params(target_db, param_listing):
    for type_name, type_params in param_listing.items():
        for param_name, param_dimens in type_params.items():
            added, updated, error = target_db.add_update_parameter_definition_item(entity_class_name='__'.join(param_dimens[0]),
                                                                                   name=param_name)
            if error:
                print("Failed to add parameter " + param_name + " due to " + error)
            nr_dimensions = len(param_dimens[0])
            data = defaultdict(list)
            header = defaultdict(list)
            tab_file = type_name + "_" + param_name + ".tab"
            with open(path + tab_file) as csv_file:
                csv_reader = csv.reader(csv_file, dialect='excel-tab')
                first_line = True
                for row in csv_reader:
                    if not first_line:
                        entity_byname = '__'.join(row[:nr_dimensions])
                        if len(param_dimens) == 2:
                            header[entity_byname].append(row[-2])
                            data[entity_byname].append(row[-1])
                        else:
                            data[entity_byname] = row[-1]
                    first_line = False
            for entity_byname, values in data.items():
                if header[entity_byname]:
                    headers = header[entity_byname]
                    data_map = api.Map(indexes=headers,
                                       values=values,
                                       index_name="horizon")
                    p_value, p_type = api.to_database(data_map)
                else:
                    p_value, p_type = api.to_database(float(values))
                added, error = target_db.add_parameter_value_item(entity_class_name='__'.join(param_dimens[0]),
                                                                  entity_byname=tuple(entity_byname.split('__')),
                                                                  parameter_definition_name=param_name,
                                                                  alternative_name=alternative_name,
                                                                  type=p_type,
                                                                  value=p_value)
                if error:
                    print("Could not add a parameter: " + error)
            print("Added parameter " + param_name)
            target_db.commit_session("Added parameter " + param_name)
    return target_db



# if len(sys.argv) < 2:
#     exit("You need to provide the name (and possibly path) of the settings file as an argument")
# with open(sys.argv[1], 'r') as yaml_file:
#     settings = yaml.safe_load(yaml_file)
# dimens_to_param = settings["dimens_to_param"]
# class_for_scalars = settings["class_for_scalars"]
# url_db = settings["target_db"]
# file = open(settings["model_data"])
# alternative_name = settings["alternative_name"]
# entities_from_entities = settings["entities_from_entities"]
# read_separate_csv = settings["read_separate_csv"]

# if len(sys.argv) > 2:
#url_db = sys.argv[1]
url_db = "sqlite:///C:/data/Toolbox_projects/EMIRE_import/EMPIRE_data.sqlite"
path = "C:/data/Toolbox_projects/EMIRE_import/tab_files/"
alternative_name = "base"

with open('param_dimens.yaml', 'r') as yaml_file:
    param_listing = yaml.safe_load(yaml_file)
with open('sets.yaml', 'r') as yaml_file:
    set_list = yaml.safe_load(yaml_file)

with DatabaseMapping(url_db) as target_db:
    target_db.purge_items('entity')
    target_db.purge_items('alternative')
    target_db.purge_items('scenario')
    target_db.purge_items('entity_class')
    target_db.commit_session("Purged alternatives")
    target_db.add_alternative_item(name=alternative_name)
    target_db.add_scenario_item(name=alternative_name)
    target_db.add_scenario_alternative_item(alternative_name=alternative_name, scenario_name=alternative_name, rank=0)
    target_db.commit_session("Added alternative and scenario " + alternative_name)

    target_db = add_seasons(target_db)
    target_db = add_sets(target_db, set_list)
    target_db = add_node_technology(target_db)
    target_db = add_params(target_db, param_listing)
