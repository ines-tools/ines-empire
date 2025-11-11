import spinedb_api as api
from spinedb_api import DatabaseMapping
from sqlalchemy.exc import DBAPIError
from spinedb_api.exception import NothingToCommit
import sys
import os
import yaml
import csv
import pandas as pd
from pathlib import Path
from collections import defaultdict
from sqlalchemy.exc import DBAPIError
from spinedb_api.exception import NothingToCommit


# Convert to a standard dictionary (optional)
def convert_to_dict(d):
    if isinstance(d, defaultdict):
        return {k: convert_to_dict(v) for k, v in d.items()}
    return d

def recursively_return_map_old(dict_in, header, level):
    
    indexes = list()
    values = list()
    for key, dict_values in dict_in.items():
        if isinstance(dict_values, dict):
            indexes.append(key)
            values.append(recursively_return_map_old(dict_values, header, level+1))
        else:
            indexes.append(key)
            values.append(dict_values)
    out_map = api.Map(indexes=indexes, values=values, index_name=header[level])
    return out_map

def add_single_parameter(target_db, data, index, header, entity_class_name, param_name, entity_byname = None):
    for key, values in data.items():
        if not entity_byname:
            entity_byname_out = tuple(key.split('__'))
        else:
            entity_byname_out = entity_byname
        if index[key]:
            indexes = index[key]
            if isinstance(indexes[0],list) and len(indexes[0]) > 1:
                table = list(list())
                for i, val in enumerate(indexes):
                    table.append(val + [values[i]])

                # Initialize a nested defaultdict
                nested_dict = lambda: defaultdict(nested_dict)
                result = nested_dict()

                # Dynamically populate the nested dictionary
                for row in table:
                    *keys, last, value = row  # Unpack all columns except the last as keys, and the last as value
                    current_level = result
                    for key in keys:
                        current_level = current_level[key]  # Traverse or create the nested structure
                    current_level[last] = value  # Assign the value at the deepest level
                
                result = convert_to_dict(result)
                values = recursively_return_map_old(result,header,0)
                p_value, p_type = api.to_database(values)
            else:   
                values = api.Map(indexes=indexes, values=values, index_name=header[-2])
                p_value, p_type = api.to_database(values)
            
        else:
            p_value, p_type = api.to_database(float(values))
        added, error = target_db.add_parameter_value_item(entity_class_name=entity_class_name,
                                                        entity_byname=entity_byname_out,
                                                        parameter_definition_name=param_name,
                                                        alternative_name=alternative_name,
                                                        type=p_type,
                                                        value=p_value)
        if error:
            print("Could not add a parameter: " + error)

    return target_db

def add_sampling_key(target_db):
    data = pd.read_csv(Path(tab_files_path,"sampling_key.csv"), index_col=[0, 1, 2, 3], skipinitialspace=True)
    year_index = data.index.get_level_values(3)
    data = data.droplevel(3)

    ind = pd.MultiIndex.from_product([level.values for level in data.index.levels])
    fulldf = pd.DataFrame(-1, columns=data.columns, index=ind)
    fulldf.update(data)
    shape = [len(level) for level in fulldf.index.levels]
    ncol = fulldf.shape[-1]
    if ncol > 1:
        shape.append(ncol)
    foo = fulldf.to_numpy().reshape(shape)

def add_node_technology(target_db):
    with open(Path(tab_files_path,"Sets_Node.tab"), encoding="utf-8") as csv_file:
        csv_reader = csv.reader(csv_file, dialect='excel-tab')
        first_line = True
        nodes = []
        for row in csv_reader:
            if not first_line:
                nodes.append(row[0])
            first_line = False
    with open(Path(tab_files_path,"Sets_Technology.tab"), encoding="utf-8") as csv_file:
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
                                                 entity_byname=(season,))
        if error:
            print("Failed to add season entity " + season + " due to error " + error)
    target_db.commit_session("Added seasons")
    return target_db

def add_sets_directly(target_db):
    
    added, error = target_db.add_entity_class_item(name='General')
    added, error = target_db.add_entity_item(entity_class_name='General', entity_byname=('General',))
    added, error = target_db.add_entity_class_item(name='CO2')
    added, error = target_db.add_entity_item(entity_class_name='CO2', entity_byname=('CO2',))
    added, error = target_db.add_entity_class_item(name='HydrogenStorageNode')
    #added, error = target_db.add_entity_class_item(name='CO2SequestrationNodes')

    """
    if os.path.isfile(tab_files_path + "CO2_CO2SequestrationNodes.tab"):
        with open(tab_files_path + "CO2_CO2SequestrationNodes.tab") as csv_file:
            csv_reader = csv.reader(csv_file, dialect='excel-tab')
            first_line = True
            for row in csv_reader:
                if not first_line:
                    entity_byname = (row[0],)
                    added, error = target_db.add_entity_item(entity_class_name='CO2SequestrationNodes', entity_byname=entity_byname)
                    if error:
                        print("error adding entity (set members): " + error)
                else:
                    header = row
                first_line = False
    """
    tab_file_path = Path(tab_files_path,"Hydrogen_StorageMaxCapacity.tab")
    if os.path.isfile(tab_file_path):
        with open(tab_file_path, encoding="utf-8") as csv_file:
            csv_reader = csv.reader(csv_file, dialect='excel-tab')
            first_line = True
            for row in csv_reader:
                if not first_line:
                    entity_byname = (row[0],)
                    added, error = target_db.add_entity_item(entity_class_name='HydrogenStorageNode', entity_byname=entity_byname)
                    if error:
                        print("error adding entity (set members): " + error)
                else:
                    header = row
                first_line = False

    return target_db

def add_sets(target_db, set_list):
    for set_header, set_names in set_list.items():
        for set_name, set_dimens in set_names.items():
            tab_file = set_header + "_" + set_name + ".tab"
            tab_file_path = Path(tab_files_path,tab_file)
            if os.path.isfile(tab_file_path):
                with open(tab_file_path, encoding="utf-8") as csv_file:
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
                            if len(set_dimens) == 1 and not set_name == set_dimens[0]:
                                added, error = target_db.add_entity_item(entity_class_name=set_name,
                                                                        entity_byname=entity_byname)
                            else:
                                added, error = target_db.add_entity_item(entity_class_name='__'.join(set_dimens),
                                                                        entity_byname=entity_byname)
                            if error:
                                print("error adding entity (set members): " + error)
                            if len(entity_byname) == 1 and set_name == set_dimens[0]:
                                added, error = target_db.add_entity_alternative_item(entity_class_name='__'.join(set_dimens),
                                                                                    entity_byname=entity_byname,
                                                                                    alternative_name=alternative_name)
                                if error:
                                    print("error adding entity_alternative: " + error)

                        first_line = False
    target_db.commit_session("Added sets")
    return target_db

def add_relationships_from_capacity(target_db):
    params_dict = dict()
    params_dict["Industry_Ammonia_InitialCapacity"] = ["AmmoniaProducers", "AmmoniaProductionPlants"]
    params_dict["Industry_Cement_InitialCapacity"] = ["CementProducers", "CementProductionPlants"]
    params_dict["Industry_Steel_InitialCapacity"] = ["SteelProducers", "SteelProductionPlants"]

    for param_name, param_dimens in params_dict.items():
        added, error = target_db.add_entity_class_item(name='__'.join(param_dimens), dimension_name_list=tuple(param_dimens))
        if error:
            print("Failed to add parameter " + param_name + " due to " + error)
        tab_file = param_name + ".tab"

        tab_file_path = Path(tab_files_path,tab_file)
        if os.path.isfile(tab_file_path):
            with open(tab_file_path, encoding="utf-8") as csv_file:
                csv_reader = csv.reader(csv_file, dialect='excel-tab')
                first_line = True
                key = param_name
                for row in csv_reader:
                    if not first_line:
                        entity_byname = tuple(row[:len(param_dimens)])
                        added, error = target_db.add_entity_item(entity_class_name='__'.join(param_dimens),entity_byname=entity_byname)
                    else:
                        header = row
                    first_line = False
    return target_db

def add_general_params(target_db):

    params_dict = dict()
    params_dict["CCSCostTSVariable"] = ["Generator"]
    params_dict["CO2Cap"] = ["General"]
    params_dict["CO2Price"] = ["General"]
    params_dict["AvailableBioEnergy"] = ["General"]
    params_dict["OffshoreConverterCapitalCost"] = ["Transmission"]
    params_dict["OffshoreConverterOMCost"] = ["Transmission"]
    params_dict["PipelineElectricityUse"] =  ["NaturalGas"]
    params_dict["ElectrolyzerFixedOMCost"] = ["Hydrogen"]
    params_dict["ElectrolyzerLifetime"] = ["Hydrogen"]
    params_dict["ElectrolyzerPlantCapitalCost"] = ["Hydrogen"]
    params_dict["ElectrolyzerPowerUse"] = ["Hydrogen"]
    params_dict["ElectrolyzerStackCapitalCost"] = ["Hydrogen"]
    params_dict["PipelineCapitalCost"] = ["Hydrogen"]
    params_dict["PipelineCompressorPowerUsage"] = ["Hydrogen"]
    params_dict["PipelineOMCostPerKM"] = ["Hydrogen"]
    params_dict["StorageCapitalCost"] = ["Hydrogen"]
    params_dict["StorageFixedOMCost"] = ["Hydrogen"]
    params_dict["Refinery_HeatConsumption"] = ["Industry"]
    params_dict["Refinery_HydrogenConsumption"] = ["Industry"]

    for param_name, param_dimens in params_dict.items():
        added, updated, error = target_db.add_update_parameter_definition_item(entity_class_name="General", name=param_name)
        if error:
            print("Failed to add parameter " + param_name + " due to " + error)
        tab_file = param_dimens[0] + "_" + param_name + ".tab"

        data = defaultdict(list)
        index = defaultdict(list)
        entity_byname = ('General',)
        tab_file_path = Path(tab_files_path,tab_file)
        if os.path.isfile(tab_file_path):
            with open(tab_file_path, encoding="utf-8") as csv_file:
                csv_reader = csv.reader(csv_file, dialect='excel-tab')
                first_line = True
                key = param_name
                for row in csv_reader:
                    if not first_line:
                        if value_dimens == 2:
                            index[key].append(row[-2])
                            data[key].append(row[-1])
                        else:
                            data[key] = row[-1]
                    else:
                        header = row
                        value_dimens = len(row)
                    first_line = False
            target_db = add_single_parameter(target_db, data, index, header, "General", param_name, entity_byname = entity_byname)
    try:
        target_db.commit_session("Added parameter " + param_name)
    except NothingToCommit:
        pass
    except DBAPIError as e:
        print("failed to commit entities and entity_alternatives")
    return target_db

def add_CO2_params(target_db):

    params_dict = dict()
    params_dict["PipelineFixedOM"] = ["CO2"]
    params_dict["PipelineElectricityUsage"] = ["CO2"]
    params_dict["PipelineCapitalCost"] = ["CO2"]

    for param_name, param_dimens in params_dict.items():

        entity_class_name = param_dimens[0]
        added, updated, error = target_db.add_update_parameter_definition_item(entity_class_name= entity_class_name, name=param_name)
        if error:
            print("Failed to add parameter " + param_name + " due to " + error)
        tab_file = param_dimens[0] + "_"+ param_name + ".tab"
        data = defaultdict(list)
        index = defaultdict(list)
        tab_file_path = Path(tab_files_path,tab_file)
        if os.path.isfile(tab_file_path):
            with open(tab_file_path, encoding="utf-8") as csv_file:
                csv_reader = csv.reader(csv_file, dialect='excel-tab')
                first_line = True
                key = param_name
                for row in csv_reader:
                    if not first_line:
                        if value_dimens == 2:
                            index[key].append(row[-2])
                            data[key].append(row[-1])
                        else:
                            data[key] = row[-1]
                    else:
                        header = row
                        value_dimens = len(row)
                    first_line = False
            target_db = add_single_parameter(target_db, data, index, header, "CO2", param_name, entity_byname = ("CO2",))
    try:
        target_db.commit_session("Added parameter " + param_name)
    except NothingToCommit:
        pass
    except DBAPIError as e:
        print("failed to commit entities and entity_alternatives")
    
    return target_db

def add_params(target_db, param_listing):
    for type_name, type_params in param_listing.items():
        for param_name, param_dimens in type_params.items():
            added, updated, error = target_db.add_update_parameter_definition_item(
                entity_class_name='__'.join(param_dimens),
                name=param_name)
            if error:
                print("Failed to add parameter " + param_name + " due to " + error)
            nr_dimensions = len(param_dimens)
            data = defaultdict(list)
            index = defaultdict(list)
            tab_file = type_name + "_" + param_name + ".tab"
            tab_file_path = Path(tab_files_path,tab_file)
            if os.path.isfile(tab_file_path):
                with open(tab_file_path, encoding="utf-8") as csv_file:
                    csv_reader = csv.reader(csv_file, dialect='excel-tab')
                    first_line = True
                    for row in csv_reader:
                        if not first_line:
                            entity_byname = '__'.join(row[:nr_dimensions])
                            if value_dimens > 2:
                                index[entity_byname].append(row[len(param_dimens):-1])
                                data[entity_byname].append(row[-1])
                            elif value_dimens == 2:
                                index[entity_byname].append(row[-2])
                                data[entity_byname].append(row[-1])
                            else:
                                data[entity_byname] = row[-1]
                        else:
                            header = row
                            value_dimens = len(row)-len(param_dimens)
                        first_line = False

                target_db = add_single_parameter(target_db, data, index, header[len(param_dimens):], '__'.join(param_dimens), param_name)
            print("Added parameter " + param_name)
                        #target_db = process_capacities(source_db, target_db)
            try:
                target_db.commit_session("Added parameter " + param_name)
            except NothingToCommit:
                pass
            except DBAPIError as e:
                print("failed to commit entities and entity_alternatives")

    return target_db


if len(sys.argv) > 1:
    url_db = sys.argv[1]
else:
    exit("Please give target database url as the first argument and the path to input file folder as second argument")
if len(sys.argv) > 2:
    tab_files_path = sys.argv[2]
else:
    exit("Please give target database url as the first argument and the path to input file folder as second argument")
if len(sys.argv) > 3:
    alternative_name = sys.argv[3]
else:
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
    target_db.add_entity_class_item(name="Horizon")
    target_db.commit_session("Added alternative and scenario " + alternative_name)

    target_db = add_seasons(target_db)
    target_db = add_sets(target_db, set_list)
    target_db = add_sets_directly(target_db)
    target_db = add_relationships_from_capacity(target_db)
    target_db = add_node_technology(target_db)
    target_db = add_params(target_db, param_listing)
    target_db = add_general_params(target_db)
    target_db = add_CO2_params(target_db)
    #target_db = add_sampling_key(target_db)
