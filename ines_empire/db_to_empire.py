from pathlib import Path

import spinedb_api as api
from spinedb_api import DatabaseMapping
import sys
import yaml
import csv
import os
#data = pd.read_csv(tab_files_path + "sampling_key.csv", sep="\t", header=[0, 1, 2, 3], engine='pyarrow')

def get_scenario(source_db):
    scenarios = source_db.get_scenario_items()
    return scenarios

def get_index_names(map,index_names):
    if isinstance(map, api.Map):
        index_names.append(map.index_name)
        index_names = get_index_names(map.values[0], index_names)
    return index_names

def nested_dict_to_table(nested_dict, parent_keys=None):
    """
    Convert a nested dictionary into a table (list of rows).
    Each row is a list of keys leading to the value and the value itself.
    """
    if parent_keys is None:
        parent_keys = []

    table = []
    for key, value in nested_dict.items():
        if isinstance(value, dict):
            # Recursively process nested dictionaries
            table.extend(nested_dict_to_table(value, parent_keys + [key]))
        else:
            # Add a row with the full key path and the value
            table.append(parent_keys + [key, value])
    return table

def write_sets(source_db, set_list):
    print("Writing sets..")
    for set_header, set_names in set_list.items():
        for set_name, set_dimens in set_names.items():
            print(set_name)
            entities = source_db.get_entity_items(entity_class_name='__'.join(set_dimens))
            tab_file = set_header+ "_" + set_name + ".tab"
            if len(entities)>0:
                with open(Path(tab_files_path, tab_file), 'w+', newline='') as csv_file:
                    csv_writer = csv.writer(csv_file, dialect='excel-tab')
                    csv_writer.writerow(set_dimens)
                    for entity in entities:
                        csv_writer.writerow(entity["entity_byname"])
    print("")


def write_params(source_db, param_listing):
    print("Writing parameters....")
    for type_name, type_params in param_listing.items():
        for param_name, param_dimens in type_params.items():
            tab_file = type_name + "_" + param_name + ".tab"
            print(param_name)
            with open(Path(tab_files_path, tab_file), 'w+', newline="") as csv_file:
                csv_writer = csv.writer(csv_file, dialect='excel-tab')
                first_value = source_db.get_parameter_value_items(entity_class_name='__'.join(param_dimens), parameter_definition_name=param_name)[0]
                param_value = api.from_database(first_value["value"], first_value["type"])
                if isinstance(param_value, api.Map):
                    index_names = get_index_names(param_value, [])
                    header = param_dimens + index_names + [param_name]
                else:
                    header = param_dimens + [param_name]
                csv_writer.writerow(header)

                for param in source_db.get_parameter_value_items(entity_class_name='__'.join(param_dimens), parameter_definition_name=param_name):
                    param_value = api.from_database(param["value"], param["type"])
                    if isinstance(param_value, api.Map):
                        param_value_dict = api.parameter_value.convert_map_to_dict(param_value)
                        param_value_table = nested_dict_to_table(param_value_dict)
                        for i in param_value_table:
                            csv_writer.writerow(list(param["entity_byname"]) + i)
                    else:
                        csv_writer.writerow(param["entity_byname"] + (str(param_value),))

def write_general(source_db):

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

    for param_name, set_name in params_dict.items():
        tab_file = set_name[0] + "_" + param_name + ".tab"
        print(param_name)
        with open(Path(tab_files_path, tab_file), 'w+', newline="") as csv_file:
            first_value = source_db.get_parameter_value_items(entity_class_name="General", parameter_definition_name=param_name)[0]
            param_value = api.from_database(first_value["value"], first_value["type"]) 
            csv_writer = csv.writer(csv_file, dialect='excel-tab')         
            if isinstance(param_value, api.Map):
                index_names = get_index_names(param_value, [])
                header = index_names + [param_name]
            else:
                header = [param_name]
            csv_writer.writerow(header)

            for param in source_db.get_parameter_value_items(entity_class_name="General", parameter_definition_name=param_name):
                param_value = api.from_database(param["value"], param["type"])
                if isinstance(param_value, api.Map):
                    param_value_dict = api.parameter_value.convert_map_to_dict(param_value)
                    param_value_table = nested_dict_to_table(param_value_dict)
                    for i in param_value_table:
                        csv_writer.writerow(i)
                else:
                    csv_writer.writerow((str(param_value),))


def write_CO2(source_db):
    params_dict = dict()
    params_dict["PipelineFixedOM"] = ["CO2"]
    params_dict["PipelineElectricityUsage"] = ["CO2"]
    params_dict["PipelineCapitalCost"] = ["CO2"]

    sets_dict = dict()
    #sets_dict["CO2SequestrationNodes"] = ["CO2", "Node"]

    for param_name, set_name in params_dict.items():
        tab_file = set_name[0] + "_"+ param_name + ".tab"
        print(param_name)
        with open(Path(tab_files_path, tab_file), 'w+', newline="") as csv_file:
            first_value = source_db.get_parameter_value_items(entity_class_name="CO2", parameter_definition_name=param_name)[0]
            param_value = api.from_database(first_value["value"], first_value["type"])     
            csv_writer = csv.writer(csv_file, dialect='excel-tab')       
            if isinstance(param_value, api.Map):
                index_names = get_index_names(param_value, [])
                header = index_names + [param_name]
            else:
                header = [param_name]
            csv_writer.writerow(header)

            for param in source_db.get_parameter_value_items(entity_class_name="CO2", parameter_definition_name=param_name):
                param_value = api.from_database(param["value"], param["type"])
                if isinstance(param_value, api.Map):
                    param_value_dict = api.parameter_value.convert_map_to_dict(param_value)
                    param_value_table = nested_dict_to_table(param_value_dict)
                    for i in param_value_table:
                        csv_writer.writerow(i)
                else:
                    csv_writer.writerow((str(param_value),))

    for set_name, set_info in sets_dict.items():
        print(set_info[0])
        tab_file = set_info[0] + "_" + set_name + ".tab"
        entities = source_db.get_entity_items(entity_class_name="CO2SequestrationNodes")
        if len(entities)>0:
            with open(Path(tab_files_path, tab_file), 'w+', newline='') as csv_file:
                csv_writer = csv.writer(csv_file, dialect='excel-tab')
                csv_writer.writerow(set_info[1:])
                for entity in entities:
                    csv_writer.writerow(entity["entity_byname"])

def main():
    print("Started reading database...")
    with DatabaseMapping(url_db) as source_db:
        write_sets(source_db, set_list)
        write_params(source_db, param_listing)
        write_general(source_db)
        write_CO2(source_db)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        url_db = sys.argv[1]
    else:
        sys.exit("Please give source database url as the first argument and the path to output folder as second argument")
    if len(sys.argv) > 2:
        tab_files_path = sys.argv[2]
    else:
        sys.exit("Please give source database url as the first argument and the path to output folder as second argument")

    if not os.path.exists(tab_files_path):
        sys.exit(f"Output folder {tab_files_path} does not exist. Please create it first or change the second argument to a path that exists.")
    with open('param_dimens.yaml', 'r') as yaml_file:
        param_listing = yaml.safe_load(yaml_file)
    with open('sets.yaml', 'r') as yaml_file:
        set_list = yaml.safe_load(yaml_file)
    main()