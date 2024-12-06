import spinedb_api as api
from spinedb_api import DatabaseMapping
import sys
import yaml
import csv
from pathlib import Path

#data = pd.read_csv(tab_files_path + "sampling_key.csv", sep="\t", header=[0, 1, 2, 3], engine='pyarrow')


def write_sets(source_db, set_list):
    print("Writing sets..")
    for set_name, set_dimens in set_list.items():
        print(set_name)
        if len(set_dimens) < 2:
            entities = source_db.get_entity_items(entity_class_name=set_name)
        else:
            entities = source_db.get_entity_items(entity_class_name='__'.join(set_dimens))
        tab_file = "Sets_" + set_name + ".tab"
        with open(tab_files_path + tab_file, 'w+', newline='') as csv_file:
            csv_writer = csv.writer(csv_file, dialect='excel-tab')
            if len(set_dimens) == 1:
                csv_writer.writerow(set_dimens)
            elif len(set_dimens) == 2:
                csv_writer.writerow("Source:_-\tUnnamed:_1")
            elif len(set_dimens) == 3:
                csv_writer.writerow("Source:_-\tUnnamed:_1\tUnnamed:_2")
            else:
                sys.exit("More than 2 set_dimens not functional. Fix the code.")
            for entity in entities:
                csv_writer.writerow(entity["entity_byname"])
    print("")


def write_params(source_db, param_listing):
    print("Writing parameters....")
    for type_name, type_params in param_listing.items():
        for param_name, param_dimens in type_params.items():
            tab_file = type_name + "_" + param_name + ".tab"
            print(param_name)
            with open(tab_files_path + tab_file, 'w+', newline="") as csv_file:
                header = []
                header.append("Source:_Toolbox")
                header.append(f"{param_dimens[0][0]}:_1")
                if len(param_dimens[0]) > 1:
                    header.append(f"{param_dimens[0][1]}:_2")
                for i in range(len(param_dimens) - 1):
                    header.append(f"{param_dimens[i + 1]}:_{i + len(param_dimens[0]) + 1}")
                csv_writer = csv.writer(csv_file, dialect='excel-tab')
                csv_writer.writerow(tuple(header))

                for param in source_db.get_parameter_value_items(entity_class_name='__'.join(param_dimens[0]),
                                                                 parameter_definition_name=param_name):
                    param_value = api.from_database(param["value"], param["type"])
                    if len(param_dimens) > 1:
                        for i, index in enumerate(param_value.indexes):
                            csv_writer.writerow(param["entity_byname"] + (index,) + (str(param_value.values[i]),))
                    else:
                        csv_writer.writerow(param["entity_byname"] + (str(param_value),))


if len(sys.argv) > 1:
    url_db = sys.argv[1]
else:
    exit("Please give source database url as the first argument and the path to output folder as second argument")
if len(sys.argv) > 2:
    tab_files_path = sys.argv[2]
else:
    exit("Please give source database url as the first argument and the path to output folder as second argument")

Path(tab_files_path).mkdir(parents=True, exist_ok=True)

with open('param_dimens.yaml', 'r') as yaml_file:
    param_listing = yaml.safe_load(yaml_file)
with open('sets.yaml', 'r') as yaml_file:
    set_list = yaml.safe_load(yaml_file)

with DatabaseMapping(url_db) as source_db:
    write_sets(source_db, set_list)
    write_params(source_db, param_listing)
