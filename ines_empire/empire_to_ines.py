import spinedb_api as api
from spinedb_api import DatabaseMapping
from ines_tools import ines_transform, ines_initialize
import numpy as np
from sqlalchemy.exc import DBAPIError
from spinedb_api.exception import NothingToCommit
import sys
from sys import exit
import yaml
import itertools
import datetime
from dateutil.relativedelta import relativedelta

if len(sys.argv) > 1:
    url_db_in = sys.argv[1]
else:
    exit("Please provide input database url and output database url as arguments. They should be of the form ""sqlite:///path/db_file.sqlite""")
if len(sys.argv) > 2:
    url_db_out = sys.argv[2]
else:
    exit("Please provide input database url and output database url as arguments. They should be of the form ""sqlite:///path/db_file.sqlite""")

with open('empire_to_ines_entities.yaml', 'r') as file:
    entities_to_copy = yaml.load(file, yaml.BaseLoader)
with open('empire_to_ines_parameters.yaml', 'r') as file:
    parameter_transforms = yaml.load(file, yaml.BaseLoader)
with open('empire_to_ines_methods.yaml', 'r') as file:
    parameter_methods = yaml.load(file, yaml.BaseLoader)
with open('empire_to_ines_entities_to_parameters.yaml', 'r') as file:
    entities_to_parameters = yaml.load(file, yaml.BaseLoader)
# with open('empire_to_ines_defaults.yaml', 'r') as file:
#     defaults = yaml.safe_load(file)
# default_unit_size = float(defaults["default_unit_size"])


def main():
    with DatabaseMapping(url_db_in) as source_db:
        with DatabaseMapping(url_db_out, upgrade=True) as target_db:
            ## Empty the database
            target_db = ines_initialize.purge_db_from_data(target_db)
            source_db = ines_initialize.fetch_data(source_db)
            target_db = ines_initialize.copy_alternatives_scenarios(source_db, target_db)

            ## Copy entites
            target_db = ines_transform.copy_entities(source_db, target_db, entities_to_copy)
            ## Create hard-coded generator types appropriately
            target_db = create_generator_entities(source_db, target_db)
            ## Copy numeric parameters(source_db, target_db, parameter_transforms)
            target_db = ines_transform.transform_parameters_use_default(source_db, target_db, parameter_transforms,
                                                                        default_alternative="base", ts_to_map=True)
            ## Copy method parameters
            target_db = ines_transform.process_methods(source_db, target_db, parameter_methods)
            ## Copy entities to parameters
            target_db = ines_transform.copy_entities_to_parameters(source_db, target_db, entities_to_parameters)
            ## Copy capacity specific parameters (manual scripting)


def create_generator_entities(source_db: DatabaseMapping, target_db: DatabaseMapping) -> DatabaseMapping:
    node__generators = source_db.get_entity_items(fetch=False,
                                                  entity_class_name="Node__Generator")
    node__gen_names = [x["name"] for x in node__generators]
    node__gen_bynames = [x["entity_byname"] for x in node__generators]
    all_generators = source_db.get_entity_items(fetch=False,
                                                entity_class_name="Generator")
    all_generator_names = [x["name"] for x in all_generators]
    thermal_generators = source_db.get_entity_items(fetch=False,
                                                    entity_class_name="ThermalGenerators")
    thermal_generator_names = [x["name"] for x in thermal_generators]
    hydro_generators = source_db.get_entity_items(fetch=False,
                                                  entity_class_name="HydroGenerator")
    hydro_generator_names = [x["name"] for x in hydro_generators]
    hydro_reservoir_generators = source_db.get_entity_items(fetch=False,
                                                            entity_class_name="HydroGeneratorWithReservoir")
    hydro_reservoir_generator_names = [x["name"] for x in hydro_reservoir_generators]
    hydro_ror_generator_names = list(set(hydro_generator_names) - set(hydro_reservoir_generator_names))
    vre_generator_names = list(set(all_generator_names) - set(hydro_generator_names) - set(thermal_generator_names))
    gen_efficiencies = source_db.get_parameter_value_items(fetch=False,
                                                           entity_class_name="Generator",
                                                           parameter_definition_name="Efficiency")
    gen_fuel_costs = source_db.get_parameter_value_items(fetch=False,
                                                         entity_class_name="Generator",
                                                         parameter_definition_name="FuelCosts")

#    gen_effs = {byname: gen_efficiency for gen_efficiency in gen_efficiencies for byname in node__gen_bynames if
#                gen_efficiency["entity_name"] == byname[1]}
#    eff = gen_effs[("node1", "gen2")]

    for node__gen in node__gen_bynames:
        for gen_efficiency in gen_efficiencies:
            if gen_efficiency["entity_name"] == node__gen[1]:
                if gen_efficiency["entity_name"] in thermal_generator_names:
                    ines_transform.copy_parameter(target_db, gen_efficiency,
                                                  class_name="unit",
                                                  param_name="efficiency",
                                                  entity_byname=('__'.join(node__gen),),
                                                  column_name=["period"])
                break
    try:
        target_db.commit_session("Added generator entities and parameters")
    except NothingToCommit:
        print("Warning! No generator entities to be added")
    except DBAPIError as e:
        print(e)
        exit("failed to commit generator entities")
    return target_db


if __name__ == "__main__":
    main()
