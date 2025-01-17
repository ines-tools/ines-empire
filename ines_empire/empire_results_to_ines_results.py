from spinedb_api import DatabaseMapping
from ines_tools import ines_transform, ines_initialize
from sqlalchemy.exc import DBAPIError
from spinedb_api.exception import NothingToCommit
import sys
from sys import exit
import yaml

if len(sys.argv) > 1:
    url_db_in = sys.argv[1]
else:
    url_db_in = "sqlite:////home/prokjt/toolbox-projects/EMPIRE-EMX-EnergyPlan/empire results.sqlite"
    # exit("Please provide input database url and output database url as arguments. They should be of the form ""sqlite:///path/db_file.sqlite""")
if len(sys.argv) > 2:
    url_db_out = sys.argv[2]
else:
    url_db_out = "sqlite:////home/prokjt/toolbox-projects/EMPIRE-EMX-EnergyPlan/ines-output-spec.sqlite"
    # exit("Please provide input database url and output database url as arguments. They should be of the form ""sqlite:///path/db_file.sqlite""")

with open('empire_results_to_ines_results_entities.yaml', 'r') as file:
    entities_to_copy = yaml.load(file, yaml.BaseLoader)
with open('empire_results_to_ines_results_parameters.yaml', 'r') as file:
    parameter_transforms = yaml.load(file, yaml.BaseLoader)
#with open('empire_results_to_ines_results_entities_to_parameters.yaml', 'r') as file:
#    entities_to_parameters = yaml.load(file, yaml.BaseLoader)


def main():
    with DatabaseMapping(url_db_in) as source_db:
        with DatabaseMapping(url_db_out, upgrade=True) as target_db:
            ## Empty the database
            target_db = ines_initialize.purge_db_from_data(target_db)
            source_db = ines_initialize.fetch_data(source_db)
            target_db = ines_initialize.copy_alternatives_scenarios(source_db, target_db)

            ## Copy entites
            target_db = ines_transform.copy_entities(source_db, target_db, entities_to_copy)
            ## Copy numeric parameters(source_db, target_db, parameter_transforms)
            target_db = ines_transform.transform_parameters(source_db,
                                                            target_db,
                                                            parameter_transforms,
                                                            ts_to_map=True)
            ## Copy entities to parameters
            # target_db = ines_transform.copy_entities_to_parameters(source_db, target_db, entities_to_parameters)
            ## Copy capacity specific parameters (manual scripting)




if __name__ == "__main__":
    main()
