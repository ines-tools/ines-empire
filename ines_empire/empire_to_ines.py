import spinedb_api as api
from spinedb_api import DatabaseMapping
from pathlib import Path
import sys
try:
    from ines_tools import ines_transform, ines_initialize
except ModuleNotFoundError:
    print("Cannot find ines tools as an installed package")
    sys.exit(-1)
from sqlalchemy.exc import DBAPIError
from spinedb_api.exception import NothingToCommit
from sys import exit
import yaml



def main():
    with DatabaseMapping(url_db_in) as source_db:
        with DatabaseMapping(url_db_out, upgrade=True) as target_db:
            ## Empty the database
            target_db = ines_initialize.purge_db_from_data(target_db)
            target_db.purge_items('parameter_value')
            target_db.purge_items('entity')
            target_db.purge_items('alternative')
            target_db.purge_items('scenario')
            source_db = ines_initialize.fetch_data(source_db)
            target_db = ines_initialize.copy_alternatives_scenarios(source_db, target_db)

            ## Copy entities
            print("Copying entities")
            target_db = ines_transform.copy_entities(source_db, target_db, entities_to_copy)
            ## Create hard-coded generator types appropriately
            print("Creating generator entities")
            target_db = create_generator_entities(source_db, target_db)

            ## Create links
            print("Creating links")
            target_db = create_links(source_db, target_db)
            
            ## Create storages
            print("Creating storages")
            target_db = create_storages(source_db, target_db)
            
            #add technology sets
            print("Creating technology sets")
            target_db = create_technology_sets(source_db, target_db)

            ## Create industry units
            print("Creating industry units")
            target_db = create_unit_template_to_nodes(source_db, target_db, industry_units)

            ## Copy numeric parameters(source_db, target_db, parameter_transforms)
            print("Copying parameters")
            target_db = ines_transform.transform_parameters(source_db,
                                                            target_db,
                                                            parameter_transforms,
                                                            use_default=False,
                                                            default_alternative="base",
                                                            ts_to_map=True)
            ## Copy method parameters
            target_db = ines_transform.process_methods(source_db, target_db, parameter_methods)
            ## Copy entities to parameters
            #target_db = ines_transform.copy_entities_to_parameters(source_db, target_db, entities_to_parameters)
            
            ## Copy capacity specific parameters (manual scripting)
            
            
            try:
                target_db.commit_session("Added empire data to ines")
            except NothingToCommit:
                print("Warning! No generator entities to be added")
            except DBAPIError as e:
                print(e)
                exit("failed to commit generator entities")
            return target_db



def create_generator_entities(source_db: DatabaseMapping, target_db: DatabaseMapping) -> DatabaseMapping:
    node__generators = source_db.find_entities(entity_class_name="Node__Generator")
    node__gen_bynames = [x["entity_byname"] for x in node__generators]
    all_generators = source_db.find_entities(entity_class_name="Generator")
    all_generator_bynames = [x["entity_byname"] for x in all_generators]
    hydro_generators = source_db.find_entities(entity_class_name="HydroGenerator")
    hydro_generator_bynames = [x["entity_byname"] for x in hydro_generators]
    hydro_reservoir_generators = source_db.find_entities(entity_class_name="HydroGeneratorWithReservoir")
    hydro_reservoir_generator_bynames = [x["entity_byname"] for x in hydro_reservoir_generators]
    hydro_ror_generator_names = list(set(hydro_generator_bynames) - set(hydro_reservoir_generator_bynames))
    #vre_generator_names = list(set(all_generator_bynames) - set(hydro_generator_bynames) - set(thermal_generator_bynames))
    gen_efficiencies = source_db.find_parameter_values(entity_class_name="Generator",parameter_definition_name="Efficiency")
    gen_fuel_costs = source_db.find_parameter_values(entity_class_name="Generator",parameter_definition_name="FuelCosts")
    fuel_cost_gens = [param["entity_byname"][0] for param in gen_fuel_costs]
    co2_contents = source_db.find_parameter_values(entity_class_name="Generator",parameter_definition_name="CO2Content")
    co2_content_gens = [param["entity_byname"][0] for param in co2_contents]

    ramping_generators = source_db.find_entities(entity_class_name="RampingGenerator")
    GeneratorTypeAvailabilitys = source_db.find_parameter_values(entity_class_name="Generator", parameter_definition_name="GeneratorTypeAvailability")
    Lifetimes = source_db.find_parameter_values(entity_class_name="Generator", parameter_definition_name="Lifetime")
    RefInitialCap = source_db.find_parameter_values(entity_class_name="Node__Generator", parameter_definition_name="RefInitialCap")

        
    for node__gen in node__gen_bynames:
        for ramp_gen in ramping_generators:
            if ramp_gen["entity_byname"][1] == node__gen[1]:
                target_db = ines_transform.add_item_to_DB(target_db, "ramp_method", [settings["default_alternative"], ("__".join(node__gen),), "unit"], "ramp_limit")
                break
        for gen_availability in GeneratorTypeAvailabilitys:
            if gen_availability["entity_byname"][0] == node__gen[1]:
                ines_transform.copy_parameter(target_db, gen_availability,
                                              class_name="unit",
                                              param_name="availability",
                                              entity_byname=('__'.join(node__gen),),
                                              column_name=["period"])
                break
        for lifetime in Lifetimes:
            if lifetime["entity_byname"][0] == node__gen[1]:
                ines_transform.copy_parameter(target_db, lifetime,
                                              class_name="unit",
                                              param_name="lifetime",
                                              entity_byname=('__'.join(node__gen),),
                                              column_name=None)
                break
        for gen_efficiency in gen_efficiencies:
            if gen_efficiency["entity_byname"][0] == node__gen[1]:
                ines_transform.copy_parameter(target_db, gen_efficiency,
                                                class_name="unit",
                                                param_name="efficiency",
                                                entity_byname=('__'.join(node__gen),),
                                                column_name=["period"])
                break
        units_existing = 0
        for gen_capacity in RefInitialCap:
            if gen_capacity["entity_byname"] == node__gen:
                capacity = api.from_database(gen_capacity["value"],gen_capacity["type"])
                capacity = float(capacity.values[0]) if isinstance(capacity, api.Map) else capacity
                units_existing = 1 if capacity > 0 else 0
                break
        target_db = ines_transform.add_item_to_DB(target_db, "units_existing", [gen_capacity["alternative_name"], ("__".join(node__gen),), "unit"], units_existing)


        #add connection to the commodity node
        if node__gen[1] in fuel_cost_gens or node__gen[1] in co2_content_gens:
            target_db.add_entity_item(entity_class_name="node", entity_byname=(node__gen[1],))
            target_db.add_entity_item(entity_class_name="node__to_unit", entity_byname=(node__gen[1],"__".join(node__gen)))

    try:
        target_db.commit_session("Added generator entities and parameters")
    except NothingToCommit:
        print("Warning! No generator entities to be added")
    except DBAPIError as e:
        print(e)
        exit("failed to commit generator entities")
    return target_db

def create_technology_sets(source_db: DatabaseMapping, target_db: DatabaseMapping) -> DatabaseMapping:

    node__generators = source_db.find_entities(entity_class_name="Node__Generator")
    technology__generators = source_db.find_entities(entity_class_name="Technology__Generator")
    node__technologys = source_db.find_entities(entity_class_name="Node__Technology")
    max_built_capacities = source_db.find_parameter_values(entity_class_name="Node__Technology", parameter_definition_name="MaxBuiltCapacity")
    max_installed_capacities = source_db.find_parameter_values(entity_class_name="Node__Technology", parameter_definition_name="MaxInstalledCapacity")
    RefInitialCaps = source_db.find_parameter_values(entity_class_name="Node__Generator", parameter_definition_name="RefInitialCap")
    InitialCapacitys = source_db.find_parameter_values(entity_class_name="Node__Generator", parameter_definition_name="InitialCapacity")
    #ScaleFactorInitialCaps = source_db.find_parameter_values(entity_class_name="Node__Generator", parameter_definition_name="ScaleFactorInitialCap")
    
    #create sets
    
    for n_t in node__technologys:
        set_byname = ("_".join((n_t["entity_byname"][0],n_t["entity_byname"][1])),)
        target_db.add_entity_item(entity_class_name="set", entity_byname=set_byname)
        for t_g in technology__generators:
            if t_g["entity_byname"][0] == n_t["entity_byname"][1]:
                for n_g in node__generators:
                    if t_g["entity_byname"][1] == n_g["entity_byname"][1] and n_t["entity_byname"][0] == n_g["entity_byname"][0]:
                        generator_byname = ("__".join((n_g["entity_byname"][0],n_g["entity_byname"][1])),)
                        ines_transform.assert_success(target_db.add_entity_item(entity_class_name="set__unit", entity_byname=(set_byname[0],generator_byname[0])))
    
    #create params, could be combined with above loop for speed, but clearer this way
    for n_t in node__technologys:
        set_byname = ("_".join((n_t["entity_byname"][0],n_t["entity_byname"][1])),)
        for param in max_built_capacities:
            if param["entity_byname"] == n_t["entity_byname"]:
                ines_transform.copy_parameter(target_db, param, class_name="set", param_name="invest_max_period", entity_byname=set_byname)
        capacity = 0
        for t_g in technology__generators:
            if t_g["entity_byname"][0] == n_t["entity_byname"][1]:
                for n_g in node__generators:
                    if t_g["entity_byname"][1] == n_g["entity_byname"][1] and n_t["entity_byname"][0] == n_g["entity_byname"][0]:
                        #calculate initial capacity
                        for param in RefInitialCaps:
                            if param["entity_byname"] == n_g["entity_byname"]:
                                capacity = capacity + api.from_database(param["value"],param["type"])
                        for param in InitialCapacitys:
                            if param["entity_byname"] == n_g["entity_byname"]:
                                data = api.from_database(param["value"],param["type"])
                                if isinstance(data, api.Map):
                                    capacity = capacity + float(data.values[0])
                                elif isinstance(data, float):
                                    capacity = capacity + data
        for param in max_installed_capacities:
            if param["entity_byname"] == n_t["entity_byname"]:
                param_value = api.from_database(param["value"],param["type"])
                target_db = ines_transform.add_item_to_DB(target_db, "invest_max_total", [param["alternative_name"], set_byname, "set"], param_value - capacity)                

    return target_db


def create_links(source_db: DatabaseMapping, target_db: DatabaseMapping) -> DatabaseMapping:
    
    Node__Node__Linetypes = source_db.find_entities(entity_class_name="Node__Node__LineType")
    TypeCapitalCosts = source_db.find_parameter_values(entity_class_name="LineType", parameter_definition_name="TypeCapitalCost")
    TypeFixedOMCosts = source_db.find_parameter_values( entity_class_name="LineType", parameter_definition_name="TypeFixedOMCost")
    lengths = source_db.find_parameter_values(entity_class_name="Node__Node", parameter_definition_name="Length")
    Capacities = source_db.find_parameter_values(entity_class_name="Node__Node", parameter_definition_name="InitialCapacity")
    MaxBuiltCapacity = source_db.find_parameter_values(entity_class_name="Node__Node", parameter_definition_name="MaxBuiltCapacity")
    MaxInstallCapacityRaw = source_db.find_parameter_values(entity_class_name="Node__Node", parameter_definition_name="MaxInstallCapacityRaw")
    Linetype_alts = source_db.get_entity_alternative_items(entity_class_name="LineType")


    for n_n_l in Node__Node__Linetypes:
        link_name = "__".join((n_n_l["entity_byname"][0],n_n_l["entity_byname"][1]))
        link_byname = (link_name,)
        alt = Linetype_alts[0]["alternative_name"]
        length_value = 0
        for length in lengths:
            if length["entity_byname"][0] == n_n_l["entity_byname"][0] and length["entity_byname"][1] == n_n_l["entity_byname"][1]:
                length_value = api.from_database(length["value"], length["type"])
        if length_value > 0:
            links = 0
            capacity_value = 0
            for capacity in Capacities:
                if capacity["entity_byname"][0] == n_n_l["entity_byname"][0] and capacity["entity_byname"][1] == n_n_l["entity_byname"][1]:
                    capacity_value_db = api.from_database(capacity["value"], capacity["type"])
                    if float(capacity_value_db.values[0]) > 0:
                        if len(capacity_value_db.values) > 1:
                            links_existing = [float(val)/float(capacity_value_db.values[0]) for val in capacity_value_db.values]
                            links = api.Map(capacity_value_db.indexes, links_existing)
                        capacity_value = float(capacity_value_db.values[0])
                        target_db = ines_transform.add_item_to_DB(target_db, "capacity", [alt, link_byname, "link"], capacity_value)
            
            target_db = ines_transform.add_item_to_DB(target_db, "links_existing", [alt, link_byname, "link"], links)
            target_db = ines_transform.add_item_to_DB(target_db, "interest_rate", [settings["default_alternative"], link_byname, "link"], float(settings["default_interest_rate"]))

            if capacity_value:
                for param in MaxBuiltCapacity:
                    if param["entity_byname"][0] == n_n_l["entity_byname"][0] and param["entity_byname"][1] == n_n_l["entity_byname"][1]:
                        param_value = api.from_database(param["value"], param["type"])
                        param_value = multiply_all_datatypes(param_value, 1/capacity_value)
                        target_db = ines_transform.add_item_to_DB(target_db, "links_invest_max_period", [alt, link_byname, "link"], param_value)
                for param in MaxInstallCapacityRaw:
                    if param["entity_byname"][0] == n_n_l["entity_byname"][0] and param["entity_byname"][1] == n_n_l["entity_byname"][1]:
                        param_value = api.from_database(param["value"], param["type"])
                        param_value = multiply_all_datatypes(param_value, 1/capacity_value)
                        target_db = ines_transform.add_item_to_DB(target_db, "links_max_cumulative", [alt, link_byname, "link"], param_value)

            for param in TypeCapitalCosts:
                if param["entity_byname"][0] == n_n_l["entity_byname"][2]:
                    param_value = api.from_database(param["value"], param["type"])
                    param_value = multiply_all_datatypes(param_value, length_value)
                    target_db = ines_transform.add_item_to_DB(target_db, "investment_cost", [alt, link_byname, "link"], param_value)
            for param in TypeFixedOMCosts:
                if param["entity_byname"][0] == n_n_l["entity_byname"][2]:
                    param_value = api.from_database(param["value"], param["type"])
                    param_value = multiply_all_datatypes(param_value, length_value)
                    target_db = ines_transform.add_item_to_DB(target_db, "fixed_cost", [alt, link_byname, "link"], param_value)
    return target_db

def multiply_all_datatypes(param_value, factor):
    if isinstance(param_value, api.Map):
        for i, val in enumerate(param_value.values):
            param_value.values[i] = round(float(val) * factor,6)
        return param_value
    elif isinstance(param_value, float):
        return round(param_value * factor)
    else:
        return param_value


def create_storages(source_db: DatabaseMapping, target_db: DatabaseMapping) -> DatabaseMapping:
    node_storages = source_db.find_entities(entity_class_name="Node__Storage")
    for n_s in node_storages:
        node_name = n_s["entity_byname"][0]
        unit_in_name = "__".join((n_s["entity_byname"][0],n_s["entity_byname"][1],"charge"))
        unit_out_name = "__".join((n_s["entity_byname"][0],n_s["entity_byname"][1],"discharge"))
        storage_name = "__".join((n_s["entity_byname"][0],n_s["entity_byname"][1]))
        target_db.add_entity_item(entity_class_name="node", entity_byname=(storage_name,))
        target_db.add_entity_item(entity_class_name="unit", entity_byname=(unit_in_name,))
        target_db.add_entity_item(entity_class_name="unit", entity_byname=(unit_out_name,))
        target_db.add_entity_item(entity_class_name="unit__to_node", entity_byname=(unit_in_name,storage_name,))
        target_db.add_entity_item(entity_class_name="unit__to_node", entity_byname=(unit_out_name,node_name,))
        target_db.add_entity_item(entity_class_name="node__to_unit", entity_byname=(node_name,unit_in_name,))
        target_db.add_entity_item(entity_class_name="node__to_unit", entity_byname=(storage_name,unit_out_name,))

        target_db = ines_transform.add_item_to_DB(target_db, "storage_interest_rate", [settings["default_alternative"], (storage_name,), "node"], float(settings["default_interest_rate"]))
        for param in source_db.find_parameter_values(entity_class_name="Node__Storage", parameter_definition_name="EnergyInitialCapacity", entity_byname = n_s["entity_byname"]):
            energy_initial_capacity = api.from_database(param["value"], param["type"])
            energy_capacity = float(energy_initial_capacity.values[0]) if isinstance(energy_initial_capacity, api.Map) else energy_initial_capacity
            target_db = ines_transform.add_item_to_DB(target_db, "storage_capacity", [settings["default_alternative"], (storage_name,), "node"], energy_capacity)
            storages_existing = 1 if energy_capacity > 0 else 0
            energy_capacity = energy_capacity if energy_capacity > 0 else float(settings["default_capacity"])
            target_db = ines_transform.add_item_to_DB(target_db, "storages_existing", [settings["default_alternative"], (storage_name,), "node"], storages_existing)
        
        for param in source_db.find_parameter_values(entity_class_name="Node__Storage", parameter_definition_name="InitialPowerCapacity", entity_byname = n_s["entity_byname"]):
            power_initial_capacity = api.from_database(param["value"], param["type"])
            power_capacity = float(power_initial_capacity.values[0]) if isinstance(power_initial_capacity, api.Map) else power_initial_capacity
            target_db = ines_transform.add_item_to_DB(target_db, "capacity", [settings["default_alternative"], (unit_in_name,storage_name), "unit__to_node"], power_capacity)
            target_db = ines_transform.add_item_to_DB(target_db, "capacity", [settings["default_alternative"], (unit_out_name,node_name), "unit__to_node"], power_capacity)
            units_existing = 1 if power_capacity > 0 else 0
            power_capacity = power_capacity if power_capacity > 0 else float(settings["default_capacity"])
            target_db = ines_transform.add_item_to_DB(target_db, "units_existing", [settings["default_alternative"], (unit_in_name,), "unit"], units_existing)
            target_db = ines_transform.add_item_to_DB(target_db, "units_existing", [settings["default_alternative"], (unit_out_name,), "unit"], units_existing)
        
        for param in source_db.find_parameter_values(entity_class_name="Node__Storage", parameter_definition_name="EnergyMaxBuiltCapacity", entity_byname = n_s["entity_byname"]):
            energy_max_built_capacity = api.from_database(param["value"], param["type"])
            val_out = multiply_all_datatypes(energy_max_built_capacity, 1/energy_capacity)
            target_db = ines_transform.add_item_to_DB(target_db, "storages_invest_max_period", [settings["default_alternative"], (storage_name,), "node"], val_out)

        for param in source_db.find_parameter_values(entity_class_name="Node__Storage", parameter_definition_name="EnergyMaxInstalledCapacity", entity_byname = n_s["entity_byname"]):
            energy_max_installed_capacity = api.from_database(param["value"], param["type"])
            val_out = multiply_all_datatypes(energy_max_installed_capacity, 1/energy_capacity)
            target_db = ines_transform.add_item_to_DB(target_db, "storages_max_cumulative", [settings["default_alternative"], (storage_name,), "node"], val_out)

        for param in source_db.find_parameter_values(entity_class_name="Node__Storage", parameter_definition_name="PowerMaxBuiltCapacity", entity_byname = n_s["entity_byname"]):
            power_max_built_capacity = api.from_database(param["value"], param["type"])
            val_out = multiply_all_datatypes(power_max_built_capacity, 1/power_capacity)
            target_db = ines_transform.add_item_to_DB(target_db, "units_invest_max_period", [settings["default_alternative"], (unit_in_name,), "unit"], val_out)
            target_db = ines_transform.add_item_to_DB(target_db, "units_invest_max_period", [settings["default_alternative"], (unit_out_name,), "unit"], val_out)

        for param in source_db.find_parameter_values(entity_class_name="Node__Storage", parameter_definition_name="PowerMaxInstalledCapacity", entity_byname = n_s["entity_byname"]):
            power_max_installed_capacity = api.from_database(param["value"], param["type"])
            val_out = multiply_all_datatypes(power_max_installed_capacity, 1/power_capacity)
            target_db = ines_transform.add_item_to_DB(target_db, "units_max_cumulative", [settings["default_alternative"], (unit_in_name,), "unit"], val_out)
            target_db = ines_transform.add_item_to_DB(target_db, "units_max_cumulative", [settings["default_alternative"], (unit_out_name,), "unit"], val_out)

        for source_class, information in storages.items():
            for source_param, target_information in information.items():
                params = source_db.find_parameter_values(entity_class_name=source_class, parameter_definition_name=source_param, entity_byname = (n_s["entity_byname"][1],))
                positions = target_information[0]
                target_param_def = target_information[1:]
                if positions == ["0"]:
                    target_entity_class = "node"
                    target_entity_byname = (node_name,)
                elif positions == ["1"]:
                    target_entity_class = "unit"
                    target_entity_byname = (unit_in_name,)
                elif positions == ["2"]:
                    target_entity_class = "unit"
                    target_entity_byname = (unit_out_name,)
                elif positions == ["3"]:
                    target_entity_class = "node"
                    target_entity_byname = (storage_name,)
                elif positions == ["1","3"]:
                    target_entity_class = "unit__to_node"
                    target_entity_byname = (unit_in_name,storage_name)
                elif positions == ["2","0"]:
                    target_entity_class = "unit__to_node"
                    target_entity_byname = (unit_in_name,storage_name)
                else:
                    print("the position array needs to be in a different format. Check the examples in storages.yaml")
                    sys.exit(-1)
                
                for param in params:
                    ines_transform.assert_success(target_db.add_update_item(
                        "parameter_value",
                        entity_class_name=target_entity_class,
                        entity_byname=target_entity_byname,
                        parameter_definition_name=target_param_def[0],
                        alternative_name=param["alternative_name"],
                        value=param["value"],
                        type=param["type"],
                    ))
        #add charging and discharging investment tying
        target_db.add_entity_item(entity_class_name="constraint", entity_byname=("_".join((unit_in_name,unit_out_name)),))
        target_db = ines_transform.add_item_to_DB(target_db, "constant", [settings["default_alternative"], ("_".join((unit_in_name,unit_out_name)),), "constraint"], 0)
        target_db = ines_transform.add_item_to_DB(target_db, "sense", [settings["default_alternative"], ("_".join((unit_in_name,unit_out_name)),), "constraint"], "equal")
        map_in = api.Map(["_".join((unit_in_name,unit_out_name))], [1.0])
        map_out = api.Map(["_".join((unit_in_name,unit_out_name))], [-1.0])
        target_db = ines_transform.add_item_to_DB(target_db, "constraint_unit_count_coefficient", [settings["default_alternative"], (unit_in_name,), "unit"], map_in)
        target_db = ines_transform.add_item_to_DB(target_db, "constraint_unit_count_coefficient", [settings["default_alternative"], (unit_out_name,), "unit"], map_out)
    
    try:
        target_db.commit_session("Added storages")
    except NothingToCommit:
        pass
    except DBAPIError as e:
        print("failed to add industry")
    return target_db

def create_unit_template_to_nodes(source_db: DatabaseMapping, target_db: DatabaseMapping, industry_units: dict, bi_directional = False) -> DatabaseMapping:

    for source_node_class, mapping in industry_units.items():
        for source_unit_class, name_mapping in mapping.items():
            for added_name, parameter_mapping in name_mapping.items():
                    
                source_units = source_db.find_entities(entity_class_name=source_unit_class)
                source_nodes = source_db.find_entities(entity_class_name=source_node_class)
                unit__nodes = source_db.find_entities(entity_class_name=source_node_class + "__" + source_unit_class)

                for unit in source_units:
                    for node in source_nodes:
                        if not any(un["entity_byname"][0] == node["name"] and un["entity_byname"][1] == unit["name"] for un in unit__nodes):
                            continue
                        unit_name = unit["name"]
                        node_name = node["name"]
                        target_unit_name = node_name + "__" + unit_name #+ "_" + added_name
                        target_node_name = node_name + "__" + added_name
                        target_db.add_entity_item(entity_class_name="unit", entity_byname=(target_unit_name,))
                        target_db.add_entity_item(entity_class_name="node", entity_byname=(target_node_name,))
                        target_db.add_entity_item(entity_class_name="unit__to_node", entity_byname=(target_unit_name, target_node_name))
                        target_db.add_entity_item(entity_class_name="node__to_unit", entity_byname=(node_name, target_unit_name))
                        if bi_directional:
                            target_db.add_entity_item(entity_class_name="unit__to_node", entity_byname=(target_unit_name, node_name))
                            target_db.add_entity_item(entity_class_name="node__to_unit", entity_byname=(node_name, target_unit_name))

                        target_db = ines_transform.add_item_to_DB(target_db, "interest_rate", [settings["default_alternative"], (target_unit_name,), "unit"], float(settings["default_interest_rate"]))
                            

                        for source_param_name, target_param_definitions in parameter_mapping.items():
                            operand_class = None
                            if isinstance(target_param_definitions, dict):
                                positions = target_param_definitions.get("positions", None)
                                operand_with = target_param_definitions.get("with", None)
                                target_param_def = target_param_definitions
                                if isinstance(operand_with, list):
                                    operand_class = operand_with[0]
                            elif isinstance(target_param_definitions, list):
                                target_param_def = target_param_definitions[1:]
                                positions = target_param_definitions[0]
                            else:
                                print('The format of the mapping in industry_units.yaml is incorrect. Look at the examples provided.')
                                sys.exit(-1)

                            source_entity_classes = [source_node_class,source_unit_class]
                            source_entity_class = "__".join([source_entity_classes[int(i)-1] for i in positions[0]])
                            source_used_classes = [source_entity_classes[int(i)-1] for i in positions[0]]

                            if operand_class:
                                if operand_class not in source_used_classes:
                                    print(f"The operand class {operand_class} is not found in the source entity classes used {source_used_classes}." +
                                            "It is required to be one of the classes to combine with the correct entity." +
                                            "Please check the industry_units.yaml file.")
                                    sys.exit(-1)
                                else:
                                    operand_index = source_used_classes.index(operand_class)
                            if positions[1] == ["1"]:
                                target_entity_class = "node"
                            elif positions[1] == ["2"]:
                                target_entity_class = "unit"
                            elif positions[1] == ["2","1"] or positions[1] == ["2","0"]:
                                target_entity_class = "unit__to_node"
                            elif positions[1] == ["1","2"] or positions[1] == ["0","2"]:
                                target_entity_class = "node__to_unit"
                            else:
                                print("the position array needs to be in a different format. Check the examples in industry_units.yaml")
                                sys.exit(-1)
                            
                            #remember that other inputs may exist for the unit, but they are gotten from the name of the unit...
                            source_entity_names = [node_name, unit_name]
                            source_entity_byname = tuple([source_entity_names[int(i)-1] for i in positions[0]])
                            target_entity_names = [node_name, target_node_name,target_unit_name]
                            target_entity_byname = tuple([target_entity_names[int(i)] for i in positions[1]])
                            
                            source_param_items = source_db.find_parameter_values(entity_class_name=source_entity_class,
                                                                                        parameter_definition_name=source_param_name,
                                                                                        entity_byname = source_entity_byname)
                            for item in source_param_items:
                                if operand_class:
                                    entity_byname = (item["entity_byname"][operand_index],)
                                else:
                                    entity_byname = item["entity_byname"]
                                value_information = ines_transform.process_parameter_transforms(
                                    entity_byname,
                                    item["value"],
                                    item["type"],
                                    target_param_def,
                                    True,
                                    source_db=source_db,
                                    source_entity_class=source_unit_class,
                                    alternative_name=item["alternative_name"],
                                )
                                if not value_information:
                                    print(f"The operand (with:) cannot be found for parameter {source_param_name} when creating industry units.")
                                    sys.exit(-1)
                                for (
                                target_parameter_name,
                                target_value,
                                ) in value_information[0].items():
                                    ines_transform.assert_success(target_db.add_update_item(
                                        "parameter_value",
                                        entity_class_name=target_entity_class,
                                        entity_byname=target_entity_byname,
                                        parameter_definition_name=target_param_def[0],
                                        alternative_name=item["alternative_name"],
                                        value=target_value,
                                        type=value_information[1],
                                    ))

    try:
        target_db.commit_session("Added industry")
    except NothingToCommit:
        pass
    except DBAPIError as e:
        print("failed to add industry")
    return target_db


if __name__ == "__main__":

    if len(sys.argv) > 1:
        url_db_in = sys.argv[1]
    else:
        exit("Please provide input database url and output database url as arguments. They should be of the form ""sqlite:///path/db_file.sqlite""")
    if len(sys.argv) > 2:
        url_db_out = sys.argv[2]
    else:
        exit("Please provide input database url and output database url as arguments. They should be of the form ""sqlite:///path/db_file.sqlite""")

    with open('empire_to_ines_settings.yaml', 'r') as file:
        settings = yaml.load(file, yaml.BaseLoader)

    with open('empire_to_ines_entities.yaml', 'r') as file:
        entities_to_copy = yaml.load(file, yaml.BaseLoader)
    with open('empire_to_ines_parameters.yaml', 'r') as file:
        parameter_transforms = yaml.load(file, yaml.BaseLoader)
    with open('empire_to_ines_methods.yaml', 'r') as file:
        parameter_methods = yaml.load(file, yaml.BaseLoader)
    with open('empire_to_ines_entities_to_parameters.yaml', 'r') as file:
        entities_to_parameters = yaml.load(file, yaml.BaseLoader)
    with open('industry_units.yaml', 'r') as file:
        industry_units = yaml.load(file, yaml.BaseLoader)
    with open('storages.yaml', 'r') as file:
        storages = yaml.load(file, yaml.BaseLoader)

    main()
