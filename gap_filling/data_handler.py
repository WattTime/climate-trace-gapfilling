import datetime
import json
import time
import numpy as np
import pandas as pd
import psycopg2 as pg2

from gap_filling.utils import parse_and_format_query_data

INSERT_MAPPING = {"ois": "original_inventory_sector", "pen": "producing_entity_name",
                  "pei": "producing_entity_id", "re": "reporting_entity", "epf": "emitted_product_formula",
                  "eq": "emission_quantity", "equ": "emission_quantity_units",
                  "st": "start_time", "et": "end_time", "cd": "created_date"}


def init_db_connect(params):
    try:
        conn = pg2.connect(user=params['db_user'],
                           password=params['db_pass'],
                           host="rds-climate-trace.watttime.org",
                           database="climatetrace")
    except Exception as e:
        print(e)
        raise Exception(e)

    # Double check in case somehow there was a silent error or issue
    if not conn:
        raise ConnectionError("Initializing the database connection failed!")
    return conn


class DataHandler:
    def __init__(self, params_file='params.json'):
        self.params_file = params_file

        self.conn = init_db_connect(self.get_params())

    def get_params(self):
        with open(self.params_file, 'r') as fid:
            params = json.load(fid)
        return params

    def get_cursor(self):
        if not self.conn:
            raise ConnectionError("No database connection has been established!")
        if self.conn.closed:
            raise ConnectionError("The database connection is closed!")

        return self.conn.cursor()

    def load_data(self, source: str, years_to_columns=False, rename_columns=True, gas=None, is_co2e=False,
                  start_date=datetime.date(2013, 1, 1)):
        curs = self.get_cursor()

        if gas is None:
            curs.execute("SELECT original_inventory_sector, producing_entity_name, producing_entity_id, reporting_entity, "
                         "emitted_product_formula, emission_quantity, emission_quantity_units, start_time "
                         "FROM ermin WHERE reporting_entity = %s AND start_time >= %s "
                         "AND carbon_equivalency_method IS NULL",
                         (source, start_date))
        else:
            if not is_co2e:
                curs.execute("SELECT original_inventory_sector, producing_entity_name, producing_entity_id, reporting_entity, "
                             "emitted_product_formula, emission_quantity, emission_quantity_units, start_time "
                             "FROM ermin WHERE reporting_entity = %s AND emitted_product_formula = %s "
                             "AND start_time >= %s AND carbon_equivalency_method IS NULL",
                             (source, gas, start_date))
            else:
                curs.execute("SELECT original_inventory_sector, producing_entity_name, producing_entity_id, reporting_entity, "
                             "emitted_product_formula, emission_quantity, emission_quantity_units, start_time "
                             "FROM ermin WHERE reporting_entity = %s AND emitted_product_formula = %s "
                             "AND start_time >= %s",
                             (source, gas, start_date))

        colnames = [desc[0] for desc in curs.description]
        return parse_and_format_query_data(pd.DataFrame(data=np.array(curs.fetchall()), columns=np.array(colnames)),
                                           years_to_columns=years_to_columns, rename_columns=rename_columns)

    def get_ghgs(self, f_gas=None):
        curs = self.get_cursor()
        if f_gas:
            curs.execute("SELECT * FROM ghgs WHERE f_gas_category = %s", (f_gas,))
        elif f_gas == 'all':
            curs.execute("SELECT * FROM ghgs WHERE f_gas_category is NOT NULL")
        else:
            curs.execute("SELECT lower_designation as gas, gwp_20yr as co2e_20, gwp_100yr as co2e_100 FROM ghgs")

        colnames = [desc[0] for desc in curs.description]
        return pd.DataFrame(data=np.array(curs.fetchall()), columns=colnames)

    def compare_values(self, value_in_db, value_inserting):
        if pd.isna(value_in_db):
            if pd.isna(value_inserting):
                return True
        else:
            return value_in_db == value_inserting

    def check_if_values_changed(self, data_row_dict):
        cols = ['subsector', 'iso3_country', 'start_date', 'end_date', 'co2_emissions_tonnes', \
                'ch4_emissions_tonnes', 'n2o_emissions_tonnes', 'total_co2e_100yrgwp', 'total_co2e_20yrgwp']

        query = "SELECT subsector, iso3_country, start_date, end_date, co2_emissions_tonnes,\
                ch4_emissions_tonnes, n2o_emissions_tonnes, total_co2e_100yrgwp, total_co2e_20yrgwp " \
                "FROM stable_ct " \
                "WHERE subsector = %(ois)s" \
                "AND iso3_country = %(pei)s "  \
                "AND start_date = %(st)s " \
                "AND end_date = %(et)s"

        curs = self.get_cursor()
        curs.execute(query, data_row_dict)
        res = curs.fetchall()

        if len(res[0]) < 1:
            return 'does not exist'

        result_dict = {}
        i = 0
        for col in cols:
            result_dict[col] = res[0][i]
            i += 1

        gas_in_insert_string = data_row_dict['epf']
        if gas_in_insert_string.split('_')[0] == 'co2e':
            cem = data_row_dict['cem']
            value_in_db = result_dict[f'total_co2e_{cem.split("-")[0]}yrgwp']
        else:
            value_in_db = result_dict[f'{gas_in_insert_string}_emissions_tonnes']

        value_inserting = data_row_dict['eq']
        match = self.compare_values(value_in_db, value_inserting)

        if match:
            return match
        if not match:
            data_row_dict['neq'] = value_inserting
            data_row_dict['md'] = datetime.datetime.now().isoformat()
            update_str = "UPDATE ermin " \
                         "SET emission_quantity = %(eq)s, modified_date = %(md)s" \
                         "WHERE original_inventory_sector = %(ois)s" \
                         "AND producing_entity_name = %(pen)s" \
                         "AND producing_entity_id = %(pei)s" \
                         "AND reporting_entity = %(re)s" \
                         "AND emitted_product_formula = %(epf)s" \
                         "AND carbon_equivalency_method = %(cem)s" \
                         "AND start_time = %(st)s" \
                         
            curs = self.get_cursor()
            curs.execute(update_str, data_row_dict)
            self.conn.commit()

            return match

    def write_data(self, data_to_insert, rows_type=None):
        # Two different kinds of inserts we'll need to perform here
        data_to_insert['created_date'] = datetime.datetime.now().isoformat()
        if rows_type == "climate-trace":
            INSERT_MAPPING["cem"] = "carbon_equivalency_method"
            insert_str = "INSERT INTO ermin (original_inventory_sector, producing_entity_name, producing_entity_id, " \
                         "reporting_entity, emitted_product_formula, emission_quantity, emission_quantity_units, " \
                         "carbon_equivalency_method, start_time, end_time, created_date) " \
                         "VALUES (%(ois)s, %(pen)s, %(pei)s, %(re)s, %(epf)s, %(eq)s, %(equ)s, %(cem)s, %(st)s, %(et)s, %(cd)s)"

        elif rows_type == "edgar":
            INSERT_MAPPING["mmd"] = "measurement_method_doi_or_url"
            insert_str = "INSERT INTO ermin (original_inventory_sector, producing_entity_name, producing_entity_id, " \
                         "reporting_entity, emitted_product_formula, emission_quantity, emission_quantity_units, " \
                         "measurement_method_doi_or_url, start_time, end_time, created_date) " \
                         "VALUES (%(ois)s, %(pen)s, %(pei)s, %(re)s, %(epf)s, %(eq)s, %(equ)s, %(mmd)s, %(st)s, %(et)s, %(cd)s)"

        curs = self.get_cursor()

        # According to the docs, executemany() is no faster than execute() in a loop, so running it in a loop
        for dti in data_to_insert.iterrows():
            print(f'inserting row {dti[0]} out of {data_to_insert.shape[0]} for sector {data_to_insert.loc[dti[0], "original_inventory_sector"]}')
            data_row_dict = {k: dti[1][INSERT_MAPPING[k]] for k in INSERT_MAPPING.keys()}
            match = self.check_if_values_changed(data_row_dict)

            if match == 'does not exist':
                pass
            else:
                continue

            try:
                curs.execute(insert_str, data_row_dict)
            except Exception as e:
                self.conn.rollback()
                t = e.pgerror
                if 'duplicate' in t.split(':')[1]:
                    match = self.check_if_values_changed(data_row_dict)
                    if match:
                        print('Duplicate encountered, skipping entry')
                    if not match:
                        print('Value changed and has been updated')
                    continue
            self.conn.commit()

