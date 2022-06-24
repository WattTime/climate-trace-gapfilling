import pandas as pd

GF_SOURCE_DATA_COLUMNS = ['edgar', 'faostat', 'climate-trace', 'edgar-projected', 'faostat-projected']
GAP_EQUATIONS = pd.read_csv(r"gap_filling/data/gap_equations.csv")
COMP_YEARS = [2020, 2019, 2018, 2017, 2016, 2015]
COL_ORDER = ['Data source', 'Country', 'Sector', 'Gas', 'Unit'] + COMP_YEARS
SECTORS = GAP_EQUATIONS['sub-sector'].values
CODE_CONVERSION = pd.read_csv('gap_filling/data/CT_ISO_Codes.csv').applymap(lambda x: x.strip(' '))

# Dictionaries Megan used to convert column names to previous names
DB_SOURCE_TO_COL_NAME = {"original_inventory_sector": "Sector",
                         "producing_entity_name": "Country",
                         "reporting_entity": "Data source",
                         "emitted_product_formula": "Gas",
                         "emission_quantity_units": "Unit"
                         }

COL_NAME_TO_DB_SOURCE = {"Sector": "original_inventory_sector",
                         "Country": "producing_entity_name",
                         "Data source": "reporting_entity",
                         "Gas": "emitted_product_formula",
                         "Unit": "emission_quantity_units"
                         }


class DatabaseColumns:
    # Class Alex used to reference new column names
    COUNTRY = "producing_entity_name"
    DATA_SOURCE = "reporting_entity"
    SECTOR = "original_inventory_sector"
    GAS = "emitted_product_formula"
    UNIT = "emission_quantity_units"
    VALUE = "emission_quantity"
    YEAR = "year"
    COUNT = "Data_count"
    PROJECTION_METHOD = "measurement_method_doi_or_url"


def get_iso3_code(country_name):
    if country_name not in CODE_CONVERSION['country_name'].values:
        return None

    return CODE_CONVERSION.loc[CODE_CONVERSION['country_name'] == country_name, 'iso3'].values[0]
