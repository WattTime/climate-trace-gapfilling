import pandas as pd

GF_SOURCE_DATA_COLUMNS = ['edgar', 'faostat', 'climate-trace', 'edgar-projected', 'faostat-projected']
GAP_EQUATIONS = pd.read_csv(r"gap_filling/data/gap_equations.csv")
COMP_YEARS = [2020, 2019, 2018, 2017, 2016, 2015]
COL_ORDER = ['Data source', 'ID', 'Sector', 'Gas', 'Unit'] + COMP_YEARS
SECTORS = GAP_EQUATIONS['sub-sector'].values
CODE_CONVERSION = pd.read_csv('gap_filling/data/CT_ISO_Codes.csv').applymap(lambda x: x.strip(' '))

# Dictionaries Megan used to convert column names to previous names
DB_SOURCE_TO_COL_NAME = {"original_inventory_sector": "Sector",
                         # "producing_entity_name": "Country",
                         "iso3_country": "ID",
                         "reporting_entity": "Data source",
                         "gas": "Gas",
                         "emissions_quantity_units": "Unit",
                         "created_date": "Created"
                         }

COL_NAME_TO_DB_SOURCE = {"Sector": "original_inventory_sector",
                         # "Country": "producing_entity_name",
                         "ID": "iso3_country",
                         "Data source": "reporting_entity",
                         "Gas": "gas",
                         "Unit": "emissions_quantity_units",
                         "Created": "created_date"
                         }


class DatabaseColumns:
    # Class Alex used to reference new column names
    COUNTRY = "producing_entity_name"
    ID = "iso3_country"
    DATA_SOURCE = "reporting_entity"
    SECTOR = "original_inventory_sector"
    GAS = "gas"
    UNIT = "emissions_quantity_units"
    VALUE = "emission_quantity"
    YEAR = "year"
    COUNT = "Data_count"
    PROJECTION_METHOD = "measurement_method_doi_or_url"
    CREATED = "created_date"


def get_country_name(iso3):
    if iso3 not in CODE_CONVERSION['iso3'].values:
        return None

    return CODE_CONVERSION.loc[CODE_CONVERSION['iso3'] == iso3, 'country_name'].values[0]
