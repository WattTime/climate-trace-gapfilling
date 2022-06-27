import datetime

import numpy as np
import pandas as pd

from gap_filling.constants import COL_NAME_TO_DB_SOURCE, DB_SOURCE_TO_COL_NAME, COMP_YEARS, COL_ORDER, get_iso3_code, \
    SECTORS


def rename_df_columns(df, db_like=False):
    if db_like:
        # This takes columns that have "file-like" names and returns the "db-like" names
        return df.rename(columns=COL_NAME_TO_DB_SOURCE)
    else:
        # This takes columns that have "db-like" names and returns the "file-like" names
        return df.rename(columns=DB_SOURCE_TO_COL_NAME)


def from_years_to_start_and_end_times(df):
    # Add start and end times, remove year from dataframe
    df["start_time"] = [datetime.date(sty, 1, 1) for sty in df["year"]]
    df["end_time"] = [datetime.date(sty, 12, 31) for sty in df["year"]]

    return df.drop(columns="year")


def from_start_and_end_times_to_years(df):
    # Parse the year from the start_time column
    start_times = df.loc[:, "start_time"]
    years = [st.year for st in start_times]
    df["start_time"] = years
    return df.rename(columns={"start_time": "year"})
    # st_col = np.where(colnames == "start_time")[0][0]
    # st_data = [d.year for d in d[:, st_col]]
    # d[:, st_col] = st_data
    # colnames[st_col] = "year"
    # return pd.DataFrame(data=d, columns=colnames)


def transform_years_to_columns(df):
    transformed_df = df.pivot(index=["Sector", "Country", "ID", "Data source", "Gas", "Unit"], columns='year',
                              values='emission_quantity').reset_index()
    missing_years = [cy for cy in COMP_YEARS if cy not in transformed_df.columns]

    transformed_df = transformed_df.reindex(columns=transformed_df.columns.tolist() + missing_years)
    return transformed_df[COL_ORDER]


def transform_years_to_rows(df):
    transformed_df = pd.melt(df,
                             id_vars=["Sector", "Country", "ID", "Data source", "Gas", "Unit"],
                             value_vars=COMP_YEARS,
                             var_name="year", value_name="emission_quantity")

    return transformed_df


def parse_and_format_query_data(my_df, years_to_columns=True, rename_columns=True, times_to_years=True):
    # This function takes data from the database and puts it in the format that the gapfilling/projection code expects
    if times_to_years:
        # Replace the start time column with the year
        my_df = from_start_and_end_times_to_years(my_df)

    if rename_columns:
        # Rename columns
        my_df = rename_df_columns(my_df, db_like=False)

    # Pandas pivot the data into rows
    if years_to_columns:
        my_df = transform_years_to_columns(my_df)

    return my_df


def parse_and_format_data_to_insert(my_df, do_melt=True, years_to_times=True, rename_columns=True,
                                    add_iso=True, add_carbon_eq=True):
    # This function takes data from the gapfilling/projection code and puts it in the database format

    if do_melt:
        # Melt the years back into their own row rather than pivoted into columns
        my_df = transform_years_to_rows(my_df)

    if years_to_times:
        # Swap years for start/end times
        my_df = from_years_to_start_and_end_times(my_df)

    if rename_columns:
        # Rename columns in the data as appropriate
        my_df = rename_df_columns(my_df, db_like=True)

    if add_iso:
        # Add the iso3 code to this data for insertion
        my_df = add_iso_value(my_df)

    if add_carbon_eq:
        # Make sure the carbon equivalency is in there for gap filled data
        my_df = add_carbon_eq_column(my_df)

    return my_df


def generate_carbon_equivalencies(dh, df, co2e_to_compute=100):
    # col_to_mult can be either 20 or 100 to compute the 20 or 100 year carbon equivalency for the data
    ghgs = dh.get_ghgs().rename(columns={"gas": "Gas"})
    # Merge on the Gas value
    merged_df = pd.merge(df, ghgs, on="Gas")

    col_to_mult = 'co2e_' + str(co2e_to_compute)

    # Multiply by the appropriate co2e value
    merged_df[COMP_YEARS] = merged_df[COMP_YEARS].multiply(merged_df[col_to_mult], axis=0)
    # Sum up the multiplied numbers and return them
    co2e_vals = merged_df.groupby(["ID", "Sector", "Data source", "Unit"], as_index=False)[COMP_YEARS].sum()
    co2e_vals["Gas"] = col_to_mult

    return co2e_vals


def add_all_gas_rows(df):
    # Drop rows with nas for country
    df.dropna(subset=['Country'], inplace=True)
    # Set the individual indexes
    gases = ['co2', 'n2o', 'ch4', 'co2e_20', 'co2e_100']
    multi_ind_col_list = ['Country', 'Sector', 'Gas']
    countries = df['Country'].unique()
    sectors = np.unique(SECTORS)

    # Reindex to ensure every country, gas, sector, and year combination has a row
    multi_index_all_years = pd.MultiIndex.from_product([countries, sectors, gases], names=multi_ind_col_list)

    print(f"Number of country-sector-gas combinations: {len(multi_index_all_years)}")

    df = df.set_index(multi_ind_col_list).reindex(multi_index_all_years, fill_value=np.nan).reset_index()
    df['Data source'] = 'climate-trace'
    df['Unit'] = 'tonnes'

    return df


def add_carbon_eq_column(df):
    cem_gases = {'co2e_20': 0, 'co2e_100': 1}
    cem_str = ['20-year', '100-year']

    # Build list of carbon equivalence strings
    cem_list = [cem_str[cem_gases[g]] if g in cem_gases.keys() else "" for g in df['emitted_product_formula']]

    # Change co2eq back
    df.replace({'emitted_product_formula': {'co2e_20': 'co2e', 'co2e_100': 'co2e'}})
    df['carbon_equivalency_method'] = cem_list

    return df


def add_iso_value(df):
    df["producing_entity_id"] = [get_iso3_code(pen) for pen in df['producing_entity_name']]

    return df


def assemble_data(gap_filled_df, co2e_20_df, co2e_100_df):
    # Join together the dataframes
    joined_df = pd.concat([co2e_20_df, co2e_100_df, gap_filled_df])
    # Need to add in placeholders for all gases
    final_df = add_all_gas_rows(joined_df)

    return final_df


def get_all_edgar_data(data_handler):
    expected_last_edgar_value_year = 2018
    columns_to_check = np.where(np.array(COMP_YEARS) > expected_last_edgar_value_year, COMP_YEARS, -1)
    # This function gets the edgar and projected edgar data from the database and returns a concatenated data frame
    edgar_data = data_handler.load_data("edgar",  gas=None, years_to_columns=True)
    projected_edgar_data = data_handler.load_data("edgar-projected", years_to_columns=True)

    # TODO: Check to make sure there's no intersection of years across edgar and projected_edgar
    # np.unique(edgar_data[columns_to_check[:2]].values)
    # if np.unique(checker) != np.nan:
    #     raise Exception("Intersecting years in the projected and base EDGAR data!!")

    return pd.concat([edgar_data, projected_edgar_data])


def get_all_faostat_data(data_handler):
    expected_last_faostat_value_year = 2019
    columns_to_check = np.where(np.array(COMP_YEARS) > expected_last_faostat_value_year, COMP_YEARS, -1)
    # This function gets the edgar and projected edgar data from the database and returns a concatenated data frame
    faostat_data = data_handler.load_data("faostat",  gas=None, years_to_columns=True)
    projected_faostat_data = data_handler.load_data("faostat-projected", years_to_columns=True)

    # TODO: Check to make sure there's no intersection of years across edgar and projected_edgar
    # np.unique(edgar_data[columns_to_check[:2]].values)
    # if np.unique(checker) != np.nan:
    #     raise Exception("Intersecting years in the projected and base EDGAR data!!")

    return pd.concat([faostat_data, projected_faostat_data])
