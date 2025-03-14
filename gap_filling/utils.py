import datetime

import numpy as np
import pandas as pd
import sys

from gap_filling.constants import (
    COL_NAME_TO_DB_SOURCE,
    DB_SOURCE_TO_COL_NAME,
    COMP_YEARS,
    COL_ORDER,
    NON_FOSSIL_SECTORS
)


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
    # start_times = df.loc[:, "start_time"]
    # years = [st.year for st in start_times]
    # df["start_time"] = years
    # return df.rename(columns={"start_time": "year"})
    # st_col = np.where(colnames == "start_time")[0][0]
    # st_data = [d.year for d in d[:, st_col]]
    # d[:, st_col] = st_data
    # colnames[st_col] = "year"
    # return pd.DataFrame(data=d, columns=colnames)

    df["start_time"] = df["start_time"].dt.year

    # Function to sum all rows for a given year but only non-annual rows if both exist (either other or month, mainly)
    def sum_not_year(group):
        if (
            "annual" in group["temporal_granularity"].values
            and len(np.unique(group["temporal_granularity"].values)) > 1
        ):
            # If annual granularity exists with another, sum the 'other'
            return group[group["temporal_granularity"] != "annual"][
                "emissions_quantity"
            ].sum()
        else:
            return group["emissions_quantity"].sum()

    summed_df = (
        df.groupby(
            [
                "original_inventory_sector",
                "iso3_country",
                "reporting_entity",
                "gas",
                "emissions_quantity_units",
                "start_time",
            ]
        )
        .apply(sum_not_year)
        .reset_index(name="emissions_quantity")
    )
    return summed_df.rename(columns={"start_time": "year"})


def transform_years_to_columns(df):
    # TODO: MMB TO REMOVE - this is just a temporary workaround
    # df = df.drop_duplicates(subset=["Sector", "ID", "Data source", "Gas", "Unit", "year"], keep='first')
    transformed_df = df.pivot(
        index=["Sector", "ID", "Data source", "Gas", "Unit"],
        columns="year",
        values="emissions_quantity",
    ).reset_index()
    missing_years = [cy for cy in COMP_YEARS if cy not in transformed_df.columns]

    transformed_df = transformed_df.reindex(
        columns=transformed_df.columns.tolist() + missing_years
    )
    return transformed_df[COL_ORDER]


def transform_years_to_rows(df):
    transformed_df = pd.melt(
        df,
        id_vars=["Sector", "ID", "Data source", "Gas", "Unit"],
        value_vars=COMP_YEARS,
        var_name="year",
        value_name="emissions_quantity",
    )

    return transformed_df


def parse_and_format_query_data(
    my_df, years_to_columns=True, rename_columns=True, times_to_years=True
):
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


def parse_and_format_data_to_insert(
    my_df, do_melt=True, years_to_times=True, rename_columns=True, add_carbon_eq=True
):
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

    # This is deprecated sine we are storing `co2e_20yr` and `co2e_100yr` in the gas column.
    # if add_carbon_eq:
    #     # Make sure the carbon equivalency is in there for gap filled data
    #     my_df = add_carbon_eq_column(my_df)

    return my_df

def assign_gas_name(df, df2):
    # Create a mapping of gas to a unique gas_name if it maps to one unique value
    gas_mapping = df2.groupby("Gas")["gas_name"].nunique()
    unique_gases = gas_mapping[gas_mapping == 1].index
    gas_to_name = df2[df2["Gas"].isin(unique_gases)].set_index("Gas")["gas_name"].to_dict()
    
    # Assign gas_name based on mapping where gas has a unique value
    df["gas_name"] = df["Gas"].map(gas_to_name)
    
    # Handle CH4 cases
    ch4_mask = df["Gas"].eq("ch4")
    non_fossil_mask = df["Sector"].isin(NON_FOSSIL_SECTORS)
    
    df.loc[ch4_mask & non_fossil_mask, "gas_name"] = "non_fossil_methane"
    df.loc[ch4_mask & ~non_fossil_mask, "gas_name"] = "fossil_methane"
    
    return df

def generate_carbon_equivalencies(dh, df, co2e_to_compute=100):
    # col_to_mult can be either 20 or 100 to compute the 20 or 100 year carbon equivalency for the data
    ghgs = dh.get_ghgs().rename(columns={"gas": "Gas"})
    df = assign_gas_name(df.copy(), ghgs)
    # Merge on the Gas value
    merged_df = pd.merge(df, ghgs, on="gas_name")

    col_to_mult = "co2e_" + str(co2e_to_compute)

    # Multiply by the appropriate co2e value
    merged_df[COMP_YEARS] = (
        merged_df[COMP_YEARS]
        .astype(float)
        .multiply(merged_df[col_to_mult].astype(float), axis=0)
    )
    # Sum up the multiplied numbers and return them
    co2e_vals = merged_df.groupby(
        ["ID", "Sector", "Data source", "Unit"], as_index=False
    )[COMP_YEARS].sum()
    co2e_vals["Gas"] = col_to_mult + "yr"

    return co2e_vals


def add_all_gas_rows(df, SECTORS):
    # Drop rows with nas for country id
    df.dropna(subset=["ID"], inplace=True)
    # Set the individual indexes
    gases = ["co2", "n2o", "ch4", "co2e_20yr", "co2e_100yr"]
    multi_ind_col_list = ["ID", "Sector", "Gas"]
    # countries = df['Country'].unique()
    ids = df["ID"].unique()
    sectors = np.unique(SECTORS)

    # Reindex to ensure every country, gas, sector, and year combination has a row
    multi_index_all_years = pd.MultiIndex.from_product(
        [ids, sectors, gases], names=multi_ind_col_list
    )

    print(f"Number of country-sector-gas combinations: {len(multi_index_all_years)}")

    df = (
        df.set_index(multi_ind_col_list)
        .reindex(multi_index_all_years, fill_value=0)
        .reset_index()
    )
    df["Data source"] = "climate-trace"
    df["Unit"] = "tonnes"
    # df['Country'] = [get_country_name(name) for name in df['ID']]

    return df


def add_carbon_eq_column(df):
    cem_gases = {"co2e_20": 0, "co2e_100": 1}
    cem_str = ["20-year", "100-year"]

    # Build list of carbon equivalence strings
    cem_list = [
        cem_str[cem_gases[g]] if g in cem_gases.keys() else "" for g in df["gas"]
    ]

    # Change co2eq back
    df.replace({"Gas": {"co2e_20": "co2e", "co2e_100": "co2e"}})
    df["carbon_equivalency_method"] = cem_list

    return df


def assemble_data(gap_filled_df, co2e_20_df, co2e_100_df, SECTORS):
    # Join together the dataframes
    joined_df = pd.concat([co2e_20_df, co2e_100_df, gap_filled_df])
    # Need to add in placeholders for all gases
    final_df = add_all_gas_rows(joined_df, SECTORS)

    return final_df


def get_all_edgar_data(data_handler, get_projected=False):
    expected_last_edgar_value_year = 2022
    columns_to_check = np.where(
        np.array(COMP_YEARS) > expected_last_edgar_value_year, COMP_YEARS, -1
    )
    # This function gets the edgar and projected edgar data from the database and returns a concatenated data frame
    edgar_data = data_handler.load_data("edgar", gas=None, years_to_columns=True)
    # TODO: MMB TO remove this and the parameter, this is just a workaround
    if not get_projected:
        return edgar_data
    projected_edgar_data = data_handler.load_data(
        "edgar-projected", years_to_columns=True
    )

    return pd.concat([edgar_data, projected_edgar_data])


def get_all_faostat_data(data_handler, get_projected=False):
    expected_last_faostat_value_year = 2022
    columns_to_check = np.where(
        np.array(COMP_YEARS) > expected_last_faostat_value_year, COMP_YEARS, -1
    )
    # This function gets the edgar and projected edgar data from the database and returns a concatenated data frame
    faostat_data = data_handler.load_data("faostat", gas=None, years_to_columns=True)
    # TODO: MMB TO remove this and the parameter, this is just a workaround
    if not get_projected:
        return faostat_data
    projected_faostat_data = data_handler.load_data(
        "faostat-projected", years_to_columns=True
    )

    return pd.concat([faostat_data, projected_faostat_data])


def get_all_ceds_data(data_handler, get_projected=False):
    expected_last_ceds_value_year = 2022
    columns_to_check = np.where(
        np.array(COMP_YEARS) > expected_last_ceds_value_year, COMP_YEARS, -1
    )
    # This function gets the ceds and projected ceds data from the database and returns a concatenated data frame
    ceds_data = data_handler.load_data("ceds", gas=None, years_to_columns=True)
    # TODO: MMB TO remove this and the parameter, this is just a workaround
    if not get_projected:
        return ceds_data
    projected_ceds_data = data_handler.load_data(
        "ceds-projected", years_to_columns=True
    )

    return pd.concat([ceds_data, projected_ceds_data])


def get_all_ceds_derived_data(data_handler, get_projected=False):
    expected_last_ceds_value_year = 2022
    columns_to_check = np.where(
        np.array(COMP_YEARS) > expected_last_ceds_value_year, COMP_YEARS, -1
    )
    # This function gets the ceds and projected ceds data from the database and returns a concatenated data frame
    ceds_data = data_handler.load_data("ceds-derived", gas=None, years_to_columns=True)
    # TODO: MMB TO remove this and the parameter, this is just a workaround
    if not get_projected:
        return ceds_data
    projected_ceds_data = data_handler.load_data(
        "ceds-derived-projected", years_to_columns=True
    )

    return pd.concat([ceds_data, projected_ceds_data])
