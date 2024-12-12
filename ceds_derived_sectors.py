"""
Note, for more complete understanding of these functions
see the ceds_derived_sectors.ipynb notebook
"""

import argparse
import datetime

import pandas as pd
import numpy as np

from gap_filling.data_handler import DataHandler
from gap_filling.edgar_projection import ProjectData
from gap_filling.utils import (
    parse_and_format_data_to_insert,
    get_all_edgar_data,
    get_all_ceds_data,
)
from gap_filling import annexI_food_bev


def initialize_data():
    """
    Initializes connection to climatetrace database
    for edgar, climate-trace, and ceds data and
    pulls relevant data using existing functions

    Parameters
    ----------
    None

    Returns
    -------
    edgar_data: pandas df
    contains all EDGAR data in country_emissions_staging
    table

    ceds_data: pandas df
    '' for CEDS

    ct_data: pandas df
    '' for climate-trace
    """

    new_db = False

    # get connections
    getedgar_conn = DataHandler()
    getct_conn = DataHandler()
    get_ceds_conn = DataHandler()

    edgar_data = get_all_edgar_data(getedgar_conn, get_projected=True)
    # Combine projected and existing data
    edgar_data = edgar_data.groupby(["ID", "Sector", "Gas"]).sum().reset_index()
    edgar_data["Data source"] = "edgar"
    edgar_data["Unit"] = "tonnes"
    # Convert column names to strings for processing
    edgar_data.columns = edgar_data.columns.astype(str)

    # Get CEDS data
    ceds_data = get_all_ceds_data(get_ceds_conn, get_projected=True)
    # Combine projected and existing data
    ceds_data = ceds_data.groupby(["ID", "Sector", "Gas"]).sum().reset_index()
    ceds_data["Data source"] = "ceds"
    ceds_data["Unit"] = "tonnes"
    # Convert column names to strings for processing
    ceds_data.columns = ceds_data.columns.astype(str)

    # Now ensure all ceds and edgar values are numpy floats
    for yr in range(2015, 2025):
        edgar_data[str(yr)] = edgar_data[str(yr)].astype(float)
        ceds_data[str(yr)] = ceds_data[str(yr)].astype(float)

    # Get the CT data from db just for reference
    ct_data = getct_conn.load_data("climate-trace", years_to_columns=True)
    ct_data.columns = ct_data.columns.astype(str)

    return edgar_data, ceds_data, ct_data


def sector_fractional_contribution(
    inventory_data,
    add_secs,
    inventory_data_contributing_sec,
    inventory_data_to_take_fraction_of,
    new_sector_name,
):
    """Calculates the fractional contribution of one
    subsector to the sum of a list of sectors. Then
    applies that fractional contribution to a final
    sector.

    Example: calculating the contribution of Glass to
    to sum of Glass + Lime + Cement + other-minerals
    and applying that fraction to the combustion from
    those same sectors.

    Parameters
    ----------
    inventory_data: pandas df
    Country-level annual emissions by sector.
    Must include the sectors to be summed. Could be
    edgar, ceds, faostat, climate-trace

    add_secs: list of strings
    list of sectors to be summed

    inventory_data_contributing_sec: pandas df
    Should be same inventory as inventory_data
    but is only for the sector(s) that you want
    to calculate the fractional contribution of.

    inventory_data_to_take_fraction_of: pandas df
    Can be from same or other inventory as inventory_data.
    Contains only one sector of which you would like to
    attribute only a fraction to the new sector.

    new_sector_name: str
    name for the new sector which is the fraction of
    inventory_data_to_take_fraction_of contributed by
    inventory_data_contributing_sec sector.

    Returns
    -------
    frac_of_other_sector: pandas df
    inventory containing the new sector, no others.

    """

    inventory_data_sum_add_secs = (
        inventory_data[inventory_data["Sector"].isin(add_secs)]
        .groupby(["ID", "Gas"])
        .sum()
        .reset_index()
    )
    # print('For testing... all add_secs sectors summed for the USA is:', \
    #       inventory_data_sum_add_secs.loc[inventory_data_sum_add_secs.ID == "USA",['Gas', '2020']])

    # Next add Glass-production so we can get its contribution to the total
    inventory_frac_subsector = pd.merge(
        inventory_data_sum_add_secs,
        inventory_data_contributing_sec,
        on=["ID", "Gas"],
        suffixes=("_total", ""),
    )
    for yr in range(2015, 2025):
        inventory_frac_subsector[f"{yr}"] = (
            inventory_frac_subsector[f"{yr}"] / inventory_frac_subsector[f"{yr}_total"]
        )
        inventory_frac_subsector.drop(columns=[f"{yr}_total"], inplace=True)
    inventory_frac_subsector.fillna(0, inplace=True)
    inventory_frac_subsector.drop(columns=["Data source"], inplace=True)

    # print('For testing... the fraction of the specified sector compared to the sum above for the USA is:', \
    #       inventory_frac_subsector.loc[inventory_frac_subsector.ID == "USA",['Gas', '2020']])

    # Now multiply edgar_frac_glass by 1.A.2.f to get the contribution of glass to 1.A.2.f
    frac_of_other_sector = pd.merge(
        inventory_frac_subsector,
        inventory_data_to_take_fraction_of,
        on=["ID", "Gas"],
        suffixes=("_frac", "_total"),
    )
    for yr in range(2015, 2025):
        frac_of_other_sector[f"{yr}"] = (
            frac_of_other_sector[f"{yr}_frac"] * frac_of_other_sector[f"{yr}_total"]
        )
        frac_of_other_sector.drop(columns=[f"{yr}_total", f"{yr}_frac"], inplace=True)
    frac_of_other_sector.fillna(0, inplace=True)
    frac_of_other_sector.drop(
        columns=["Sector_total", "Sector_frac", "Unit_frac", "Unit_total"], inplace=True
    )
    frac_of_other_sector["Sector"] = new_sector_name
    frac_of_other_sector["Unit"] = "tonnes"
    return frac_of_other_sector


def test_combustion_fractions(
    ceds_lime_comb, ceds_glass_comb, ceds_mineral_comb, ceds_cement_comb, ceds_data
):
    """
    Test to ensure that the fractional contributions of
    lime, glass, cement and 'other' minerals add up to
    the total.

    Parameters
    ----------
    ceds_lime_comb: pandas df
    contains estimated combustion emissions from CEDS
    ceds_{glass, mineral, cement}_comb: all as above

    ceds_data: pandas df
    contains all CEDS data from country-emissions-staging table

    Returns
    -------
    boolean:
    True if test is passed.
    """

    # Combine all four dfs:
    dfs_to_combine = [
        ceds_cement_comb,
        ceds_mineral_comb,
        ceds_glass_comb,
        ceds_lime_comb,
    ]
    comb_df = dfs_to_combine[0]
    suffixes = ["_cement", "_other", "_glass", "_lime"]
    for i in range(1, len(dfs_to_combine)):
        comb_df = pd.merge(
            comb_df, dfs_to_combine[i], on=["ID", "Gas"], suffixes=("", suffixes[i])
        )
    comb_df.columns = [
        col + suffixes[0] if col in np.arange(2015, 2025).astype(str).tolist() else col
        for col in comb_df.columns
    ]

    # Now sum them
    for yr in range(2015, 2025):
        comb_df[str(yr)] = (
            comb_df[f"{yr}_cement"]
            + comb_df[f"{yr}_glass"]
            + comb_df[f"{yr}_lime"]
            + comb_df[f"{yr}_other"]
        )

    comb_df = comb_df[np.arange(2015, 2025).astype(str).tolist() + ["Gas"] + ["ID"]]

    # Now merge with 1A2e CEDS data and take difference by year
    ceds_comb_data = ceds_data[
        ceds_data["Sector"] == "1A2f_Ind-Comb-Non-metalic-minerals"
    ]
    merged_df = pd.merge(
        comb_df, ceds_comb_data, on=["ID", "Gas"], suffixes=("_summed", "_total")
    )

    test_to_be_zero = []
    for yr in range(2015, 2025):
        test_to_be_zero.append(
            np.nansum(merged_df[f"{str(yr)}_summed"] - merged_df[f"{str(yr)}_total"])
        )

    return all([x < 1e-5 for x in test_to_be_zero])


def main():
    """
    Overall function to derive new CEDS-related sectors
    to be included in 2024 gap equations. Calculates new
    sectors based on a combination of EDGAR, CEDS, and
    Climate-TRACE country-level data, and writes the
    new sectors (reporting-entity='ceds-derived') to the
    country_emissions_staging table.

    Parameters
    ----------
    None

    Returns
    -------
    None
    """

    # Initialize data:
    edgar_data, ceds_data, ct_data = initialize_data()

    # Now create new CEDS sectors:

    # Need to handle proportions of sectors
    # First get sum of all sectors:
    add_secs = [
        "2.A.3 Glass Production",
        "2.A.1 Cement production",
        "2.A.2 Lime production",
        "2.A.4 Other Process Uses of Carbonates",
    ]

    ceds_lime_comb = sector_fractional_contribution(
        edgar_data,
        add_secs,
        edgar_data[edgar_data["Sector"] == "2.A.2 Lime production"],
        ceds_data[ceds_data["Sector"] == "1A2f_Ind-Comb-Non-metalic-minerals"],
        "lime-combustion",
    )

    ceds_glass_comb = sector_fractional_contribution(
        edgar_data,
        add_secs,
        edgar_data[edgar_data["Sector"] == "2.A.3 Glass Production"],
        ceds_data[ceds_data["Sector"] == "1A2f_Ind-Comb-Non-metalic-minerals"],
        "glass-combustion",
    )

    ceds_mineral_comb = sector_fractional_contribution(
        edgar_data,
        add_secs,
        edgar_data[edgar_data["Sector"] == "2.A.4 Other Process Uses of Carbonates"],
        ceds_data[ceds_data["Sector"] == "1A2f_Ind-Comb-Non-metalic-minerals"],
        "misc-mineral-industry-combustion",
    )

    # Check to make sure everything worked
    # Get cement combustion for completion for test:
    ceds_cement_comb = sector_fractional_contribution(
        edgar_data,
        add_secs,
        edgar_data[edgar_data["Sector"] == "2.A.1 Cement production"],
        ceds_data[ceds_data["Sector"] == "1A2f_Ind-Comb-Non-metalic-minerals"],
        "cement-combustion",
    )

    test_result = test_combustion_fractions(
        ceds_lime_comb, ceds_glass_comb, ceds_mineral_comb, ceds_cement_comb, ceds_data
    )
    if not test_result:
        raise Exception(
            "Testing on fractional apportionment of CEDS combustion emissions failed... check functions..."
        )
    else:
        print(
            "Testing of fractional apportionment of CEDS combustion emissions passed!"
        )

    # Get food-bev direct emissions:
    food_bev_direct = annexI_food_bev.main()

    # Combine all dfs
    ceds_derived_df = pd.concat(
        [ceds_mineral_comb, ceds_lime_comb, ceds_glass_comb, food_bev_direct]
    )
    ceds_derived_df["Unit"] = "tonnes"
    ceds_derived_df["Data source"] = "ceds-derived"
    ceds_derived_df = ceds_derived_df.sort_values(
        by=["ID", "Sector", "Gas"]
    ).reset_index(drop=True)

    # Prepare data for insertion into db
    # Need to convert columns to integers
    def convert_column_to_int(col):
        try:
            return int(col)
        except ValueError:
            return col

    ceds_derived_df.rename(columns=convert_column_to_int, inplace=True)

    data_to_insert = parse_and_format_data_to_insert(ceds_derived_df)
    data_to_insert["created_date"] = datetime.datetime.now().isoformat()

    for proj in [False, True]:

        if proj:
            yrs_to_write = [2023, 2024]
            data_to_insert.loc[
                pd.to_datetime(data_to_insert.start_time).dt.year.isin(yrs_to_write),
                "reporting_entity",
            ] = "ceds-derived-projected"
        else:
            yrs_to_write = [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022]

        # write to db
        write_conn = DataHandler()
        write_conn.insert_with_update(
            data_to_insert[
                pd.to_datetime(data_to_insert.start_time).dt.year.isin(yrs_to_write)
            ],
            "country_emissions_staging",
        )
    return


if __name__ == "__main__":
    main()
