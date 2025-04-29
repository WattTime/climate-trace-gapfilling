import numpy as np
import pandas as pd
from datetime import datetime

from gap_filling.constants import (
    COMP_YEARS,
    COL_ORDER,
    GF_SOURCE_DATA_COLUMNS,
    get_country_name,
)
from gap_filling.data_handler import get_usgs_activity_data


def prepare_df(concat_df):
    concat_df = concat_df[concat_df["Data source"].isin(GF_SOURCE_DATA_COLUMNS)]
    concat_df = concat_df[COL_ORDER]

    return concat_df


def data_cleaning(sectors_gap_filled):
    # Check for values < -2 to set them to nan; small negative values can just be zero
    filled_vals = sectors_gap_filled[COMP_YEARS].to_numpy(dtype=float)
    filled_vals[np.where(filled_vals < -2)] = 0
    filled_vals[(filled_vals < 0) & (filled_vals > -2)] = 0
    sectors_gap_filled[COMP_YEARS] = filled_vals
    return sectors_gap_filled


def fill_all_sector_gaps(input_df, ge=None, output_intermediate_data=False):

    GE = ge

    GE = GE[GE["inventory"].isin(GF_SOURCE_DATA_COLUMNS)]
    GE = GE.rename(
        columns={
            "sub_inventory": "Data source",
            "subinv_units": "Sector",
            "sub-sector": "to_be_gap_filled",
        }
    ).drop(columns="inventory")
    input_df = input_df.replace("edgar-projected", "edgar")
    input_df = input_df.replace("faostat-projected", "faostat")
    input_df = input_df.replace("ceds-projected", "ceds")
    input_df = input_df.replace("ceds-derived-projected", "ceds-derived")

    # Merge in the values for the appropriate sector rows (including the sector to be gap filled)
    df_merge = input_df.merge(GE, how="right")
    # df_merge = df_merge.dropna(axis=0, subset='Unit')

    df_merge = df_merge.dropna(axis=0, subset=["Unit"])
    if len(df_merge["Unit"].unique()) != 1:
        raise Exception("Cannot currently operate on values of different units!")

    # Multiply by the sign in the gap equations
    df_merge[COMP_YEARS] = df_merge[COMP_YEARS].multiply(df_merge["values"], axis=0)

    # For each (Country, Gas, and sector to be gap filled) group, sum up all the corresponding values per year
    # sectors_gap_filled = df_merge.groupby(
    #     ["ID", "Gas", "to_be_gap_filled", "Unit"], as_index=False
    # )[COMP_YEARS].sum()

    sectors_gap_filled = df_merge.groupby(["ID", "Gas", "to_be_gap_filled", "Unit"], as_index=False)\
        [COMP_YEARS].agg(lambda x: np.nan if x.isna().any() else x.sum())

    # Add in removed columns and rename others
    sectors_gap_filled.rename(columns={"to_be_gap_filled": "Sector"}, inplace=True)
    sectors_gap_filled["Data source"] = "climate-trace"
    sectors_gap_filled["Country"] = [
        get_country_name(name) for name in sectors_gap_filled["ID"]
    ]
    sectors_gap_filled["Created"] = datetime.now().isoformat()

    if output_intermediate_data:
        sectors_gap_filled.to_csv('20231020_gap_fill_before_clean.csv')
    new_ct_entries = data_cleaning(sectors_gap_filled)

    return new_ct_entries[COL_ORDER]


def update_based_on_activity(df, summarize_global_ef=False):
    update_df = df.copy()

    #Retrieve USGS activity data
    usgs_df = get_usgs_activity_data()

    # Precompute lime mask and filter data once
    lime_mask = update_df["Sector"] == "lime"

    # Process only for "co2" (extendable to other gases)
    for gas in ["co2", "ch4", "n2o"]:
        gas_mask = lime_mask & (update_df["Gas"] == gas)

        # Merge once outside the loop
        merged_df = update_df.loc[gas_mask].merge(
            usgs_df[["ID"] + COMP_YEARS], on="ID", suffixes=["", "_activity"], how="left"
        )

        # Compute global EF per year
        global_efs = merged_df[[str(yr) for yr in COMP_YEARS]].sum().values / merged_df[[f"{yr}_activity" for yr in COMP_YEARS]].sum().values

        # Compute inferred emissions using global EF
        inferred_emissions = merged_df[[f"{yr}_activity" for yr in COMP_YEARS]].mul(global_efs)

        #Rename columns:
        inferred_emissions = inferred_emissions.rename(columns=
            {f"{yr}_activity": yr for yr in COMP_YEARS}
        )

        # Apply inferred emissions only where original values were zero
        zero_mask = (update_df.loc[gas_mask, COMP_YEARS] == 0).reset_index(drop=True)
        update_df.loc[gas_mask, COMP_YEARS] = np.where(
            zero_mask & inferred_emissions.notna(),
            inferred_emissions,
            update_df.loc[gas_mask, COMP_YEARS]
        )

        if summarize_global_ef:
            # Summarize EF values
            print(f"{gas} global lime EF summary: {global_efs}")
    return update_df
