import numpy as np

from gap_filling.constants import COMP_YEARS, COL_ORDER, GAP_EQUATIONS, GF_SOURCE_DATA_COLUMNS, get_country_name


def prepare_df(concat_df):
    concat_df = concat_df[concat_df['Data source'].isin(GF_SOURCE_DATA_COLUMNS)]
    concat_df = concat_df[COL_ORDER]

    return concat_df


def data_cleaning(sectors_gap_filled):
    # Check for values < -2 to set them to nan; small negative values can just be zero
    filled_vals = sectors_gap_filled[COMP_YEARS].to_numpy(dtype=float)
    filled_vals[np.where(filled_vals < -2)] = None
    filled_vals[(filled_vals < 0) & (filled_vals > -2)] = 0
    sectors_gap_filled[COMP_YEARS] = filled_vals
    return sectors_gap_filled


def fill_all_sector_gaps(input_df, ge=None):
    if ge is None:
        GE = GAP_EQUATIONS
    else:
        GE = ge

    GE = GE[GE['inventory'].isin(GF_SOURCE_DATA_COLUMNS)]
    GE = GE.rename(columns={"sub_inventory": "Data source", "subinv_units": "Sector", "sub-sector": "to_be_gap_filled"})\
        .drop(columns="inventory")
    input_df = input_df.replace('edgar-projected', 'edgar')
    input_df = input_df.replace('faostat-projected', 'faostat')

    # Merge in the values for the appropriate sector rows (including the sector to be gap filled)
    df_merge = input_df.merge(GE, how='right')
    df_merge = df_merge.dropna(axis=0, subset='Unit')

    # df_merge = df_merge.dropna(axis=0, subset=['Unit'])
    if len(df_merge["Unit"].unique()) != 1:
        raise Exception("Cannot currently operate on values of different units!")

    # Multiply by the sign in the gap equations
    df_merge[COMP_YEARS] = df_merge[COMP_YEARS].multiply(df_merge['values'], axis=0)

    # For each (Country, Gas, and sector to be gap filled) group, sum up all the corresponding values per year
    sectors_gap_filled = df_merge.groupby(["ID", "Gas", "to_be_gap_filled", "Unit"], as_index=False)[COMP_YEARS].sum()

    # Add in removed columns and rename others
    sectors_gap_filled.rename(columns={"to_be_gap_filled": "Sector"}, inplace=True)
    sectors_gap_filled["Data source"] = "climate-trace"
    sectors_gap_filled["Country"] = [get_country_name(name) for name in sectors_gap_filled["ID"]]
    new_ct_entries = data_cleaning(sectors_gap_filled)

    return new_ct_entries[COL_ORDER]
