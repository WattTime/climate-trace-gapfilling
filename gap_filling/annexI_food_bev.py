import scipy.stats as st
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from gap_filling.data_handler import DataHandler
from gap_filling.utils import get_all_ceds_data


def quantify_country_scaling_factor(df, sec1, sec2, country, show_plots=True):
    gas = 'CO2'

    sel = (df['Sector'].isin([sec1, sec2])) & (df["Party"] == country)
    subset_df = df.loc[sel,:]
    sec1_vals = subset_df.loc[subset_df.Sector==sec1, np.arange(1990,2022).astype(str)].values.flatten().astype(float)
    sec2_vals = subset_df.loc[subset_df.Sector==sec2, np.arange(1990,2022).astype(str)].values.flatten().astype(float)

    nan_mask = np.argwhere((~np.isnan((sec1_vals))) & (~np.isnan(sec2_vals))).flatten()
    
    if show_plots:
        plt.scatter(sec1_vals[nan_mask], sec2_vals[nan_mask])

    #Conduct regression
    reg = st.linregress(sec1_vals[nan_mask], sec2_vals[nan_mask])

    #If negative or pvalue<0.1: take mean ratio OR fewer than 3 points, otherwise take slope
    if reg.slope > 0 and reg.pvalue < 0.1 and len(sec1_vals[nan_mask]) > 4:
        factor = reg.slope
        method = 'slope'
    else:
        factor = np.nanmean(sec2_vals[nan_mask] / sec1_vals[nan_mask])
        method = 'mean_ratio'

    if show_plots:
        plt.plot(np.linspace(np.nanmin(sec1_vals) * 0.9, np.nanmax(sec1_vals) * 1.1, 10), 
                np.linspace(np.nanmin(sec1_vals) * 0.9, np.nanmax(sec1_vals) * 1.1, 10) * reg.slope + reg.intercept,
                '--')


        plt.xlabel(f'{sec1} kt/yr')
        plt.ylabel(f'{sec2} kt/yr')

        plt.title(f'{gas.upper()} emissions')
        plt.show()

    return factor, method


def calculate_scaling_factors(df):

    sec1 = '1.A.2.e  Food Processing, Beverages and Tobacco'
    sec2 = '2.H.2  Food and Beverages Industry'

    scaling_factor_dict = {}
    countries = ["Australia", "Ireland", "Japan", "Latvia", "Netherlands", "Norway"]
    for country in countries:
        scaling_factor_dict[country] = quantify_country_scaling_factor(df, sec1, sec2, country, show_plots=False)
    
    iso3_dict = {
        "Australia": "AUS",
        "Ireland": "IRL",
        "Japan": "JPN",
        "Latvia": "LVA",
        "Netherlands": "NLD",
        "Norway": "NOR"
    }

    #First get country code for each relevant country
    country_code = [iso3_dict[c] for c in countries]

    sf_df = pd.DataFrame({
        "ID": country_code,
        "2H2_per_1A2e": [scaling_factor_dict[c][0] for c in countries]
    })
    sf_df.to_csv('gap_filling/data/2H2_per_1A2e_AnnexI_scaling_factors.csv')
    
    return sf_df


def main():
    new_db = False
    # get connection
    get_ceds_conn = DataHandler(new_db)
    #Get CEDS data
    ceds_data = get_all_ceds_data(get_ceds_conn)
    #Convert column names to strings for processing
    ceds_data.columns = ceds_data.columns.astype(str)
    #Now ensure all ceds and edgar values are numpy floats
    for yr in range(2015,2023):
        ceds_data[str(yr)] = ceds_data[str(yr)].astype(float)

    #Get AnnexI data
    df = pd.read_csv(f"./gap_filling/data/CO2_annual_1A2e_2H2_emissions_in_kt.csv")
    df.replace(to_replace=["NE", "NO", "IE", "NA", "NO,IE"], value=np.nan, inplace=True)
    df.Sector = df.Sector.astype(str).str.strip()
    df = df[df["Party"] != 'European Union (Convention)']
    df.dropna().reset_index(drop=True, inplace=True)

    #Calculate scaling factors
    sf_df = calculate_scaling_factors(df)

    #Next, get subset of ceds data with those countries and 1.A.2.e sector
    sel = (ceds_data["ID"].isin(sf_df["ID"].values)) & (ceds_data["Sector"] == "1A2e_Ind-Comb-Food-tobacco") & \
        (ceds_data["Gas"] == "co2")

    #Scale 1A2e data by country-specific scaling factors and send back the dataframe
    sector_ceds_df = ceds_data[sel]
    for col in np.arange(2015,2023).astype(str):
        for iso in sf_df["ID"].values:
            sel_new = (sel) & (ceds_data["ID"] == iso)
            sector_ceds_df.loc[sel_new, col] *= sf_df.loc[sf_df["ID"] == iso, "2H2_per_1A2e"].values[0]
    
    sector_ceds_df.loc[:, "Sector"] = "2.H.2-food-beverage-and-tobacco-direct"

    return sector_ceds_df.copy()