import argparse
import datetime

import pandas as pd

from gap_filling.data_handler import DataHandler
from gap_filling.edgar_projection import ProjectData
from gap_filling.fill_gaps import fill_all_sector_gaps, prepare_df
from gap_filling.utils import (parse_and_format_data_to_insert, get_all_edgar_data, get_all_faostat_data, generate_carbon_equivalencies,
                               assemble_data)


def process_all(args):
    ############################
    # Get the data
    ############################
    # Init the Data Handler


    ############################
    # Project Data
    ############################
    # Project the Edgar Data
    ############################
    # proj_edgar = ProjectData(db_params_file_path=args.params_file, source="edgar")
    # proj_edgar.load()
    # proj_edgar.clean()
    # df_projections = proj_edgar.project()
    # df_projections_final = proj_edgar.prepare_to_write(df_projections)
    # # Write results to the DB
    # df_projections_final = df_projections_final.drop(columns='measurement_method_doi_or_url')
    # dh.insert_with_update(df_projections_final, 'country_emissions')

    ############################
    # Project the FAOSTAT Data
    ############################

    # proj_edgar = ProjectData(db_params_file_path=args.params_file, source="faostat")
    # proj_edgar.load()
    # proj_edgar.clean()
    # df_projections = proj_edgar.project()
    # df_projections_final = proj_edgar.prepare_to_write(df_projections)
    # # Write results to the DB
    # df_projections_final = df_projections_final.drop(columns='measurement_method_doi_or_url')
    # dh.insert_with_update(df_projections_final, 'country_emissions')
    ############################
    # Fill Gaps
    ############################
    # Get the newly projected edgar data from db
    dh = DataHandler(new_db=False)
    edgar_data = get_all_edgar_data(dh, get_projected=True)
    # Get the FAOSTAT data from db
    faostat_data = get_all_faostat_data(dh, get_projected=True)

    # Get the CT data from db
    dh = DataHandler(new_db=False)
    ct_data = dh.load_data("climate-trace", years_to_columns=True)


    # Gap fill on projected data
    concat_df = pd.concat([edgar_data, faostat_data, ct_data])
    df = prepare_df(concat_df)
    gap_filled_data = fill_all_sector_gaps(df)

    # Generate the co2e data
    co2e_20_data = generate_carbon_equivalencies(dh, gap_filled_data, co2e_to_compute=20)
    co2e_100_data = generate_carbon_equivalencies(dh, gap_filled_data, co2e_to_compute=100)

    # This function generates placeholders for every sector, country, and gas combination and merges the dataframes
    assembled_df = assemble_data(gap_filled_data, co2e_20_data, co2e_100_data)

    # These data need to undergo a transformation before we can insert them into the db
    data_to_insert = parse_and_format_data_to_insert(assembled_df)
    data_to_insert['created_date'] = datetime.datetime.now().isoformat()
    # Write results to the DB
    dh = DataHandler(new_db=False)
    dh.insert_with_update(data_to_insert, 'country_emissions')



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Code to fill gaps in the Climate-Trace data using EDGAR')
    parser.add_argument('-p', '--params_file', dest='params_file', type=str,
                        help='location of db connection params', default='params.json')
    args = parser.parse_args()
    process_all(args)
