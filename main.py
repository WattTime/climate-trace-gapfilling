import argparse

import pandas as pd

from gap_filling.data_handler import DataHandler
from gap_filling.edgar_projection import ProjectEdgarData
from gap_filling.fill_gaps import fill_all_sector_gaps, prepare_df
from gap_filling.utils import (parse_and_format_data_to_insert, get_all_edgar_data, get_all_faostat_data, generate_carbon_equivalencies,
                               assemble_data)


def process_all(args):
    ############################
    # Get the data
    ############################
    # Init the Data Handler
    dh = DataHandler(args.params_file)

    ############################
    # Project Data
    ############################
    # Project the Edgar Data
    proj_edgar = ProjectEdgarData(db_params_file_path=args.params_file)
    proj_edgar.load()
    proj_edgar.clean()
    df_projections = proj_edgar.project()
    df_projections_final = proj_edgar.prepare_to_write(df_projections)
    # Write results to the DB
    # dh.write_data(data_to_insert=df_projections_final, rows_type='edgar')

    ############################
    # Fill Gaps
    ############################
    # Get the newly projected edgar data from db
    edgar_data = get_all_edgar_data(dh)
    # Get the CT data from db
    ct_data = dh.load_data("climate-trace", years_to_columns=True)
    # Get the FAOSTAT data from db
    faostat_data = get_all_faostat_data(dh)

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
    # Write results to the DB
    dh.write_data(data_to_insert, rows_type='climate-trace')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Code to fill gaps in the Climate-Trace data using EDGAR')
    parser.add_argument('-p', '--params_file', dest='params_file', type=str,
                        help='location of db connection params', default='params.json')
    args = parser.parse_args()
    process_all(args)
