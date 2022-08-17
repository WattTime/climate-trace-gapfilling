import numpy as np
import pandas as pd

from gap_filling.edgar_projection import ProjectData
from gap_filling.constants import DatabaseColumns as DbColumns

CO2 = "co2"
N2O = "n2o"
ECUADOR = "Ecuador"
USA = "USA"
ONE_A = "1.A"
ONE_B = "1.B"


def test_regression():
    proj_edgar = ProjectData(db_params_file_path="no_needed")
    proj_edgar.data = pd.read_csv("tests/data/sample_fake_data.csv")
    proj_edgar.sectors_to_use_regression = ["1.B"]

    proj_edgar.clean()
    df_project = proj_edgar.project()
    # Check a few specific values
    assert all(df_project.loc[(df_project[DbColumns.SECTOR] == ONE_B) &
                              (df_project[DbColumns.GAS] == CO2) &
                              (df_project[DbColumns.COUNTRY] == ECUADOR), DbColumns.VALUE] == np.array([7, 8, 9]))

    assert all(df_project.loc[(df_project[DbColumns.SECTOR] == ONE_B) &
                              (df_project[DbColumns.GAS] == N2O) &
                              (df_project[DbColumns.COUNTRY] == ECUADOR), DbColumns.VALUE] == np.array([14, 16, 18]))


def test_baseline_forward_fill():
    proj_edgar = ProjectData(db_params_file_path="no_needed")
    proj_edgar.data = pd.read_csv("tests/data/sample_fake_data.csv")
    proj_edgar.sectors_to_use_regression = ["1.B"]

    proj_edgar.clean()
    df_project = proj_edgar.project()

    # Check a few specific values
    assert all(df_project.loc[(df_project[DbColumns.SECTOR] == ONE_A) &
                              (df_project[DbColumns.GAS] == CO2) &
                              (df_project[DbColumns.COUNTRY] == ECUADOR), DbColumns.VALUE] == 4)
    assert all(df_project.loc[(df_project[DbColumns.SECTOR] == ONE_A) &
                              (df_project[DbColumns.GAS] == N2O) &
                              (df_project[DbColumns.COUNTRY] == USA), DbColumns.VALUE] == 4.37)


def test_data_cleaning():
    proj_edgar = ProjectData(db_params_file_path="no_needed")
    proj_edgar.data = pd.read_csv("tests/data/sample_fake_data.csv")
    proj_edgar.sectors_to_use_regression = ["1.B"]

    proj_edgar.clean()

    # Expected size of data after expanding for every country, sector, gas combination + 6 years of data
    assert proj_edgar.data.shape[0] == 48
    # Double check the years included
    assert proj_edgar.data[DbColumns.YEAR].min() == proj_edgar.start_training_year
    assert proj_edgar.data[DbColumns.YEAR].max() == proj_edgar.end_training_year
    # Check that a missing year was filled with nan
    assert np.isnan(proj_edgar.data.loc[(proj_edgar.data[DbColumns.SECTOR] == ONE_A) &
                                        (proj_edgar.data[DbColumns.GAS] == CO2) &
                                        (proj_edgar.data[DbColumns.COUNTRY] == ECUADOR) &
                                        (proj_edgar.data[DbColumns.YEAR] == 2013), DbColumns.VALUE].item())
    # Check that a certain value remained unchanged
    assert proj_edgar.data.loc[(proj_edgar.data[DbColumns.SECTOR] == ONE_A) &
                               (proj_edgar.data[DbColumns.GAS] == N2O) &
                               (proj_edgar.data[DbColumns.COUNTRY] == USA) &
                               (proj_edgar.data[DbColumns.YEAR] == 2018), DbColumns.VALUE].item() == 4.37
    assert proj_edgar.full_df.shape[0] == 18
