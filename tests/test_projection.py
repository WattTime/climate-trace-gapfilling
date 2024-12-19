import numpy as np
import pandas as pd

from gap_filling.edgar_projection import ProjectData
from gap_filling.constants import DatabaseColumns as DbColumns

CO2 = "co2"
N2O = "n2o"
ECUADOR = "ECD"  # No idea if this is the right code, but its a placeholder
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
    assert all(
        df_project.loc[
            (df_project[DbColumns.SECTOR] == ONE_B)
            & (df_project[DbColumns.GAS] == CO2)
            & (df_project[DbColumns.ID] == ECUADOR),
            DbColumns.VALUE,
        ]
        == np.array([7, 8, 9])
    )

    assert all(
        df_project.loc[
            (df_project[DbColumns.SECTOR] == ONE_B)
            & (df_project[DbColumns.GAS] == N2O)
            & (df_project[DbColumns.ID] == ECUADOR),
            DbColumns.VALUE,
        ]
        == np.array([14, 16, 18])
    )


def test_baseline_forward_fill():
    proj_edgar = ProjectData(db_params_file_path="no_needed")
    proj_edgar.data = pd.read_csv("tests/data/sample_fake_data.csv")
    proj_edgar.sectors_to_use_regression = ["1.B"]

    proj_edgar.clean()
    df_project = proj_edgar.project()

    # Check a few specific values
    assert all(
        df_project.loc[
            (df_project[DbColumns.SECTOR] == ONE_A)
            & (df_project[DbColumns.GAS] == CO2)
            & (df_project[DbColumns.ID] == ECUADOR),
            DbColumns.VALUE,
        ]
        == 4
    )
    assert all(
        df_project.loc[
            (df_project[DbColumns.SECTOR] == ONE_A)
            & (df_project[DbColumns.GAS] == N2O)
            & (df_project[DbColumns.ID] == USA),
            DbColumns.VALUE,
        ]
        == 4.37
    )
