from typing import List
import time

import numpy as np
import pandas as pd
from datetime import datetime
from sklearn.linear_model import LinearRegression

from gap_filling.data_handler import DataHandler
from gap_filling.constants import DatabaseColumns as DbColumns
from gap_filling.utils import from_years_to_start_and_end_times


class ProjectData:
    def __init__(self, db_params_file_path, source: str = "edgar"):
        self.source = source
        self.sectors_to_use_regression = None
        if self.source == "edgar":
            # Sectors better suited for regression selected after thorough analysis of edgar data
            self.sectors_to_use_regression = ["1.A.1.a Main Activity Electricity and Heat Production",
                                              "1.A.2 Manufacturing Industries and Construction",
                                              "2.A.3 Glass Production",
                                              "4.D Wastewater Treatment and Discharge"]
            self.do_regression = True  # True if sectors_to_use_regression is not None
            # Last available year of data
            self.end_training_year: int = 2021
            # Number of years to project forward
            self.years_forward = np.array([1])
            self.predicted_years = np.array([2022])

        elif self.source == "faostat":
            self.do_regression = False  # Must be changed if any faostat sectors switch to a different method
            # Last available year of data
            self.end_training_year: int = 2020
            # Number of years to project forward
            self.years_forward = np.array([1, 2])
            self.predicted_years = np.array([2021, 2022])

        else:
            raise ValueError("Only 'edgar' and 'faostat' acceptable as inputs.")

        self.expected_emission_quantity_units = "tonnes"
        self.db_params_file = db_params_file_path

        self.group_list: List[str] = [DbColumns.ID, DbColumns.SECTOR, DbColumns.GAS]
        self.group_list_with_year: List[str] = self.group_list + [DbColumns.YEAR]

        # Regression training window (6 years total)
        self.regression_training_window = 6
        self.start_training_year: int = self.end_training_year - self.regression_training_window + 1

        # Number of data points needed to perform regression
        self.min_years_for_regression = 4

        # Preallocate empty data frame because I hate warning messages
        self.data = pd.DataFrame()

    def load(self):
        dh = DataHandler(new_db=False)
        self.data = dh.load_data(self.source, rename_columns=False)

    def clean(self):
        # Before anything else, confirm the unit
        assert all(self.data[DbColumns.UNIT] == self.expected_emission_quantity_units), "Units must all be tonnes."

        # Filter down to the specific training years
        training_interval_mask = (self.data[DbColumns.YEAR] >= self.start_training_year) & (
                self.data[DbColumns.YEAR] <= self.end_training_year)
        self.data = self.data.loc[training_interval_mask].copy()

        # Sort the data
        self.data.sort_values(by=self.group_list_with_year, inplace=True)
        self.data.reset_index(drop=True, inplace=True)  # Reset the index after sort

        # Add data counts (count of number of years with non nan data for each country id / sector / gas)
        self._add_data_counts()

    def _add_data_counts(self):
        """
        Count the number of data points in each x_ijk (unique country ID i, sector j, and gas k combination). If the number
        of counts is equal to the length of the date range, then that means the particular x_ijk point has data for
        every year in the date range.

        """
        df_count = self.data.groupby(self.group_list).count()
        self.data = self.data.merge(df_count.rename(
            columns={DbColumns.VALUE: DbColumns.COUNT})[[DbColumns.COUNT]].reset_index())

    def project(self):

        if self.do_regression:
            # Apply regression to appropriate sectors for the dataframe with no missing data
            regr_mask = (self.data[DbColumns.SECTOR].isin(self.sectors_to_use_regression) &
                         (self.data[DbColumns.COUNT] >= self.min_years_for_regression))
            df_regression_full = self._apply_regression(self.data[regr_mask].copy())

            # Apply forward fill to data not used for regression and where there is at least one data point
            ffill_mask = (~regr_mask & (self.data[DbColumns.COUNT] > 0))
            df_baseline_results = self._apply_baseline_forward_fill(self.data[ffill_mask].copy())

        else:
            # Apply forward fill to all data with at least one data point
            ffill_mask = (self.data[DbColumns.COUNT] > 0)
            df_baseline_results = self._apply_baseline_forward_fill(self.data[ffill_mask].copy())

        # Drop nans -- no need to write nans.
        df_baseline_results.dropna(subset=[DbColumns.VALUE], inplace=True)

        if self.do_regression:
            df_projections = pd.concat([df_regression_full, df_baseline_results], ignore_index=True)
        else:
            df_projections = df_baseline_results

        return df_projections

    def _apply_baseline_forward_fill(self, df: pd.DataFrame):
        """
        Take the latest available year of data (such as 2018) and populate rows for projection years with those values
        :param df:
        :return:
        """
        # First, filter to the final year of training data. We will need to just project this forward in time.
        df_baseline = df[df[DbColumns.YEAR] == self.end_training_year].copy()

        # Fill each future year column with latest year's data
        for predict_year in self.predicted_years:
            df_baseline.loc[:, predict_year] = df_baseline.loc[:, DbColumns.VALUE]

        # Drop columns with training data info
        df_baseline.drop(columns=[DbColumns.YEAR, DbColumns.VALUE], inplace=True)

        # Melt to make columns into a single column
        df_baseline_predictions_only = df_baseline.melt(id_vars=self.group_list, value_vars=self.predicted_years,
                                                        var_name=DbColumns.YEAR, value_name=DbColumns.VALUE)
        df_baseline_predictions_only[DbColumns.YEAR] = df_baseline_predictions_only[DbColumns.YEAR].astype(int)
        df_baseline_predictions_only[DbColumns.PROJECTION_METHOD] = "forward_fill"

        return df_baseline_predictions_only

    def _apply_regression(self, df: pd.DataFrame):
        """
        For each sector, country ID, and gas, train a regression model and use that regression model to predict the next 3
         years.
        Cannot contain any missing data during training years
        """
        df_training_data = df.copy()

        # Pre-allocate columns to fill w regression results. Each columns will be the year that the prediction is for
        for predict_year in self.predicted_years:
            df_training_data.loc[:, predict_year] = np.nan

        # Group by country ID, sector, and gas and do projection
        reg_results = df_training_data.groupby(self.group_list).apply(self._func_apply)

        # Drop columns with training data info
        reg_results.drop(columns=[DbColumns.YEAR, DbColumns.VALUE], inplace=True)
        # Drop duplicates to get one row per group with prediction columns
        reg_results.drop_duplicates(inplace=True)
        df_reg_predictions_only = reg_results.melt(id_vars=self.group_list, value_vars=self.predicted_years,
                                                   var_name=DbColumns.YEAR, value_name=DbColumns.VALUE)
        df_reg_predictions_only[DbColumns.YEAR] = df_reg_predictions_only[DbColumns.YEAR].astype(int)

        # Make sure we have the correct years left
        assert np.all(df_reg_predictions_only[DbColumns.YEAR].unique() == self.predicted_years)

        df_reg_predictions_only[DbColumns.PROJECTION_METHOD] = "linear_regression"
        return df_reg_predictions_only

    def _func_apply(self, df):
        for n_year_ahead in self.years_forward:
            testing_year = np.array([[self.end_training_year + n_year_ahead]])
            df.loc[:, self.end_training_year + n_year_ahead] = self.model(
                df, testing_year)
        return df

    @staticmethod
    def model(df, x_test):
        y = df.loc[~df[DbColumns.VALUE].isna(), [DbColumns.VALUE]].values
        X = df.loc[~df[DbColumns.VALUE].isna(), [DbColumns.YEAR]].values
        return np.squeeze(LinearRegression().fit(X, y).predict(x_test))

    def prepare_to_write(self, df_projections: pd.DataFrame):
        # Add a few more columns to wrap up data projection
        df_projections[DbColumns.UNIT] = self.expected_emission_quantity_units
        df_projections[DbColumns.DATA_SOURCE] = f"{self.source}-projected"
        df_projections[DbColumns.CREATED] = datetime.now().isoformat()
        df_projections = from_years_to_start_and_end_times(df_projections)

        return df_projections


if __name__ == "__main__":
    proj_edgar = ProjectData(db_params_file_path="params.json", source="edgar")
    proj_edgar.load()
    proj_edgar.clean()
    df_projections = proj_edgar.project()
    df_projections_final = proj_edgar.prepare_to_write(df_projections)
