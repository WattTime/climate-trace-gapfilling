from typing import List

import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression

from gap_filling.data_handler import DataHandler
from gap_filling.constants import DatabaseColumns as DbColumns
from gap_filling.utils import from_years_to_start_and_end_times, add_iso_value


class ProjectEdgarData:
    def __init__(self, db_params_file_path):
        self.sectors_to_use_regression = ["1.A.1.a Main Activity Electricity and Heat Production",
                                          "1.A.2 Manufacturing Industries and Construction",
                                          "2.A.3 Glass Production",
                                          "4.D Wastewater Treatment and Discharge"]

        self.source = "faostat"
        self.expected_emission_quantity_units = "tonnes"
        self.db_params_file = db_params_file_path

        self.group_list: List[str] = [DbColumns.ID, DbColumns.COUNTRY, DbColumns.SECTOR, DbColumns.GAS]
        self.group_list_with_year: List[str] = self.group_list + [DbColumns.YEAR]

        # Number of years to project forward
        self.years_forward = np.array([1, 2, 3])
        self.predicted_years = np.array([2020, 2021])

        # Regression training window (6 years total)
        self.regression_training_window = 6
        self.end_training_year: int = 2018
        self.start_training_year: int = self.end_training_year - self.regression_training_window + 1

        # Preallocate empty data frames because I hate warning messages
        self.data = pd.DataFrame()
        self.full_df = pd.DataFrame()
        self.some_missing_df = pd.DataFrame()
        self.all_missing_df = pd.DataFrame()

    def load(self):
        dh = DataHandler(self.db_params_file)
        self.data = dh.load_data(self.source, rename_columns=False)

    def clean(self):
        # Before anything else, confirm the unit
        assert all(self.data[DbColumns.UNIT] == self.expected_emission_quantity_units), "Units must all be tonnes."

        self.codes_list = self.data[DbColumns.ID].unique()
        self.country_list = self.data[DbColumns.COUNTRY].unique()
        self.sector_list = self.data[DbColumns.SECTOR].unique()
        self.gases_list = self.data[DbColumns.GAS].unique()

        index_date_range = np.arange(self.start_training_year, self.end_training_year + 1)

        # Filter down to the specific training years
        training_interval_mask = (self.data[DbColumns.YEAR] >= self.start_training_year) & (
                self.data[DbColumns.YEAR] <= self.end_training_year)
        self.data = self.data.loc[training_interval_mask].copy()

        # Remove duplicates
        self.data.drop_duplicates(inplace=True)

        # Reindex to ensure every country, gas, sector, and year combination has a row
        self._reindex(index_date_range)

        # Add data counts (count of number of years with non nan data for
        self._add_data_counts()

        # Separate out data based on how much is missing
        self.full_df = self.data[self.data[DbColumns.COUNT] == len(index_date_range)].sort_values(
            by=self.group_list_with_year).reset_index(drop=True)

        self.some_missing_df = self.data[
            (self.data[DbColumns.COUNT] > 0) & (self.data[DbColumns.COUNT] < len(index_date_range))].sort_values(
            by=self.group_list_with_year, ignore_index=True).reset_index(drop=True)

        self.all_missing_df = self.data[self.data[DbColumns.COUNT] == 0].sort_values(by=self.group_list_with_year,
                                                                                     ignore_index=True).reset_index(
            drop=True)

    def _reindex(self, index_date_range):
        # Reindex to ensure every country, gas, sector, and year combination has a row
        self.multi_index_all_years = pd.MultiIndex.from_product(
            [self.codes_list, self.country_list, self.sector_list, self.gases_list, index_date_range], names=self.group_list_with_year)

        print(f"Number of country-sector-gas combinations: {len(self.multi_index_all_years)}")

        self.data = self.data.set_index(self.group_list_with_year).reindex(
            self.multi_index_all_years, fill_value=np.nan).reset_index()

    def _add_data_counts(self):
        """
        Count the number of data points in each x_ijk (unique country i, sector j, and gas k combination). If the number
        of counts is equal to the length of the date range, then that means the particular x_ijk point has data for
        every year in the date range.

        """
        df_count = self.data.groupby(self.group_list).count()
        self.data = self.data.merge(df_count.rename(
            columns={DbColumns.VALUE: DbColumns.COUNT})[[DbColumns.COUNT]].reset_index())

    def project(self):

        # Apply regression to appropriate sectors for the dataframe with no missing data
        try:
            df_regression_full = self._apply_regression(
                self.full_df[self.full_df[DbColumns.SECTOR].isin(self.sectors_to_use_regression)].copy())
            regression = True
        except:
            regression = False
            pass

        # Apply forward fill to appropriate sectors for all partial or full data
        df_no_missing = pd.concat([self.full_df, self.some_missing_df], ignore_index=True)
        df_baseline_results = self._apply_baseline_forward_fill(
            df_no_missing[~df_no_missing[DbColumns.SECTOR].isin(self.sectors_to_use_regression)].copy())

        # Drop nans -- no need to write nans.
        df_baseline_results.dropna(subset=[DbColumns.VALUE], inplace=True)

        if regression:
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
        For each sector, country, and gas, train a regression model and use that regression model to predict the next 3
         years.
        Cannot contain any missing data during training years
        """
        df_training_data = df.copy()

        # Pre-allocate columns to fill w regression results. Each columns will be the year that the prediction is for
        for predict_year in self.predicted_years:
            df_training_data.loc[:, predict_year] = np.nan

        # Group by country, sector, and gas and do projection
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
        y = df[[DbColumns.VALUE]].values
        X = df[[DbColumns.YEAR]].values
        return np.squeeze(LinearRegression().fit(X, y).predict(x_test))

    def prepare_to_write(self, df_projections: pd.DataFrame):
        # Add a few more columns to wrap up data projection
        df_projections[DbColumns.UNIT] = self.expected_emission_quantity_units
        df_projections[DbColumns.DATA_SOURCE] = f"{self.source}-projected"
        df_projections = from_years_to_start_and_end_times(df_projections)
        df_projections = add_iso_value(df_projections)

        return df_projections


if __name__ == "__main__":
    proj_edgar = ProjectEdgarData(db_params_file_path="params.json")
    proj_edgar.load()
    proj_edgar.clean()
    df_projections = proj_edgar.project()
    df_projections_final = proj_edgar.prepare_to_write(df_projections)
