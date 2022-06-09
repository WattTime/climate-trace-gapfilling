import pandas as pd
from pandas.testing import assert_frame_equal
import numpy as np
from gapfilling.fill_gaps import fill_all_sector_gaps
from gapfilling.constants import COL_ORDER

# Test cases that need to be written:
def test_fill_all_sector_gaps_with_nan_return():
    ge_tc = ["sub-sector","inventory","sub_inventory","subinv_ipcc_code","subinv_units","values","gas output","notes"]
    ge_td = [["other-fossil-fuel-operations","edgar","edgar","1.B.2","Oil and Natural Gas",1,"co2",None],
             ["other-fossil-fuel-operations","edgar","edgar","1.A.1.bc","Petroleum Refining - Manufacture of Solid Fuels and Other Energy Industries",1,"co2",None],
             ["other-fossil-fuel-operations","climate-trace","climate-trace","1.B.2.a.i; 1.B.2.a.ii; 1.B.2.a.iii; 1.B.2.b.i; 1.B.2.b.ii; 1.B.2.c","oil-and-gas-production-and-transport",-1,"CO2",None],
             ["other-fossil-fuel-operations","climate-trace","climate-trace","1.B.2.a.iv; 1.B.2.a.v; 1.B.2.b.iii; 1.B.2.b.v","oil-and-gas-refining",-1,"co2",None]]
    test_ge = pd.DataFrame(data=ge_td, columns=ge_tc)

    tc = ["Country", "Gas", "Data source", "Sector", "Unit", 2015, 2016, 2017, 2018, 2019, 2020]
    td = [["United States of America", "co2", "climate-trace", "oil-and-gas-production-and-transport", "tonnes", 100, 100, 100, 100, 100, 100],
          ["United States of America", "co2", "climate-trace", "oil-and-gas-refining", "tonnes", 10, 10, 10, 10, 10, 10],
          ["United States of America", "co2", "edgar", "Oil and Natural Gas", "tonnes", 10, 10, 10, 10, 10, 10],
          ["United States of America", "co2", "edgar", "Petroleum Refining - Manufacture of Solid Fuels and Other Energy Industries", "tonnes", 10, 10, 10, 10, 10, 10]]
    test_df = pd.DataFrame(data=td, columns=tc)
    result_df = fill_all_sector_gaps(test_df, ge=test_ge)

    truth_data = [["United States of America", "co2", "climate-trace", "other-fossil-fuel-operations", "tonnes", np.nan, np.nan, np.nan, np.nan, np.nan, np.nan]]
    expected_df = pd.DataFrame(data=truth_data, columns=tc)[COL_ORDER]

    assert_frame_equal(expected_df, result_df)

def test_fill_all_sector_gaps_with_zero_return():
    ge_tc = ["sub-sector","inventory","sub_inventory","subinv_ipcc_code","subinv_units","values","gas output","notes"]
    ge_td = [["other-fossil-fuel-operations","edgar","edgar","1.B.2","Oil and Natural Gas",1,"co2",None],
             ["other-fossil-fuel-operations","edgar","edgar","1.A.1.bc","Petroleum Refining - Manufacture of Solid Fuels and Other Energy Industries",1,"co2",None],
             ["other-fossil-fuel-operations","climate-trace","climate-trace","1.B.2.a.i; 1.B.2.a.ii; 1.B.2.a.iii; 1.B.2.b.i; 1.B.2.b.ii; 1.B.2.c","oil-and-gas-production-and-transport",-1,"CO2",None],
             ["other-fossil-fuel-operations","climate-trace","climate-trace","1.B.2.a.iv; 1.B.2.a.v; 1.B.2.b.iii; 1.B.2.b.v","oil-and-gas-refining",-1,"co2",None]]
    test_ge = pd.DataFrame(data=ge_td, columns=ge_tc)

    tc = ["Country", "Gas", "Data source", "Sector", "Unit", 2015, 2016, 2017, 2018, 2019, 2020]
    td = [["United States of America", "co2", "climate-trace", "oil-and-gas-production-and-transport", "tonnes", 10, 10, 10, 10, 10, 10],
          ["United States of America", "co2", "climate-trace", "oil-and-gas-refining", "tonnes", 10, 10, 10, 10, 10, 10],
          ["United States of America", "co2", "edgar", "Oil and Natural Gas", "tonnes", 10, 10, 10, 10, 10, 10],
          ["United States of America", "co2", "edgar", "Petroleum Refining - Manufacture of Solid Fuels and Other Energy Industries", "tonnes", 10, 10, 10, 10, 10, 10]]
    test_df = pd.DataFrame(data=td, columns=tc)
    result_df = fill_all_sector_gaps(test_df, ge=test_ge)

    truth_data = [["United States of America", "co2", "climate-trace", "other-fossil-fuel-operations", "tonnes", 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]]
    expected_df = pd.DataFrame(data=truth_data, columns=tc)[COL_ORDER]

    assert assert_frame_equal(expected_df, result_df) is None


def test_fill_all_sector_gaps_with_good_return():
    ge_tc = ["sub-sector","inventory","sub_inventory","subinv_ipcc_code","subinv_units","values","gas output","notes"]
    ge_td = [["other-fossil-fuel-operations","edgar","edgar","1.B.2","Oil and Natural Gas",1,"co2",None],
             ["other-fossil-fuel-operations","edgar","edgar","1.A.1.bc","Petroleum Refining - Manufacture of Solid Fuels and Other Energy Industries",1,"co2",None],
             ["other-fossil-fuel-operations","climate-trace","climate-trace","1.B.2.a.i; 1.B.2.a.ii; 1.B.2.a.iii; 1.B.2.b.i; 1.B.2.b.ii; 1.B.2.c","oil-and-gas-production-and-transport",-1,"CO2",None],
             ["other-fossil-fuel-operations","climate-trace","climate-trace","1.B.2.a.iv; 1.B.2.a.v; 1.B.2.b.iii; 1.B.2.b.v","oil-and-gas-refining",-1,"co2",None]]
    test_ge = pd.DataFrame(data=ge_td, columns=ge_tc)

    tc = ["Country", "Gas", "Data source", "Sector", "Unit", 2015, 2016, 2017, 2018, 2019, 2020]
    td = [["United States of America", "co2", "climate-trace", "oil-and-gas-production-and-transport", "tonnes", 10, 10, 10, 10, 10, 10],
          ["United States of America", "co2", "climate-trace", "oil-and-gas-refining", "tonnes", 10, 10, 10, 10, 10, 10],
          ["United States of America", "co2", "edgar", "Oil and Natural Gas", "tonnes", 110, 110, 110, 110, 110, 110],
          ["United States of America", "co2", "edgar", "Petroleum Refining - Manufacture of Solid Fuels and Other Energy Industries", "tonnes", 110, 110, 110, 110, 110, 110]]
    test_df = pd.DataFrame(data=td, columns=tc)

    truth_data = [["United States of America", "co2", "climate-trace", "other-fossil-fuel-operations", "tonnes", 200.0, 200.0, 200.0, 200.0, 200.0, 200.0]]

    expected_df = pd.DataFrame(data=truth_data, columns=tc)[COL_ORDER]
    result_df = fill_all_sector_gaps(test_df, ge=test_ge)

    assert assert_frame_equal(expected_df, result_df) is None

