# Climate Trace Metamodeling: Gap Filling

## Getting Started

### Development Installation
Make sure you have [Python 3.8](https://www.python.org/downloads/release/python-3811/) or above. You can check in the command line with `python --version`

To create a virtual environment within this head directory (`cd climate-trace=metamodeling`), run
`python -m venv venv`. The second "venv" can be any name of your virtual environment. (I used `ct_venv`).

Run the following to activate virtual environment to use and disable to stop using:
- nix (Mac OS, Linux): `source venv/bin/activate`
- win (windows): `venv\Scripts\activate.bat` or `venv\Scripts\activate.ps1` for powershell 
- `deactivate` will deactivate your virtual env

Once your environment is activated, run `pip install --default-timeout=1000 -r requirements.txt`. The increased default timeout is for pandas, since it is a large file and takes additional time to download.

Run `pip install -e .` to install local packages via `setup.py`.

### Running
- First, you will need to add your database credentials to the `params.json` file.
- Next, you can run the `main.py` file to run all actions. This will:
  - Load Climate TRACE and EDGAR data
  - Project EDGAR data forward in time
     - Write that projected data to the database
  - Load the projected data and fill Climate TRACE gaps with both EDGAR and EDGAR-projected
  - Write the filled data to the database


### Testing
We use the `pytest` library for unit testing the functionality. Simply run `pytest tests` from the head directory to run all tests in the folder.

## Summary of logic used

### Gap filling
#### Gap filling value interpretation (contained in the `data_cleaning` function of `fill_gaps.py`)

- If the output of the gap filled value is very negative (less than -2), change the value to NaN

- If the value is slightly negative (between -2 and 0), change the value to 0


#### Complete CT dataset generation (contained in the `add_all_gas_rows` function of `utils.py`)

- For every country, every sector, we initialize rows with a value of zero in the gapfilling code for: co2, ch4, nh4, co2e_20yr, co2e_100yr

#### Interpretation of 0 vs nan
- If the value is nan, this means that we expect there to be data for the particular observation but none was given. If the value is zero, it means there are no emissions for that observation, which includes when there are *no emissions possible*. For example, a particular sector may not emit a certain gas.

### Projection

#### When to use regression
- Sectors designated to use regression are defined in the `__init__` function of the `ProjectData` class
- Regression is used when the following are **both true**:
   - The sector is part of the designated sector list
   - There are 4 or more available data points within the 6 year training window (no more than 2 NaNs)
  
 #### When to use forward fill
- Forward filling (using the most recent year of data and repeating that value for future years) is used when **either** ..
   - The sector is part of the designated regression sector list AND there are less than 4 available data points within the 6 year training window (more than 2 NaNs)
      - The sector is not part of the designated regression sector list
