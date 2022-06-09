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

Run `pip install .` to install local packages via `setup.py`.

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
