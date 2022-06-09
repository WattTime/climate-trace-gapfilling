# Gap Filling Code Refactor

### Development Installation
Make sure you have [Python 3.8](https://www.python.org/downloads/release/python-3811/) or above. You can check in the command line with `python --version`

To create a virtual environment within this head directory (`cd climate-trace=metamodeling`), run
`python -m venv venv`. The second "venv" can be any name of your virtual environment. (I used `ct_venv`).

Run the following to activate virtual environment to use and disable to stop using:
- nix: `source venv/bin/activate`
- win: `venv\Scripts\activate.bat` or `venv\Scripts\activate.ps1` for powershell 
- `deactivate` will deactivate your virtual env

Once your environment is activated, run `pip install --default-timeout=1000 -r requirements.txt`. The increased default timeout is for pandas, since it is a large file and takes additional time to download.

Add your environment to jupyter notebook so that you can use the same environment there. I always follow [this](https://janakiev.com/blog/jupyter-virtual-envs/) resource to recall how to do this. All I needed to do was run ` python -m ipykernel install --user --name=ct_venv`.
