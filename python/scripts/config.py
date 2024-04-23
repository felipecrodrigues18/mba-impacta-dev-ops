import os
local_path = os.path.abspath(__file__)
python_path = os.path.dirname(os.path.dirname(local_path))
repo_path = os.path.dirname(python_path)

configs = {
    "repo_path": repo_path,
    "meta_path": f"{python_path}/scripts/metadado.xlsx",
    "raw_path": f"{repo_path}/data/raw/raw_",
    "work_path": f"{repo_path}/data/work/work_cadastro.csv",
}