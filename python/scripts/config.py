import os
local_path = os.path.abspath(__file__)
scripts_path = os.path.dirname(local_path)
python_path = os.path.dirname(scripts_path)

configs = {
    "python_path": python_path,
    "logs_path": f"{python_path}/logs",
    "meta_path": f"{python_path}/scripts/metadado.xlsx",
    "raw_path": f"{python_path}/data/raw/raw_",
    "work_path": f"{python_path}/data/work/work_cadastro.csv",
}