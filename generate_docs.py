import os

from lazydocs import generate_docs

src_root_path = os.path.dirname(os.path.realpath(__file__))
output_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "docs")

generate_docs(
    ["lib_airflow"],
    src_root_path=src_root_path,
    src_base_url="https://github.com/RADar-AZDelta/lib_airflow/blob/main/",
    output_path=output_path,
    overview_file="README.md",
)
