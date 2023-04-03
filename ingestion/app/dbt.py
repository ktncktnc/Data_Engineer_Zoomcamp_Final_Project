from prefect import flow
from prefect_dbt.cli.commands import DbtCoreOperation

@flow
def trigger_dbt_transformation() -> str:
    result = DbtCoreOperation(
        commands=["dbt run"],
        project_dir="/home/ktnc/Data_Engineer_Zoomcamp_Final_Project/transformation/gh_archive",
        profiles_dir="/home/ktnc/.dbt/"
    ).run()
    return result
