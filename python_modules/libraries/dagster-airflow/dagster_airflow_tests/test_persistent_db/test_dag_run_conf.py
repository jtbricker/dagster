import datetime
import os
import tempfile

from airflow.models import Variable
from dagster._core.instance import AIRFLOW_EXECUTION_DATE_STR
from dagster_airflow import (
    make_dagster_definitions_from_airflow_dags_path,
    make_persistent_airflow_db_resource,
)

from dagster_airflow_tests.marks import requires_persistent_db

DAG_RUN_CONF_DAG = """
from airflow import models

from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
import datetime

default_args = {"start_date": datetime.datetime(2023, 2, 1)}

with models.DAG(
    dag_id="dag_run_conf_dag", default_args=default_args, schedule_interval='0 0 * * *',
) as dag_run_conf_dag:
    def test_function(**kwargs):
        Variable.set("CONFIGURATION_VALUE", kwargs['config_value'])

    PythonOperator(
        task_id="previous_macro_test",
        python_callable=test_function,
        provide_context=True,
        op_kwargs={'config_value': "{{dag_run.conf["configuration_key"]}}"}
    )
"""


@requires_persistent_db
def test_dag_run_conf_persistent(postgres_airflow_db: str) -> None:
    with tempfile.TemporaryDirectory() as dags_path:
        with open(os.path.join(dags_path, "dag.py"), "wb") as f:
            f.write(bytes(DAG_RUN_CONF_DAG.encode("utf-8")))

        airflow_db = make_persistent_airflow_db_resource(
            uri=postgres_airflow_db, dag_run_config={"configuration_key": "foo"}
        )

        definitions = make_dagster_definitions_from_airflow_dags_path(
            dags_path, resource_defs={"airflow_db": airflow_db}
        )
        job = definitions.get_job_def("dag_run_conf_dag")

        result = job.execute_in_process(
            tags={AIRFLOW_EXECUTION_DATE_STR: datetime.datetime(2023, 2, 2).isoformat()}
        )
        assert result.success
        assert Variable.get("CONFIGURATION_VALUE") == "foo"
