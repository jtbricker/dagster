import importlib
import os
from typing import List, Optional

import airflow
from airflow.models.connection import Connection
from dagster import (
    Noneable,
    Array,
    DagsterRun,
    Field,
    InitResourceContext,
    ResourceDefinition,
    _check as check,
)

from dagster_airflow.resources.airflow_db import AirflowDatabase
from dagster_airflow.utils import (
    create_airflow_connections,
    is_airflow_2_loaded_in_environment,
    serialize_connections,
)


class AirflowPersistentDatabase(AirflowDatabase):
    """
    A persistent Airflow database Dagster resource.

    """

    def __init__(self, dagster_run: DagsterRun, uri: str, dag_run_config: Optional[dict] = None):
        self.uri = uri
        super().__init__(dagster_run=dagster_run, dag_run_config=dag_run_config)

    @staticmethod
    def _initialize_database(uri: str, connections: List[Connection] = []):
        if is_airflow_2_loaded_in_environment():
            os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = uri
            importlib.reload(airflow.configuration)
            importlib.reload(airflow.settings)
            importlib.reload(airflow)
        else:
            os.environ["AIRFLOW__CORE__SQL_ALCHEMY_CONN"] = uri
            importlib.reload(airflow)
        create_airflow_connections(connections)

    @staticmethod
    def from_resource_context(context: InitResourceContext) -> "AirflowPersistentDatabase":
        uri = context.resource_config["uri"]
        AirflowPersistentDatabase._initialize_database(
            uri=uri, connections=[Connection(**c) for c in context.resource_config["connections"]]
        )
        return AirflowPersistentDatabase(
            dagster_run=check.not_none(context.dagster_run, "Context must have run"), uri=uri
        )


def make_persistent_airflow_db_resource(
    uri: Optional[str] = "",
    connections: List[Connection] = [],
    dag_run_config: Optional[dict] = None,
) -> ResourceDefinition:
    """
    Creates a Dagster resource that provides an persistent Airflow database.

    Args:
        uri: SQLAlchemy URI of the Airflow DB to be used
        connections (List[Connection]): List of Airflow Connections to be created in the Airflow DB
        dag_run_config (Optional[dict]): dag_run configuration to be used when creating a DagRun

    Returns:
        ResourceDefinition: The persistent Airflow DB resource

    """
    serialized_connections = serialize_connections(connections)
    airflow_db_resource_def = ResourceDefinition(
        resource_fn=AirflowPersistentDatabase.from_resource_context,
        config_schema={
            "uri": Field(
                str,
                default_value=uri,
                is_required=False,
            ),
            "connections": Field(
                Array(inner_type=dict),
                default_value=serialized_connections,
                is_required=False,
            ),
            "dag_run_config": Field(
                Noneable(dict),
                default_value=dag_run_config,
                is_required=False,
            ),
        },
        description="Persistent Airflow DB to be used by dagster-airflow ",
    )
    return airflow_db_resource_def
