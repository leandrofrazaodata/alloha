import logging
import os
from datetime import datetime, timedelta
from typing import List, Optional

import pytz
from airflow.models import Variable
from cosmos import (
    DbtTaskGroup,
    ExecutionConfig,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
)
from cosmos.constants import LoadMode
from cosmos.profiles import DatabricksTokenProfileMapping
from airflow.models import DagRun
from airflow import AirflowException


class AirflowUtil:
    def __init__(self, environment: str = None) -> None:
        """Inicializa o objeto com base na variável de ambiente "env"."""
        self.environment: str = (
            Variable.get("env") if not environment else environment
        )
        logging.info(f"Using environment {self.environment}.")

    @staticmethod
    def adjust_time(
        hours: int = 0, timezone: pytz.timezone = pytz.utc, venda_hora=False
    ) -> datetime:
        """
        Ajusta o tempo no fuso horário especificado e subtrai ou adiciona um determinado número de horas.

        Args:
            hours (int): Número de horas a serem ajustadas. Use valores negativos para subtrair horas.
            timezone (timezone): Um objeto timezone conforme definido pelo pytz.

        Returns:
            str: Data e hora formatadas como "AAAAMMDDTHH0050".
        """
        # Obtém a hora atual no fuso horário especificado
        now = datetime.now(timezone)

        # Ajusta a hora conforme o valor fornecido em "hours"
        adjusted_time = now + timedelta(hours=hours)

        # Formata o horário para o formato desejado
        if venda_hora:
            formatted_timestamp = adjusted_time.strftime("%Y%m%dT%H0000")
        else:
            formatted_timestamp = adjusted_time.strftime("%Y%m%dT%H%M%S")

        return datetime.strptime(formatted_timestamp, "%Y%m%dT%H%M%S").replace(
            tzinfo=pytz.utc
        )

    @staticmethod
    def get_most_recent_dag_run(dt, dag_id, manually_triggered = False):
        dag_runs = DagRun.find(dag_id=dag_id)            
        
        if manually_triggered:
            range = timedelta(days=0)
        else:
            range = timedelta(days=1)
            dag_runs_filtered = []
            for dag_run in dag_runs:
                if 'manual' not in dag_run.run_id:
                    dag_runs_filtered.append(dag_run)
            dag_runs = dag_runs_filtered
        
        dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
                    
        if dag_runs:
            if dag_runs[0].execution_date.date() == (datetime.now() - range).date():
                return dag_runs[0].execution_date
            else:
                print(f'No run today for {dag_id}')
                raise AirflowException

    def get_catalogs(self):
        """
        Retorna os nomes dos catálogos com base no ambiente atual configurado para a instância.

        Retorna uma tupla contendo os nomes dos catálogos específicos para o ambiente:
        - Ambiente "prd" retorna ("gold", "silver")
        - Ambiente "dev" retorna ("gold_dev", "silver_dev")

        Raises:
            KeyError: Se a variável 'env' não for encontrada nas Variáveis do Airflow.
            ValueError: Se o ambiente especificado não for reconhecido.

        Returns:
            tuple: Nomes dos catálogos correspondentes ao ambiente atual.
        """
        environment_catalogs = {
            "prd": ("gold", "silver"),
            "dev": ("gold_dev", "silver_dev"),
        }

        try:
            if self.environment in environment_catalogs:
                return environment_catalogs[self.environment]
            else:
                raise ValueError(
                    f"Unexpected environment value: {self.environment}"
                )
        except KeyError:
            logging.error("Variable 'env' not found in Airflow Variables.")
            raise
        except ValueError as e:
            logging.error(f"Invalid value for environment: {e}")
            raise

    def create_dbt_task_group(
        self,
        group_id: str,
        profile_name: str,
        catalog: str,
        schema: str,
        select_paths: List[str],
        exclude: Optional[List[str]] = None,
        host: str = "dbc-aa960f0c-f7ef.cloud.databricks.com",
        http_path: str = "/sql/1.0/warehouses/b5ac507cf171aff5",
        threads: int = 10,
    ):
        """
        Cria um grupo de tarefas dbt com suas respectivas configurações.

        Args:
            group_id (str): Identificador único para o grupo de tarefas.
            profile_name (str): Nome do perfil dbt.
            catalog (str): Nome do catálogo.
            http_path (str): Caminho HTTP.
            select_paths (List[str]): Lista de caminhos a serem selecionados.
            exclude (Optional[List[str]]): Lista de caminhos a serem excluídos, ou None. Default é None.
            host (str): Nome do host.
            schema (str): Nome do schema.
            threads (int): Número de threads a serem utilizados. Default é 4.

        Returns:
            DbtTaskGroup: Grupo de tarefas dbt configurado.
        """
        # Configuração do projeto dbt
        project_config = ProjectConfig(
            dbt_project_path=f"{os.environ['AIRFLOW_HOME']}/dags/dbt/",
            env_vars={
                "PYTHONPATH": "/home/airflow/.local/lib/python3.11/site-packages:${PYTHONPATH}"
            },
        )

        # Configuração de execução do dbt
        execution_config = ExecutionConfig(dbt_executable_path="/usr/local/bin/dbt")

        return DbtTaskGroup(
            group_id=group_id,
            project_config=project_config,
            profile_config=ProfileConfig(
                profile_name=profile_name,
                target_name=self.environment,
                profile_mapping=DatabricksTokenProfileMapping(
                    conn_id="databricks_default",
                    profile_args={
                        "catalog": catalog,
                        "host": host,
                        "schema": schema,
                        "http_path": http_path,
                        "threads": threads,
                    },
                ),
            ),
            execution_config=execution_config,
            render_config=RenderConfig(
                dbt_deps=False,
                load_method=LoadMode.DBT_LS,
                select=select_paths,
                exclude=exclude,
            ),
        )
