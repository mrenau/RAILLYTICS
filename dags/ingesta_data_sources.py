# dags/ingesta_data_sources.py
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.sdk import dag, task
from airflow.sdk.exceptions import AirflowSkipException

from raillytics.ingesta.airflow_ref import referencia_airflow
from raillytics.ingesta.download import download
from raillytics.ingesta.sources import load_sources

# Rutas del lado del contenedor (ver docker-compose.yml: volúmenes
# ../config:/opt/airflow/raillytics_config y ../data:/opt/airflow/raillytics_data).
CONFIG_PATH = Path("/opt/airflow/raillytics_config/data_sources.yml")
BRONZE_STAGING_ROOT = Path("/opt/airflow/raillytics_data/bronze")
# Cuarentena de los ficheros que no pasan los quality gates de descarga
# (raillytics.calidad.ficheros). Hermano del staging: L1 no lo ve.
BRONZE_REJECTED_ROOT = Path("/opt/airflow/raillytics_data/bronze_rejected")


@dag(
    dag_id="ingesta_data_sources",
    schedule="@daily",
    # Sin start_date Airflow 3 no programa el DAG aunque tenga schedule (solo
    # corre a mano). Con catchup=False no rellena el pasado: la primera
    # ejecución programada es la del día siguiente a despausarlo.
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["ingesta", "bronze"],
)
def ingesta_data_sources():
    @task
    def download_source(source_id: str, **contexto) -> str:
        # Imports dentro de la tarea: el dag-processor no necesita duckdb para parsear el DAG.
        from raillytics.calidad import QualityGateError, registrar_calidad
        from raillytics.utils.cargas import registrar_carga

        sources_by_id = {s.id: s for s in load_sources(CONFIG_PATH)}
        source = sources_by_id[source_id]
        # Trazabilidad: una fila por descarga en <bucket gold>/_trazabilidad/cargas/ y una
        # por quality gate en .../calidad/, con el mismo run_id (MinIO y credenciales
        # salen de las variables MINIO_* del contenedor).
        # La referencia de Airflow queda en la trazabilidad: el dashboard «Lineage de cargas» enlaza desde ella al log de esta tarea.
        parametros = {"format": source.format, "airflow": referencia_airflow(contexto)}
        with registrar_carga("bronze_download", "bronze", parametros=parametros) as ejecucion:
            with ejecucion.tabla(source.id, origen=source.url) as carga:
                descarga = download(source, BRONZE_STAGING_ROOT, BRONZE_REJECTED_ROOT)
                carga.destino = str(descarga.path)
                carga.bytes = descarga.bytes
                registrar_calidad(descarga.gates, "bronze_download", "bronze", ejecucion.run_id)
                # Fichero en cuarentena: la tarea falla (queda en rojo en Airflow y con
                # estado error en la trazabilidad) y el fichero no llega a L1.
                if not descarga.aceptada:
                    raise QualityGateError(descarga.gates)
        return str(descarga.path)

    # Predicción diaria de demanda del corredor AVE Madrid–Barcelona (raillytics.prediccion): se lanza al final y solo a
    # petición (`make 00_ingest` pasa predecir=true en la conf del DAG); las ejecuciones programadas solo ingestan.
    # No espera a L1/L2 (apps Spark que corren fuera de Airflow): usa lo que ya haya procesado en el lake.
    # all_done: que falle la descarga de una fuente no impide predecir con lo que haya.
    @task(trigger_rule="all_done", execution_timeout=timedelta(minutes=30))
    def predecir(**contexto) -> str:
        conf = contexto["dag_run"].conf or {}
        if not conf.get("predecir"):
            raise AirflowSkipException("la predicción solo se lanza a petición (make 00_ingest); esta ejecución solo ingesta")
        # Import dentro de la tarea: el dag-processor no necesita pandas/duckdb para parsear el DAG.
        from raillytics.prediccion.servicio import predecir_desde_entorno

        hoy = (contexto.get("logical_date") or datetime.now(timezone.utc)).date()
        resultado = predecir_desde_entorno(
            conf.get("trimestre") or None, hoy, os.environ, version_prompt=conf.get("prompt") or None
        )
        return str(resultado.ruta)

    source_ids = [source.id for source in load_sources(CONFIG_PATH)]
    descargas = download_source.expand(source_id=source_ids)
    descargas >> predecir()


ingesta_data_sources()
