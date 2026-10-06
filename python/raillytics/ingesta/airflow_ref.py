"""Referencia de Airflow de una descarga: lo necesario para enlazar su tarea (y su log) en la UI desde el dashboard de lineage.

Va en `parametros.airflow` de la fila de trazabilidad. El dataset `lineage_cargas` (raillytics.lineage.datasets) arma el enlace
`<UI de Airflow>/dags/<dag_id>/runs/<run_id>/tasks/<task_id>[/mapped/<map_index>]`.
"""
from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def referencia_airflow(contexto: Mapping[str, Any]) -> dict[str, Any] | None:
    """{dag_id, run_id, task_id, map_index} del contexto de una tarea de Airflow, o None si no hay lo mínimo para enlazarla."""
    ti = contexto.get("ti") or contexto.get("task_instance")
    dag = contexto.get("dag")
    referencia = {
        "dag_id": getattr(ti, "dag_id", None) or getattr(dag, "dag_id", None),
        "run_id": getattr(ti, "run_id", None) or contexto.get("run_id"),
        "task_id": getattr(ti, "task_id", None),
        "map_index": getattr(ti, "map_index", -1),   # -1: tarea sin mapear
    }
    if not all(referencia[clave] for clave in ("dag_id", "run_id", "task_id")):
        return None
    return referencia
