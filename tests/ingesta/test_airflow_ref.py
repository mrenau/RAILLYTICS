"""La referencia de Airflow que cada descarga deja en su trazabilidad (parametros.airflow) para enlazar su log desde el dashboard de lineage."""
from types import SimpleNamespace

from raillytics.ingesta.airflow_ref import referencia_airflow


def _ti(**kw):
    base = dict(dag_id="ingesta_data_sources", run_id="manual__2026-10-06T10:00:00+00:00", task_id="download_source", map_index=2)
    return SimpleNamespace(**{**base, **kw})


def test_toma_dag_run_tarea_e_indice_de_la_instancia_de_tarea():
    assert referencia_airflow({"ti": _ti()}) == {
        "dag_id": "ingesta_data_sources", "run_id": "manual__2026-10-06T10:00:00+00:00", "task_id": "download_source", "map_index": 2,
    }


def test_una_tarea_sin_mapear_tiene_indice_menos_uno():
    assert referencia_airflow({"ti": _ti(map_index=-1)})["map_index"] == -1


def test_si_la_instancia_no_trae_el_indice_se_asume_que_no_esta_mapeada():
    ti = SimpleNamespace(dag_id="d", run_id="r", task_id="t")

    assert referencia_airflow({"ti": ti})["map_index"] == -1


def test_completa_lo_que_falte_con_el_resto_del_contexto():
    ti = SimpleNamespace(task_id="t", map_index=0)
    contexto = {"ti": ti, "run_id": "r1", "dag": SimpleNamespace(dag_id="d1")}

    assert referencia_airflow(contexto) == {"dag_id": "d1", "run_id": "r1", "task_id": "t", "map_index": 0}


def test_fuera_de_airflow_no_hay_referencia():
    assert referencia_airflow({}) is None
    assert referencia_airflow({"ti": SimpleNamespace()}) is None   # sin lo mínimo para enlazar no se inventa nada
