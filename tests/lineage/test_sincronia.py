"""Los datasets de lineage versionados son exactamente los que genera `make lineage` con el repositorio actual."""
import os

import pytest

from raillytics.lineage.__main__ import RAIZ, generar
from raillytics.lineage.datasets import AIRFLOW_URL_POR_DEFECTO


def test_los_datasets_versionados_estan_al_dia_con_el_repositorio():
    # La misma URL de Airflow que usa `make lineage` (AIRFLOW_UI_URL del .env, que `make test` exporta).
    url = os.environ.get("AIRFLOW_UI_URL") or AIRFLOW_URL_POR_DEFECTO
    desfasados = [
        str(ruta.relative_to(RAIZ)) for ruta, texto in generar(RAIZ, url).items()
        if not ruta.is_file() or ruta.read_text(encoding="utf-8") != texto
    ]

    assert desfasados == [], f"ejecuta `make lineage`: {desfasados}"


def test_generar_dos_veces_da_lo_mismo_aunque_los_datasets_de_lineage_ya_existan():
    # Regresión: el grafo se leía a sí mismo (sus datasets citaban rutas de Gold en las descripciones) y cada pasada cambiaba el resultado.
    assert generar(RAIZ, AIRFLOW_URL_POR_DEFECTO) == generar(RAIZ, AIRFLOW_URL_POR_DEFECTO)


@pytest.mark.parametrize("url", ["http://localhost:8080", "http://airflow.interno:9999/"])
def test_la_url_de_airflow_solo_cambia_los_enlaces(url):
    base = generar(RAIZ, AIRFLOW_URL_POR_DEFECTO)
    otra = generar(RAIZ, url)
    distintos = {r.name for r in base if base[r] != otra[r]}

    assert distintos <= {"lineage_cargas.yaml"}
