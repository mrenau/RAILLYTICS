"""El Makefile no debe depender de variables que el entorno de Windows ya define (PROMPT en cmd.exe)."""
import json
import os
import shlex
import shutil
import subprocess
from pathlib import Path

import pytest

RAIZ = Path(__file__).resolve().parents[2]

pytestmark = pytest.mark.skipif(shutil.which("make") is None, reason="necesita GNU make")


def _make_n(objetivo, *argumentos, **entorno):
    resultado = subprocess.run(
        ["make", "-n", objetivo, *argumentos],
        cwd=RAIZ, env=dict(os.environ, **entorno), capture_output=True, text=True,
    )
    return resultado.stdout


def _receta(**entorno):
    return _make_n("07_prediccion", "TRIMESTRE=2026-T4", **entorno)


def test_prediccion_sample_lanza_el_generador_de_fuentes_sinteticas():
    salida = _make_n("prediccion-sample", MUESTRA_ARGS="--hasta 2026-T3")

    assert "-m raillytics.prediccion.muestra --hasta 2026-T3" in salida


def test_la_plantilla_por_defecto_resiste_la_variable_PROMPT_de_cmd_exe():
    assert "--prompt eventos_v1" in _receta(PROMPT="$P$G")


def test_la_plantilla_se_elige_con_PRED_PROMPT():
    assert "--prompt demanda_v2" in _receta(PRED_PROMPT="demanda_v2")


def _conf_del_dag(*argumentos):
    salida = _make_n("00_ingest", *argumentos)
    linea = next(l for l in salida.splitlines() if "dags trigger" in l)
    return salida, json.loads(shlex.split(linea)[-1])  # el último argumento es el JSON de --conf


def test_00_ingest_levanta_ollama_antes_de_disparar_el_dag():
    salida, _ = _conf_del_dag()

    assert salida.index("up -d --wait ollama") < salida.index("run --rm ollama-init") < salida.index("airflow dags trigger ingesta_data_sources")


def test_00_ingest_pide_la_prediccion_al_dag_con_el_trimestre_y_la_plantilla():
    _, conf = _conf_del_dag("TRIMESTRE=2026-T4", "PRED_PROMPT=demanda_v2")

    assert conf == {"predecir": True, "trimestre": "2026-T4", "prompt": "demanda_v2"}


def test_00_ingest_sin_trimestre_deja_que_el_dag_use_el_trimestre_en_curso():
    _, conf = _conf_del_dag()

    assert conf == {"predecir": True, "trimestre": "", "prompt": "eventos_v1"}


def test_el_dag_lee_las_mismas_claves_de_conf_que_envia_el_makefile():
    dag = (RAIZ / "dags" / "ingesta_data_sources.py").read_text(encoding="utf-8")

    for clave in ("predecir", "trimestre", "prompt"):
        assert f'conf.get("{clave}")' in dag
    assert 'trigger_rule="all_done"' in dag  # que falle una descarga no impide predecir con lo que haya


def test_04_silver_lanza_la_app_de_streaming_silver():
    salida = _make_n("04_silver")

    assert 'runMain raillytics.silver.SilverBuilderApp' in salida


def test_los_targets_numerados_siguen_el_orden_del_pipeline_en_la_ayuda():
    ayuda = subprocess.run(["make", "help"], cwd=RAIZ, capture_output=True, text=True).stdout
    objetivos = ["00_ingest", "01_raw-uploader", "02_parquet-converter", "03_silver-sample", "04_silver", "05_gold", "06_superset-import", "07_prediccion"]

    posiciones = [ayuda.index(f"  {objetivo} ") for objetivo in objetivos]

    assert posiciones == sorted(posiciones)


@pytest.mark.parametrize("antiguo", ["04_gold", "05_superset-import", "06_prediccion"])
def test_los_nombres_antiguos_de_los_targets_ya_no_existen(antiguo):
    resultado = subprocess.run(["make", "-n", antiguo], cwd=RAIZ, capture_output=True, text=True)

    assert resultado.returncode != 0 and "No rule to make target" in resultado.stderr


# ---------------------------------------------------------------------------------- datos de la CNMC antes de predecir

def test_07_prediccion_asegura_los_datos_de_la_cnmc_antes_de_predecir():
    salida = _receta()

    assert "scripts/carga_e2e.py" in salida and "--asegurar-cnmc --trimestre 2026-T4" in salida
    assert salida.index("--asegurar-cnmc") < salida.index("-m raillytics.prediccion --trimestre 2026-T4")


def test_pred_cnmc_no_omite_la_comprobacion():
    salida = _receta(PRED_CNMC="no")

    assert "--asegurar-cnmc" not in salida and "-m raillytics.prediccion --trimestre 2026-T4" in salida


def test_la_comprobacion_no_se_lanza_de_verdad_con_make_n():
    # Una receta con $(MAKE) se ejecuta incluso con -n (ver E2E_MAKE): la comprobación podría arrancar la descarga.
    assert "make -n" not in _make_n("07_prediccion", "TRIMESTRE=2026-T4") and "$(MAKE)" not in (RAIZ / "Makefile").read_text(
        encoding="utf-8"
    ).split("07_prediccion:")[1].split("\n\n")[0]
