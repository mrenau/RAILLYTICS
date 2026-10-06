"""Los datasets de lineage: el SQL cruza el grafo declarado con lo observado en la trazabilidad (cargas y calidad) del lake."""
import json
from pathlib import Path

import duckdb
import pytest

from raillytics.lineage.datasets import AIRFLOW_URL_POR_DEFECTO, EXPR_NODO_CARGA, EXPR_NODO_GATE, construir_datasets
from raillytics.lineage.grafo import construir

RAIZ = Path(__file__).resolve().parents[2]
GRAFO = construir(RAIZ)
DATASETS = construir_datasets(GRAFO, AIRFLOW_URL_POR_DEFECTO)

# Todas las parejas (proceso, tabla) que el lake registra de verdad (2026-10-06): cada una debe caer en un nodo del grafo.
CARGAS_REALES = [
    ("bronze_download", "cnmc_indicadores"), ("bronze_download", "crtm"), ("bronze_download", "renfe_trip_updates"),
    ("bronze_l1_raw_uploader", "cnmc_viajeros_corredor"), ("bronze_l1_raw_uploader", "crtm"),
    ("bronze_l2_parquet_converter", "cnmc_precio_mensual"), ("bronze_l2_parquet_converter", "crtm/stops"),
    ("bronze_l2_parquet_converter", "crtm/stop_times"), ("bronze_l2_parquet_converter", "renfe_vehicle_positions"),
    ("gold_build", "dim_estacion"), ("gold_build", "dim_fecha"), ("gold_build", "dim_linea"), ("gold_build", "dim_operador"),
    ("gold_build", "fact_mercado_trimestral"), ("gold_build", "fact_precio_mensual"), ("gold_build", "fact_precio_trimestral"),
    ("gold_build", "fact_puntualidad"), ("gold_build", "fact_viajeros"),
    ("prediccion_demanda", "fact_prediccion_demanda"), ("prediccion_demanda", "prediccion_demanda_ave_mad_bcn"),
    ("prediccion_muestra", "aemet"), ("prediccion_muestra", "demanda_trimestral"), ("prediccion_muestra", "eventos"),
    ("prediccion_muestra", "festivos"),
    ("silver_builder", "cnmc_precio_mensual"), ("silver_builder", "cnmc_trimestral"), ("silver_builder", "cnmc_viajeros_producto"),
    ("silver_sample", "puntualidad_enriquecida"), ("silver_sample", "viajeros_enriquecidos"),
]
GATES_REALES = [
    ("bronze_download", "cnmc_indicadores"), ("bronze_l1_raw_uploader", "crtm"), ("bronze_l2_parquet_converter", "crtm/stops"),
    ("gold_build", "gold_dim_fecha"), ("gold_build", "silver_cnmc_trimestral"), ("silver_builder", "silver_cnmc_trimestral"),
    ("quality_gates", "silver_cnmc_viajeros_corredor"), ("prediccion_demanda", "prediccion_demanda_ave_mad_bcn"),
]


def _sql(nombre: str, tmp_path: Path) -> str:
    return DATASETS[nombre]["sql"].replace("s3://raillytics-gold", tmp_path.as_posix())


@pytest.fixture
def lake(tmp_path):
    """Un `_trazabilidad` pequeño: dos ejecuciones de Silver (la última, con error), L2 con varios miembros de un ZIP, una descarga
    de Airflow con su referencia y una predicción; más gates con un fallo bloqueante, un aviso y un gate que nunca se declaró."""
    con = duckdb.connect()
    for sub in ("cargas", "calidad"):
        (tmp_path / "_trazabilidad" / sub).mkdir(parents=True)

    def cargas(filas):
        valores = ", ".join(
            "(" + ", ".join("NULL" if v is None else ("'" + v.replace("'", "''") + "'" if isinstance(v, str) else str(v)) for v in f) + ")" for f in filas)
        con.execute(
            f"COPY (SELECT run_id, proceso, capa, tabla, origen, destino, filas, bytes, CAST(inicio AS TIMESTAMP) AS inicio, "
            "CAST(fin AS TIMESTAMP) AS fin, duracion_s, estado, error, parametros "
            f"FROM (VALUES {valores}) t(run_id, proceso, capa, tabla, origen, destino, filas, bytes, inicio, fin, "
            "duracion_s, estado, error, parametros)) "
            f"TO '{(tmp_path / '_trazabilidad/cargas/c.parquet').as_posix()}' (FORMAT PARQUET)")

    ts = lambda h, m=0: f"2026-10-06 {h:02d}:{m:02d}:00"   # noqa: E731
    airflow = json.dumps({"format": "csv", "airflow": {"dag_id": "ingesta_data_sources", "run_id": "manual__2026-10-06T10:00:00+00:00",
                                                       "task_id": "download_source", "map_index": 2}})
    def hostil(run, dag="ingesta_data_sources", tarea="download_source", idx=0):
        return json.dumps({"airflow": {"dag_id": dag, "run_id": run, "task_id": tarea, "map_index": idx}})

    cargas([
        ("r-x1", "bronze_download", "bronze", "cnmc_indicadores", "u", "d", None, 1, ts(6), ts(6, 1), 1.0, "ok", None, hostil('x"><img src=x onerror=alert(1)>')),
        ("r-x2", "bronze_download", "bronze", "cnmc_indicadores", "u", "d", None, 1, ts(6, 2), ts(6, 3), 1.0, "ok", None, hostil("r1", dag="d'--<b>")),
        ("r-x3", "bronze_download", "bronze", "cnmc_indicadores", "u", "d", None, 1, ts(6, 4), ts(6, 5), 1.0, "ok", None, hostil("r1", tarea='t" onclick="x')),
        ("r-x4", "bronze_download", "bronze", "cnmc_indicadores", "u", "d", None, 1, ts(6, 6), ts(6, 7), 1.0, "ok", None, hostil("r1", idx="2;<script>")),
        ("r-dl", "bronze_download", "bronze", "cnmc_indicadores", "https://x/e.csv", "/stg/e.csv", None, 900, ts(8), ts(8, 1), 1.0, "ok", None, airflow),
        ("r-l2", "bronze_l2_parquet_converter", "bronze", "crtm/stops", "l1", "l2/crtm/stops", 97, 1, ts(8, 5), ts(8, 6), 1.0, "ok", None, None),
        ("r-l2", "bronze_l2_parquet_converter", "bronze", "crtm/routes", "l1", "l2/crtm/routes", 10, 1, ts(8, 5), ts(8, 7), 2.0, "ok", None, None),
        ("r-s1", "silver_builder", "silver", "cnmc_trimestral", "l2", "silver", 500, 1, ts(9), ts(9, 1), 3.0, "ok", None, None),
        ("r-s2", "silver_builder", "silver", "cnmc_trimestral", "l2", "silver", 554, 1, ts(11), ts(11, 1), 4.0, "error", "cuarentena: gate roto", None),
        ("r-g1", "gold_build", "gold", "fact_mercado_trimestral", "s3a://raillytics-silver", "gold", 112, 1, ts(12), ts(12, 1), 5.0, "ok", None, None),
        ("r-p1", "prediccion_demanda", "ml", "prediccion_demanda_ave_mad_bcn", "mistral-nemo · eventos_v1", "x.csv", 92, 1, ts(13), ts(13, 1), 63.0, "ok", None, None),
        ("r-q", "quality_gates", "silver", None, None, None, None, None, ts(14), ts(14, 1), 1.0, "ok", None, None),
    ])
    gates = [
        ("g1", "silver_builder", "silver", "silver_cnmc_trimestral", "filas_minimas", "filas_min", "bloqueante", "ok", ts(9)),
        ("g2", "silver_builder", "silver", "silver_cnmc_trimestral", "grano_unico", "unico", "bloqueante", "fallo", ts(11)),
        ("g2", "silver_builder", "silver", "silver_cnmc_trimestral", "serie_actualizada", "sql", "aviso", "fallo", ts(11)),
        ("g3", "silver_builder", "silver", "silver_cnmc_trimestral", "grano_unico", "unico", "bloqueante", "ok", ts(7)),   # más antigua: no cuenta
        ("g4", "bronze_download", "bronze", "cnmc_indicadores", "tamano_minimo", "fichero", "bloqueante", "ok", ts(8)),
        ("g5", "gold_build", "gold", "gold_fact_mercado_trimestral", "filas_minimas", "filas_min", "bloqueante", "ok", ts(12)),
        ("g6", "gold_build", "gold", "gold_fact_mercado_trimestral", "grano_unico", "unico", "bloqueante", "fallo", ts(12)),
    ]
    valores = ", ".join(f"('{r}', '{p}', '{c}', '{t}', '{g}', '{ti}', '{s}', '{res}', 1.0, '>= 1', NULL, TIMESTAMP '{i}')" for r, p, c, t, g, ti, s, res, i in gates)
    con.execute(f"COPY (SELECT * FROM (VALUES {valores}) t(run_id, proceso, capa, tabla, gate, tipo, severidad, resultado, valor, umbral, detalle, inicio)) "
                f"TO '{(tmp_path / '_trazabilidad/calidad/q.parquet').as_posix()}' (FORMAT PARQUET)")

    def consulta(nombre, columnas="*", resto=""):
        return con.execute(f"SELECT {columnas} FROM ({_sql(nombre, tmp_path)}) t {resto}").fetchall()

    consulta.columnas = lambda nombre: [d[0] for d in con.execute(f"SELECT * FROM ({_sql(nombre, tmp_path)}) t LIMIT 0").description]
    return consulta


# ---------------------------------------------------------------------------------------------- coherencia

@pytest.mark.parametrize("nombre", sorted(DATASETS))
def test_las_columnas_declaradas_son_exactamente_las_que_devuelve_el_sql(lake, nombre):
    declaradas = [c["column_name"] for c in DATASETS[nombre]["columns"]]

    assert declaradas == lake.columnas(nombre)


def test_toda_pareja_proceso_tabla_que_se_registra_cae_en_un_nodo_del_grafo(tmp_path):
    ids = {n.id for n in GRAFO.nodos}
    con = duckdb.connect()
    filas = ", ".join(f"('{p}', '{t}')" for p, t in CARGAS_REALES)
    expr = EXPR_NODO_CARGA

    nodos = con.execute(f"SELECT proceso, tabla, {expr} AS nodo FROM (VALUES {filas}) t(proceso, tabla)").fetchall()

    assert [(p, t) for p, t, n in nodos if n not in ids] == []


def test_todo_gate_que_se_registra_cae_en_un_nodo_del_grafo():
    ids = {n.id for n in GRAFO.nodos}
    filas = ", ".join(f"('{p}', '{t}')" for p, t in GATES_REALES)
    expr = EXPR_NODO_GATE

    nodos = duckdb.connect().execute(f"SELECT proceso, tabla, {expr} AS nodo FROM (VALUES {filas}) t(proceso, tabla)").fetchall()

    assert [(p, t) for p, t, n in nodos if n not in ids] == []


# ---------------------------------------------------------------------------------------------- nodos

def test_hay_una_fila_por_nodo_declarado(lake):
    assert lake("lineage_nodos", "count(*), count(DISTINCT nodo)") == [(len(GRAFO.nodos), len(GRAFO.nodos))]


def test_la_ultima_ejecucion_manda_y_un_error_o_un_gate_bloqueante_ponen_el_nodo_en_error(lake):
    fila = lake("lineage_nodos", "estado, ultimas_filas, n_cargas, n_errores, ultimo_error", "WHERE nodo = 'silver:cnmc_trimestral'")[0]

    assert fila == ("error", 554, 2, 1, "cuarentena: gate roto")


def test_los_gates_cuentan_solo_su_ultima_evaluacion(lake):
    fila = lake("lineage_nodos", "gates_total, gates_ok, gates_fallidos, gates_aviso", "WHERE nodo = 'silver:cnmc_trimestral'")[0]

    assert fila == (3, 1, 1, 1)   # filas_minimas ok, grano_unico fallo bloqueante (la de las 07:00 es antigua), serie_actualizada aviso


def test_un_nodo_con_gate_bloqueante_fallido_esta_en_error_aunque_su_carga_fuera_bien(lake):
    # La carga de gold:fact_mercado_trimestral terminó «ok», pero su regla de grano único (bloqueante) falló.
    assert lake("lineage_aristas", "estado", "WHERE destino = 'gold:fact_mercado_trimestral'") == [("ok",)]
    assert lake("lineage_nodos", "estado, gates_fallidos", "WHERE nodo = 'gold:fact_mercado_trimestral'") == [("error", 1)]


def test_los_ficheros_de_un_zip_se_agrupan_en_el_nodo_l2_de_su_fuente_y_suman_filas(lake):
    assert lake("lineage_nodos", "ultimas_filas, estado", "WHERE nodo = 'l2:crtm'") == [(107, "ok")]


def test_un_nodo_trazable_que_nunca_se_ha_ejecutado_figura_sin_ejecutar_y_los_externos_no_aplican(lake):
    assert lake("lineage_nodos", "estado", "WHERE nodo = 'silver:cnmc_precio_mensual'") == [("sin ejecutar",)]
    assert {e for (e,) in lake("lineage_nodos", "DISTINCT estado", "WHERE capa IN ('Fuente', 'Dashboard', 'Dataset de Superset', 'Modelo LLM')")} == {"no aplica"}


def test_el_gate_de_la_descarga_se_asigna_al_staging_de_la_fuente(lake):
    assert lake("lineage_nodos", "gates_total, estado", "WHERE nodo = 'staging:cnmc_indicadores'") == [(1, "ok")]


# ---------------------------------------------------------------------------------------------- aristas

def test_hay_una_fila_por_arista_declarada_con_su_proceso_y_su_transformacion(lake):
    assert lake("lineage_aristas", "count(*)")[0][0] == len(GRAFO.aristas)
    fila = lake("lineage_aristas", "categoria_origen, categoria_destino, proceso, transformacion",
                "WHERE origen = 'l2:cnmc_indicadores' AND destino = 'silver:cnmc_trimestral'")[0]
    assert fila[:3] == ("Bronze L2", "Silver", "silver_builder") and "silver/cnmc_trimestral.sql" in fila[3]


def test_la_arista_toma_el_estado_y_las_filas_de_la_ultima_carga_de_su_destino(lake):
    fila = lake("lineage_aristas", "estado, filas", "WHERE destino = 'silver:cnmc_trimestral' AND proceso = 'silver_builder'")[0]

    assert fila == ("error", 554)


def test_el_peso_crece_con_las_filas_pero_nunca_es_cero(lake):
    pesos = dict(lake("lineage_aristas", "destino, peso", "WHERE proceso IN ('silver_builder', 'bronze_download')"))

    assert pesos["silver:cnmc_precio_mensual"] == 1.0                 # sin filas
    assert pesos["silver:cnmc_trimestral"] > 6                        # ln(555) ≈ 6,3 sumado a 1
    assert all(p >= 1 for p in pesos.values())


# ---------------------------------------------------------------------------------------------- cargas (zoom por ejecución)

def test_una_ejecucion_de_gold_se_expande_en_un_salto_por_cada_dependencia_de_su_tabla(lake):
    saltos = lake("lineage_cargas", "origen, destino", "WHERE run_id = 'r-g1'")

    assert saltos == [("silver:cnmc_trimestral", "gold:fact_mercado_trimestral")]


def test_el_salto_lleva_el_error_de_su_ejecucion(lake):
    assert lake("lineage_cargas", "estado, error", "WHERE run_id = 'r-s2'") == [("error", "cuarentena: gate roto")]


def test_la_descarga_de_airflow_lleva_un_enlace_al_log_de_su_tarea(lake):
    enlace = lake("lineage_cargas", "enlace_log", "WHERE run_id = 'r-dl'")[0][0]

    assert enlace.startswith(f'<a href="{AIRFLOW_URL_POR_DEFECTO}/dags/ingesta_data_sources/runs/manual__2026-10-06T10%3A00%3A00%2B00%3A00')
    assert "/tasks/download_source" in enlace and "mapped/2" in enlace and 'target="_blank"' in enlace


def test_las_cargas_que_no_son_de_airflow_no_tienen_enlace(lake):
    assert lake("lineage_cargas", "enlace_log", "WHERE run_id = 'r-s1'") == [(None,)]


# ---------------------------------------------------------------------------------------------- reglas

def test_una_regla_declarada_que_no_se_ha_evaluado_figura_sin_evaluar(lake):
    fila = lake("lineage_reglas", "resultado, severidad", "WHERE nodo = 'silver:cnmc_precio_mensual' AND gate = 'filas_minimas'")[0]

    assert fila == ("sin evaluar", "bloqueante")


def test_una_regla_evaluada_muestra_su_ultimo_resultado_y_su_descripcion_declarada(lake):
    fila = lake("lineage_reglas", "resultado, regla, tipo", "WHERE nodo = 'silver:cnmc_trimestral' AND gate = 'grano_unico'")[0]

    assert fila[0] == "fallo" and "sin duplicados por" in fila[1] and fila[2] == "unico"


def test_un_gate_que_solo_existe_en_codigo_aparece_aunque_no_este_en_el_yaml(lake):
    fila = lake("lineage_reglas", "resultado, tipo", "WHERE nodo = 'staging:cnmc_indicadores' AND gate = 'tamano_minimo'")[0]

    assert fila == ("ok", "fichero")


def test_el_airflow_se_configura_y_no_se_ata_a_localhost():
    otro = construir_datasets(GRAFO, "http://airflow.interno:9999")

    assert "http://airflow.interno:9999/dags/" in otro["lineage_cargas"]["sql"]


# ---------------------------------------------------------------------------------------------- el enlace no se arma con datos hostiles

@pytest.mark.parametrize("run", ["r-x1", "r-x2", "r-x3"])
def test_un_run_dag_o_tarea_con_caracteres_de_html_no_genera_enlace(lake, run):
    assert lake("lineage_cargas", "enlace_log", f"WHERE run_id = '{run}'") == [(None,)]


def test_un_indice_de_tarea_que_no_es_un_numero_no_se_cuela_en_la_ruta(lake):
    enlace = lake("lineage_cargas", "enlace_log", "WHERE run_id = 'r-x4'")[0][0]

    assert enlace is None or "<script>" not in enlace
    assert enlace is None or "mapped/" not in enlace


def test_un_enlace_legitimo_solo_contiene_caracteres_seguros(lake):
    import re
    enlace = lake("lineage_cargas", "enlace_log", "WHERE run_id = 'r-dl'")[0][0]
    href = re.search(r'href="([^"]+)"', enlace).group(1)

    assert re.fullmatch(r"[A-Za-z0-9_./:%-]+", href)


@pytest.mark.parametrize("url", [
    'http://x"onmouseover="a', "javascript:alert(1)", "http://a b", "http://x/<script>", "ftp://airflow", "", "http://x'--",
])
def test_la_url_de_airflow_se_valida_al_generar(url):
    with pytest.raises(ValueError, match="AIRFLOW_UI_URL"):
        construir_datasets(GRAFO, url)


@pytest.mark.parametrize("url", ["http://localhost:8080", "https://airflow.interno:9999/", "http://airflow.interno/base/ruta"])
def test_urls_de_airflow_razonables_se_aceptan(url):
    assert construir_datasets(GRAFO, url)["lineage_cargas"]["sql"]
