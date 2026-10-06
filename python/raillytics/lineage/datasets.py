"""Datasets de Superset del dashboard de lineage: cruzan el grafo declarado con la trazabilidad real del lake.

El grafo declarado (raillytics.lineage.grafo) se incrusta en el SQL como listas `VALUES`; lo observado se lee de
`s3://raillytics-gold/_trazabilidad/{cargas,calidad}/` (raillytics.utils.cargas y raillytics.calidad). Un fichero YAML por dataset
se genera con `python -m raillytics.lineage` (`make lineage`) y un test comprueba que lo versionado coincide con el grafo actual.

  lineage_nodos     un nodo por fila: estado de su última carga, gates y errores (tablas y KPIs)
  lineage_aristas   un salto por fila con el estado/filas de la última carga de su destino (el mapa global: Sankey y Graph)
  lineage_cargas    un salto por (ejecución, salto) observado, con error y enlace al log de Airflow (el zoom por run_id)
  lineage_reglas    cada regla de calidad, declarada u observada, con su último resultado

Cada app registra sus tablas de una forma (`crtm/stops`, `silver_cnmc_trimestral`, …): EXPR_NODO_CARGA y EXPR_NODO_GATE las llevan al id
del nodo. Los tests comprueban que toda pareja (proceso, tabla) real cae en un nodo existente.
"""
from __future__ import annotations

import re
import uuid
from typing import Any

from raillytics.lineage.grafo import CAPAS, Grafo
from raillytics.prediccion.publicacion import GOLD_TABLA as GOLD_PREDICCION

AIRFLOW_URL_POR_DEFECTO = "http://localhost:8080"
DATABASE_UUID = "c34b683c-9913-5c2c-b6a6-7d986acb824b"   # Raillytics Gold (DuckDB)
CARGAS_PARQUET = "s3://raillytics-gold/_trazabilidad/cargas/*.parquet"
CALIDAD_PARQUET = "s3://raillytics-gold/_trazabilidad/calidad/*.parquet"
# Capas cuyos nodos aparecen en la trazabilidad (los demás son externos al lake: la fuente, el generador, el modelo, Superset).
CAPAS_TRAZABLES = ("staging", "l1", "l2", "silver", "pred", "gold")

# (proceso, tabla) de la tabla `cargas` -> id del nodo. Un ZIP de L2 registra una fila por fichero (`crtm/stops`): se agrupan en la fuente.
EXPR_NODO_CARGA = f"""CASE proceso
    WHEN 'bronze_download' THEN 'staging:' || tabla
    WHEN 'bronze_l1_raw_uploader' THEN 'l1:' || tabla
    WHEN 'bronze_l2_parquet_converter' THEN 'l2:' || split_part(tabla, '/', 1)
    WHEN 'silver_builder' THEN 'silver:' || tabla
    WHEN 'silver_sample' THEN 'silver:' || tabla
    WHEN 'gold_build' THEN 'gold:' || tabla
    WHEN 'prediccion_muestra' THEN 'l2:muestra_' || tabla
    WHEN 'prediccion_demanda' THEN CASE WHEN tabla = '{GOLD_PREDICCION}' THEN 'gold:' ELSE 'pred:' END || tabla
  END"""

# (proceso, tabla) de la tabla `calidad` -> id del nodo. Los gates de Silver los evalúan varios procesos (silver_builder, gold_build,
# quality_gates) sobre `silver_<tabla>`; los de Gold sobre `gold_<tabla>`.
EXPR_NODO_GATE = """CASE
    WHEN proceso = 'bronze_download' THEN 'staging:' || tabla
    WHEN proceso = 'bronze_l1_raw_uploader' THEN 'l1:' || tabla
    WHEN proceso = 'bronze_l2_parquet_converter' THEN 'l2:' || split_part(tabla, '/', 1)
    WHEN proceso = 'prediccion_demanda' THEN 'pred:' || tabla
    WHEN starts_with(tabla, 'silver_') THEN 'silver:' || substr(tabla, 8)
    WHEN starts_with(tabla, 'gold_') THEN 'gold:' || substr(tabla, 6)
  END"""

_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "raillytics/lineage")


def _q(valor: Any) -> str:
    if valor is None:
        return "NULL"
    if isinstance(valor, bool):
        return "true" if valor else "false"
    if isinstance(valor, (int, float)):
        return str(valor)
    return "'" + str(valor).replace("'", "''") + "'"


def _values(filas: list[tuple]) -> str:
    return ",\n    ".join("(" + ", ".join(_q(v) for v in fila) + ")" for fila in filas)


def _cte_nodos(g: Grafo) -> str:
    filas = [(n.id, CAPAS[n.capa][1], CAPAS[n.capa][0], n.nombre, n.descripcion, n.ruta, n.capa in CAPAS_TRAZABLES) for n in g.nodos]
    return f"nodos(nodo, capa, orden_capa, nombre, descripcion, ruta, trazable) AS (VALUES\n    {_values(filas)})"


def _cte_aristas(g: Grafo) -> str:
    filas = [(a.origen, a.destino, a.proceso, a.transformacion, a.ruta) for a in g.aristas]
    return f"aristas(origen, destino, proceso, transformacion, ruta) AS (VALUES\n    {_values(filas)})"


def _cte_reglas(g: Grafo) -> str:
    filas = [(r.nodo, r.gate, r.tipo, r.severidad, r.descripcion) for r in g.reglas]
    return f"declaradas(nodo, gate, tipo, severidad, descripcion) AS (VALUES\n    {_values(filas)})"


_CTE_CARGAS = f"""carga AS (
    SELECT {EXPR_NODO_CARGA} AS nodo, run_id, proceso, estado, filas, duracion_s, error, parametros,
           timezone('Europe/Madrid', timezone('UTC', inicio)) AS inicio
    FROM read_parquet('{CARGAS_PARQUET}', union_by_name = true)
),
-- Una fila por (nodo, ejecución): un ZIP de L2 registra un fichero por fila y se suman sus filas.
por_run AS (
    SELECT nodo, run_id, any_value(proceso) AS proceso, min(inicio) AS inicio, sum(filas) AS filas, sum(duracion_s) AS duracion_s,
           CASE WHEN bool_or(estado = 'error') THEN 'error' ELSE 'ok' END AS estado,
           string_agg(DISTINCT error, ' | ') AS error, any_value(parametros) AS parametros
    FROM carga WHERE nodo IS NOT NULL GROUP BY nodo, run_id
),
ultimo AS (SELECT * FROM por_run QUALIFY row_number() OVER (PARTITION BY nodo ORDER BY inicio DESC) = 1),
ultimo_proceso AS (SELECT * FROM por_run QUALIFY row_number() OVER (PARTITION BY nodo, proceso ORDER BY inicio DESC) = 1),
historico AS (
    SELECT nodo, count(*) AS n_cargas, sum(CASE WHEN estado = 'error' THEN 1 ELSE 0 END) AS n_errores FROM por_run GROUP BY nodo
)"""

_CTE_GATES = f"""gate AS (
    SELECT {EXPR_NODO_GATE} AS nodo, gate, tipo, severidad, resultado, valor, umbral, detalle, run_id,
           timezone('Europe/Madrid', timezone('UTC', inicio)) AS inicio
    FROM read_parquet('{CALIDAD_PARQUET}', union_by_name = true)
),
-- Solo cuenta la ÚLTIMA evaluación de cada regla de cada nodo.
gate_ultima AS (SELECT * FROM gate WHERE nodo IS NOT NULL QUALIFY row_number() OVER (PARTITION BY nodo, gate ORDER BY inicio DESC) = 1),
resumen_gates AS (
    SELECT nodo, count(*) AS gates_total,
           sum(CASE WHEN resultado = 'ok' THEN 1 ELSE 0 END) AS gates_ok,
           sum(CASE WHEN resultado <> 'ok' AND severidad = 'bloqueante' THEN 1 ELSE 0 END) AS gates_fallidos,
           sum(CASE WHEN resultado <> 'ok' AND severidad <> 'bloqueante' THEN 1 ELSE 0 END) AS gates_aviso
    FROM gate_ultima GROUP BY nodo
)"""

_PESO = "round(1 + ln(1 + coalesce({f}, 0)), 3)"   # el grosor de un salto crece con las filas, pero nunca es cero y no aplasta a los pequeños


# Caracteres que puede llevar cada componente del enlace. Los valores salen de `parametros.airflow` (y un run_id lo elige quien dispara el DAG),
# y la tabla que muestra el enlace lo renderiza como HTML: sin esta lista blanca, un valor con `"` o `<` se saldría del atributo href.
_COMPONENTE_RUN = "[A-Za-z0-9_.:+-]+"
_COMPONENTE_ID = "[A-Za-z0-9_.-]+"
_URL_AIRFLOW = re.compile(r"https?://[A-Za-z0-9._-]+(:[0-9]{1,5})?(/[A-Za-z0-9._~/-]*)?")


def _validar_url_airflow(url: str) -> str:
    url = url.rstrip("/")
    if not _URL_AIRFLOW.fullmatch(url):
        raise ValueError(f"AIRFLOW_UI_URL {url!r} no es una URL http(s) simple (solo letras, dígitos y . _ - / : ); se incrusta en HTML")
    return url


def _enlace_airflow(url: str) -> str:
    p = lambda clave: f"json_extract_string(r.parametros, '$.airflow.{clave}')"   # noqa: E731
    seguro = (f"regexp_full_match({p('run_id')}, '{_COMPONENTE_RUN}') AND regexp_full_match({p('dag_id')}, '{_COMPONENTE_ID}') "
              f"AND regexp_full_match({p('task_id')}, '{_COMPONENTE_ID}')")
    run = f"replace(replace({p('run_id')}, '+', '%2B'), ':', '%3A')"
    indice = f"try_cast({p('map_index')} AS INTEGER)"   # un entero o nada: el texto original nunca llega a la ruta
    mapeada = f"CASE WHEN {indice} >= 0 THEN '/mapped/' || CAST({indice} AS VARCHAR) ELSE '' END"
    return (f"CASE WHEN {seguro} THEN '<a href=\"{url}/dags/' || {p('dag_id')} || '/runs/' || {run} || "
            f"'/tasks/' || {p('task_id')} || {mapeada} || '\" target=\"_blank\" rel=\"noopener noreferrer\">log de Airflow</a>' END")


def _sql_nodos(g: Grafo) -> str:
    return f"""-- Lineage · un nodo por fila (grafo declarado en el repositorio + última carga y gates observados en el lake).
-- Se genera con `make lineage` (raillytics.lineage): no lo edites a mano.
WITH {_cte_nodos(g)},
{_CTE_CARGAS},
{_CTE_GATES}
SELECT
    n.nodo, n.capa, n.orden_capa, n.nombre, n.descripcion, n.ruta,
    CASE
        WHEN NOT n.trazable THEN 'no aplica'
        WHEN u.nodo IS NULL AND g.nodo IS NULL THEN 'sin ejecutar'
        WHEN u.estado = 'error' OR coalesce(g.gates_fallidos, 0) > 0 THEN 'error'
        WHEN coalesce(g.gates_aviso, 0) > 0 THEN 'aviso'
        ELSE 'ok'
    END AS estado,
    u.inicio AS ultima_carga, u.run_id AS ultimo_run, u.filas AS ultimas_filas, u.duracion_s, u.error AS ultimo_error,
    coalesce(h.n_cargas, 0) AS n_cargas, coalesce(h.n_errores, 0) AS n_errores,
    coalesce(g.gates_total, 0) AS gates_total, coalesce(g.gates_ok, 0) AS gates_ok,
    coalesce(g.gates_fallidos, 0) AS gates_fallidos, coalesce(g.gates_aviso, 0) AS gates_aviso
FROM nodos n
LEFT JOIN ultimo u ON u.nodo = n.nodo
LEFT JOIN historico h ON h.nodo = n.nodo
LEFT JOIN resumen_gates g ON g.nodo = n.nodo
ORDER BY n.orden_capa, n.nodo"""


def _sql_aristas(g: Grafo) -> str:
    return f"""-- Lineage · un salto por fila: de qué nodo a cuál, con qué proceso y transformación, y el estado de la última carga de su destino.
-- Se genera con `make lineage` (raillytics.lineage): no lo edites a mano.
WITH {_cte_nodos(g)},
{_cte_aristas(g)},
{_CTE_CARGAS}
SELECT
    a.origen, a.destino, o.capa AS categoria_origen, d.capa AS categoria_destino, a.proceso, a.transformacion, a.ruta,
    CASE WHEN NOT d.trazable THEN 'no aplica' WHEN u.nodo IS NULL THEN 'sin ejecutar' ELSE u.estado END AS estado,
    u.filas, u.inicio AS ultima_carga, u.run_id AS ultimo_run,
    {_PESO.format(f="u.filas")} AS peso
FROM aristas a
JOIN nodos o ON o.nodo = a.origen
JOIN nodos d ON d.nodo = a.destino
LEFT JOIN ultimo_proceso u ON u.nodo = a.destino AND u.proceso = a.proceso
ORDER BY o.orden_capa, a.origen, a.destino"""


def _sql_cargas(g: Grafo, airflow_url: str) -> str:
    return f"""-- Lineage · cada salto de cada ejecución observada, con su error y, si la hizo Airflow, el enlace al log de su tarea.
-- Un proceso que registra una tabla (p. ej. Gold) se expande en un salto por cada tabla de la que depende.
-- Se genera con `make lineage` (raillytics.lineage): no lo edites a mano.
WITH {_cte_nodos(g)},
{_cte_aristas(g)},
{_CTE_CARGAS}
SELECT
    r.run_id, r.proceso, a.origen, a.destino, o.capa AS categoria_origen, d.capa AS categoria_destino, a.transformacion,
    r.estado, r.filas, r.inicio, r.duracion_s, r.error,
    {_enlace_airflow(airflow_url)} AS enlace_log,
    {_PESO.format(f="r.filas")} AS peso
FROM por_run r
JOIN aristas a ON a.destino = r.nodo AND a.proceso = r.proceso
JOIN nodos o ON o.nodo = a.origen
JOIN nodos d ON d.nodo = a.destino
ORDER BY r.inicio DESC, a.destino, a.origen"""


def _sql_reglas(g: Grafo) -> str:
    return f"""-- Lineage · cada regla de calidad con su último resultado: las declaradas en config/quality_gates.yml (aunque nunca se hayan evaluado)
-- y las que solo existen en código (los gates de fichero de la descarga, el de L1 y los de L2).
-- Se genera con `make lineage` (raillytics.lineage): no lo edites a mano.
WITH {_cte_reglas(g)},
{_CTE_GATES}
SELECT
    coalesce(d.nodo, q.nodo) AS nodo,
    coalesce(d.gate, q.gate) AS gate,
    coalesce(d.tipo, q.tipo) AS tipo,
    coalesce(d.severidad, q.severidad) AS severidad,
    coalesce(d.descripcion, '(regla definida en código)') AS regla,
    coalesce(q.resultado, 'sin evaluar') AS resultado,
    q.valor, q.umbral, q.detalle, q.inicio AS evaluada, q.run_id
FROM declaradas d
FULL OUTER JOIN gate_ultima q ON q.nodo = d.nodo AND q.gate = d.gate
ORDER BY 1, 2"""


# ------------------------------------------------------------------------------------------------ definición de cada dataset

def _col(nombre: str, verbose: str, tipo: str = "VARCHAR", fecha: bool = False) -> dict:
    return {"column_name": nombre, "verbose_name": verbose, "is_dttm": fecha, "is_active": True, "type": tipo, "advanced_data_type": None,
            "groupby": True, "filterable": True, "expression": None, "description": None, "python_date_format": None, "extra": None}


def _metrica(nombre: str, verbose: str, expresion: str, formato: str = ",d") -> dict:
    return {"metric_name": nombre, "verbose_name": verbose, "metric_type": None, "expression": expresion, "description": None,
            "d3format": formato, "extra": None, "warning_text": None}


def _dataset(nombre: str, descripcion: str, sql: str, columnas: list[dict], metricas: list[dict], dttm: str | None) -> dict:
    sql = "\n".join(linea.rstrip() for linea in sql.splitlines())
    return {
        "table_name": nombre, "main_dttm_col": dttm, "description": descripcion, "default_endpoint": None, "offset": 0,
        "cache_timeout": None, "schema": None, "catalog": None, "sql": sql, "params": None, "template_params": None,
        "filter_select_enabled": True, "fetch_values_predicate": None, "extra": None, "normalize_columns": False,
        "always_filter_main_dttm": False, "uuid": str(uuid.uuid5(_NAMESPACE, nombre)), "metrics": metricas, "columns": columnas,
        "version": "1.0.0", "database_uuid": DATABASE_UUID,
    }


def construir_datasets(g: Grafo, airflow_url: str = AIRFLOW_URL_POR_DEFECTO) -> dict[str, dict]:
    airflow_url = _validar_url_airflow(airflow_url)
    nodos = [
        _col("nodo", "Nodo"), _col("capa", "Capa"), _col("orden_capa", "Orden de la capa", "INTEGER"), _col("nombre", "Nombre"),
        _col("descripcion", "Qué es"), _col("ruta", "Fichero o URL"), _col("estado", "Estado"),
        _col("ultima_carga", "Última carga", "TIMESTAMP", True), _col("ultimo_run", "Última ejecución"),
        _col("ultimas_filas", "Filas de la última carga", "BIGINT"), _col("duracion_s", "Duración (s)", "DOUBLE"),
        _col("ultimo_error", "Último error"), _col("n_cargas", "Cargas", "BIGINT"), _col("n_errores", "Cargas con error", "BIGINT"),
        _col("gates_total", "Gates", "BIGINT"), _col("gates_ok", "Gates OK", "BIGINT"),
        _col("gates_fallidos", "Gates bloqueantes fallidos", "BIGINT"), _col("gates_aviso", "Gates con aviso", "BIGINT"),
    ]
    saltos = [
        _col("origen", "Origen"), _col("destino", "Destino"), _col("categoria_origen", "Capa de origen"),
        _col("categoria_destino", "Capa de destino"), _col("proceso", "Proceso"), _col("transformacion", "Transformación"),
        _col("ruta", "Fichero de la transformación"), _col("estado", "Estado"), _col("filas", "Filas", "BIGINT"),
        _col("ultima_carga", "Última carga", "TIMESTAMP", True), _col("ultimo_run", "Última ejecución"),
        _col("peso", "Grosor", "DOUBLE"),
    ]
    cargas = [
        _col("run_id", "Ejecución"), _col("proceso", "Proceso"), _col("origen", "Origen"), _col("destino", "Destino"),
        _col("categoria_origen", "Capa de origen"), _col("categoria_destino", "Capa de destino"),
        _col("transformacion", "Transformación"), _col("estado", "Estado"), _col("filas", "Filas", "BIGINT"),
        _col("inicio", "Inicio", "TIMESTAMP", True), _col("duracion_s", "Duración (s)", "DOUBLE"), _col("error", "Error"),
        _col("enlace_log", "Log"), _col("peso", "Grosor", "DOUBLE"),
    ]
    reglas = [
        _col("nodo", "Nodo"), _col("gate", "Regla"), _col("tipo", "Tipo"), _col("severidad", "Severidad"),
        _col("regla", "Qué comprueba"), _col("resultado", "Resultado"), _col("valor", "Valor", "DOUBLE"), _col("umbral", "Umbral"),
        _col("detalle", "Detalle"), _col("evaluada", "Última evaluación", "TIMESTAMP", True), _col("run_id", "Ejecución"),
    ]
    return {
        "lineage_nodos": _dataset(
            "lineage_nodos",
            "Un nodo del lineage por fila (fuente, staging, L1, L2, Silver, Gold, predicción, dataset, dashboard) con el estado de su última carga.",
            _sql_nodos(g), nodos,
            [_metrica("n_nodos", "Nodos", "COUNT(*)"),
             _metrica("n_con_error", "Nodos con error", "SUM(CASE WHEN estado = 'error' THEN 1 ELSE 0 END)"),
             _metrica("n_sin_ejecutar", "Nodos sin ejecutar", "SUM(CASE WHEN estado = 'sin ejecutar' THEN 1 ELSE 0 END)"),
             _metrica("n_gates_fallidos", "Gates bloqueantes fallidos", "SUM(gates_fallidos)")],
            "ultima_carga"),
        "lineage_aristas": _dataset(
            "lineage_aristas",
            "Mapa global del lineage: un salto entre nodos por fila, con el estado de la última carga de su destino.",
            _sql_aristas(g), saltos,
            [_metrica("n_saltos", "Saltos", "COUNT(*)"), _metrica("peso_total", "Grosor", "SUM(peso)", ",.2f")],
            "ultima_carga"),
        "lineage_cargas": _dataset(
            "lineage_cargas",
            "Cada salto de cada ejecución observada (zoom por run_id), con su error y el enlace al log de Airflow.",
            _sql_cargas(g, airflow_url), cargas,
            [_metrica("n_saltos", "Saltos", "COUNT(*)"), _metrica("peso_total", "Grosor", "SUM(peso)", ",.2f"),
             _metrica("n_errores", "Saltos con error", "SUM(CASE WHEN estado = 'error' THEN 1 ELSE 0 END)")],
            "inicio"),
        "lineage_reglas": _dataset(
            "lineage_reglas",
            "Reglas de calidad de cada nodo con su último resultado; las declaradas que nunca se evaluaron figuran «sin evaluar».",
            _sql_reglas(g), reglas,
            [_metrica("n_reglas", "Reglas", "COUNT(*)"),
             _metrica("n_fallidas", "Reglas fallidas", "SUM(CASE WHEN resultado NOT IN ('ok', 'sin evaluar') THEN 1 ELSE 0 END)"),
             _metrica("n_sin_evaluar", "Reglas sin evaluar", "SUM(CASE WHEN resultado = 'sin evaluar' THEN 1 ELSE 0 END)")],
            "evaluada"),
    }
