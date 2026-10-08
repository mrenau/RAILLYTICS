"""Datasets de Superset para el Catálogo de Datos y Glosario de Términos."""
from __future__ import annotations

import uuid
from typing import Any

from raillytics.catalog.loader import CatalogoGlosario

DATABASE_UUID = "c34b683c-9913-5c2c-b6a6-7d986acb824b"   # Raillytics Gold (DuckDB)
_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "raillytics/catalog")


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


def _col(nombre: str, verbose: str, tipo: str = "VARCHAR", fecha: bool = False) -> dict:
    return {
        "column_name": nombre,
        "verbose_name": verbose,
        "is_dttm": fecha,
        "is_active": True,
        "type": tipo,
        "advanced_data_type": None,
        "groupby": True,
        "filterable": True,
        "expression": None,
        "description": None,
        "python_date_format": None,
        "extra": None,
    }


def _metrica(nombre: str, verbose: str, expresion: str, formato: str = ",d") -> dict:
    return {
        "metric_name": nombre,
        "verbose_name": verbose,
        "metric_type": None,
        "expression": expresion,
        "description": None,
        "d3format": formato,
        "extra": None,
        "warning_text": None,
    }


def _dataset(nombre: str, descripcion: str, sql: str, columnas: list[dict], metricas: list[dict], dttm: str | None = None) -> dict:
    sql = "\n".join(linea.rstrip() for linea in sql.splitlines())
    return {
        "table_name": nombre,
        "main_dttm_col": dttm,
        "description": descripcion,
        "default_endpoint": None,
        "offset": 0,
        "cache_timeout": None,
        "schema": None,
        "catalog": None,
        "sql": sql,
        "params": None,
        "template_params": None,
        "filter_select_enabled": True,
        "fetch_values_predicate": None,
        "extra": None,
        "normalize_columns": False,
        "always_filter_main_dttm": False,
        "uuid": str(uuid.uuid5(_NAMESPACE, nombre)),
        "metrics": metricas,
        "columns": columnas,
        "version": "1.0.0",
        "database_uuid": DATABASE_UUID,
    }


def _link_dash(dash: str) -> str:
    return f'<a href="/superset/dashboard/{dash}/" target="_blank" style="display:inline-block;background:#2b6cb0;color:#ffffff;padding:2px 7px;margin:2px 3px;border-radius:4px;text-decoration:none;font-size:11px;font-weight:600;">📊 {dash}</a>'


def _badge_gate(gate: str) -> str:
    return f'<span style="display:inline-block;background:#f7fafc;color:#4a5568;padding:2px 6px;margin:2px 3px;border-radius:4px;font-size:11px;border:1px solid #cbd5e0;">🛡️ {gate}</span>'


def _badge_term(term: str) -> str:
    return f'<span style="display:inline-block;background:#e6fffa;color:#234e52;padding:2px 6px;margin:2px 3px;border-radius:4px;font-size:11px;border:1px solid #b2f5ea;font-weight:600;">📖 {term}</span>'


def _badge_table(t: str) -> str:
    return f'<span style="display:inline-block;background:#feebc8;color:#744210;padding:2px 6px;margin:2px 3px;border-radius:4px;font-size:11px;border:1px solid #fbd38d;">🗄️ {t}</span>'


def _badge_downstream(item: str) -> str:
    if item.startswith("dashboard:"):
        dash = item.split(":", 1)[1]
        return _link_dash(dash)
    return f'<span style="display:inline-block;background:#edf2f7;color:#2d3748;padding:2px 6px;margin:2px 3px;border-radius:4px;font-size:11px;">{item}</span>'


def sql_data_catalog(cat: CatalogoGlosario) -> str:
    filas = [
        (
            t.tabla,
            t.nombre,
            t.capa,
            t.orden_capa,
            t.dominio,
            t.descripcion,
            t.grano,
            ", ".join(t.claves) if t.claves else "-",
            t.formato,
            t.proceso or "-",
            t.frecuencia or "-",
            ", ".join(_badge_gate(g) for g in t.quality_gates) if t.quality_gates else "-",
            ", ".join(t.upstream) if t.upstream else "-",
            ", ".join(_badge_downstream(d) for d in t.downstream) if t.downstream else "-",
            ", ".join(_badge_term(term) for term in t.terminos_glosario) if t.terminos_glosario else "-",
            len(t.columnas),
        )
        for t in cat.tablas
    ]
    return f"""-- Catálogo de Datos de Raillytics: datasets declarados en config/data_catalog.yml
-- Se genera con `make catalog` (raillytics.catalog): no lo edites a mano.
WITH catalog_tablas(tabla, nombre, capa, orden_capa, dominio, descripcion, grano, claves, formato, proceso, frecuencia, quality_gates, upstream, downstream, terminos_glosario, n_columnas) AS (VALUES
    {_values(filas)}
)
SELECT * FROM catalog_tablas ORDER BY orden_capa, tabla"""


def sql_catalogo_columnas(cat: CatalogoGlosario) -> str:
    filas = [
        (
            t.tabla,
            t.capa,
            t.dominio,
            c.nombre,
            c.tipo,
            c.es_clave,
            _badge_term(c.termino_glosario) if c.termino_glosario else "-",
            c.descripcion or "-",
        )
        for t in cat.tablas
        for c in t.columnas
    ]
    return f"""-- Diccionario de Columnas de Raillytics: campos declarados en config/data_catalog.yml
-- Se genera con `make catalog` (raillytics.catalog): no lo edites a mano.
WITH catalog_columnas(tabla, capa, dominio, columna, tipo, es_clave, termino_glosario, descripcion) AS (VALUES
    {_values(filas)}
)
SELECT * FROM catalog_columnas ORDER BY tabla, columna"""


def sql_glosario_terminos(cat: CatalogoGlosario) -> str:
    filas = [
        (
            t.termino,
            t.dominio,
            t.tipo,
            t.definicion,
            t.formula or "-",
            t.sinonimos or "-",
            ", ".join(_badge_table(tbl.strip()) for tbl in t.tablas_relacionadas.split(",") if tbl.strip()) if t.tablas_relacionadas else "-",
            ", ".join(_link_dash(d) for d in t.dashboards_relacionados) if t.dashboards_relacionados else "-",
            ", ".join(_badge_gate(g) for g in t.quality_gates) if t.quality_gates else "-",
            ", ".join(_badge_term(term) for term in t.terminos_relacionados) if t.terminos_relacionados else "-",
        )
        for t in cat.terminos
    ]
    return f"""-- Glosario de Términos de Raillytics: conceptos oficiales declarados en config/glosario.yml
-- Se genera con `make catalog` (raillytics.catalog): no lo edites a mano.
WITH glosario(termino, dominio, tipo, definicion, formula, sinonimos, tablas_relacionadas, dashboards_relacionados, quality_gates, terminos_relacionados) AS (VALUES
    {_values(filas)}
)
SELECT * FROM glosario ORDER BY dominio, termino"""


def sql_catalogo_relaciones(cat: CatalogoGlosario) -> str:
    filas: list[tuple[str, str, str, str, str, int]] = []

    for t in cat.terminos:
        cat_t = f"Término {t.tipo}"
        for d in t.dashboards_relacionados:
            filas.append((t.termino, f"Dashboard: {d}", "se explota en", cat_t, "Dashboard", 2))
        for g in t.quality_gates:
            filas.append((t.termino, f"Gate: {g}", "regla asociada", cat_t, "Quality Gate", 1))
        for rel in t.terminos_relacionados:
            filas.append((t.termino, rel, "relacionado con", cat_t, "Término Negocio", 1))
        for tbl in [x.strip() for x in t.tablas_relacionadas.split(",") if x.strip()]:
            filas.append((t.termino, tbl, "almacenado en", cat_t, "Dataset", 2))

    for t in cat.tablas:
        cat_tbl = f"Capa {t.capa}"
        for g in t.quality_gates:
            filas.append((t.tabla, f"Gate: {g}", "valida calidad", cat_tbl, "Quality Gate", 1))
        for d in t.downstream:
            if d.startswith("dashboard:"):
                dash = d.split(":", 1)[1]
                filas.append((t.tabla, f"Dashboard: {dash}", "alimenta", cat_tbl, "Dashboard", 3))
            elif d.startswith("dataset:"):
                ds = d.split(":", 1)[1]
                filas.append((t.tabla, ds, "publica en", cat_tbl, "Dataset", 2))
        for u in t.upstream:
            if ":" in u:
                u_nombre = u.split(":", 1)[1]
                filas.append((u_nombre, t.tabla, "transforma a", "Capa Upstream", cat_tbl, 1))

    vistos = set()
    filas_unicas = []
    for f in filas:
        clave = (f[0], f[1])
        if clave not in vistos and f[0] != f[1]:
            vistos.add(clave)
            filas_unicas.append(f)

    return f"""-- Grafo de Gobernanza de Raillytics: red de conceptos, datasets, gates y dashboards
-- Se genera con `make catalog` (raillytics.catalog): no lo edites a mano.
WITH relaciones(origen, destino, relacion, categoria_origen, categoria_destino, peso) AS (VALUES
    {_values(filas_unicas)}
)
SELECT origen AS termino, origen, destino, relacion, categoria_origen, categoria_destino, peso FROM relaciones ORDER BY categoria_origen, origen"""


def construir_datasets_catalogo(cat: CatalogoGlosario) -> dict[str, dict]:
    cols_catalog = [
        _col("tabla", "Tabla"),
        _col("nombre", "Nombre"),
        _col("capa", "Capa"),
        _col("orden_capa", "Orden de la capa", "INTEGER"),
        _col("dominio", "Dominio"),
        _col("descripcion", "Descripción"),
        _col("grano", "Grano"),
        _col("claves", "Claves primarias"),
        _col("formato", "Formato"),
        _col("proceso", "Proceso productor"),
        _col("frecuencia", "Frecuencia"),
        _col("quality_gates", "Quality Gates"),
        _col("upstream", "Orígenes (Upstream)"),
        _col("downstream", "Destinos (Downstream)"),
        _col("terminos_glosario", "Términos del glosario"),
        _col("n_columnas", "Columnas", "BIGINT"),
    ]
    cols_columnas = [
        _col("tabla", "Tabla"),
        _col("capa", "Capa"),
        _col("dominio", "Dominio"),
        _col("columna", "Columna"),
        _col("tipo", "Tipo de dato"),
        _col("es_clave", "Es clave", "BOOLEAN"),
        _col("termino_glosario", "Término del glosario"),
        _col("descripcion", "Descripción"),
    ]
    cols_glosario = [
        _col("termino", "Término"),
        _col("dominio", "Dominio"),
        _col("tipo", "Tipo"),
        _col("definicion", "Definición"),
        _col("formula", "Fórmula"),
        _col("sinonimos", "Sinónimos"),
        _col("tablas_relacionadas", "Tablas relacionadas"),
        _col("dashboards_relacionados", "Dashboards relacionados"),
        _col("quality_gates", "Quality Gates asociados"),
        _col("terminos_relacionados", "Términos relacionados"),
    ]
    cols_relaciones = [
        _col("termino", "Término"),
        _col("origen", "Origen"),
        _col("destino", "Destino"),
        _col("relacion", "Relación"),
        _col("categoria_origen", "Categoría origen"),
        _col("categoria_destino", "Categoría destino"),
        _col("peso", "Peso", "INTEGER"),
    ]

    return {
        "data_catalog": _dataset(
            "data_catalog",
            "Catálogo unificado de tablas del Data Lakehouse con metadatos de capa, dominio, grano, claves, proceso y reglas de calidad.",
            sql_data_catalog(cat),
            cols_catalog,
            [
                _metrica("total_tablas", "Total tablas", "COUNT(*)"),
                _metrica("tablas_gold", "Tablas Gold", "SUM(CASE WHEN capa = 'Gold' THEN 1 ELSE 0 END)"),
                _metrica("tablas_silver", "Tablas Silver", "SUM(CASE WHEN capa = 'Silver' THEN 1 ELSE 0 END)"),
                _metrica("tablas_bronze", "Tablas Bronze", "SUM(CASE WHEN capa LIKE 'Bronze%' THEN 1 ELSE 0 END)"),
            ],
        ),
        "catalogo_columnas": _dataset(
            "catalogo_columnas",
            "Diccionario de columnas y esquemas técnicos de todas las tablas catalogadas en Raillytics.",
            sql_catalogo_columnas(cat),
            cols_columnas,
            [
                _metrica("total_columnas", "Total columnas", "COUNT(*)"),
                _metrica("columnas_clave", "Columnas clave", "SUM(CASE WHEN es_clave THEN 1 ELSE 0 END)"),
            ],
        ),
        "glosario_terminos": _dataset(
            "glosario_terminos",
            "Glosario oficial de conceptos de negocio ferroviario y arquitectura técnica de la plataforma.",
            sql_glosario_terminos(cat),
            cols_glosario,
            [
                _metrica("total_terminos", "Total términos", "COUNT(*)"),
                _metrica("terminos_negocio", "Términos de negocio", "SUM(CASE WHEN tipo = 'Negocio' THEN 1 ELSE 0 END)"),
                _metrica("terminos_tecnicos", "Términos técnicos", "SUM(CASE WHEN tipo = 'Técnico' THEN 1 ELSE 0 END)"),
            ],
        ),
        "catalogo_relaciones": _dataset(
            "catalogo_relaciones",
            "Red relacional de gobernanza de datos: vincula conceptos del glosario, tablas del lakehouse, quality gates y dashboards.",
            sql_catalogo_relaciones(cat),
            cols_relaciones,
            [
                _metrica("total_relaciones", "Total relaciones", "COUNT(*)"),
                _metrica("peso_total", "Peso total", "SUM(peso)"),
            ],
        ),
    }
