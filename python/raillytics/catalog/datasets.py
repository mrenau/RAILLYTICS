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
            ", ".join(t.quality_gates) if t.quality_gates else "-",
            ", ".join(t.upstream) if t.upstream else "-",
            ", ".join(t.downstream) if t.downstream else "-",
            ", ".join(t.terminos_glosario) if t.terminos_glosario else "-",
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
            c.termino_glosario or "-",
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
            t.tablas_relacionadas or "-",
            ", ".join(t.dashboards_relacionados) if t.dashboards_relacionados else "-",
            ", ".join(t.quality_gates) if t.quality_gates else "-",
            ", ".join(t.terminos_relacionados) if t.terminos_relacionados else "-",
        )
        for t in cat.terminos
    ]
    return f"""-- Glosario de Términos de Raillytics: conceptos oficiales declarados en config/glosario.yml
-- Se genera con `make catalog` (raillytics.catalog): no lo edites a mano.
WITH glosario(termino, dominio, tipo, definicion, formula, sinonimos, tablas_relacionadas, dashboards_relacionados, quality_gates, terminos_relacionados) AS (VALUES
    {_values(filas)}
)
SELECT * FROM glosario ORDER BY dominio, termino"""


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
    }
