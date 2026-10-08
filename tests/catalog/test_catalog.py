"""Tests del Catálogo de Datos y Glosario de Términos (configuración, carga y datasets)."""
from pathlib import Path

import duckdb
import pytest

from raillytics.catalog.__main__ import RAIZ, generar
from raillytics.catalog.datasets import construir_datasets_catalogo
from raillytics.catalog.loader import cargar_catalogo_y_glosario


@pytest.fixture(scope="module")
def catalogo():
    return cargar_catalogo_y_glosario(RAIZ)


def test_catalogo_carga_todas_las_tablas_con_campos_obligatorios(catalogo):
    assert len(catalogo.tablas) >= 20
    ids_tablas = {t.tabla for t in catalogo.tablas}
    assert {"fact_mercado_trimestral", "fact_viajeros", "fact_puntualidad", "cnmc_trimestral", "dim_estacion"} <= ids_tablas

    for t in catalogo.tablas:
        assert t.tabla and t.nombre and t.capa and t.dominio and t.grano
        assert t.orden_capa >= 0
        assert t.formato in ("Parquet", "Delta Lake", "Parquet (L2) / CSV (L1)", "Parquet (L2) / JSON (L1)", "Parquet (L2) / ZIP (L1)")


def test_glosario_carga_terminos_de_negocio_y_tecnicos(catalogo):
    assert len(catalogo.terminos) >= 20
    terminos = {t.termino for t in catalogo.terminos}
    assert {"Corredor Ferroviario", "Cuota de Mercado (Market Share)", "Puntualidad Comercial", "Arquitectura Medallion", "Quality Gate (Control de Calidad)"} <= terminos

    for t in catalogo.terminos:
        assert t.termino and t.dominio and t.definicion
        assert t.tipo in ("Negocio", "Técnico")


def test_diccionario_de_columnas_contiene_atributos_tipados(catalogo):
    todas_columnas = [c for t in catalogo.tablas for c in t.columnas]
    assert len(todas_columnas) >= 80

    for c in todas_columnas:
        assert c.nombre and c.tipo
        assert isinstance(c.es_clave, bool)


def test_los_datasets_versionados_estan_sincronizados_con_el_repositorio():
    esperados = generar(RAIZ)
    desfasados = [
        str(ruta.relative_to(RAIZ)) for ruta, texto in esperados.items()
        if not ruta.is_file() or ruta.read_text(encoding="utf-8") != texto
    ]
    assert desfasados == [], f"ejecuta `make catalog`: {desfasados}"


def test_los_datasets_se_ejecutan_en_duckdb_sin_errores(catalogo):
    datasets = construir_datasets_catalogo(catalogo)
    con = duckdb.connect()

    for nombre, d in datasets.items():
        res = con.execute(d["sql"]).fetchall()
        assert len(res) > 0, f"{nombre} no devolvió filas"
        columnas_reales = [desc[0] for desc in con.description]
        columnas_declaradas = [c["column_name"] for c in d["columns"]]
        assert columnas_reales == columnas_declaradas, f"desajuste de columnas en {nombre}"
