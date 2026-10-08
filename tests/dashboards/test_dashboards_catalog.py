"""Dashboard «Catálogo de datos y Glosario de términos»: inventario de datasets, diccionario y glosario."""
import pytest

from test_dashboards_corredor import DASHBOARDS, DATASETS, GRAFICOS, _graficos_de

DASHBOARD = DASHBOARDS["catalogo_glosario"]
CATALOG_DATASETS = ("data_catalog", "catalogo_columnas", "glosario_terminos")
ESPERADOS = {
    "catalogo_kpi_tablas",
    "catalogo_kpi_columnas",
    "catalogo_kpi_gold",
    "catalogo_kpi_silver",
    "catalogo_distribucion_capas",
    "catalogo_tabla_datasets",
    "catalogo_tabla_columnas",
    "glosario_kpi_terminos",
    "glosario_kpi_negocio",
    "glosario_kpi_tecnicos",
    "glosario_tabla_terminos",
}


def _columnas_usadas(grafico):
    p = grafico["params"]
    columnas = set(p.get("all_columns") or []) | set(p.get("groupby") or [])
    if p.get("x_axis"):
        columnas.add(p["x_axis"])
    return columnas


def test_el_dashboard_tiene_sus_once_graficos_y_usan_datasets_de_catalogo():
    graficos = _graficos_de("catalogo_glosario")

    assert set(graficos) == ESPERADOS
    assert {g["dataset_uuid"] for g in graficos.values()} <= {DATASETS[d]["uuid"] for d in CATALOG_DATASETS}


def test_cada_grafico_usa_columnas_y_metricas_existentes():
    por_uuid = {d["uuid"]: d for d in DATASETS.values()}
    for nombre, grafico in _graficos_de("catalogo_glosario").items():
        dataset = por_uuid[grafico["dataset_uuid"]]
        p = grafico["params"]
        metricas = set(p.get("metrics") or []) | ({p["metric"]} if isinstance(p.get("metric"), str) else set())

        assert _columnas_usadas(grafico) <= {c["column_name"] for c in dataset["columns"]}, nombre
        assert metricas <= {m["metric_name"] for m in dataset["metrics"]}, (nombre, metricas)


def test_los_filtros_nativos_apuntan_a_columnas_reales():
    por_uuid = {d["uuid"]: d for d in DATASETS.values()}
    filtros = {f["name"]: f for f in DASHBOARD["metadata"]["native_filter_configuration"]}

    assert set(filtros) == {"Capa", "Dominio (Catálogo)", "Dominio (Glosario)", "Tipo de término"}
    for nombre, filtro in filtros.items():
        destino = filtro["targets"][0]
        assert destino["column"]["name"] in {c["column_name"] for c in por_uuid[destino["datasetUuid"]]["columns"]}, nombre


def test_el_dashboard_se_describe_y_tiene_su_slug():
    assert DASHBOARD["slug"] == "catalogo-datos-glosario"
    assert "Catálogo de datos y Glosario de términos" in DASHBOARD["dashboard_title"]
    assert DASHBOARD["published"] is True
    assert "datasets" in DASHBOARD["description"].lower()


def test_la_introduccion_del_dashboard_enlaza_con_lineage_y_trazabilidad():
    textos = [c["meta"]["code"] for c in DASHBOARD["position"].values() if isinstance(c, dict) and c.get("type") == "MARKDOWN"]

    assert any("lineage-cargas" in t and "trazabilidad-cargas" in t for t in textos)
