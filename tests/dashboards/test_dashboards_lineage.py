"""Dashboard «Lineage de cargas»: grafo de la fuente al dashboard con estado, reglas de calidad, errores y logs."""
import pytest

from test_dashboards_corredor import DASHBOARDS, DATASETS, GRAFICOS, _graficos_de

DASHBOARD = DASHBOARDS["lineage_cargas"]
LINEAGE = ("lineage_nodos", "lineage_aristas", "lineage_cargas", "lineage_reglas")
ESPERADOS = {
    "lineage_kpi_nodos", "lineage_kpi_con_error", "lineage_kpi_sin_ejecutar", "lineage_kpi_gates_fallidos",
    "lineage_sankey", "lineage_grafo", "lineage_grafo_carga", "lineage_tabla_nodos", "lineage_tabla_reglas", "lineage_tabla_cargas", "lineage_tabla_logs",
}


def _columnas_usadas(grafico):
    p = grafico["params"]
    columnas = set(p.get("all_columns") or []) | set(p.get("groupby") or [])
    columnas |= {p[k] for k in ("source", "target", "source_category", "target_category") if p.get(k)}
    return columnas


def test_el_dashboard_tiene_sus_once_graficos_y_todos_usan_datasets_de_lineage():
    graficos = _graficos_de("lineage_cargas")

    assert set(graficos) == ESPERADOS
    assert {g["dataset_uuid"] for g in graficos.values()} <= {DATASETS[d]["uuid"] for d in LINEAGE}


def test_cada_grafico_usa_columnas_y_metricas_que_existen_en_su_dataset():
    por_uuid = {d["uuid"]: d for d in DATASETS.values()}
    for nombre, grafico in _graficos_de("lineage_cargas").items():
        dataset = por_uuid[grafico["dataset_uuid"]]
        metrica = grafico["params"].get("metric")

        assert _columnas_usadas(grafico) <= {c["column_name"] for c in dataset["columns"]}, nombre
        if metrica:   # las tablas en modo «raw» no llevan métrica
            assert metrica in {m["metric_name"] for m in dataset["metrics"]}, (nombre, metrica)


def test_el_sankey_y_los_dos_grafos_unen_origen_con_destino_y_colorean_por_capa():
    graficos = _graficos_de("lineage_cargas")

    assert (graficos["lineage_sankey"]["viz_type"], graficos["lineage_sankey"]["params"]["source"]) == ("sankey_v2", "origen")
    for nombre in ("lineage_grafo", "lineage_grafo_carga"):
        p = graficos[nombre]["params"]
        assert graficos[nombre]["viz_type"] == "graph_chart"
        assert (p["source"], p["target"]) == ("origen", "destino")
        assert (p["source_category"], p["target_category"]) == ("categoria_origen", "categoria_destino")


def test_solo_la_tabla_de_logs_renderiza_html_y_no_lleva_texto_libre_del_lake():
    graficos = _graficos_de("lineage_cargas")
    for nombre, grafico in graficos.items():
        if grafico["viz_type"] == "table":
            assert grafico["params"]["allow_render_html"] == (nombre == "lineage_tabla_logs"), nombre
    # Con HTML activado se renderizan TODAS las columnas de texto: ahí no puede ir `error` ni `run_id` (texto que no controlamos).
    columnas_html = set(graficos["lineage_tabla_logs"]["params"]["all_columns"])
    assert "enlace_log" in columnas_html and not columnas_html & {"error", "run_id", "origen", "ultimo_error", "detalle"}
    assert "enlace_log" not in graficos["lineage_tabla_cargas"]["params"]["all_columns"]


def test_la_tabla_de_logs_solo_muestra_filas_que_tienen_enlace():
    filtros = _graficos_de("lineage_cargas")["lineage_tabla_logs"]["params"]["adhoc_filters"]

    assert any(f.get("sqlExpression") == "enlace_log IS NOT NULL" for f in filtros)


def test_los_filtros_apuntan_a_columnas_reales_y_solo_afectan_a_los_graficos_de_su_dataset():
    por_uuid = {d["uuid"]: d for d in DATASETS.values()}
    colocados = {c["meta"]["chartId"]: c["meta"]["uuid"] for c in DASHBOARD["position"].values() if isinstance(c, dict) and c.get("type") == "CHART"}
    dataset_de = {cid: GRAFICOS_POR_UUID[u]["dataset_uuid"] for cid, u in colocados.items()}
    filtros = {f["name"]: f for f in DASHBOARD["metadata"]["native_filter_configuration"]}

    assert set(filtros) == {"Capa", "Estado", "Nodo (reglas)", "Ejecución", "Proceso"}
    for nombre, filtro in filtros.items():
        destino = filtro["targets"][0]
        assert destino["column"]["name"] in {c["column_name"] for c in por_uuid[destino["datasetUuid"]]["columns"]}, nombre
        afectados = set(colocados) - set(filtro["scope"]["excluded"])
        # Aplicar un filtro a un gráfico de otro dataset lo rompería (no tiene esa columna).
        assert {dataset_de[c] for c in afectados} == {destino["datasetUuid"]}, nombre


def test_el_filtro_de_ejecucion_permite_elegir_varias_y_filtra_por_run_id():
    filtro = next(f for f in DASHBOARD["metadata"]["native_filter_configuration"] if f["name"] == "Ejecución")

    assert filtro["targets"][0]["column"]["name"] == "run_id" and filtro["controlValues"]["multiSelect"]


def test_el_dashboard_se_describe_y_tiene_su_slug():
    assert DASHBOARD["slug"] == "lineage-cargas" and DASHBOARD["dashboard_title"] == "Lineage de cargas"
    assert "lineage" in DASHBOARD["description"].lower() and "_trazabilidad" in DASHBOARD["description"]
    assert DASHBOARD["published"] is True


def test_la_explicacion_del_dashboard_esta_en_su_primera_fila():
    textos = [c["meta"]["code"] for c in DASHBOARD["position"].values() if isinstance(c, dict) and c.get("type") == "MARKDOWN"]

    assert any("sin ejecutar" in t and "make lineage" in t for t in textos)


GRAFICOS_POR_UUID = {g["uuid"]: g for g in GRAFICOS.values()}
