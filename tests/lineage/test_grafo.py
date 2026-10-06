"""El grafo de lineage declarado: sale del propio repositorio (fuentes, SQL de Silver y Gold, gates, predicción y dashboards)."""
from pathlib import Path

import pytest
import yaml

from raillytics.lineage.grafo import CAPAS, Grafo, construir, descripcion_de_sql, sin_comentarios

RAIZ = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def real() -> Grafo:
    return construir(RAIZ)


def _escribir(ruta: Path, texto: str) -> None:
    ruta.parent.mkdir(parents=True, exist_ok=True)
    ruta.write_text(texto, encoding="utf-8")


@pytest.fixture
def mini(tmp_path):
    """Un repo mínimo: una fuente CSV con tabla Silver, una tabla Gold y un dataset con su dashboard."""
    _escribir(tmp_path / "config" / "data_sources.yml", yaml.safe_dump({"sources": [
        {"id": "ejemplo", "name": "Fuente de ejemplo", "url": "https://example.invalid/e.csv", "format": "csv",
         "silver": [{"tabla": "ejemplo_trim", "modo": "snapshot"}]},
        {"id": "otra", "name": "Sin silver", "url": "https://example.invalid/o.json", "format": "json"},
    ]}))
    _escribir(tmp_path / "src/main/resources/silver/ejemplo_trim.sql", "-- Silver ejemplo_trim: trimestres de ejemplo.\nSELECT 1 FROM entrada")
    _escribir(tmp_path / "src/main/resources/gold/fact_ejemplo.sql",
              "-- Fact ejemplo: no debe leer silver_comentado de este comentario.\nSELECT * FROM silver_ejemplo_trim -- y gold_tampoco\n")
    _escribir(tmp_path / "config/quality_gates.yml", yaml.safe_dump({"opcionales": [], "tablas": {
        "silver_ejemplo_trim": [{"nombre": "filas_minimas", "tipo": "filas_min", "minimo": 5, "severidad": "bloqueante"},
                                {"nombre": "claves", "tipo": "no_nulos", "columnas": ["a", "b"], "severidad": "aviso"}],
        "gold_fact_ejemplo": [{"nombre": "grano", "tipo": "unico", "columnas": ["x"], "severidad": "bloqueante"}],
    }}))
    _escribir(tmp_path / "config/prediccion.yml", yaml.safe_dump({"origenes": {}}))
    base = tmp_path / "dashboards/superset/raillytics_gold"
    _escribir(base / "datasets/Raillytics_Gold_DuckDB/vista_ejemplo.yaml", yaml.safe_dump({
        "table_name": "vista_ejemplo", "uuid": "ds-1",
        "sql": "SELECT * FROM read_parquet('s3://raillytics-gold/fact_ejemplo/*.parquet')"}))
    _escribir(base / "datasets/Raillytics_Gold_DuckDB/solo_trazabilidad.yaml", yaml.safe_dump({
        "table_name": "solo_trazabilidad", "uuid": "ds-2",
        "sql": "SELECT * FROM read_parquet('s3://raillytics-gold/_trazabilidad/cargas/*.parquet')"}))
    # Como los de lineage: leen `_trazabilidad` y llevan, en una descripción incrustada, la ruta de una tabla de Gold.
    _escribir(base / "datasets/Raillytics_Gold_DuckDB/meta_lineage.yaml", yaml.safe_dump({
        "table_name": "meta_lineage", "uuid": "ds-3",
        "sql": "-- ver s3://raillytics-gold/fact_ejemplo/\nSELECT * FROM read_parquet('s3://raillytics-gold/_trazabilidad/cargas/*.parquet')"}))
    _escribir(base / "charts/g1.yaml", yaml.safe_dump({"uuid": "ch-1", "dataset_uuid": "ds-1", "slice_name": "g"}))
    _escribir(base / "charts/g2.yaml", yaml.safe_dump({"uuid": "ch-2", "dataset_uuid": "ds-2", "slice_name": "t"}))
    _escribir(base / "dashboards/panel.yaml", yaml.safe_dump({
        "slug": "panel-ejemplo", "dashboard_title": "Panel de ejemplo",
        "position": {"CHART-a": {"type": "CHART", "meta": {"uuid": "ch-1"}}, "CHART-b": {"type": "CHART", "meta": {"uuid": "ch-2"}}}}))
    return construir(tmp_path)


def _aristas(g: Grafo) -> set[tuple[str, str]]:
    return {(a.origen, a.destino) for a in g.aristas}


# ---------------------------------------------------------------------------------------------- mini-repo

def test_cada_fuente_recorre_fuente_staging_l1_l2_con_el_proceso_que_hace_cada_salto(mini):
    saltos = {(a.origen, a.destino): a.proceso for a in mini.aristas}

    assert saltos[("fuente:ejemplo", "staging:ejemplo")] == "bronze_download"
    assert saltos[("staging:ejemplo", "l1:ejemplo")] == "bronze_l1_raw_uploader"
    assert saltos[("l1:ejemplo", "l2:ejemplo")] == "bronze_l2_parquet_converter"


def test_una_tabla_silver_declarada_sale_del_l2_de_su_fuente_y_apunta_a_su_sql(mini):
    arista = next(a for a in mini.aristas if a.destino == "silver:ejemplo_trim")

    assert (arista.origen, arista.proceso) == ("l2:ejemplo", "silver_builder")
    assert arista.ruta == "src/main/resources/silver/ejemplo_trim.sql"
    assert "silver/ejemplo_trim.sql" in arista.transformacion


def test_una_fuente_sin_silver_termina_en_su_l2(mini):
    assert not [a for a in mini.aristas if a.origen == "l2:otra"]


def test_gold_depende_de_las_tablas_que_cita_su_sql_sin_contar_los_comentarios(mini):
    assert _aristas(mini) >= {("silver:ejemplo_trim", "gold:fact_ejemplo")}
    assert not [a for a in mini.aristas if a.destino == "gold:fact_ejemplo" and a.origen != "silver:ejemplo_trim"]


def test_el_dataset_cuelga_de_su_tabla_gold_y_el_dashboard_de_sus_datasets(mini):
    assert {("gold:fact_ejemplo", "dataset:vista_ejemplo"), ("dataset:vista_ejemplo", "dashboard:panel-ejemplo")} <= _aristas(mini)


def test_un_dataset_que_solo_lee_la_trazabilidad_no_entra_en_el_grafo_de_datos(mini):
    ids = {n.id for n in mini.nodos}

    assert "dataset:solo_trazabilidad" not in ids
    assert "dataset:meta_lineage" not in ids       # lee la trazabilidad aunque su texto cite una tabla de Gold


def test_los_nodos_silver_y_gold_toman_su_descripcion_del_primer_comentario_del_sql(mini):
    nodos = {n.id: n for n in mini.nodos}

    assert nodos["silver:ejemplo_trim"].descripcion == "Silver ejemplo_trim: trimestres de ejemplo."
    assert nodos["fuente:ejemplo"].descripcion == "Fuente de ejemplo"


def test_las_reglas_de_calidad_se_asignan_al_nodo_de_su_tabla_con_una_descripcion_legible(mini):
    reglas = {(r.nodo, r.gate): r for r in mini.reglas}

    assert reglas[("silver:ejemplo_trim", "filas_minimas")].descripcion == "al menos 5 filas"
    assert reglas[("silver:ejemplo_trim", "claves")].severidad == "aviso"
    assert reglas[("silver:ejemplo_trim", "claves")].descripcion == "sin nulos en a, b"
    assert reglas[("gold:fact_ejemplo", "grano")].descripcion == "sin duplicados por x"


def test_sin_comentarios_quita_los_de_linea_y_los_de_bloque():
    assert sin_comentarios("SELECT a -- silver_x\nFROM t /* gold_y */ WHERE 1").split() == ["SELECT", "a", "FROM", "t", "WHERE", "1"]


def test_descripcion_de_sql_es_el_primer_comentario_sin_el_guion():
    assert descripcion_de_sql("-- Hola mundo.\n-- Segunda línea\nSELECT 1") == "Hola mundo."
    assert descripcion_de_sql("SELECT 1") == ""


# ---------------------------------------------------------------------------------------------- repo real

def test_toda_arista_une_nodos_que_existen_y_los_nodos_no_se_repiten(real):
    ids = [n.id for n in real.nodos]

    assert len(ids) == len(set(ids))
    assert all(a.origen in set(ids) and a.destino in set(ids) for a in real.aristas)
    assert len(real.aristas) == len(_aristas(real)) or len({(a.origen, a.destino, a.proceso) for a in real.aristas}) == len(real.aristas)


def test_toda_capa_de_un_nodo_es_una_capa_conocida(real):
    assert {n.capa for n in real.nodos} <= set(CAPAS)


def test_el_grafo_no_tiene_ciclos(real):
    sucesores: dict[str, list[str]] = {}
    for a in real.aristas:
        sucesores.setdefault(a.origen, []).append(a.destino)
    estado: dict[str, int] = {}

    def visitar(n: str) -> None:
        assert estado.get(n) != 1, f"ciclo en {n}"
        if estado.get(n) == 2:
            return
        estado[n] = 1
        for s in sucesores.get(n, []):
            visitar(s)
        estado[n] = 2

    for n in {n.id for n in real.nodos}:
        visitar(n)


def _alcanzable(g: Grafo, desde: str, hasta: str) -> bool:
    sucesores: dict[str, list[str]] = {}
    for a in g.aristas:
        sucesores.setdefault(a.origen, []).append(a.destino)
    pendientes, vistos = [desde], set()
    while pendientes:
        n = pendientes.pop()
        if n == hasta:
            return True
        if n not in vistos:
            vistos.add(n)
            pendientes.extend(sucesores.get(n, []))
    return False


@pytest.mark.parametrize("desde, hasta", [
    ("fuente:cnmc_indicadores", "dashboard:mercado-corredor"),       # el recorrido completo de la demanda real
    ("fuente:cnmc_precio_mensual", "dashboard:mercado-corredor"),
    ("gen:silver_sample", "dashboard:demanda-ferroviaria"),          # el Silver sintético hasta el dashboard
    ("silver:cnmc_trimestral", "gold:fact_prediccion_demanda"),      # la predicción lee la demanda real
    ("gen:muestra_prediccion", "gold:fact_prediccion_demanda"),
    ("modelo:ollama", "dashboard:prediccion-demanda"),
])
def test_recorridos_completos_del_repo_real(real, desde, hasta):
    assert _alcanzable(real, desde, hasta), f"no hay camino {desde} → {hasta}"


def test_las_dos_fuentes_nuevas_de_la_cnmc_llegan_a_su_tabla_silver(real):
    assert {("l2:cnmc_viajeros_producto", "silver:cnmc_viajeros_producto"),
            ("l2:cnmc_viajeros_corredor", "silver:cnmc_viajeros_corredor")} <= _aristas(real)


def test_gold_recupera_de_que_silver_depende_cada_tabla(real):
    # La trazabilidad solo guarda «origen = s3a://raillytics-silver»: el detalle sale del SQL.
    assert ("silver:cnmc_trimestral", "gold:fact_mercado_trimestral") in _aristas(real)
    assert ("silver:viajeros_enriquecidos", "gold:dim_estacion") in _aristas(real)


def test_ningun_dataset_de_trazabilidad_ni_de_lineage_aparece_como_nodo_de_datos(real):
    datasets = {n.nombre for n in real.nodos if n.capa == "dataset"}

    assert {"cargas", "calidad", "frescura_tablas"}.isdisjoint(datasets)
    assert not any(d.startswith("lineage_") for d in datasets)


def test_cada_tabla_con_gates_en_el_yaml_tiene_su_nodo(real):
    nodos = {n.id for n in real.nodos}

    assert {r.nodo for r in real.reglas} <= nodos
    assert "silver:cnmc_viajeros_corredor" in {r.nodo for r in real.reglas}
