from datetime import date
from pathlib import Path

import pytest
import yaml

from raillytics.prediccion.entradas import (
    EntradaError,
    cargar_config,
    cargar_entradas,
    cargar_trimestrales,
    raiz_bronze,
    resolver_origen,
)
from raillytics.prediccion.trimestre import Trimestre
from raillytics.utils.lake import LakeLayout, connect

RAIZ = Path(__file__).resolve().parents[2]

CONSULTAS = {
    "trimestrales": (
        "SELECT concat(anio, '-T', trim) AS trimestre, sum(viajeros) AS viajeros "
        "FROM read_parquet('{silver}/demanda/*.parquet') WHERE corredor = 'AVE-MAD-BCN' GROUP BY 1 ORDER BY 1"
    ),
    "festivos": "SELECT fecha, nombre FROM read_parquet('{silver}/festivos/*.parquet')",
    "eventos": "SELECT fecha, descripcion, ciudad FROM read_parquet('{silver}/eventos/*.parquet')",
    "meteo": (
        "SELECT fecha, ciudad, tmed AS temperatura_media, prec AS precipitacion_mm "
        "FROM read_parquet('{silver}/meteo/*.parquet')"
    ),
}


def _parquet(con, destino, select):
    destino.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"COPY ({select}) TO '{destino.as_posix()}' (FORMAT PARQUET)")


@pytest.fixture
def lake(tmp_path):
    con = connect()
    silver = tmp_path / "silver"
    _parquet(con, silver / "demanda" / "demanda.parquet", """
        SELECT * FROM (VALUES
          (2025, 1, 'AVE-MAD-BCN', 600000), (2025, 1, 'AVE-MAD-BCN', 400000),
          (2025, 1, 'OTRO', 999), (2025, 2, 'AVE-MAD-BCN', 650000)
        ) t(anio, trim, corredor, viajeros)""")
    _parquet(con, silver / "festivos" / "festivos.parquet", """
        SELECT * FROM (VALUES (DATE '2026-12-25', 'Navidad'), (DATE '2026-12-08', 'Inmaculada')) t(fecha, nombre)""")
    _parquet(con, silver / "eventos" / "eventos.parquet", """
        SELECT * FROM (VALUES
          (DATE '2026-11-29', 'Partido de liga', 'MAD'), (DATE '2026-11-29', 'Concierto', NULL::VARCHAR)
        ) t(fecha, descripcion, ciudad)""")
    _parquet(con, silver / "meteo" / "meteo.parquet", """
        SELECT * FROM (VALUES
          (DATE '2025-12-01', 'MAD', 9.5, 0.4), (DATE '2025-12-02', 'MAD', 8.0, 2.0)
        ) t(fecha, ciudad, tmed, prec)""")
    layout = LakeLayout(silver_root=silver.as_posix(), gold_root=(tmp_path / "gold").as_posix())
    return layout, con


def test_carga_los_cuatro_origenes_con_el_contrato(lake):
    layout, con = lake

    entradas = cargar_entradas(CONSULTAS, layout, con, env={})

    assert list(entradas.trimestrales["trimestre"]) == ["2025-T1", "2025-T2"]
    assert list(entradas.trimestrales["viajeros"]) == [1_000_000, 650_000]  # ambos sentidos sumados, sin 'OTRO'
    assert str(entradas.trimestrales["viajeros"].dtype) == "int64"
    assert entradas.trimestrales_dict() == {Trimestre(2025, 1): 1_000_000, Trimestre(2025, 2): 650_000}
    assert sorted(entradas.festivos["fecha"]) == [date(2026, 12, 8), date(2026, 12, 25)]
    assert sorted(entradas.eventos["ciudad"]) == ["", "MAD"]  # NULL -> ""
    assert list(entradas.meteo.columns) == ["fecha", "ciudad", "temperatura_media", "precipitacion_mm"]


def test_la_ciudad_de_la_meteo_se_normaliza(lake):
    layout, con = lake
    consultas = dict(
        CONSULTAS,
        meteo="SELECT DATE '2025-12-01' AS fecha, ' bcn ' AS ciudad, 10.0 AS temperatura_media, 0.0 AS precipitacion_mm",
    )

    assert list(cargar_entradas(consultas, layout, con, env={}).meteo["ciudad"]) == ["BCN"]


def test_los_origenes_pueden_estar_vacios_salvo_que_el_nivel_los_necesite(lake):
    layout, con = lake
    consultas = dict(CONSULTAS, eventos="SELECT DATE '2026-01-01' AS fecha, 'x' AS descripcion, 'MAD' AS ciudad WHERE false")

    assert cargar_entradas(consultas, layout, con, env={}).eventos.empty


def test_resolver_origen_sustituye_los_marcadores(tmp_path):
    layout = LakeLayout(silver_root="/s", gold_root="/g")

    sql = "{bronze}/a {silver}/b {gold}/c"

    assert resolver_origen(sql, layout, {"BRONZE_ROOT": "/b"}) == "/b/a /s/b /g/c"
    assert resolver_origen(sql, layout, {}) == "s3://raillytics-bronze/a /s/b /g/c"


def test_raiz_bronze_sale_del_entorno():
    assert raiz_bronze({"BRONZE_ROOT": "/tmp/b"}) == "/tmp/b"
    assert raiz_bronze({"MINIO_BUCKET_BRONZE": "mi-bronze"}) == "s3://mi-bronze"
    assert raiz_bronze({}) == "s3://raillytics-bronze"


def test_un_origen_inexistente_nombra_la_fuente_y_la_ruta(lake):
    layout, con = lake
    consultas = dict(
        CONSULTAS, eventos="SELECT fecha, descripcion, ciudad FROM read_parquet('{silver}/sin_ingestar/*.parquet')"
    )

    with pytest.raises(EntradaError) as error:
        cargar_entradas(consultas, layout, con, env={})

    assert "origen 'eventos'" in str(error.value) and "sin_ingestar" in str(error.value)


def test_el_error_de_duckdb_se_resume_sin_el_fragmento_de_sql(lake):
    layout, con = lake
    consultas = dict(
        CONSULTAS, eventos="SELECT fecha, descripcion, ciudad FROM read_parquet('{silver}/sin_ingestar/*.parquet')"
    )

    with pytest.raises(EntradaError) as error:
        cargar_entradas(consultas, layout, con, env={})

    mensaje = str(error.value)
    assert "LINE" not in mensaje and "^" not in mensaje and "\n" not in mensaje  # una sola línea legible
    assert "origen 'eventos'" in mensaje and "No files found" in mensaje and "sin_ingestar" in mensaje
    assert mensaje.count("sin_ingestar") == 1  # la ruta no se repite


def test_un_error_de_columna_conserva_las_columnas_candidatas_para_arreglar_la_consulta(lake):
    layout, con = lake
    consultas = dict(CONSULTAS, festivos="SELECT fecha, nope AS nombre FROM read_parquet('{silver}/festivos/*.parquet')")

    with pytest.raises(EntradaError) as error:
        cargar_entradas(consultas, layout, con, env={})

    mensaje = str(error.value)
    assert 'Referenced column "nope" not found' in mensaje
    assert 'Candidate bindings: "nombre"' in mensaje  # DuckDB sugiere la columna más parecida: es la pista que hay que conservar
    assert "festivos/*.parquet" in mensaje  # y, como la ruta no va en este error, se añade aparte
    assert "LINE" not in mensaje and "^" not in mensaje


def test_columnas_distintas_del_contrato_se_rechazan(lake):
    layout, con = lake
    consultas = dict(CONSULTAS, trimestrales="SELECT 2025 AS anio, 1 AS viajeros")

    with pytest.raises(EntradaError, match=r"exactamente las columnas \['trimestre', 'viajeros'\]"):
        cargar_entradas(consultas, layout, con, env={})


@pytest.mark.parametrize(
    "origen, sql, mensaje",
    [
        ("festivos", "SELECT DATE '2026-12-25' AS fecha, NULL::VARCHAR AS nombre", "nulos en columnas obligatorias"),
        (
            "festivos",
            "SELECT * FROM (VALUES (DATE '2026-12-25', 'A'), (DATE '2026-12-25', 'B')) t(fecha, nombre)",
            "duplicad",
        ),
        (
            "meteo",
            "SELECT DATE '2025-12-01' AS fecha, 'SEV' AS ciudad, 10.0 AS temperatura_media, 0.0 AS precipitacion_mm",
            "ciudades no soportadas",
        ),
        (
            "meteo",
            "SELECT * FROM (VALUES (DATE '2025-12-01', 'MAD', 1.0, 1.0), (DATE '2025-12-01', 'MAD', 2.0, 2.0)) "
            "t(fecha, ciudad, temperatura_media, precipitacion_mm)",
            "duplicad",
        ),
        ("trimestrales", "SELECT '2025-Q1' AS trimestre, 100 AS viajeros", "formato inválido"),
        ("trimestrales", "SELECT '2025-T1' AS trimestre, 100.5 AS viajeros", "entero"),
    ],
)
def test_datos_que_incumplen_el_contrato_fallan_cerrado(lake, origen, sql, mensaje):
    layout, con = lake

    with pytest.raises(EntradaError, match=mensaje) as error:
        cargar_entradas(dict(CONSULTAS, **{origen: sql}), layout, con, env={})

    assert f"origen '{origen}'" in str(error.value)


def test_cargar_config_devuelve_una_consulta_por_origen(tmp_path):
    fichero = tmp_path / "prediccion.yml"
    fichero.write_text(
        "origenes:\n"
        + "".join(f"  {nombre}:\n    sql: |\n      SELECT 1\n" for nombre in CONSULTAS),
        encoding="utf-8",
    )

    assert cargar_config(fichero) == {nombre: "SELECT 1\n" for nombre in CONSULTAS}


def test_cargar_config_pide_cada_origen(tmp_path):
    fichero = tmp_path / "prediccion.yml"
    fichero.write_text("origenes:\n  festivos:\n    sql: SELECT 1\n", encoding="utf-8")

    with pytest.raises(EntradaError, match="origenes.trimestrales.sql"):
        cargar_config(fichero)


def test_cargar_config_falla_si_no_existe(tmp_path):
    with pytest.raises(EntradaError, match="no existe"):
        cargar_config(tmp_path / "no_existe.yml")


def test_la_config_por_defecto_del_repo_define_los_cuatro_origenes_y_el_opcional_cnmc():
    consultas = cargar_config(RAIZ / "config" / "prediccion.yml")

    assert set(consultas) == set(CONSULTAS) | {"cnmc"}
    assert all("SELECT" in sql for sql in consultas.values())


# ---------------------------------------------------------------------------------- origen opcional `cnmc`

CNMC_SQL = (
    "SELECT * FROM (VALUES ('2025-T1', 'RENFE', 600000, 800000), ('2025-T1', 'IRYO', 400000, NULL)) "
    "t(trimestre, operador, viajeros, plazas_ofertadas)"
)


def test_el_origen_cnmc_es_opcional_y_sin_el_las_entradas_lo_dejan_en_none(lake):
    layout, con = lake

    assert cargar_entradas(CONSULTAS, layout, con, env={}).cnmc is None


def test_con_el_origen_cnmc_se_carga_por_operador_con_el_contrato(lake):
    layout, con = lake

    cnmc = cargar_entradas(dict(CONSULTAS, cnmc=CNMC_SQL), layout, con, env={}).cnmc

    assert list(cnmc.columns) == ["trimestre", "operador", "viajeros", "plazas_ofertadas"]
    assert list(cnmc["viajeros"]) == [600_000, 400_000]
    assert cnmc["plazas_ofertadas"].isna().sum() == 1  # las plazas pueden faltar (la CNMC no las da antes de 2018)


def test_el_origen_cnmc_con_otras_columnas_falla_con_el_nombre_del_origen(lake):
    layout, con = lake

    with pytest.raises(EntradaError, match="origen 'cnmc'.*columnas"):
        cargar_entradas(dict(CONSULTAS, cnmc="SELECT '2025-T1' AS trimestre, 1 AS viajeros"), layout, con, env={})


def test_cargar_config_lee_el_origen_cnmc_si_esta_y_no_lo_exige(tmp_path):
    base = {n: {"sql": s} for n, s in CONSULTAS.items()}
    sin = tmp_path / "sin.yml"
    sin.write_text(yaml.safe_dump({"origenes": base}), encoding="utf-8")
    con = tmp_path / "con.yml"
    con.write_text(yaml.safe_dump({"origenes": {**base, "cnmc": {"sql": CNMC_SQL}}}), encoding="utf-8")

    assert "cnmc" not in cargar_config(sin)
    assert cargar_config(con)["cnmc"] == CNMC_SQL


def test_cargar_trimestrales_solo_necesita_ese_origen(lake):
    layout, con = lake

    assert cargar_trimestrales({"trimestrales": CONSULTAS["trimestrales"]}, layout, con, env={}) == {
        Trimestre(2025, 1): 1_000_000, Trimestre(2025, 2): 650_000,
    }
