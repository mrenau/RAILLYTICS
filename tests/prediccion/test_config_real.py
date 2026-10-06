"""La consulta `trimestrales` real de config/prediccion.yml, ejecutada sobre un Silver de prueba (sin red ni MinIO)."""
from pathlib import Path

import duckdb

from raillytics.prediccion.entradas import cargar_config, resolver_origen
from raillytics.utils.lake import LakeLayout

RAIZ = Path(__file__).resolve().parents[2]


def test_trimestrales_suma_los_operadores_del_corredor_sin_el_total_ni_otros_corredores(tmp_path):
    silver = tmp_path / "silver" / "cnmc_trimestral"
    silver.mkdir(parents=True)
    duckdb.connect().execute(
        f"""COPY (SELECT * FROM (VALUES
              (2026, 1, 'Madrid-Barcelona', 'RENFE', 1000), (2026, 1, 'Madrid-Barcelona', 'IRYO', 500), (2026, 1, 'Madrid-Barcelona', 'TOTAL', NULL),
              (2026, 2, 'Madrid-Barcelona', 'RENFE', 1100), (2026, 2, 'Madrid-Sevilla', 'RENFE', 777)
            ) t(anio, trimestre, corredor, operador_id, viajeros))
            TO '{(silver / 'cnmc_trimestral.parquet').as_posix()}' (FORMAT PARQUET)"""
    )
    layout = LakeLayout(silver_root=(tmp_path / "silver").as_posix(), gold_root=(tmp_path / "gold").as_posix())
    sql = resolver_origen(cargar_config(RAIZ / "config" / "prediccion.yml")["trimestrales"], layout, {})

    assert duckdb.connect().execute(sql).fetchall() == [("2026-T1", 1500), ("2026-T2", 1100)]


def test_la_config_ya_no_lee_la_demanda_sintetica_pero_si_las_otras_tres_fuentes():
    consultas = cargar_config(RAIZ / "config" / "prediccion.yml")

    assert "muestra_" not in consultas["trimestrales"]
    assert all("muestra_" in consultas[origen] for origen in ("festivos", "eventos", "meteo"))


def test_cnmc_da_viajeros_y_plazas_por_operador_del_corredor_sin_el_total_ni_otros_corredores(tmp_path):
    silver = tmp_path / "silver" / "cnmc_trimestral"
    silver.mkdir(parents=True)
    duckdb.connect().execute(
        f"""COPY (SELECT * FROM (VALUES
              (2026, 1, 'Madrid-Barcelona', 'RENFE', 1000, 1500), (2026, 1, 'Madrid-Barcelona', 'IRYO', 500, NULL),
              (2026, 1, 'Madrid-Barcelona', 'TOTAL', NULL, NULL), (2026, 1, 'Madrid-Sevilla', 'RENFE', 777, 900)
            ) t(anio, trimestre, corredor, operador_id, viajeros, plazas_ofertadas))
            TO '{(silver / 'cnmc_trimestral.parquet').as_posix()}' (FORMAT PARQUET)"""
    )
    layout = LakeLayout(silver_root=(tmp_path / "silver").as_posix(), gold_root=(tmp_path / "gold").as_posix())
    sql = resolver_origen(cargar_config(RAIZ / "config" / "prediccion.yml")["cnmc"], layout, {})

    assert duckdb.connect().execute(sql).fetchall() == [("2026-T1", "IRYO", 500, None), ("2026-T1", "RENFE", 1000, 1500)]


def test_cnmc_lee_la_cnmc_real_no_la_muestra_sintetica():
    assert "muestra_" not in cargar_config(RAIZ / "config" / "prediccion.yml")["cnmc"]
