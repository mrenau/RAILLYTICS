"""Tests de detección de anomalías de volumen y duración en la trazabilidad de cargas."""
from pathlib import Path

import duckdb
import pytest
import yaml

RAIZ = Path(__file__).resolve().parents[2]
DATASET_CARGAS = yaml.safe_load(
    (RAIZ / "dashboards/superset/raillytics_gold/datasets/Raillytics_Gold_DuckDB/cargas.yaml").read_text(encoding="utf-8")
)


def test_sql_cargas_calcula_estadisticas_y_detecta_anomalias(tmp_path):
    # Crear un parquet sintético de cargas con una carga anómala en volumen y una lenta en duración
    con = duckdb.connect()
    cargas_dir = tmp_path / "cargas"
    cargas_dir.mkdir(parents=True)

    filas = [
        # dim_fecha suele tener 365 filas y 2.0s
        ("r1", "gold_build", "gold", "dim_fecha", "s", "d", 365, 100, "2026-10-01 10:00:00", "2026-10-01 10:00:02", 2.0, "ok", None, None, "cli", "h", "u"),
        ("r2", "gold_build", "gold", "dim_fecha", "s", "d", 365, 100, "2026-10-02 10:00:00", "2026-10-02 10:00:02", 2.1, "ok", None, None, "cli", "h", "u"),
        ("r3", "gold_build", "gold", "dim_fecha", "s", "d", 365, 100, "2026-10-03 10:00:00", "2026-10-03 10:00:02", 1.9, "ok", None, None, "cli", "h", "u"),
        # Carga con anomalía de volumen grave: solo 5 filas (< 0.4 * 365)
        ("r4", "gold_build", "gold", "dim_fecha", "s", "d", 5, 10, "2026-10-04 10:00:00", "2026-10-04 10:00:02", 2.0, "ok", None, None, "cli", "h", "u"),
        # Carga con anomalía de duración: tarda 15s (> 3x de 2s)
        ("r5", "gold_build", "gold", "dim_fecha", "s", "d", 365, 100, "2026-10-05 10:00:00", "2026-10-05 10:00:15", 15.0, "ok", None, None, "cli", "h", "u"),
    ]

    def _val(err):
        return f"'{err}'" if err is not None else "NULL"

    valores = ", ".join(
        f"('{r}', '{p}', '{c}', '{t}', '{o}', '{d}', {f}, {b}, TIMESTAMP '{i}', TIMESTAMP '{fin}', {dur}, '{st}', "
        f"{_val(err)}, NULL, '{lan}', '{ej}', '{us}')"
        for r, p, c, t, o, d, f, b, i, fin, dur, st, err, _, lan, ej, us in filas
    )

    con.execute(
        f"COPY (SELECT * FROM (VALUES {valores}) t(run_id, proceso, capa, tabla, origen, destino, filas, bytes, inicio, fin, duracion_s, estado, error, parametros, lanzado_por, ejecutor, usuario)) "
        f"TO '{(cargas_dir / 'c.parquet').as_posix()}' (FORMAT PARQUET)"
    )

    # Sustituir ruta S3 por tmp_path
    sql = DATASET_CARGAS["sql"].replace("s3://raillytics-gold/_trazabilidad/cargas/*.parquet", (cargas_dir / "*.parquet").as_posix())

    resultado = con.execute(
        f"SELECT run_id, filas, media_filas, desviacion_filas_pct, anomalia_volumen, duracion_s, media_duracion, anomalia_duracion FROM ({sql}) t ORDER BY run_id"
    ).fetchall()

    assert len(resultado) == 5
    por_run = {r[0]: r for r in resultado}

    # r1..r3 son normales
    assert por_run["r1"][4] == "normal" and por_run["r1"][7] == "normal"

    # r4 tiene volumen anómalo
    assert por_run["r4"][4] == "anómalo"
    assert por_run["r4"][3] < -50.0  # fuerte caída porcentual

    # r5 tiene duración anormalmente lenta
    assert por_run["r5"][7] == "lenta"
    assert por_run["r5"][5] == 15.0


def test_metricas_de_anomalias_en_cargas_estan_declaradas():
    nombres_metricas = {m["metric_name"] for m in DATASET_CARGAS["metrics"]}
    assert "n_anomalias_volumen" in nombres_metricas
    assert "n_anomalias_duracion" in nombres_metricas
