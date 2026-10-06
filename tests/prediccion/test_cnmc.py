"""Comprobación de que el lake tiene los datos de la CNMC que exige un trimestre (la usa `make 07_prediccion` antes de predecir)."""
from pathlib import Path

import duckdb
import pytest

from raillytics.prediccion import cnmc
from raillytics.prediccion.cnmc import Cobertura, evaluar_cobertura
from raillytics.prediccion.trimestre import Trimestre

RAIZ = Path(__file__).resolve().parents[2]
T = Trimestre.parse
PUBLICADOS = {T(f"{a}-T{n}"): 1_000_000 + 10_000 * i for i, (a, n) in enumerate(
    (a, n) for a in (2025, 2026) for n in (1, 2, 3, 4))}
# 2025-T1 .. 2026-T4 en el dict anterior; los tests recortan lo que necesitan.
HASTA_2026_T2 = {t: v for t, v in PUBLICADOS.items() if t <= T("2026-T2")}


def test_hay_datos_si_estan_el_mismo_trimestre_del_ano_anterior_el_ultimo_publicado_y_su_gemelo():
    cobertura = evaluar_cobertura(HASTA_2026_T2, T("2026-T4"))

    assert cobertura.suficiente and cobertura.ultimo == T("2026-T2")
    assert "2025-T4" in cobertura.descripcion and "2026-T2" in cobertura.descripcion


def test_sin_ningun_trimestre_publicado_anterior_faltan_datos():
    cobertura = evaluar_cobertura({T("2026-T4"): 5}, T("2026-T4"))

    assert not cobertura.suficiente and "ningún trimestre publicado anterior" in cobertura.motivo


def test_un_ultimo_publicado_a_mas_de_dos_trimestres_cuenta_como_desactualizado():
    # La CNMC publica con ~1 trimestre de retraso: dos de distancia es lo normal; tres, que falta una descarga.
    cobertura = evaluar_cobertura(HASTA_2026_T2, T("2027-T1"))

    assert not cobertura.suficiente
    assert "2026-T2" in cobertura.motivo and "3 trimestres" in cobertura.motivo


def test_si_falta_el_mismo_trimestre_del_ano_anterior_faltan_datos():
    publicados = {t: v for t, v in HASTA_2026_T2.items() if t != T("2025-T4")}

    cobertura = evaluar_cobertura(publicados, T("2026-T4"))

    assert not cobertura.suficiente and "2025-T4" in cobertura.motivo


def test_si_falta_el_gemelo_del_ultimo_publicado_faltan_datos():
    publicados = {t: v for t, v in HASTA_2026_T2.items() if t != T("2025-T2")}

    cobertura = evaluar_cobertura(publicados, T("2026-T4"))

    assert not cobertura.suficiente and "2025-T2" in cobertura.motivo


def test_un_trimestre_ya_publicado_se_evalua_sin_mirar_su_propio_dato():
    # Backtest: 2026-T2 ya está publicado, pero el nivel solo usa lo anterior (2026-T1, 2025-T2 y 2025-T1).
    cobertura = evaluar_cobertura(HASTA_2026_T2, T("2026-T2"))

    assert cobertura.suficiente and cobertura.ultimo == T("2026-T1")


# ---------------------------------------------------------------------------------------- lectura del lake

def _silver(tmp_path, filas):
    carpeta = tmp_path / "silver" / "cnmc_trimestral"
    carpeta.mkdir(parents=True)
    valores = ", ".join(f"({a}, {t}, 'Madrid-Barcelona', 'RENFE', {v})" for a, t, v in filas)
    duckdb.connect().execute(
        f"COPY (SELECT * FROM (VALUES {valores}) t(anio, trimestre, corredor, operador_id, viajeros)) "
        f"TO '{(carpeta / 'cnmc_trimestral.parquet').as_posix()}' (FORMAT PARQUET)"
    )


def _env(tmp_path):
    return {
        "SILVER_ROOT": (tmp_path / "silver").as_posix(),
        "GOLD_ROOT": (tmp_path / "gold").as_posix(),
        "PREDICCION_CONFIG": str(RAIZ / "config" / "prediccion.yml"),
    }


def test_comprobar_lee_silver_con_la_consulta_de_la_config_real(tmp_path):
    _silver(tmp_path, [(2025, 2, 10), (2025, 4, 12), (2026, 2, 11)])

    cobertura = cnmc.comprobar(T("2026-T4"), _env(tmp_path))

    assert cobertura.suficiente and cobertura.ultimo == T("2026-T2")


def test_comprobar_con_silver_incompleto_dice_que_trimestres_faltan(tmp_path):
    _silver(tmp_path, [(2025, 2, 10), (2026, 2, 11)])

    cobertura = cnmc.comprobar(T("2026-T4"), _env(tmp_path))

    assert not cobertura.suficiente and "2025-T4" in cobertura.motivo


def test_comprobar_sin_silver_no_falla_dice_que_no_hay_datos(tmp_path):
    cobertura = cnmc.comprobar(T("2026-T4"), _env(tmp_path))

    assert not cobertura.suficiente and "cnmc_trimestral" in cobertura.motivo


# ---------------------------------------------------------------------------------------- CLI

def test_la_cli_sale_con_0_si_hay_datos_y_con_3_si_faltan(tmp_path, capsys):
    _silver(tmp_path, [(2025, 2, 10), (2025, 4, 12), (2026, 2, 11)])
    env = _env(tmp_path)

    assert cnmc.main(["--trimestre", "2026-T4"], env) == 0
    assert "datos de la CNMC" in capsys.readouterr().out
    assert cnmc.main(["--trimestre", "2027-T4"], env) == cnmc.SALIDA_FALTAN_DATOS
    assert "faltan datos" in capsys.readouterr().out.lower()


def test_la_cobertura_es_un_valor_inmutable():
    cobertura = Cobertura(T("2026-T4"), True, "", T("2026-T2"), "ok")
    with pytest.raises(Exception):
        cobertura.suficiente = False
