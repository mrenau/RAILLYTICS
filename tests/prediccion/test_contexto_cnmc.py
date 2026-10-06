"""El bloque de datos de la CNMC que lleva el prompt cnmc_v1: lo calcula el código, el LLM solo lo interpreta."""
import math

import pandas as pd
import pytest

from raillytics.prediccion.contexto_cnmc import SIN_DATOS, construir_contexto, relacion_esperada
from raillytics.prediccion.trimestre import Trimestre

T = Trimestre.parse


def _df(datos):
    """{'2025-T4': {'RENFE': (viajeros, plazas), ...}} -> DataFrame con el contrato del origen `cnmc`."""
    filas = [(t, op, v, p) for t, ops in datos.items() for op, (v, p) in ops.items()]
    return pd.DataFrame(filas, columns=["trimestre", "operador", "viajeros", "plazas_ofertadas"])


DATOS = {
    "2024-T4": {"RENFE": (540_000, 600_000), "IRYO": (270_000, 300_000), "OUIGO": (90_000, 100_000)},
    "2025-T4": {"RENFE": (600_000, 800_000), "IRYO": (300_000, 400_000), "OUIGO": (100_000, 200_000)},
    "2026-T1": {"RENFE": (500_000, 800_000), "IRYO": (250_000, 400_000), "OUIGO": (50_000, 200_000)},
    # El propio objetivo y posteriores no deben salir: son datos que aún no existirían al predecir.
    "2026-T4": {"RENFE": (1, 1)},
}


def test_lista_los_trimestres_publicados_anteriores_con_viajeros_plazas_y_relacion():
    texto = construir_contexto(_df(DATOS), T("2026-T4"), 1_050_000)

    assert "- 2025-T4: 1.000.000 viajeros · 1.400.000 plazas ofertadas · relación viajeros/plazas 71,4 %" in texto
    assert "- 2026-T1: 800.000 viajeros · 1.400.000 plazas ofertadas · relación viajeros/plazas 57,1 %" in texto
    assert "- 2026-T4" not in texto


def test_anade_la_variacion_interanual_cuando_esta_el_mismo_trimestre_del_ano_anterior():
    texto = construir_contexto(_df(DATOS), T("2026-T4"), 1_050_000)

    assert "interanual +11,1 %" in texto            # 2025-T4 (1.000.000) frente a 2024-T4 (900.000)
    linea_t1 = next(l for l in texto.splitlines() if l.startswith("- 2026-T1"))
    assert "interanual" not in linea_t1              # no hay 2025-T1


def test_muestra_las_cuotas_por_operador_ordenadas_de_mayor_a_menor():
    texto = construir_contexto(_df(DATOS), T("2026-T4"), 1_050_000)

    assert "cuotas RENFE 60 % / IRYO 30 % / OUIGO 10 %" in texto


def test_la_relacion_esperada_es_el_total_esperado_entre_las_plazas_del_mismo_trimestre_del_ano_anterior():
    assert relacion_esperada(_df(DATOS), T("2026-T4"), 1_050_000) == pytest.approx(0.75)

    texto = construir_contexto(_df(DATOS), T("2026-T4"), 1_050_000)
    assert "Relación viajeros/plazas esperada de 2026-T4: 75,0 %" in texto


def test_el_techo_sale_de_la_maxima_relacion_observada_y_no_de_suponer_que_100_es_un_tren_lleno():
    # La CNMC cuenta viajeros y plazas con criterios distintos (la relación pasa del 100 % en varios trimestres), así que el
    # techo compara con lo ya visto: la máxima relación publicada (90 %, 2024-T4) entre la esperada (75 %) = 1,20 veces.
    texto = construir_contexto(_df(DATOS), T("2026-T4"), 1_050_000)

    assert "90,0 % en 2024-T4" in texto and "1,20 veces la demanda media" in texto
    assert "no es una ocupación literal" in texto


def test_si_la_relacion_esperada_iguala_o_supera_la_maxima_observada_no_hay_margen():
    texto = construir_contexto(_df(DATOS), T("2026-T4"), 1_500_000)   # 107 % frente a un máximo observado del 90 %

    assert "Sin margen" in texto and "107,1 %" in texto and "veces la demanda media" not in texto


def test_sin_plazas_no_calcula_relaciones_pero_conserva_viajeros_y_cuotas():
    datos = {"2025-T4": {"RENFE": (600_000, None), "IRYO": (400_000, None)}}

    texto = construir_contexto(_df(datos), T("2026-T4"), 1_000_000)

    assert "- 2025-T4: 1.000.000 viajeros" in texto and "relación" not in texto
    assert relacion_esperada(_df(datos), T("2026-T4"), 1_000_000) is None


@pytest.mark.parametrize("datos", [None, pd.DataFrame([], columns=["trimestre", "operador", "viajeros", "plazas_ofertadas"])])
def test_sin_datos_de_la_cnmc_el_texto_lo_avisa_sin_inventar_nada(datos):
    assert construir_contexto(datos, T("2026-T4"), 1_000_000) == SIN_DATOS


def test_como_mucho_ocho_trimestres_los_mas_recientes():
    datos = {f"{a}-T{n}": {"RENFE": (100, 200)} for a in range(2020, 2026) for n in (1, 2, 3, 4)}

    lineas = [l for l in construir_contexto(_df(datos), T("2026-T1"), 100).splitlines() if l.startswith("- 20")]

    assert len(lineas) == 8 and lineas[0].startswith("- 2024-T1") and lineas[-1].startswith("- 2025-T4")


def test_un_valor_nulo_de_viajeros_no_rompe_el_resumen():
    datos = {"2025-T4": {"RENFE": (600_000, 800_000), "IRYO": (math.nan, 400_000)}}

    assert "2025-T4" in construir_contexto(_df(datos), T("2026-T4"), 1_000_000)
