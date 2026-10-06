"""Fuentes SINTÉTICAS de la predicción: deben encajar con las consultas que trae config/prediccion.yml."""
from datetime import date, datetime
from pathlib import Path

import duckdb
import pytest

from raillytics.prediccion.entradas import cargar_config, cargar_entradas
from raillytics.prediccion.muestra import (
    FUENTES,
    demanda_trimestral,
    escribir_muestra,
    eventos,
    festivos,
    generar_muestra,
    main,
    meteo,
)
from raillytics.prediccion.nivel import calcular_nivel
from raillytics.prediccion.ollama import OllamaSettings
from raillytics.prediccion.normalizar import IndiceDia
from raillytics.prediccion.servicio import ejecutar
from raillytics.prediccion.trimestre import Trimestre
from raillytics.utils.lake import LakeLayout, connect

RAIZ = Path(__file__).resolve().parents[2]
HASTA = Trimestre(2026, 3)


def test_la_demanda_cubre_los_trimestres_pedidos_con_dos_sentidos_y_solo_el_corredor_madrid_barcelona():
    df = demanda_trimestral(HASTA, 11, 42)
    mad = df[df["corredor"] == "AVE-MAD-BCN"]

    trimestres = sorted(set(zip(mad["anio"], mad["trimestre"])))
    assert len(trimestres) == 11 and trimestres[0] == (2024, 1) and trimestres[-1] == (2026, 3)
    assert (mad.groupby(["anio", "trimestre"])["sentido"].nunique() == 2).all()
    assert set(df["corredor"]) == {"AVE-MAD-BCN"}  # el ejemplo es solo del corredor: ningún otro
    assert (df["viajeros"] > 0).all()


def test_la_demanda_tiene_estacionalidad_y_crecimiento_y_es_determinista():
    df = demanda_trimestral(HASTA, 11, 42)
    total = df[df["corredor"] == "AVE-MAD-BCN"].groupby(["anio", "trimestre"])["viajeros"].sum()

    assert total[(2025, 3)] > total[(2025, 1)]  # el verano mueve más que el invierno
    assert sum(total[(2026, t)] for t in (1, 2, 3)) > sum(total[(2025, t)] for t in (1, 2, 3))
    assert demanda_trimestral(HASTA, 11, 42).equals(df)
    assert not demanda_trimestral(HASTA, 11, 43).equals(df)


def test_los_festivos_incluyen_los_nacionales_hasta_un_anio_despues_del_ultimo_trimestre():
    df = festivos(2024, 2027)

    assert (date(2026, 12, 25), "Natividad del Señor") in set(zip(df["fecha"], df["nombre"]))
    assert min(df["fecha"]).year == 2024 and max(df["fecha"]).year == 2027


def test_los_eventos_son_de_muestra_de_madrid_o_barcelona_y_hay_alguno_en_el_trimestre_a_predecir():
    df = eventos(2024, 2027, 42)

    assert df["descripcion"].str.endswith("(muestra)").all()  # nunca se confunden con datos reales
    assert set(df["ciudad"]) <= {"MAD", "BCN"}
    assert eventos(2024, 2027, 42).equals(df)
    en_t4 = [f for f in df["fecha"] if Trimestre.de_fecha(f) == Trimestre(2026, 4)]
    assert len(en_t4) >= 1


def test_la_meteo_cubre_cada_dia_de_las_dos_ciudades_con_valores_plausibles():
    df = meteo(date(2024, 1, 1), date(2026, 9, 30), 42)
    dias = (date(2026, 9, 30) - date(2024, 1, 1)).days + 1

    assert df.groupby("ciudad")["fecha"].nunique().to_dict() == {"BCN": dias, "MAD": dias}
    assert df["tmed"].between(-10, 45).all() and (df["prec"] >= 0).all()
    mes = df[df["ciudad"] == "MAD"].assign(mes=lambda x: x["fecha"].dt.month).groupby("mes")["tmed"].mean()
    assert mes[7] > mes[1] + 10  # julio mucho más cálido que enero


@pytest.fixture
def lake(tmp_path):
    bronze = tmp_path / "bronze"
    layout = LakeLayout(silver_root=(tmp_path / "silver").as_posix(), gold_root=(tmp_path / "gold").as_posix())
    env = {"BRONZE_ROOT": bronze.as_posix(), "SILVER_ROOT": layout.silver_root, "GOLD_ROOT": layout.gold_root}
    return layout, env, connect(), bronze


def _silver_cnmc_desde_la_muestra(layout, con, muestra):
    """`trimestrales` de la config real lee el Silver de la CNMC (make 04_silver), no la muestra: se le da uno con la forma real."""
    destino = Path(layout.silver_root) / "cnmc_trimestral"
    destino.mkdir(parents=True)
    demanda = muestra["demanda_trimestral"]
    con.register("demanda_muestra", demanda[demanda["corredor"] == "AVE-MAD-BCN"])
    con.execute(
        "COPY (SELECT anio, trimestre, 'Madrid-Barcelona' AS corredor, 'RENFE' AS operador_id, sum(viajeros) AS viajeros, "
        "CAST(NULL AS BIGINT) AS plazas_ofertadas "
        f"FROM demanda_muestra GROUP BY anio, trimestre) TO '{(destino / 'cnmc_trimestral.parquet').as_posix()}' (FORMAT PARQUET)"
    )


def test_escribir_muestra_usa_prefijos_propios_que_nunca_se_mezclan_con_las_fuentes_reales(lake):
    layout, env, con, bronze = lake

    escritos = escribir_muestra(con, layout, bronze.as_posix(), generar_muestra(HASTA, 11, 42), {"hasta": "2026-T3"})

    assert set(escritos) == set(FUENTES) == {"demanda_trimestral", "festivos", "eventos", "aemet"}
    for fuente, ruta in escritos.items():
        assert Path(ruta) == bronze / "l2" / f"muestra_{fuente}" / "sintetico" / f"{fuente}.parquet" and Path(ruta).is_file()
    assert list(Path(layout.cargas_dir).glob("*.parquet"))  # queda constancia de la carga


def test_la_muestra_cumple_el_contrato_de_la_config_real_y_permite_calcular_el_nivel(lake):
    layout, env, con, bronze = lake
    muestra = generar_muestra(HASTA, 11, 42)
    escribir_muestra(con, layout, bronze.as_posix(), muestra, {})
    _silver_cnmc_desde_la_muestra(layout, con, muestra)

    entradas = cargar_entradas(cargar_config(RAIZ / "config" / "prediccion.yml"), layout, con, env)

    assert len(entradas.trimestrales) == 11 and not entradas.festivos.empty and not entradas.eventos.empty
    assert set(entradas.meteo["ciudad"]) == {"MAD", "BCN"}
    nivel = calcular_nivel(entradas.trimestrales_dict(), Trimestre(2026, 4))
    assert nivel.total > 0 and nivel.ultimo == HASTA


class _ClienteFalso:
    settings = OllamaSettings(url="http://falso", modelo="falso", num_ctx=1024, timeout_s=1, seed=1)

    def comprobar(self):
        pass

    def generar_indices(self, prompt, dias, reintentos=2):
        return [IndiceDia(d, 1.3 if d.weekday() >= 5 else 1.0, "fin de semana" if d.weekday() >= 5 else "laborable") for d in dias]


def test_con_la_muestra_corre_toda_la_prediccion_con_la_config_y_el_prompt_reales(lake, tmp_path):
    layout, env, con, bronze = lake
    muestra = generar_muestra(HASTA, 11, 42)
    escribir_muestra(con, layout, bronze.as_posix(), muestra, {})
    _silver_cnmc_desde_la_muestra(layout, con, muestra)
    env = dict(
        env,
        PREDICCION_CONFIG=str(RAIZ / "config" / "prediccion.yml"),
        PROMPTS_DIR=str(RAIZ / "config" / "prompts"),
        PREDICCIONES_ROOT=str(tmp_path / "salida"),
    )

    resultado = ejecutar(
        Trimestre(2026, 4), version_prompt="demanda_v2", total_manual=None, solo_nivel=False, env=env,
        layout=layout, con=con, cliente=_ClienteFalso(), ahora=datetime(2026, 10, 1, 16, 51, 0), imprimir=lambda _: None,
    )

    assert len(resultado.dataframe) == 92 and resultado.ruta.is_file()
    assert resultado.dataframe["viajeros_previstos"].sum() == resultado.total_esperado


def test_el_cli_genera_las_cuatro_fuentes_y_avisa_de_que_son_sinteticas(lake, capsys):
    layout, env, _, bronze = lake

    codigo = main(["--hasta", "2026-T3", "--semilla", "1"], env=env)

    salida = capsys.readouterr().out
    assert codigo == 0 and "NO son datos reales" in salida and "make 07_prediccion" in salida
    assert len(list((bronze / "l2").glob("muestra_*/sintetico/*.parquet"))) == 4


@pytest.mark.parametrize("argumentos", [["--hasta", "2026-Q3"], ["--trimestres", "0"], ["--trimestres", "mucho"]])
def test_el_cli_rechaza_argumentos_invalidos(argumentos):
    with pytest.raises(SystemExit) as salida:
        main(argumentos, env={})

    assert salida.value.code == 2
