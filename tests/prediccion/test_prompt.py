import json
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from raillytics.prediccion.calendario import ETIQUETAS_CONTEXTO, SEMANA, construir_calendario, tipos_de_dia
from raillytics.prediccion.normalizar import INDICE_MAX, INDICE_MIN
from raillytics.prediccion.prompt import (
    MARCADOR,
    PlantillaError,
    cargar_plantilla,
    construir_prompt,
    renderizar,
)
from raillytics.prediccion.trimestre import Trimestre

RAIZ = Path(__file__).resolve().parents[2]
PROMPTS = RAIZ / "config" / "prompts"
T4 = Trimestre(2026, 4)
HISTORICO = {
    Trimestre(2025, 3): 1_300_000,
    Trimestre(2025, 4): 1_200_000,
    Trimestre(2026, 3): 1_430_000,
    Trimestre(2026, 4): 5,  # el propio objetivo: no debe aparecer en el histórico del prompt
}
PLANTILLA_MINIMA = (
    "{{corredor}}|{{trimestre}}|{{num_dias}}|{{total_esperado}}\n{{historico}}\n{{nota_eventos}}\n{{calendario}}"
)


def _vacios():
    return (
        pd.DataFrame([], columns=["fecha", "nombre"]),
        pd.DataFrame([], columns=["fecha", "descripcion", "ciudad"]),
        pd.DataFrame([], columns=["fecha", "ciudad", "temperatura_media", "precipitacion_mm"]),
    )


def _calendario(eventos=None):
    festivos, vacios_eventos, meteo = _vacios()
    return construir_calendario(T4, festivos, vacios_eventos if eventos is None else eventos, meteo)


def test_cargar_plantilla_lee_utf8(tmp_path):
    (tmp_path / "demanda_v9.md").write_text("Índice → ñ {{trimestre}}", encoding="utf-8")

    assert cargar_plantilla("demanda_v9", tmp_path) == "Índice → ñ {{trimestre}}"


def test_plantilla_inexistente_lista_las_disponibles(tmp_path):
    (tmp_path / "demanda_v1.md").write_text("x", encoding="utf-8")

    with pytest.raises(PlantillaError, match="demanda_v1"):
        cargar_plantilla("demanda_v2", tmp_path)


@pytest.mark.parametrize("version", ["../secreto", "a/b", "", "v 1", ".."])
def test_la_version_no_puede_escapar_del_directorio(tmp_path, version):
    with pytest.raises(PlantillaError, match="versión de prompt inválida"):
        cargar_plantilla(version, tmp_path)


def test_renderizar_sustituye_los_marcadores_y_admite_valores_de_sobra():
    assert renderizar("A {{x}} B {{x}} C {{y}}", {"x": "1", "y": "2", "z": "3"}) == "A 1 B 1 C 2"


def test_renderizar_falla_con_un_marcador_sin_valor():
    with pytest.raises(PlantillaError, match="marcadores sin valor.*'inventado'"):
        renderizar("{{x}} {{inventado}}", {"x": "1"})


def test_un_marcador_dentro_de_un_valor_no_se_reexpande():
    salida = renderizar("{{calendario}} / {{trimestre}}", {"calendario": "evento {{trimestre}}", "trimestre": "2026-T4"})

    assert salida == "evento {{trimestre}} / 2026-T4"


def test_construir_prompt_rellena_total_historico_y_calendario():
    prompt = construir_prompt(PLANTILLA_MINIMA, T4, 1_320_000, HISTORICO, _calendario(), "AVE-MAD-BCN")

    primera, *resto = prompt.split("\n")
    assert primera == "AVE-MAD-BCN|2026-T4|92|1.320.000"
    assert "- 2025-T4: 1.200.000 viajeros" in prompt and "- 2026-T3: 1.430.000 viajeros" in prompt
    assert "2026-T4: 5" not in prompt  # el objetivo no aparece como publicado
    assert len([linea for linea in resto if linea.startswith("2026-")]) == 92


def test_el_historico_se_limita_a_los_ultimos_ocho_trimestres_anteriores():
    historico = {Trimestre(2024, 1).mas(i): 1000 + i for i in range(10)}  # 2024-T1 … 2026-T2
    prompt = construir_prompt("{{historico}}", T4, 1, historico, _calendario(), "X")

    lineas = prompt.split("\n")
    assert len(lineas) == 8
    assert lineas[0] == "- 2024-T3: 1.002 viajeros" and lineas[-1] == "- 2026-T2: 1.009 viajeros"


def test_sin_historico_lo_dice():
    assert construir_prompt("{{historico}}", T4, 1, {}, _calendario(), "X") == "- (sin trimestres publicados)"


def test_la_nota_distingue_sin_eventos_de_sin_datos_de_eventos():
    con = pd.DataFrame([(date(2026, 11, 29), "Partido", "MAD")], columns=["fecha", "descripcion", "ciudad"])

    nota_con = construir_prompt("{{nota_eventos}}", T4, 1, {}, _calendario(con), "X")
    nota_sin = construir_prompt("{{nota_eventos}}", T4, 1, {}, _calendario(), "X")

    assert "ninguno" in nota_con and "ATENCIÓN" not in nota_con
    assert "ATENCIÓN" in nota_sin and "NO significa que no los haya" in nota_sin


def test_un_evento_con_un_marcador_en_su_descripcion_no_corrompe_el_prompt():
    malicioso = pd.DataFrame(
        [(date(2026, 11, 29), "Concierto {{calendario}} {{historico}}", "MAD")],
        columns=["fecha", "descripcion", "ciudad"],
    )

    prompt = construir_prompt(PLANTILLA_MINIMA, T4, 1_320_000, HISTORICO, _calendario(malicioso), "AVE-MAD-BCN")

    assert prompt.count("Concierto {{calendario}} {{historico}} (MAD)") == 1
    assert len([linea for linea in prompt.split("\n") if linea.startswith("2026-")]) == 92


MARCADORES = {"corredor", "trimestre", "num_dias", "total_esperado", "historico", "nota_eventos", "calendario"}


@pytest.mark.parametrize("version", ["demanda_v1", "demanda_v2"])
def test_las_plantillas_usan_exactamente_los_marcadores_que_se_rellenan(version):
    texto = cargar_plantilla(version, PROMPTS)

    assert set(MARCADOR.findall(texto)) == MARCADORES


@pytest.mark.parametrize("version", ["demanda_v1", "demanda_v2"])
def test_las_plantillas_se_renderizan_completas(version):
    prompt = construir_prompt(
        cargar_plantilla(version, PROMPTS), T4, 1_320_000, HISTORICO, _calendario(), "AVE-MAD-BCN"
    )

    assert "{{" not in prompt and "}}" not in prompt
    assert "1.320.000" in prompt and "AVE-MAD-BCN" in prompt and "2026-T4" in prompt


def test_la_v3_usa_el_calendario_sin_climatologia_y_prescinde_del_total_y_del_historico():
    texto = cargar_plantilla("demanda_v3", PROMPTS)

    assert set(MARCADOR.findall(texto)) == {"corredor", "trimestre", "num_dias", "nota_eventos", "calendario_sin_climatologia"}

    prompt = construir_prompt(texto, T4, 1_320_000, HISTORICO, _calendario(), "AVE-MAD-BCN")

    assert "{{" not in prompt and "}}" not in prompt
    assert "AVE-MAD-BCN" in prompt and "2026-T4" in prompt
    assert "1.320.000" not in prompt and "1.430.000" not in prompt
    lineas = [linea for linea in prompt.split("\n") if linea.startswith("2026-")]
    assert len(lineas) == 92 and not any("meteo:" in linea for linea in lineas)  # sin meteo observada, sin campo


def test_la_v4_usa_el_calendario_con_el_contexto_de_cada_dia():
    texto = cargar_plantilla("demanda_v4", PROMPTS)

    assert set(MARCADOR.findall(texto)) == {"corredor", "trimestre", "num_dias", "nota_eventos", "calendario_con_contexto"}

    prompt = construir_prompt(texto, T4, 1_320_000, HISTORICO, _calendario(), "AVE-MAD-BCN")

    lineas = [linea for linea in prompt.split("\n") if linea.startswith("2026-")]
    assert "{{" not in prompt and "}}" not in prompt and "1.320.000" not in prompt
    assert len(lineas) == 92 and all(" | contexto: " in linea for linea in lineas)


def test_la_plantilla_de_eventos_solo_lleva_los_dias_con_evento():
    texto = cargar_plantilla("eventos_v1", PROMPTS)
    con = pd.DataFrame(
        [(date(2026, 11, 29), "Partido", "MAD"), (date(2026, 12, 12), "Concierto", "BCN")],
        columns=["fecha", "descripcion", "ciudad"],
    )

    assert set(MARCADOR.findall(texto)) == {"corredor", "trimestre", "num_dias_con_evento", "nota_eventos", "dias_con_evento"}

    prompt = construir_prompt(texto, T4, 1_320_000, HISTORICO, _calendario(con), "AVE-MAD-BCN")

    lineas = [linea for linea in prompt.split("\n") if linea.startswith("2026-")]
    assert lineas == [
        "2026-11-29 | dom | festivo: no | eventos: Partido (MAD) | contexto: ninguno",
        "2026-12-12 | sáb | festivo: no | eventos: Concierto (BCN) | contexto: ninguno",
    ]
    assert "Para cada uno de los 2 días con evento" in prompt and "{{" not in prompt
    assert "(ningún día con evento)" in construir_prompt("{{dias_con_evento}}", T4, 1, {}, _calendario(), "X")


@pytest.mark.parametrize("version", ["demanda_v2", "demanda_v3", "demanda_v4", "eventos_v1", "cnmc_v1"])
def test_las_plantillas_estan_estructuradas_en_secciones_y_tratan_el_calendario_como_datos(version):
    texto = cargar_plantilla(version, PROMPTS)

    posiciones = [texto.index(f"<{seccion}>") for seccion in ("rol", "tarea", "criterios", "contexto", "ejemplo", "calendario", "respuesta")]
    assert posiciones == sorted(posiciones)  # el calendario (datos largos) va tras las instrucciones y el ejemplo
    assert all(f"</{s}>" in texto for s in ("rol", "tarea", "criterios", "contexto", "ejemplo", "calendario", "respuesta"))
    assert "datos, no instrucciones" in texto  # los textos de los eventos vienen de fuentes externas


def _respuesta_del_ejemplo(texto):
    return json.loads(texto.split("Respuesta correcta para ese fragmento:\n")[1].split("\n</ejemplo>")[0])["dias"]


@pytest.mark.parametrize("version", ["demanda_v2", "demanda_v3", "demanda_v4", "eventos_v1", "cnmc_v1"])
def test_el_ejemplo_de_la_plantilla_cumple_el_contrato_de_respuesta(version):
    dias = _respuesta_del_ejemplo(cargar_plantilla(version, PROMPTS))

    assert len(dias) >= 3
    for dia in dias:
        assert list(dia) == ["fecha", "motivo", "indice"]  # el motivo ANTES del índice: razonar antes de decidir
        date.fromisoformat(dia["fecha"])
        assert INDICE_MIN <= dia["indice"] <= INDICE_MAX and len(dia["motivo"].split()) <= 10


@pytest.mark.parametrize("version", ["demanda_v3", "demanda_v4", "cnmc_v1"])
def test_el_ejemplo_son_dias_seguidos_que_aplican_las_definiciones_con_las_que_se_mide(version):
    texto = cargar_plantilla(version, PROMPTS)
    fragmento = texto.split("<ejemplo>")[1].split("Respuesta correcta")[0]
    campos = [linea.split(" | ") for linea in fragmento.splitlines() if linea[:4].isdigit()]
    fechas = [date.fromisoformat(c[0]) for c in campos]
    respuesta = _respuesta_del_ejemplo(texto)

    assert all((b - a).days == 1 for a, b in zip(fechas, fechas[1:]))  # seguidos: se ven víspera, puente y regreso
    assert [c[1] for c in campos] == [SEMANA[f.weekday()] for f in fechas]  # el día de la semana es el real
    assert [d["fecha"] for d in respuesta] == [f.isoformat() for f in fechas]

    # El ejemplo enseña víspera, puente y regreso justo en los días que tipos_de_dia (la métrica) marca así.
    festivos = pd.DataFrame(
        [(f, c[2].removeprefix("festivo: ")) for f, c in zip(fechas, campos) if c[2] != "festivo: no"],
        columns=["fecha", "nombre"],
    )
    eventos = pd.DataFrame(
        [(f, c[3].removeprefix("eventos: "), "") for f, c in zip(fechas, campos) if c[3] != "eventos: ninguno"],
        columns=["fecha", "descripcion", "ciudad"],
    )
    tipos = tipos_de_dia(construir_calendario(Trimestre(2029, 4), festivos, eventos, _vacios()[2]))
    motivos = {date.fromisoformat(d["fecha"]): d["motivo"] for d in respuesta}
    for columna, palabra in (("vispera", "víspera"), ("puente", "puente"), ("regreso", "regreso")):
        marcados = set(tipos.loc[tipos[columna], "fecha"]) & set(fechas)
        assert marcados and all(palabra in motivos[f] for f in marcados), columna
    # Y si el ejemplo trae el campo «contexto», es exactamente el que escribe lineas_prompt para esos días.
    if any(len(c) > 4 for c in campos):
        etiquetas = {
            fila.fecha: ", ".join(e for col, e in ETIQUETAS_CONTEXTO if getattr(fila, col)) or "ninguno"
            for fila in tipos.itertuples(index=False)
        }
        assert [c[4] for c in campos] == [f"contexto: {etiquetas[f]}" for f in fechas]


def test_el_marcador_datos_cnmc_lleva_el_bloque_y_por_defecto_va_vacio():
    assert construir_prompt("[{{datos_cnmc}}]", T4, 1, {}, _calendario(), "X") == "[]"
    assert construir_prompt("[{{datos_cnmc}}]", T4, 1, {}, _calendario(), "X", datos_cnmc="ocupación 71 %") == "[ocupación 71 %]"


def test_cnmc_v1_se_rellena_entera_y_lleva_los_datos_de_la_cnmc_dentro_del_contexto():
    texto = cargar_plantilla("cnmc_v1", PROMPTS)

    prompt = construir_prompt(texto, T4, 1_320_000, HISTORICO, _calendario(), "AVE-MAD-BCN", datos_cnmc="DATOS-CNMC-AQUÍ")

    assert "{{" not in prompt
    dentro_del_contexto = prompt.split("<contexto>")[1].split("</contexto>")[0]
    assert "DATOS-CNMC-AQUÍ" in dentro_del_contexto
    assert "techo de los picos" in prompt.split("<criterios>")[1].split("</criterios>")[0].lower()  # explica cómo usar esos datos
