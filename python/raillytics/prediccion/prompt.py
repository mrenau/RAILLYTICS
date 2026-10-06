"""Plantilla versionada del prompt (config/prompts/demanda_vN.md) y su relleno.

Los marcadores `{{nombre}}` se sustituyen en UNA sola pasada: un marcador que aparezca dentro de un
valor insertado (p. ej. en la descripción de un evento) no se vuelve a expandir.
"""
from __future__ import annotations

import re
from collections.abc import Mapping
from pathlib import Path

from raillytics.prediccion.calendario import Calendario, lineas_prompt
from raillytics.prediccion.nivel import miles
from raillytics.prediccion.trimestre import Trimestre

MARCADOR = re.compile(r"\{\{(\w+)\}\}")
MAX_HISTORICO = 8
_VERSION = re.compile(r"^[A-Za-z0-9_-]+$")

NOTA_CON_EVENTOS = "Hay datos de eventos para este trimestre: los días sin evento figuran como «ninguno»."
NOTA_SIN_EVENTOS = (
    "ATENCIÓN: no hay datos de eventos para este trimestre. «sin datos» NO significa que no los haya: "
    "no supongas que son días tranquilos."
)


class PlantillaError(ValueError):
    """La plantilla del prompt no existe o usa un marcador desconocido."""


def cargar_plantilla(version: str, directorio: Path) -> str:
    if not _VERSION.match(version):
        raise PlantillaError(f"versión de prompt inválida {version!r}: solo letras, dígitos, '_' y '-'")
    ruta = directorio / f"{version}.md"
    if not ruta.is_file():
        disponibles = sorted(p.stem for p in directorio.glob("*.md")) if directorio.is_dir() else []
        raise PlantillaError(f"no existe la plantilla {ruta}. Disponibles: {', '.join(disponibles) or 'ninguna'}")
    return ruta.read_text(encoding="utf-8")


def renderizar(plantilla: str, valores: Mapping[str, str]) -> str:
    sin_valor = sorted(set(MARCADOR.findall(plantilla)) - set(valores))
    if sin_valor:
        raise PlantillaError(f"la plantilla usa marcadores sin valor: {', '.join(repr(m) for m in sin_valor)}")
    return MARCADOR.sub(lambda coincidencia: valores[coincidencia.group(1)], plantilla)


def construir_prompt(
    plantilla: str,
    trimestre: Trimestre,
    total_esperado: int,
    historico: Mapping[Trimestre, int],
    calendario: Calendario,
    corredor: str,
    datos_cnmc: str = "",
) -> str:
    previos = sorted(t for t in historico if t < trimestre)[-MAX_HISTORICO:]
    lineas_historico = "\n".join(f"- {t}: {miles(historico[t])} viajeros" for t in previos)
    lineas = lineas_prompt(calendario)
    con_contexto = lineas_prompt(calendario, climatologia=False, contexto=True)
    con_evento = [linea for linea, eventos in zip(con_contexto, calendario.dias["eventos"]) if eventos]
    return renderizar(
        plantilla,
        {
            "corredor": corredor,
            "trimestre": str(trimestre),
            "num_dias": str(len(lineas)),
            "total_esperado": miles(total_esperado),
            "historico": lineas_historico or "- (sin trimestres publicados)",
            "nota_eventos": NOTA_CON_EVENTOS if calendario.eventos_con_datos else NOTA_SIN_EVENTOS,
            "calendario": "\n".join(lineas),
            # Sin la meteo de climatología (igual para todo el mes): solo la observada, si la hay.
            "calendario_sin_climatologia": "\n".join(lineas_prompt(calendario, climatologia=False)),
            # Además, con el campo «contexto»: víspera, puente, regreso o junto a un evento, escrito en la línea de cada día.
            "calendario_con_contexto": "\n".join(con_contexto),
            # Modo eventos (plantillas eventos_vN): solo los días con evento, que son los únicos que valora el LLM.
            "dias_con_evento": "\n".join(con_evento) or "(ningún día con evento)",
            "num_dias_con_evento": str(len(con_evento)),
            # Plantillas cnmc_vN: viajeros, plazas, ocupación y cuotas reales (raillytics.prediccion.contexto_cnmc).
            "datos_cnmc": datos_cnmc,
        },
    )
