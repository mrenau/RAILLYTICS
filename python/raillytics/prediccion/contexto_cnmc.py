"""Bloque de datos de la CNMC para el prompt cnmc_v1 (marcador `{{datos_cnmc}}`).

La CNMC solo publica agregados trimestrales, pero de ahí salen dos pistas para repartir el trimestre por días: la relación
viajeros/plazas ofertadas, que da una referencia de cuánto se puede apretar un pico, y la tendencia interanual. Las calcula el
código (el LLM no hace aritmética): el prompt solo le pide interpretarlas.

OJO: no es una ocupación. La CNMC cuenta viajeros y plazas con criterios distintos y la relación pasa del 100 % en varios
trimestres (Renfe llega al 115 %), así que 100 % no significa «tren lleno». Por eso el techo de los picos no se calcula como
1/ocupación, sino comparando la relación esperada con la MÁXIMA ya publicada: con las mismas plazas se movió hasta esa relación.

Entrada: el origen `cnmc` de config/prediccion.yml (trimestre, operador, viajeros, plazas_ofertadas), una fila por trimestre
y operador del corredor. Solo se usan trimestres ANTERIORES al objetivo.
"""
from __future__ import annotations

import pandas as pd

from raillytics.prediccion.nivel import miles
from raillytics.prediccion.trimestre import Trimestre

MAX_TRIMESTRES = 8
SIN_DATOS = (
    "ATENCIÓN: no hay datos de la CNMC (viajeros y plazas por operador) para este trimestre. No supongas relaciones ni "
    "cuotas: reparte solo con el calendario."
)


def _pct(valor: float, decimales: int = 1) -> str:
    return f"{valor * 100:.{decimales}f}".replace(".", ",") + " %"


def _por_trimestre(cnmc: pd.DataFrame) -> pd.DataFrame:
    """Viajeros y plazas del corredor por trimestre (suma de operadores), indexado por Trimestre."""
    agregado = cnmc.groupby("trimestre")[["viajeros", "plazas_ofertadas"]].sum(min_count=1)
    agregado.index = [Trimestre.parse(t) for t in agregado.index]
    return agregado.sort_index()


def _hay_datos(cnmc: pd.DataFrame | None) -> bool:
    return cnmc is not None and not cnmc.empty


def _relacion(por_trimestre: pd.DataFrame, trimestre: Trimestre) -> float | None:
    viajeros, plazas = por_trimestre.loc[trimestre, "viajeros"], por_trimestre.loc[trimestre, "plazas_ofertadas"]
    if pd.isna(viajeros) or pd.isna(plazas) or plazas <= 0:
        return None
    return viajeros / plazas


def relacion_esperada(cnmc: pd.DataFrame | None, objetivo: Trimestre, total_esperado: int) -> float | None:
    """Total esperado entre las plazas ofertadas el mismo trimestre del año anterior (None si no hay plazas)."""
    if not _hay_datos(cnmc):
        return None
    por_trimestre = _por_trimestre(cnmc)
    base = objetivo.menos(4)
    if base not in por_trimestre.index:
        return None
    plazas = por_trimestre.loc[base, "plazas_ofertadas"]
    if pd.isna(plazas) or plazas <= 0:
        return None
    return total_esperado / plazas


def _cuotas(cnmc: pd.DataFrame, trimestre: Trimestre) -> str:
    filas = cnmc[cnmc["trimestre"] == str(trimestre)].dropna(subset=["viajeros"])
    total = filas["viajeros"].sum()
    if filas.empty or total <= 0:
        return ""
    por_operador = filas.groupby("operador")["viajeros"].sum().sort_values(ascending=False)
    return " / ".join(f"{op} {_pct(v / total, 0)}" for op, v in por_operador.items())


def _linea(cnmc: pd.DataFrame, por_trimestre: pd.DataFrame, trimestre: Trimestre) -> str:
    viajeros, plazas = por_trimestre.loc[trimestre, "viajeros"], por_trimestre.loc[trimestre, "plazas_ofertadas"]
    partes = [f"{miles(int(viajeros))} viajeros"]
    relacion = _relacion(por_trimestre, trimestre)
    if relacion is not None:
        partes += [f"{miles(int(plazas))} plazas ofertadas", f"relación viajeros/plazas {_pct(relacion)}"]
    anterior = trimestre.menos(4)
    if anterior in por_trimestre.index and por_trimestre.loc[anterior, "viajeros"] > 0:
        variacion = (viajeros / por_trimestre.loc[anterior, "viajeros"] - 1) * 100
        partes.append(f"interanual {variacion:+.1f} %".replace(".", ","))
    cuotas = _cuotas(cnmc, trimestre)
    if cuotas:
        partes.append(f"cuotas {cuotas}")
    return f"- {trimestre}: " + " · ".join(partes)


def _techo(por_trimestre: pd.DataFrame, previos: list[Trimestre], objetivo: Trimestre, relacion: float) -> str:
    base = objetivo.menos(4)
    relaciones = {t: r for t in previos if (r := _relacion(por_trimestre, t)) is not None}
    encabezado = (
        f"Relación viajeros/plazas esperada de {objetivo}: {_pct(relacion)} (total esperado entre las plazas ofertadas en {base}). "
        "La CNMC cuenta viajeros y plazas con criterios distintos y la relación pasa del 100 % en varios trimestres: "
        "no es una ocupación literal, solo sirve para comparar con lo ya visto."
    )
    if not relaciones:
        return encabezado
    mejor = max(relaciones, key=relaciones.get)
    maxima = relaciones[mejor]
    if relacion >= maxima:
        return (
            f"{encabezado} Sin margen: iguala o supera la máxima de los últimos trimestres ({_pct(maxima)} en {mejor}), "
            "así que los picos no pueden separarse mucho de un día normal."
        )
    techo = f"{maxima / relacion:.2f}".replace(".", ",")
    return (
        f"{encabezado} La máxima de los últimos trimestres es {_pct(maxima)} en {mejor}: con las mismas plazas ya se movió hasta "
        f"{techo} veces la demanda media diaria esperada. Es la referencia del techo de los picos (vísperas, regresos, domingos)."
    )


def construir_contexto(cnmc: pd.DataFrame | None, objetivo: Trimestre, total_esperado: int) -> str:
    if not _hay_datos(cnmc):
        return SIN_DATOS
    por_trimestre = _por_trimestre(cnmc)
    previos = [t for t in por_trimestre.index if t < objetivo][-MAX_TRIMESTRES:]
    if not previos:
        return SIN_DATOS
    lineas = [
        "Datos reales de la CNMC del corredor (larga distancia alta velocidad, servicio comercial, ambos sentidos), "
        "trimestres ya publicados:",
        *(_linea(cnmc, por_trimestre, t) for t in previos),
    ]
    relacion = relacion_esperada(cnmc, objetivo, total_esperado)
    if relacion is not None:
        lineas.append(_techo(por_trimestre, previos, objetivo, relacion))
    return "\n".join(lineas)
