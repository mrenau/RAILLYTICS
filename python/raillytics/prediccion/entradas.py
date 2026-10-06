"""Lectura de los orígenes de la predicción desde el lake (el que alimentan los DAGs de Airflow).

Cada origen (trimestrales, festivos, eventos, meteo) es un SELECT de DuckDB definido en
config/prediccion.yml. La consulta traduce lo que haya en el lake al contrato de abajo; este módulo
ejecuta la consulta y comprueba el resultado. Falla cerrado: si un origen falta o no cumple el
contrato, lanza EntradaError con el nombre de la fuente y las rutas consultadas.

Marcadores de ruta admitidos en la consulta: {bronze}, {silver} y {gold} (bucket o directorio local).
"""
from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

import duckdb
import pandas as pd
import yaml

from raillytics.prediccion.trimestre import Trimestre
from raillytics.utils.lake import LakeLayout

CIUDADES = ("MAD", "BCN")


class EntradaError(Exception):
    """Un origen de la predicción falta o no cumple su contrato."""


@dataclass(frozen=True)
class Contrato:
    columnas: tuple[str, ...]
    clave: tuple[str, ...]          # combinación de columnas que no puede repetirse (vacía: sin restricción)
    obligatorias: tuple[str, ...]   # columnas sin nulos


CONTRATOS = {
    "trimestrales": Contrato(("trimestre", "viajeros"), ("trimestre",), ("trimestre", "viajeros")),
    "festivos": Contrato(("fecha", "nombre"), ("fecha",), ("fecha", "nombre")),
    "eventos": Contrato(("fecha", "descripcion", "ciudad"), (), ("fecha", "descripcion")),
    "meteo": Contrato(
        ("fecha", "ciudad", "temperatura_media", "precipitacion_mm"), ("fecha", "ciudad"), ("fecha", "ciudad")
    ),
    # Detalle de la CNMC por operador (viajeros y plazas): lo usan las plantillas con `{{datos_cnmc}}` (cnmc_vN).
    # Las plazas pueden faltar (la CNMC no las da antes de 2018).
    "cnmc": Contrato(
        ("trimestre", "operador", "viajeros", "plazas_ofertadas"), ("trimestre", "operador"), ("trimestre", "operador", "viajeros")
    ),
}
# Orígenes que la config puede omitir: sin ellos la predicción funciona igual (solo no hay bloque de la CNMC en el prompt).
OPCIONALES = frozenset({"cnmc"})


@dataclass(frozen=True)
class Entradas:
    trimestrales: pd.DataFrame
    festivos: pd.DataFrame
    eventos: pd.DataFrame
    meteo: pd.DataFrame
    cnmc: pd.DataFrame | None = None

    def trimestrales_dict(self) -> dict[Trimestre, int]:
        return {
            Trimestre.parse(t): int(v) for t, v in zip(self.trimestrales["trimestre"], self.trimestrales["viajeros"])
        }


def raiz_bronze(env: Mapping[str, str]) -> str:
    return env.get("BRONZE_ROOT") or f"s3://{env.get('MINIO_BUCKET_BRONZE', 'raillytics-bronze')}"


def resolver_origen(sql: str, layout: LakeLayout, env: Mapping[str, str]) -> str:
    return (
        sql.replace("{bronze}", raiz_bronze(env)).replace("{silver}", layout.silver_root).replace("{gold}", layout.gold_root)
    )


def cargar_config(ruta: Path) -> dict[str, str]:
    """Lee config/prediccion.yml y devuelve la consulta de cada origen."""
    if not ruta.is_file():
        raise EntradaError(f"no existe la configuración de entradas {ruta}")
    datos = yaml.safe_load(ruta.read_text(encoding="utf-8")) or {}
    origenes = datos.get("origenes") if isinstance(datos, dict) else None
    if not isinstance(origenes, dict):
        raise EntradaError(f"{ruta}: falta la clave 'origenes'")
    consultas: dict[str, str] = {}
    for nombre in CONTRATOS:
        entrada = origenes.get(nombre)
        sql = entrada.get("sql") if isinstance(entrada, dict) else None
        if nombre in OPCIONALES and entrada is None:
            continue
        if not isinstance(sql, str) or not sql.strip():
            raise EntradaError(f"{ruta}: falta 'origenes.{nombre}.sql'")
        consultas[nombre] = sql
    return consultas


def cargar_entradas(
    consultas: Mapping[str, str],
    layout: LakeLayout,
    con: duckdb.DuckDBPyConnection,
    env: Mapping[str, str] = os.environ,
) -> Entradas:
    return Entradas(**{
        nombre: _cargar(nombre, consultas[nombre], layout, con, env)
        for nombre in CONTRATOS
        if nombre in consultas or nombre not in OPCIONALES
    })


def _cargar(
    nombre: str, sql: str, layout: LakeLayout, con: duckdb.DuckDBPyConnection, env: Mapping[str, str]
) -> pd.DataFrame:
    sql = resolver_origen(sql, layout, env)
    try:
        df = con.execute(sql).df()
    except duckdb.Error as exc:
        motivo = _resumir(exc)
        # La ruta ya suele ir en el motivo (IO Error); solo se añade la que no esté, para no repetirla.
        rutas = [r for r in re.findall(r"read_parquet\('([^']+)'", sql) if r not in motivo]
        consultadas = f" Rutas consultadas: {rutas}." if rutas else ""
        raise EntradaError(
            f"origen '{nombre}': no se pudo leer ({motivo}).{consultadas} "
            "¿Ha ingestado Airflow esta fuente? Revisa también su consulta en config/prediccion.yml"
        ) from exc
    return _validar(nombre, df)


def cargar_trimestrales(
    consultas: Mapping[str, str],
    layout: LakeLayout,
    con: duckdb.DuckDBPyConnection,
    env: Mapping[str, str] = os.environ,
) -> dict[Trimestre, int]:
    """Solo el origen `trimestrales` (la demanda de la CNMC): para comprobar su cobertura sin exigir los otros tres."""
    df = _cargar("trimestrales", consultas["trimestrales"], layout, con, env)
    return {Trimestre.parse(t): int(v) for t, v in zip(df["trimestre"], df["viajeros"])}


def _resumir(exc: Exception) -> str:
    """Mensaje de DuckDB en una sola línea: sin el fragmento de SQL (`LINE n: …` y su `^`), que solo estorba al leerlo.

    Se conservan las pistas útiles, como `Candidate bindings: "fecha", "tmed"` en un error de columna.
    """
    lineas = [linea.strip() for linea in str(exc).splitlines()]
    return " ".join(linea for linea in lineas if linea and not linea.startswith("LINE ") and set(linea) != {"^"})


def _validar(nombre: str, df: pd.DataFrame) -> pd.DataFrame:
    contrato = CONTRATOS[nombre]
    if tuple(df.columns) != contrato.columnas:
        raise EntradaError(
            f"origen '{nombre}': la consulta debe devolver exactamente las columnas {list(contrato.columnas)} "
            f"(en ese orden) y devuelve {list(df.columns)}"
        )
    con_nulos = [c for c in contrato.obligatorias if df[c].isna().any()]
    if con_nulos:
        raise EntradaError(f"origen '{nombre}': hay nulos en columnas obligatorias {con_nulos}")
    df = df.copy()
    try:
        if "fecha" in df.columns:
            df["fecha"] = pd.to_datetime(df["fecha"]).dt.date
        _normalizar_por_origen(nombre, df)
    except (ValueError, TypeError) as exc:
        raise EntradaError(f"origen '{nombre}': {exc}") from exc
    if contrato.clave and df.duplicated(list(contrato.clave)).any():
        raise EntradaError(
            f"origen '{nombre}': hay filas duplicadas por {list(contrato.clave)}; ajusta la consulta "
            "(filtro o agregación) para devolver una sola fila por clave"
        )
    return df.reset_index(drop=True)


def _normalizar_por_origen(nombre: str, df: pd.DataFrame) -> None:
    if nombre == "trimestrales":
        df["trimestre"] = df["trimestre"].astype(str).str.strip()
        invalidos = []
        for texto in df["trimestre"]:
            try:
                Trimestre.parse(texto)
            except ValueError:
                invalidos.append(texto)
        if invalidos:
            raise ValueError(f"trimestre con formato inválido (se espera AAAA-Tn): {invalidos[:5]}")
        viajeros = pd.to_numeric(df["viajeros"])
        if (viajeros != viajeros.round()).any():
            raise ValueError("viajeros debe ser un número entero")
        df["viajeros"] = viajeros.round().astype("int64")
    elif nombre == "cnmc":
        df["trimestre"] = df["trimestre"].astype(str).str.strip()
        for texto in df["trimestre"]:
            Trimestre.parse(texto)
        df["operador"] = df["operador"].astype(str).str.strip()
        df["viajeros"] = pd.to_numeric(df["viajeros"])
        df["plazas_ofertadas"] = pd.to_numeric(df["plazas_ofertadas"])
    elif nombre == "festivos":
        df["nombre"] = df["nombre"].astype(str).str.strip()
    elif nombre == "eventos":
        df["descripcion"] = df["descripcion"].astype(str).str.strip()
        df["ciudad"] = df["ciudad"].fillna("").astype(str).str.strip()
    elif nombre == "meteo":
        df["ciudad"] = df["ciudad"].astype(str).str.strip().str.upper()
        fuera = sorted(set(df["ciudad"]) - set(CIUDADES))
        if fuera:
            raise ValueError(f"ciudades no soportadas {fuera}: la consulta debe devolver solo {list(CIUDADES)}")
        df["temperatura_media"] = pd.to_numeric(df["temperatura_media"])
        df["precipitacion_mm"] = pd.to_numeric(df["precipitacion_mm"])
