"""¿Tiene el lake los datos de la CNMC que exige un trimestre? (`make 07_prediccion` lo comprueba antes de predecir.)

El nivel esperado (raillytics.prediccion.nivel) necesita tres trimestres publicados: el mismo del año anterior (T-4), el
último publicado (U) y su gemelo (U-4). Además U debe estar cerca de T: la CNMC publica con ~1 trimestre de retraso, así que
a más de MAX_DISTANCIA_ULTIMO trimestres lo normal es que falte una descarga y no que la CNMC no haya publicado.

Uso:  python -m raillytics.prediccion.cnmc --trimestre 2026-T4
      Sale con 0 si hay datos, con 3 si faltan (el Makefile y scripts/carga_e2e.py descargan entonces) y con 1 si falla.
"""
from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

import duckdb
from dotenv import find_dotenv, load_dotenv

from raillytics.prediccion.entradas import EntradaError, cargar_config, cargar_trimestrales
from raillytics.prediccion.trimestre import Trimestre
from raillytics.utils.lake import LakeLayout, S3Settings, connect

# Trimestres entre el último publicado y el objetivo que se dan por normales (retraso de publicación de la CNMC).
MAX_DISTANCIA_ULTIMO = 2
SALIDA_FALTAN_DATOS = 3


@dataclass(frozen=True)
class Cobertura:
    objetivo: Trimestre
    suficiente: bool
    motivo: str                 # por qué no basta (vacío si basta)
    ultimo: Trimestre | None    # último trimestre publicado anterior al objetivo
    descripcion: str            # qué trimestres se usarán, para el mensaje


def evaluar_cobertura(publicados: Mapping[Trimestre, int], objetivo: Trimestre) -> Cobertura:
    anteriores = [t for t in publicados if t < objetivo]
    if not anteriores:
        return Cobertura(objetivo, False, f"no hay ningún trimestre publicado anterior a {objetivo}", None, "")
    ultimo = max(anteriores)
    distancia = objetivo.distancia(ultimo)
    if distancia > MAX_DISTANCIA_ULTIMO:
        motivo = (
            f"el último trimestre publicado es {ultimo}, a {distancia} trimestres de {objetivo} "
            f"(lo normal es como mucho {MAX_DISTANCIA_ULTIMO})"
        )
        return Cobertura(objetivo, False, motivo, ultimo, "")
    base, referencia = objetivo.menos(4), ultimo.menos(4)
    faltan = [t for t in (base, referencia) if t not in publicados]
    if faltan:
        return Cobertura(objetivo, False, "faltan los trimestres " + ", ".join(str(t) for t in faltan), ultimo, "")
    return Cobertura(objetivo, True, "", ultimo, f"{base}, {referencia} y {ultimo} publicados")


def comprobar(objetivo: Trimestre, env: Mapping[str, str]) -> Cobertura:
    """Lee la demanda del corredor de Silver (la consulta `trimestrales` de config/prediccion.yml) y evalúa la cobertura.

    Un lake que no se puede leer (MinIO parado, Silver sin construir) no es un error: es «no hay datos».
    """
    layout = LakeLayout.from_env(env)
    try:
        con = connect(S3Settings.from_env(env) if layout.uses_s3 else None)
        consultas = cargar_config(Path(env.get("PREDICCION_CONFIG") or "config/prediccion.yml"))
        publicados = cargar_trimestrales(consultas, layout, con, env)
    except (EntradaError, duckdb.Error, ValueError, OSError) as exc:
        return Cobertura(objetivo, False, f"no se pudo leer silver/cnmc_trimestral ({exc})", None, "")
    return evaluar_cobertura(publicados, objetivo)


def main(argv: Sequence[str] | None = None, env: Mapping[str, str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m raillytics.prediccion.cnmc",
        description="Comprueba que el lake tiene los datos de la CNMC que exige un trimestre.",
    )
    parser.add_argument("--trimestre", required=True, help="trimestre objetivo, p. ej. 2026-T4")
    args = parser.parse_args(argv)
    if env is None:
        load_dotenv(find_dotenv(usecwd=True))
        env = os.environ
    try:
        objetivo = Trimestre.parse(args.trimestre)
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    cobertura = comprobar(objetivo, env)
    if cobertura.suficiente:
        print(f"Hay datos de la CNMC para {objetivo}: {cobertura.descripcion}")
        return 0
    print(f"Faltan datos de la CNMC para {objetivo}: {cobertura.motivo}")
    return SALIDA_FALTAN_DATOS


if __name__ == "__main__":
    raise SystemExit(main())
