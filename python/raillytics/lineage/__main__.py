"""Genera los datasets de Superset del dashboard de lineage a partir del repositorio.

Uso:  python -m raillytics.lineage            escribe dashboards/superset/raillytics_gold/datasets/*/lineage_*.yaml
      python -m raillytics.lineage --check    no escribe: sale con 1 si lo versionado no coincide con el grafo actual
      (o:  make lineage)

AIRFLOW_UI_URL (o --airflow-url) fija la dirección de la UI de Airflow que usan los enlaces a los logs.
"""
from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Sequence
from pathlib import Path

import yaml

from raillytics.lineage.datasets import AIRFLOW_URL_POR_DEFECTO, construir_datasets
from raillytics.lineage.grafo import BASE_DASHBOARDS, construir

RAIZ = Path(__file__).resolve().parents[3]
CARPETA = BASE_DASHBOARDS / "datasets" / "Raillytics_Gold_DuckDB"


class _Dumper(yaml.SafeDumper):
    pass


def _texto(dumper: yaml.SafeDumper, valor: str):
    # El SQL, que ocupa varias líneas, como bloque literal (`|-`): se puede leer y revisar en un diff.
    return dumper.represent_scalar("tag:yaml.org,2002:str", valor, style="|" if "\n" in valor else None)


_Dumper.add_representer(str, _texto)


def renderizar(dataset: dict) -> str:
    return yaml.dump(dataset, Dumper=_Dumper, sort_keys=False, allow_unicode=True, width=10_000)


def generar(raiz: Path, airflow_url: str) -> dict[Path, str]:
    return {raiz / CARPETA / f"{nombre}.yaml": renderizar(d) for nombre, d in construir_datasets(construir(raiz), airflow_url).items()}


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m raillytics.lineage", description=__doc__.splitlines()[0])
    parser.add_argument("--check", action="store_true", help="solo comprueba que los YAML versionados están al día")
    parser.add_argument("--airflow-url", default=os.environ.get("AIRFLOW_UI_URL") or AIRFLOW_URL_POR_DEFECTO)
    args = parser.parse_args(argv)

    esperados = generar(RAIZ, args.airflow_url)
    desfasados = [ruta for ruta, texto in esperados.items() if not ruta.is_file() or ruta.read_text(encoding="utf-8") != texto]
    if args.check:
        for ruta in desfasados:
            print(f"desfasado: {ruta.relative_to(RAIZ)} (ejecuta `make lineage`)", file=sys.stderr)
        return 1 if desfasados else 0
    for ruta, texto in esperados.items():
        ruta.write_text(texto, encoding="utf-8")
        print(f"{'actualizado' if ruta in desfasados else 'sin cambios'}: {ruta.relative_to(RAIZ)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
