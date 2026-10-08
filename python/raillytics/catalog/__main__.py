"""Genera los datasets de Superset del Catálogo de Datos y Glosario de Términos a partir de los ficheros de configuración.

Uso:  python -m raillytics.catalog            escribe dashboards/superset/raillytics_gold/datasets/*/data_catalog.yaml, etc.
      python -m raillytics.catalog --check    no escribe: sale con 1 si lo versionado no coincide con el repo actual
      (o:  make catalog)
"""
from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from pathlib import Path

import yaml

from raillytics.catalog.datasets import construir_datasets_catalogo
from raillytics.catalog.loader import cargar_catalogo_y_glosario

RAIZ = Path(__file__).resolve().parents[3]
CARPETA = RAIZ / "dashboards" / "superset" / "raillytics_gold" / "datasets" / "Raillytics_Gold_DuckDB"


class _Dumper(yaml.SafeDumper):
    pass


def _texto(dumper: yaml.SafeDumper, valor: str):
    return dumper.represent_scalar("tag:yaml.org,2002:str", valor, style="|" if "\n" in valor else None)


_Dumper.add_representer(str, _texto)


def renderizar(dataset: dict) -> str:
    return yaml.dump(dataset, Dumper=_Dumper, sort_keys=False, allow_unicode=True, width=10_000)


def generar(raiz: Path) -> dict[Path, str]:
    cat = cargar_catalogo_y_glosario(raiz)
    return {
        CARPETA / f"{nombre}.yaml": renderizar(d)
        for nombre, d in construir_datasets_catalogo(cat).items()
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m raillytics.catalog", description=__doc__.splitlines()[0])
    parser.add_argument("--check", action="store_true", help="solo comprueba que los YAML versionados están al día")
    args = parser.parse_args(argv)

    esperados = generar(RAIZ)
    desfasados = [ruta for ruta, texto in esperados.items() if not ruta.is_file() or ruta.read_text(encoding="utf-8") != texto]
    if args.check:
        for ruta in desfasados:
            print(f"desfasado: {ruta.relative_to(RAIZ)} (ejecuta `make catalog`)", file=sys.stderr)
        return 1 if desfasados else 0
    for ruta, texto in esperados.items():
        ruta.parent.mkdir(parents=True, exist_ok=True)
        ruta.write_text(texto, encoding="utf-8")
        print(f"{'actualizado' if ruta in desfasados else 'sin cambios'}: {ruta.relative_to(RAIZ)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
