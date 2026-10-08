"""Carga y validación del Catálogo de Datos y Glosario de Términos con auto-enriquecimiento desde el Grafo."""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class ColumnaInfo:
    nombre: str
    tipo: str
    descripcion: str = ""
    es_clave: bool = False
    termino_glosario: str | None = None


@dataclass(frozen=True)
class TablaInfo:
    tabla: str
    nombre: str
    capa: str
    orden_capa: int
    dominio: str
    descripcion: str
    grano: str
    claves: list[str] = field(default_factory=list)
    formato: str = "Parquet"
    proceso: str = ""
    frecuencia: str = ""
    quality_gates: list[str] = field(default_factory=list)
    upstream: list[str] = field(default_factory=list)
    downstream: list[str] = field(default_factory=list)
    columnas: list[ColumnaInfo] = field(default_factory=list)
    terminos_glosario: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class TerminoInfo:
    termino: str
    dominio: str
    tipo: str
    definicion: str
    formula: str | None = None
    sinonimos: str = ""
    tablas_relacionadas: str = ""
    dashboards_relacionados: list[str] = field(default_factory=list)
    quality_gates: list[str] = field(default_factory=list)
    terminos_relacionados: list[str] = field(default_factory=list)


@dataclass
class CatalogoGlosario:
    tablas: list[TablaInfo] = field(default_factory=list)
    terminos: list[TerminoInfo] = field(default_factory=list)


def _como_lista(valor: Any) -> list[str]:
    if not valor:
        return []
    if isinstance(valor, list):
        return [str(v).strip() for v in valor if str(v).strip()]
    return [p.strip() for p in str(valor).split(",") if p.strip()]


def _resolver_nodo_id(tabla: str, capa: str) -> str | None:
    if capa == "Gold":
        return f"gold:{tabla}"
    if capa == "Silver":
        return f"silver:{tabla}"
    if capa == "Bronze L2":
        return f"l2:{tabla}"
    if capa == "Bronze L1":
        return f"l1:{tabla}"
    if capa == "Predicción":
        return f"pred:{tabla}"
    return None


def cargar_catalogo_y_glosario(raiz: Path) -> CatalogoGlosario:
    ruta_cat = raiz / "config" / "data_catalog.yml"
    ruta_glo = raiz / "config" / "glosario.yml"

    # Auto-enriquecimiento de dependencias (upstream/downstream) y quality gates desde el grafo
    grafo = None
    try:
        from raillytics.lineage.grafo import construir
        grafo = construir(raiz)
    except Exception:
        pass

    terminos: list[TerminoInfo] = []
    if ruta_glo.is_file():
        datos_glo = yaml.safe_load(ruta_glo.read_text(encoding="utf-8")) or {}
        for item in datos_glo.get("terminos", []):
            terminos.append(
                TerminoInfo(
                    termino=item["termino"],
                    dominio=item.get("dominio", "General"),
                    tipo=item.get("tipo", "Negocio"),
                    definicion=item.get("definicion", ""),
                    formula=item.get("formula"),
                    sinonimos=item.get("sinonimos") or "",
                    tablas_relacionadas=item.get("tablas_relacionadas") or "",
                    dashboards_relacionados=_como_lista(item.get("dashboards_relacionados")),
                    quality_gates=_como_lista(item.get("quality_gates")),
                    terminos_relacionados=_como_lista(item.get("terminos_relacionados")),
                )
            )

    tablas: list[TablaInfo] = []
    if ruta_cat.is_file():
        datos_cat = yaml.safe_load(ruta_cat.read_text(encoding="utf-8")) or {}
        for t in datos_cat.get("tablas", []):
            cols = [
                ColumnaInfo(
                    nombre=c["nombre"],
                    tipo=c.get("tipo", "VARCHAR"),
                    descripcion=c.get("descripcion", ""),
                    es_clave=c.get("es_clave", False),
                    termino_glosario=c.get("termino_glosario"),
                )
                for c in t.get("columnas", [])
            ]

            capa = t.get("capa", "Gold")
            nodo_id = _resolver_nodo_id(t["tabla"], capa)
            up_auto = sorted({a.origen for a in grafo.aristas if a.destino == nodo_id}) if (grafo and nodo_id) else []
            down_auto = sorted({a.destino for a in grafo.aristas if a.origen == nodo_id}) if (grafo and nodo_id) else []
            gates_auto = sorted({r.gate for r in grafo.reglas if r.nodo == nodo_id}) if (grafo and nodo_id) else []

            upstream = t.get("upstream") or up_auto
            downstream = t.get("downstream") or down_auto
            quality_gates = t.get("quality_gates") or gates_auto

            # Auto-enriquecimiento de términos del glosario asociados a la tabla
            term_de_tabla = [
                term.termino for term in terminos
                if t["tabla"] in _como_lista(term.tablas_relacionadas)
            ]
            term_de_cols = [c.termino_glosario for c in cols if c.termino_glosario]
            term_explicitos = _como_lista(t.get("terminos_glosario"))
            terminos_glosario = sorted(set(term_de_tabla + term_de_cols + term_explicitos))

            tablas.append(
                TablaInfo(
                    tabla=t["tabla"],
                    nombre=t.get("nombre", t["tabla"]),
                    capa=capa,
                    orden_capa=int(t.get("orden_capa", 6)),
                    dominio=t.get("dominio", "General"),
                    descripcion=t.get("descripcion", ""),
                    grano=t.get("grano", ""),
                    claves=t.get("claves", []),
                    formato=t.get("formato", "Parquet"),
                    proceso=t.get("proceso", ""),
                    frecuencia=t.get("frecuencia", ""),
                    quality_gates=quality_gates,
                    upstream=upstream,
                    downstream=downstream,
                    columnas=cols,
                    terminos_glosario=terminos_glosario,
                )
            )

    return CatalogoGlosario(
        tablas=sorted(tablas, key=lambda x: (x.orden_capa, x.tabla)),
        terminos=sorted(terminos, key=lambda x: (x.dominio, x.termino)),
    )
