"""Orquestación de la predicción: entradas → nivel → prompt → LLM → gates → CSV, con trazabilidad.

Todo lo que puede fallar (leer el lake, calcular el nivel, el LLM, los gates, escribir) ocurre dentro
de `registrar_carga`, así que cualquier error queda registrado con `estado = error` y no se escribe
ningún CSV. `--solo-nivel` solo calcula e imprime: no llama al LLM ni registra nada.
"""
from __future__ import annotations

import logging
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Protocol

import duckdb
import pandas as pd

from raillytics.calidad.registro import exigir, registrar_calidad
from raillytics.prediccion.cache import CacheLLM, ClienteConCache
from raillytics.prediccion.calendario import Calendario, construir_calendario
from raillytics.prediccion.contexto_cnmc import construir_contexto
from raillytics.prediccion.entradas import Entradas, cargar_config, cargar_entradas, raiz_bronze
from raillytics.prediccion.gates import TABLA, evaluar_gates
from raillytics.prediccion.nivel import NivelEsperado, calcular_nivel, desviacion_relativa, miles
from raillytics.prediccion.normalizar import IndiceDia, normalizar_indices, repartir
from raillytics.prediccion.ollama import OllamaClient, OllamaSettings
from raillytics.prediccion.prompt import cargar_plantilla, construir_prompt
from raillytics.prediccion.publicacion import GOLD_TABLA, construir_gold, publicar_gold
from raillytics.prediccion.reglas import RUTA_POR_DEFECTO as RUTA_REGLAS
from raillytics.prediccion.reglas import cargar_reglas, combinar, es_modo_eventos, indices_base
from raillytics.prediccion.salida import (
    CORREDOR,
    RAIZ_POR_DEFECTO,
    construir_dataframe,
    escribir_csv,
    formatear_resumen,
    resumen_coherencia,
)
from raillytics.prediccion.trimestre import Trimestre
from raillytics.utils.cargas import registrar_carga
from raillytics.utils.lake import LakeLayout, S3Settings, connect, is_s3

logger = logging.getLogger(__name__)

PROCESO = "prediccion_demanda"
CAPA = "ml"
VERSION_PROMPT_POR_DEFECTO = "eventos_v1"


class ClienteLLM(Protocol):
    settings: OllamaSettings

    def comprobar(self) -> None: ...

    def generar_indices(self, prompt: str, dias: Sequence[date]) -> list[IndiceDia]: ...


@dataclass(frozen=True)
class Resultado:
    nivel: NivelEsperado | None
    total_esperado: int
    ruta: Path | None
    dataframe: pd.DataFrame | None
    resumen: str | None


def conectar(layout: LakeLayout, env: Mapping[str, str]) -> duckdb.DuckDBPyConnection:
    """Conexión DuckDB al lake: con MinIO si Bronze/Silver/Gold son buckets, en memoria si son directorios locales."""
    usa_s3 = layout.uses_s3 or is_s3(raiz_bronze(env))
    return connect(S3Settings.from_env(env) if usa_s3 else None)


def _preparar(
    trimestre: Trimestre,
    total_manual: int | None,
    env: Mapping[str, str],
    layout: LakeLayout,
    con: duckdb.DuckDBPyConnection,
    imprimir: Callable[[str], None],
) -> tuple[Entradas, dict[Trimestre, int], NivelEsperado | None, int, bool]:
    """Lee los cuatro orígenes y fija el total esperado (calculado o manual). El último valor: ¿se leen fuentes sintéticas?"""
    consultas = cargar_config(Path(env.get("PREDICCION_CONFIG") or "config/prediccion.yml"))
    sinteticas = [origen for origen, sql in consultas.items() if "muestra_" in sql]
    if sinteticas:
        imprimir(
            f"AVISO: los orígenes {', '.join(sinteticas)} leen fuentes SINTÉTICAS (muestra_*): los resultados NO son reales. "
            "Cuando existan las fuentes reales, apunta config/prediccion.yml a ellas"
        )
    entradas = cargar_entradas(consultas, layout, con, env)
    publicados = entradas.trimestrales_dict()
    nivel = None
    if total_manual is None:
        nivel = calcular_nivel(publicados, trimestre)
        total = nivel.total
        imprimir(nivel.describir())
    else:
        total = total_manual
        imprimir(f"{trimestre}: total esperado fijado a mano: {miles(total)} viajeros")
    if trimestre in publicados:
        real = publicados[trimestre]
        imprimir(
            f"{trimestre} ya está publicado ({miles(real)} viajeros reales): "
            f"el total esperado se desvía {desviacion_relativa(total, real):+.2%}"
        )
    return entradas, publicados, nivel, total, bool(sinteticas)


def _prompt(
    trimestre: Trimestre,
    version_prompt: str,
    total: int,
    publicados: dict[Trimestre, int],
    entradas: Entradas,
    env: Mapping[str, str],
) -> tuple[str, Calendario]:
    """Calendario + plantilla + prompt final: lo que se muestra con --mostrar-prompt es lo que se envía."""
    calendario = construir_calendario(trimestre, entradas.festivos, entradas.eventos, entradas.meteo)
    plantilla = cargar_plantilla(version_prompt, Path(env.get("PROMPTS_DIR") or "config/prompts"))
    datos_cnmc = construir_contexto(entradas.cnmc, trimestre, total)
    return construir_prompt(plantilla, trimestre, total, publicados, calendario, CORREDOR, datos_cnmc), calendario


def _estado_cache(cliente: ClienteLLM) -> str:
    return {True: "acierto", False: "fallo", None: "desactivada"}[getattr(cliente, "resultado_en_cache", None)]


def _indices_modo_eventos(
    cliente: ClienteLLM, prompt: str, calendario: Calendario, env: Mapping[str, str], parametros: dict
) -> tuple[list[IndiceDia], list[str]]:
    """Modo eventos (plantillas eventos_vN): reglas fijas para todos los días y el LLM solo para los días con evento.

    Si el trimestre no tiene ningún día con evento no se llama al LLM. Devuelve los índices y las fechas cuyo motivo
    escribió el LLM, que son las que revisa el gate del día de la semana.
    """
    reglas = cargar_reglas(Path(env.get("REGLAS_DEMANDA") or RUTA_REGLAS))
    parametros["reglas"] = reglas.como_dict()
    con_evento = [d for d, eventos in zip(calendario.dias["fecha"], calendario.dias["eventos"]) if eventos]
    factores: list[IndiceDia] = []
    if con_evento:
        cliente.comprobar()
        factores = cliente.generar_indices(prompt, con_evento)
        parametros["cache"] = _estado_cache(cliente)
    else:
        parametros["cache"] = "sin llamada"
    indices = combinar(indices_base(calendario, reglas), factores, calendario, reglas)
    return indices, [d.isoformat() for d in con_evento]


def ejecutar(
    trimestre: Trimestre,
    *,
    version_prompt: str,
    total_manual: int | None,
    solo_nivel: bool,
    mostrar_prompt: bool = False,
    env: Mapping[str, str],
    layout: LakeLayout,
    con: duckdb.DuckDBPyConnection,
    cliente: ClienteLLM | None,
    ahora: datetime | None = None,
    imprimir: Callable[[str], None] = print,
) -> Resultado:
    if solo_nivel:
        _, _, nivel, total, _ = _preparar(trimestre, total_manual, env, layout, con, imprimir)
        return Resultado(nivel, total, None, None, None)
    if mostrar_prompt:  # imprime el prompt y termina: ni se comprueba Ollama ni se gasta GPU ni se registra carga
        entradas, publicados, nivel, total, _ = _preparar(trimestre, total_manual, env, layout, con, imprimir)
        prompt, _ = _prompt(trimestre, version_prompt, total, publicados, entradas, env)
        imprimir(prompt)
        return Resultado(nivel, total, None, None, None)
    if cliente is None:
        raise ValueError("hace falta un cliente del LLM salvo con solo_nivel o mostrar_prompt")

    ahora = ahora or datetime.now(timezone.utc).replace(tzinfo=None)
    s = cliente.settings
    parametros = {
        "trimestre": str(trimestre),
        "modelo": s.modelo,
        "version_prompt": version_prompt,
        "seed": s.seed,
        "num_ctx": s.num_ctx,
        "total_manual": total_manual,
    }
    raiz = Path(env.get("PREDICCIONES_ROOT") or RAIZ_POR_DEFECTO)
    with registrar_carga(PROCESO, CAPA, layout, con, parametros=parametros) as ejecucion:
        with ejecucion.tabla(TABLA, origen=f"{s.modelo} · {version_prompt}") as carga:
            entradas, publicados, nivel, total, sinteticas = _preparar(trimestre, total_manual, env, layout, con, imprimir)
            prompt, calendario = _prompt(trimestre, version_prompt, total, publicados, entradas, env)
            dias = trimestre.dias()
            fechas_llm = None
            if es_modo_eventos(version_prompt):
                indices, fechas_llm = _indices_modo_eventos(cliente, prompt, calendario, env, parametros)
            else:
                cliente.comprobar()
                indices = cliente.generar_indices(prompt, dias)
                parametros["cache"] = _estado_cache(cliente)
            valores = [i.indice for i in indices]
            df = construir_dataframe(
                dias, indices, repartir(valores, total), normalizar_indices(valores),
                trimestre=trimestre, modelo=s.modelo, version_prompt=version_prompt,
                run_id=ejecucion.run_id, generado_en=ahora,
            )
            resultados = evaluar_gates(df, dias, total, calendario.eventos_con_datos, valores, fechas_llm=fechas_llm)
            registrar_calidad(resultados, PROCESO, CAPA, ejecucion.run_id, layout, con)
            for r in resultados:
                if not r.pasa and not r.bloquea:
                    imprimir(f"AVISO {r.gate}: {r.detalle}")
            exigir(resultados)
            # Primero Gold y después el CSV: si Gold falla no queda un CSV sin su fila de trazabilidad en el dashboard.
            gold = construir_gold(df, calendario, total_esperado=total, datos_sinteticos=sinteticas)
            with ejecucion.tabla(GOLD_TABLA, origen=f"{s.modelo} · {version_prompt}") as carga_gold:
                carga_gold.destino = publicar_gold(con, layout, gold)
                carga_gold.filas = len(gold)
            ruta = escribir_csv(df, raiz, trimestre, version_prompt, ahora)
            carga.destino = str(ruta)
            carga.filas = len(df)
            carga.bytes = ruta.stat().st_size
    resumen = formatear_resumen(resumen_coherencia(df, calendario))
    imprimir(f"CSV escrito: {ruta}")
    imprimir(resumen)
    return Resultado(nivel, total, ruta, df, resumen)


def crear_cliente(env: Mapping[str, str], *, imprimir: Callable[[str], None] = print, usar_cache: bool = True) -> ClienteLLM:
    """El cliente de Ollama, envuelto en la caché de resultados salvo que se desactive (--sin-cache o PREDICCION_CACHE=0)."""
    cliente = OllamaClient(OllamaSettings.from_env(env), imprimir=imprimir)
    if not usar_cache or env.get("PREDICCION_CACHE", "1").strip().lower() in ("0", "false", "no", "off"):
        return cliente
    return ClienteConCache(cliente, CacheLLM(Path(env.get("PREDICCION_CACHE_DIR") or "data/cache/prediccion")), imprimir)


def predecir_desde_entorno(
    trimestre: str | None,
    hoy: date,
    env: Mapping[str, str],
    *,
    version_prompt: str | None = None,
    imprimir: Callable[[str], None] = print,
) -> Resultado:
    """Punto de entrada del DAG de Airflow: el lake, Ollama y las rutas salen del entorno del contenedor.

    Sin `trimestre` se predice el trimestre en curso según `hoy`; sin `version_prompt`, el de PRED_PROMPT o el de por defecto.
    Los errores se propagan como excepciones (Airflow marca la tarea en rojo).
    """
    objetivo = Trimestre.parse(trimestre) if trimestre else Trimestre.de_fecha(hoy)
    layout = LakeLayout.from_env(env)
    return ejecutar(
        objetivo,
        version_prompt=version_prompt or env.get("PRED_PROMPT") or VERSION_PROMPT_POR_DEFECTO,
        total_manual=None,
        solo_nivel=False,
        env=env,
        layout=layout,
        con=conectar(layout, env),
        cliente=crear_cliente(env, imprimir=imprimir),
        imprimir=imprimir,
    )
