"""Carga end-to-end de Raillytics: descarga -> Bronze L1 -> L2 -> Silver -> Gold -> predicción de demanda.

Lo lanza `make carga-e2e`. Las apps de streaming (01_raw-uploader, 02_parquet-converter y 04_silver) no terminan solas, así
que este script las arranca una a una como subprocesos, espera a que se queden sin trabajo pendiente y las para:

  L1      termina cuando el staging (data/bronze) no tiene ficheros y su checkpoint está asentado.
  L2      termina cuando data/bronze_l1_done no tiene ficheros y los checkpoints de todas las fuentes están asentados.
  Silver  termina cuando cada tabla declarada en config/data_sources.yml ha confirmado algún batch y, durante
          --silencio-silver segundos, no ha confirmado ninguno más (Silver no deja huella en disco que diga «he acabado»).

Un checkpoint de Spark está «asentado» cuando cada offsets/N tiene su commits/N: parar a mitad de batch dejaría ficheros
movidos sin su fila de trazabilidad (al reintentar se omiten sin registrarse). Una fase que ya no tiene nada pendiente al
empezar se salta entera (no se paga el arranque de sbt).

La predicción va al final y no con el flag `predecir` del DAG: necesita Silver (silver/cnmc_trimestral), que aún no existe
cuando acaba la descarga.

Con --asegurar-cnmc --trimestre AAAA-Tn no hace la carga completa: comprueba que Silver tiene los datos de la CNMC que exige
ese trimestre (raillytics.prediccion.cnmc) y solo si faltan hace descarga -> L1 -> L2 -> Silver. Es lo que `make 07_prediccion`
ejecuta antes de predecir.

Uso:  python scripts/carga_e2e.py [--make make] [--compose "docker compose ..."] [--trimestre 2026-T4]
                                  [--desde PASO] [--sin-prediccion] [--asegurar-cnmc]
"""
from __future__ import annotations

import argparse
import json
import os
import shlex
import signal
import subprocess
import sys
import time
import urllib.request
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
DAG_ID = "ingesta_data_sources"
SERVICIO_AIRFLOW = "airflow-scheduler"
PASOS = ("infra", "descarga", "l1", "l2", "silver", "gold", "prediccion")

# Estados finales de una tarea de Airflow que cuentan como fallo (la ejecución del DAG puede salir «success» igualmente:
# su hoja, `predecir`, usa trigger_rule=all_done).
ESTADOS_FALLIDOS = {"failed", "upstream_failed"}


class CargaError(RuntimeError):
    """Un paso de la carga no ha podido completarse; el mensaje dice cuál y por qué."""


# ----------------------------------------------------------------------------------------------------- utilidades puras

def ficheros_pendientes(raiz: Path) -> list[Path]:
    """Ficheros bajo `raiz`, sin los ocultos (.gitkeep, .crc): son los que L1/L2 todavía no han movido."""
    if not raiz.is_dir():
        return []
    return sorted(p for p in raiz.rglob("*") if p.is_file() and not p.name.startswith("."))


def _ids(directorio: Path) -> set[str]:
    if not directorio.is_dir():
        return set()
    return {p.name for p in directorio.iterdir() if p.is_file() and not p.name.startswith(".")}


def lotes_confirmados(checkpoint: Path) -> int:
    """Número de micro-batches confirmados (commits/N) de una query de Structured Streaming."""
    return len(_ids(checkpoint / "commits"))


def checkpoint_asentado(checkpoint: Path) -> bool:
    """True si no hay ningún batch empezado (offsets/N) sin confirmar (commits/N). Sin checkpoint no hay nada a medias."""
    return _ids(checkpoint / "offsets") <= _ids(checkpoint / "commits")


def checkpoints_de(raiz: Path) -> list[Path]:
    """Un directorio de checkpoint por query: los hijos de `raiz` que tienen offsets/."""
    if not raiz.is_dir():
        return []
    return sorted(p for p in raiz.iterdir() if (p / "offsets").is_dir())


class Sostenida:
    """Una condición que debe mantenerse `segundos` seguidos (y con la misma `clave`) para darse por buena."""

    def __init__(self, segundos: float, reloj: Callable[[], float] = time.monotonic) -> None:
        self.segundos = segundos
        self.reloj = reloj
        self._desde: float | None = None
        self._clave: object = None

    def actualizar(self, condicion: bool, clave: object = None) -> bool:
        if not condicion or clave != self._clave:
            self._desde = None
        self._clave = clave
        if not condicion:
            return False
        if self._desde is None:
            self._desde = self.reloj()
        return self.reloj() - self._desde >= self.segundos


def json_de(salida: str):
    """Primer JSON (lista u objeto) que aparece en la salida de un comando que mezcla logs y datos."""
    decodificador = json.JSONDecoder()
    texto = salida
    for linea_inicio in (i for i, c in enumerate(texto) if c in "[{" and (i == 0 or texto[i - 1] == "\n")):
        try:
            valor, _ = decodificador.raw_decode(texto[linea_inicio:])
        except ValueError:
            continue
        if isinstance(valor, (list, dict)):
            return valor
    raise CargaError(f"no hay JSON en la salida del comando: {salida.strip()[:300]!r}")


def trimestre_en_curso(hoy: date) -> str:
    return f"{hoy.year}-T{(hoy.month - 1) // 3 + 1}"


# ----------------------------------------------------------------------------------------------------- contexto y comandos

@dataclass
class Contexto:
    make: list[str]
    compose: list[str]
    trimestre: str
    logs: Path
    timeout_fase: float
    silencio_silver: float
    estable: float = 6.0                   # segundos que L1/L2 deben seguir «asentadas» antes de pararlas
    sondeo: float = 2.0
    staging: Path = field(default_factory=lambda: Path(os.environ.get("STAGING_ROOT", "data/bronze")))
    l1_hecho: Path = field(default_factory=lambda: Path(os.environ.get("L1_DONE_ROOT", "data/bronze_l1_done")))
    checkpoints: Path = field(default_factory=lambda: Path(os.environ.get("CHECKPOINT_ROOT", "data/checkpoints")))


def log(mensaje: str) -> None:
    print(f"[{datetime.now():%H:%M:%S}] {mensaje}", flush=True)


def ejecutar(comando: Sequence[str], **kwargs) -> subprocess.CompletedProcess:
    return subprocess.run(list(comando), capture_output=True, text=True, encoding="utf-8", errors="replace", **kwargs)


def make(ctx: Contexto, objetivo: str, *variables: str) -> None:
    """`make <objetivo>` en primer plano (los pasos batch); su salida va a la terminal."""
    comando = [*ctx.make, objetivo, *variables]
    if subprocess.run(comando).returncode != 0:
        raise CargaError(f"`{' '.join(comando)}` ha fallado (la causa está en su salida, más arriba)")


def airflow(ctx: Contexto, *args: str) -> str:
    resultado = ejecutar([*ctx.compose, "exec", "-T", SERVICIO_AIRFLOW, "airflow", *args])
    if resultado.returncode != 0:
        raise CargaError(f"`airflow {' '.join(args)}` ha fallado: {(resultado.stderr or resultado.stdout).strip()[-400:]}")
    return resultado.stdout


def esperar(condicion: Callable[[], bool], timeout: float, descripcion: str, sondeo: float = 2.0) -> None:
    limite = time.monotonic() + timeout
    while not condicion():
        if time.monotonic() > limite:
            raise CargaError(f"tiempo agotado ({timeout:.0f} s) esperando: {descripcion}")
        time.sleep(sondeo)


# ----------------------------------------------------------------------------------------------------- paso: infra

def _minio_vivo() -> bool:
    endpoint = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000").rstrip("/")
    try:
        with urllib.request.urlopen(f"{endpoint}/minio/health/live", timeout=3) as respuesta:
            return respuesta.status == 200
    except OSError:
        return False


def _dag_visible(ctx: Contexto) -> bool:
    try:
        return any(d.get("dag_id") == DAG_ID for d in json_de(airflow(ctx, "dags", "list", "-o", "json")))
    except CargaError:
        return False


def paso_infra(ctx: Contexto) -> None:
    make(ctx, "up")
    log("esperando a MinIO y a que Airflow haya cargado el DAG...")
    esperar(_minio_vivo, 300, f"MinIO ({os.environ.get('MINIO_ENDPOINT', 'http://localhost:9000')})")
    esperar(lambda: _dag_visible(ctx), 300, f"el DAG {DAG_ID} en Airflow", sondeo=5)


# ----------------------------------------------------------------------------------------------------- paso: descarga

def paso_descarga(ctx: Contexto, timeout: float = 900) -> list[str]:
    """Dispara el DAG de descarga (sin predicción) y espera a que acabe. Devuelve las tareas que han fallado."""
    dags = json_de(airflow(ctx, "dags", "list", "-o", "json"))
    pausado = next((str(d.get("is_paused")).lower() == "true" for d in dags if d.get("dag_id") == DAG_ID), False)
    if pausado:
        # Un DAG pausado deja el run en cola para siempre. Pasa en una BD de Airflow recién creada.
        log(f"el DAG {DAG_ID} estaba pausado: se activa (también empezará a correr su @daily)")
        airflow(ctx, "dags", "unpause", DAG_ID)

    run_id = f"carga_e2e_{datetime.now(timezone.utc):%Y%m%dT%H%M%S}"
    airflow(ctx, "dags", "trigger", DAG_ID, "--run-id", run_id, "--conf", json.dumps({"predecir": False}))
    log(f"DAG {DAG_ID} disparado (run_id={run_id}); esperando a que termine la descarga...")

    def estado() -> str:
        runs = json_de(airflow(ctx, "dags", "list-runs", DAG_ID, "-o", "json"))
        return next((r.get("state", "") for r in runs if r.get("run_id") == run_id), "")

    esperar(lambda: estado() in ("success", "failed"), timeout, f"que termine el run {run_id} del DAG", sondeo=5)

    tareas = json_de(airflow(ctx, "tasks", "states-for-dag-run", DAG_ID, run_id, "-o", "json"))
    fallidas = sorted({t["task_id"] + (f"[{t['map_index']}]" if t.get("map_index", -1) not in (-1, None) else "")
                       for t in tareas if t.get("state") in ESTADOS_FALLIDOS})
    if fallidas:
        log(f"AVISO: tareas de descarga fallidas ({', '.join(fallidas)}); se sigue con lo ya descargado "
            f"(detalle en la UI de Airflow, run {run_id})")
    return fallidas


# ----------------------------------------------------------------------------------------------------- pasos de streaming

def _parar(proceso: subprocess.Popen, espera: float = 90) -> None:
    """Para el árbol de procesos (make -> sbt -> java): primero con SIGTERM para que la JVM cierre limpia, luego a la fuerza."""
    if proceso.poll() is not None:
        return
    if os.name == "nt":
        subprocess.run(["taskkill", "/F", "/T", "/PID", str(proceso.pid)], capture_output=True)
        proceso.wait()
        return
    for senal in (signal.SIGTERM, signal.SIGKILL):
        try:
            os.killpg(proceso.pid, senal)
        except ProcessLookupError:
            return
        try:
            proceso.wait(timeout=espera if senal == signal.SIGTERM else 10)
            return
        except subprocess.TimeoutExpired:
            continue


def _cola(ruta: Path, lineas: int = 25) -> str:
    try:
        return "".join(ruta.read_text(encoding="utf-8", errors="replace").splitlines(keepends=True)[-lineas:])
    except OSError:
        return ""


def correr_stream(ctx: Contexto, nombre: str, objetivo: str, terminado: Callable[[], bool]) -> None:
    """Arranca `make <objetivo>` en segundo plano y lo para cuando `terminado()` se cumple.

    Falla si el proceso muere antes (un stream que cae por error no debe darse por «terminado») o si pasa `timeout_fase`.
    La salida completa queda en `<logs>/<nombre>.log`.
    """
    ctx.logs.mkdir(parents=True, exist_ok=True)
    ruta_log = ctx.logs / f"{nombre}.log"
    opciones = {"start_new_session": True} if os.name != "nt" else {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP}
    with open(ruta_log, "w", encoding="utf-8") as salida:
        proceso = subprocess.Popen([*ctx.make, objetivo], stdout=salida, stderr=subprocess.STDOUT,
                                   stdin=subprocess.DEVNULL, **opciones)
        log(f"{objetivo} en marcha (log: {ruta_log})")
        limite = time.monotonic() + ctx.timeout_fase
        try:
            while not terminado():
                if proceso.poll() is not None:
                    raise CargaError(f"{objetivo} ha terminado con código {proceso.returncode} antes de quedarse sin trabajo. "
                                     f"Últimas líneas de {ruta_log}:\n{_cola(ruta_log)}")
                if time.monotonic() > limite:
                    raise CargaError(f"{objetivo}: tiempo agotado ({ctx.timeout_fase:.0f} s) sin quedarse sin trabajo. "
                                     f"Últimas líneas de {ruta_log}:\n{_cola(ruta_log)}")
                time.sleep(ctx.sondeo)
        finally:
            _parar(proceso)
    log(f"{objetivo} parado")


def _l1_sin_trabajo(ctx: Contexto) -> bool:
    return not ficheros_pendientes(ctx.staging) and checkpoint_asentado(ctx.checkpoints / "l1-raw")


def _l2_sin_trabajo(ctx: Contexto) -> bool:
    return not ficheros_pendientes(ctx.l1_hecho) and all(checkpoint_asentado(c) for c in checkpoints_de(ctx.checkpoints / "l2"))


def _con_estabilidad(ctx: Contexto, sin_trabajo: Callable[[], bool]) -> Callable[[], bool]:
    sostenida = Sostenida(ctx.estable)
    return lambda: sostenida.actualizar(sin_trabajo())


def _fase_ingesta(ctx: Contexto, nombre: str, objetivo: str, sin_trabajo: Callable[[], bool]) -> None:
    if sin_trabajo():
        log(f"{nombre}: nada pendiente, se salta {objetivo}")
        return
    correr_stream(ctx, nombre, objetivo, _con_estabilidad(ctx, sin_trabajo))


def paso_l1(ctx: Contexto) -> None:
    _fase_ingesta(ctx, "l1", "01_raw-uploader", lambda: _l1_sin_trabajo(ctx))


def paso_l2(ctx: Contexto) -> None:
    _fase_ingesta(ctx, "l2", "02_parquet-converter", lambda: _l2_sin_trabajo(ctx))


def tablas_silver_esperadas() -> list[str]:
    sys.path.insert(0, str(RAIZ / "python"))
    from raillytics.ingesta.sources import load_sources

    ruta = Path(os.environ.get("DATA_SOURCES_CONFIG", "config/data_sources.yml"))
    return [t.tabla for s in load_sources(ruta) for t in s.silver]


def paso_silver(ctx: Contexto) -> None:
    tablas = tablas_silver_esperadas()
    if not tablas:
        raise CargaError("ninguna fuente de config/data_sources.yml declara tablas Silver")
    dirs = {t: ctx.checkpoints / "silver" / t for t in tablas}
    sostenida = Sostenida(ctx.silencio_silver)

    def terminado() -> bool:
        confirmados = {t: lotes_confirmados(d) for t, d in dirs.items()}
        todas = all(n > 0 for n in confirmados.values())
        asentadas = all(checkpoint_asentado(d) for d in dirs.values())
        # La clave es el nº de batches confirmados: cada batch nuevo reinicia la cuenta del silencio.
        return sostenida.actualizar(todas and asentadas, clave=tuple(sorted(confirmados.items())))

    try:
        correr_stream(ctx, "silver", "04_silver", terminado)
    except CargaError as e:
        sin_batch = [t for t, d in dirs.items() if lotes_confirmados(d) == 0]
        if sin_batch:
            raise CargaError(f"{e}\nTablas Silver sin ningún batch confirmado: {', '.join(sin_batch)} "
                             "(¿su fuente no llegó a L2? Mira data/bronze_rejected y el log de L2)") from e
        raise


# ----------------------------------------------------------------------------------------------------- pasos batch

def paso_gold(ctx: Contexto) -> None:
    # El Gold necesita el detalle diario SINTÉTICO (viajeros_enriquecidos / puntualidad_enriquecida): no existe como dato abierto.
    make(ctx, "03_silver-sample")
    make(ctx, "05_gold")


def paso_prediccion(ctx: Contexto) -> None:
    make(ctx, "llm-up")
    make(ctx, "prediccion-sample")     # festivos, eventos y meteo sintéticos (a prefijos propios, no pisan datos reales)
    # PRED_CNMC=no: la CNMC acaba de descargarse y pasar por Silver en esta misma carga, no hace falta comprobarla otra vez.
    make(ctx, "07_prediccion", f"TRIMESTRE={ctx.trimestre}", "PRED_CNMC=no")


# ----------------------------------------------------------------------------------------------------- datos de la CNMC

def asegurar_cnmc(trimestre: str, comprobar: Callable[[], object], descargar: Callable[[], None]) -> int:
    """Comprueba que hay datos de la CNMC para `trimestre`; si faltan, los descarga y vuelve a comprobar.

    Tras descargar no bloquea aunque la CNMC siga sin cubrir el trimestre (puede no haber publicado aún): avisa y deja que el
    predictor decida con lo que haya. Devuelve el código de salida: 0, o 1 si la descarga falla.
    """
    cobertura = comprobar()
    if cobertura.suficiente:
        log(f"Hay datos de la CNMC para {trimestre}: {cobertura.descripcion}")
        return 0
    log(f"Faltan datos de la CNMC para {trimestre} ({cobertura.motivo}): se descargan antes de predecir")
    try:
        descargar()
    except CargaError as e:
        log(f"✗ descarga de la CNMC: {e}")
        return 1
    cobertura = comprobar()
    if cobertura.suficiente:
        log(f"Datos de la CNMC listos para {trimestre}: {cobertura.descripcion}")
    else:
        log(f"AVISO: tras descargar, la CNMC sigue sin cubrir {trimestre} ({cobertura.motivo}); "
            "puede que aún no haya publicado más. Se predice con lo que hay")
    return 0


def _comprobar_cnmc(trimestre: str) -> object:
    sys.path.insert(0, str(RAIZ / "python"))
    from raillytics.prediccion.cnmc import comprobar
    from raillytics.prediccion.trimestre import Trimestre

    return comprobar(Trimestre.parse(trimestre), os.environ)


def _descargar_cnmc(ctx: Contexto) -> None:
    # `make up` solo si hace falta: relanza los contenedores *-init y tarda ~20 s.
    if not (_minio_vivo() and _dag_visible(ctx)):
        paso_infra(ctx)
    paso_descarga(ctx)
    paso_l1(ctx)
    paso_l2(ctx)
    paso_silver(ctx)


# ----------------------------------------------------------------------------------------------------- main

def parsear(argv: Sequence[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="carga_e2e.py", description="Carga end-to-end de Raillytics (ver `make carga-e2e`).")
    p.add_argument("--make", default="make", help="comando de make (el Makefile pasa $(MAKE))")
    p.add_argument("--compose", default="docker compose -f docker/docker-compose.yml --env-file .env",
                   help="comando de docker compose (el Makefile pasa $(COMPOSE))")
    p.add_argument("--trimestre", default="", help="trimestre a predecir, AAAA-Tn (por defecto, el en curso)")
    p.add_argument("--desde", choices=PASOS, default=PASOS[0], help="empieza en este paso (para reanudar tras un fallo)")
    p.add_argument("--sin-prediccion", action="store_true", help="no ejecuta la predicción (no necesita Ollama)")
    p.add_argument("--asegurar-cnmc", action="store_true",
                   help="solo comprueba (y si faltan, descarga) los datos de la CNMC que exige --trimestre; no hace la carga completa")
    p.add_argument("--timeout-fase", type=float, default=1800, help="segundos máximos de cada app de streaming (por defecto 1800)")
    p.add_argument("--silencio-silver", type=float, default=75,
                   help="segundos sin batches nuevos para dar Silver por terminado (por defecto 75: 2,5 veces su trigger de 30 s)")
    return p.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parsear(argv)
    os.chdir(RAIZ)
    try:
        from dotenv import find_dotenv, load_dotenv
        load_dotenv(find_dotenv(usecwd=True))      # sin `make` (que ya exporta el .env) las rutas salen de aquí
    except ImportError:
        pass

    if args.asegurar_cnmc and not args.trimestre:
        print("--asegurar-cnmc necesita --trimestre AAAA-Tn", file=sys.stderr)
        return 2

    ctx = Contexto(
        make=shlex.split(args.make), compose=shlex.split(args.compose),
        trimestre=args.trimestre or trimestre_en_curso(date.today()),
        logs=Path(os.environ.get("E2E_LOGS", "data/logs/carga_e2e")),
        timeout_fase=args.timeout_fase, silencio_silver=args.silencio_silver,
    )
    if args.asegurar_cnmc:
        return asegurar_cnmc(ctx.trimestre, lambda: _comprobar_cnmc(ctx.trimestre), lambda: _descargar_cnmc(ctx))
    if args.sin_prediccion and args.desde == "prediccion":
        print("--desde prediccion y --sin-prediccion se contradicen", file=sys.stderr)
        return 2
    pasos: list[tuple[str, Callable[[], object]]] = [
        ("infra", lambda: paso_infra(ctx)),
        ("descarga", lambda: paso_descarga(ctx)),
        ("l1", lambda: paso_l1(ctx)),
        ("l2", lambda: paso_l2(ctx)),
        ("silver", lambda: paso_silver(ctx)),
        ("gold", lambda: paso_gold(ctx)),
    ]
    if not args.sin_prediccion:
        pasos.append(("prediccion", lambda: paso_prediccion(ctx)))
    pasos = pasos[PASOS.index(args.desde):]    # los nombres de `pasos` siguen el orden de PASOS

    fallidas: list[str] = []
    duraciones: list[tuple[str, float]] = []
    inicio = time.monotonic()
    for nombre, paso in pasos:
        log(f"▶ {nombre}")
        t0 = time.monotonic()
        try:
            resultado = paso()
        except CargaError as e:
            log(f"✗ {nombre}: {e}")
            log(f"Para reanudar tras corregirlo: make carga-e2e E2E_ARGS=\"--desde {nombre}\"")
            return 1
        except KeyboardInterrupt:
            log("interrumpido")
            return 130
        if nombre == "descarga" and isinstance(resultado, list):
            fallidas = resultado
        duraciones.append((nombre, time.monotonic() - t0))

    log("✓ carga end-to-end completada en " + f"{(time.monotonic() - inicio) / 60:.1f} min (" +
        ", ".join(f"{n} {d:.0f}s" for n, d in duraciones) + ")")
    if fallidas:
        log(f"  Descargas fallidas: {', '.join(fallidas)}")
    log("  Revisa el resultado con: make cargas · make calidad")
    return 0


if __name__ == "__main__":
    sys.exit(main())
