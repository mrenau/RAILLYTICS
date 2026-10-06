# Makefile — Raillytics: tareas de carga y procesamiento (capa Bronze)
#
# Requiere GNU Make. En Linux/macOS ya viene instalado.
# En Windows NO viene por defecto — instálalo con uno de:
#   choco install make
#   scoop install make
# (o usa WSL / Git Bash, que también lo suelen traer)
#
# Uso:  make            -> muestra esta ayuda
#       make <target>

.DEFAULT_GOAL := help
.PHONY: help up down install-dev-env install-hooks test test-python test-scala 00_ingest 01_raw-uploader 02_parquet-converter 03_silver-sample 04_silver 05_gold 06_superset-import 07_prediccion prediccion-sample llm-up llm-down quality-gates carga-e2e lineage cargas calidad clean

# .env está en formato KEY=value, que es sintaxis de Makefile válida — así no
# hace falta `source .env` (no funciona igual en Windows) y las variables se
# exportan a los subprocesos (sbt, docker compose) igual en Linux que en
# Windows, sin depender del shell.
-include .env
export

# Entorno virtual de desarrollo. VENV_BASE_PYTHON es el intérprete con el que
# se crea: debe ser 3.9–3.12 porque numpy 1.26 y pyarrow 16 no publican wheels
# para 3.13+. En Windows se usa el launcher `py` para elegir la versión aunque
# `python` apunte a otra. Se puede sobreescribir:
#   make install-dev-env VENV_BASE_PYTHON=python3.11
VENV := .venv

# Los módulos de python/raillytics se lanzan con `python -m` (targets 03/04);
# pytest ya resuelve python/ vía pyproject.toml, pero make tiene que exportarlo.
PYTHONPATH := python

ifeq ($(OS),Windows_NT)
  # Spark/Hadoop en Windows necesita winutils.exe: HADOOP_HOME se define en .env
  # (ver .env.example) y el `export` de arriba lo pasa a sbt y al resto de
  # subprocesos.
  PYTHON ?= python
  VENV_BASE_PYTHON ?= py -3.12
  VENV_PY = $(VENV)/Scripts/python
  INSTALL_HOOKS_CMD = powershell -ExecutionPolicy Bypass -File scripts/install-githooks.ps1
else
  PYTHON ?= python3
  VENV_BASE_PYTHON ?= python3
  VENV_PY = $(VENV)/bin/python
  INSTALL_HOOKS_CMD = ./scripts/install-githooks.sh
endif

# sbt siempre a través del wrapper: permite varios sbt a la vez en el proyecto
# (01_ y 02_ en paralelo, o 04_/quality-gates con ellos en marcha) sin el
# "Create a new server? y/n", y en Windows evita que Ctrl+C deje colgado el
# "¿Desea terminar el trabajo por lotes (S/N)?".
SBT = $(PYTHON) scripts/run_sbt.py

COMPOSE = docker compose -f docker/docker-compose.yml --env-file .env

help:
	@echo "Targets disponibles:"
	@echo "  up                 Levanta MinIO + Airflow + Postgres + Superset (docker compose)"
	@echo "  down               Para el stack de docker compose"
	@echo "  install-dev-env    Crea .venv, instala requirements.txt, git hooks y .env"
	@echo "  install-hooks      Instala los git hooks de .githooks/ (core.hooksPath)"
	@echo "  test               Corre los tests de Python y de Scala"
	@echo "  test-python        Corre solo los tests de Python (pytest)"
	@echo "  test-scala         Corre solo los tests de Scala (sbt test)"
	@echo "  00_ingest             Levanta Ollama y dispara el DAG de descarga en Airflow, con la predicción de demanda al final (TRIMESTRE=2026-T4)"
	@echo "  01_raw-uploader       Lanza la app Spark L1 raw-uploader (primer plano)"
	@echo "  02_parquet-converter  Lanza la app Spark L2 parquet-converter (primer plano)"
	@echo "  03_silver-sample      Genera un Silver sintético en MinIO (sustituto de los jobs PySpark)"
	@echo "  04_silver             Silver real en streaming: lee Bronze L2 y construye las tablas Silver con quality gates (primer plano; terminal aparte)"
	@echo "  05_gold               Construye la capa Gold con la app Spark (Silver -> Parquet en raillytics-gold), con quality gates"
	@echo "  06_superset-import    Reimporta los dashboards de dashboards/superset/ en Superset"
	@echo "  07_prediccion         Predicción diaria de demanda AVE Madrid-Barcelona con un LLM (make 07_prediccion TRIMESTRE=2026-T4)"
	@echo "                        (PRED_PROMPT=demanda_v2 elige la plantilla; PRED_ARGS=\"--solo-nivel\", \"--sin-cache\" o \"--total-esperado N\" pasan opciones)"
	@echo "                        (antes comprueba los datos de la CNMC del trimestre y los descarga si faltan; PRED_CNMC=no lo omite)"
	@echo "  prediccion-sample     Genera fuentes SINTETICAS de la prediccion (demanda trimestral, festivos, eventos y meteo) en Bronze L2"
	@echo "  llm-up                Levanta Ollama (perfil llm del compose) y descarga OLLAMA_MODEL (LLM_GPU=1 reserva la GPU NVIDIA)"
	@echo "  llm-down              Para y elimina los contenedores de Ollama (los modelos se conservan en su volumen)"
	@echo "  quality-gates      Evalúa config/quality_gates.yml sobre Silver y Gold del lake (app Spark; falla si hay gates bloqueantes)"
	@echo "                     (make quality-gates QG_ARGS=silver | gold | <tabla> para acotar)"
	@echo "  carga-e2e          Carga end-to-end: sube el stack, descarga, L1, L2, Silver, Gold y predicción, parando cada stream al acabar"
	@echo "                     (TRIMESTRE=2026-T4 fija el trimestre a predecir; E2E_ARGS=\"--sin-prediccion\" o \"--desde silver\" para acotar)"
	@echo "  lineage            Regenera los datasets del dashboard de lineage desde el repo (luego: make 06_superset-import)"
	@echo "  cargas             Muestra las últimas cargas registradas (trazabilidad del lake)"
	@echo "  calidad            Muestra los últimos resultados de quality gates registrados"
	@echo "  clean              Borra directorios de staging/checkpoints generados"

up:
	$(COMPOSE) up -d

stop:
	$(COMPOSE) stop

down:
	$(COMPOSE) down

# Todo en Python/make (sin if/test del shell) para que funcione igual con
# cmd.exe en Windows que con sh en Linux. Es idempotente: el venv y las
# dependencias solo se rehacen si falta el venv o cambia requirements.txt.
install-dev-env: $(VENV)/.deps-installed install-hooks
	$(VENV_PY) -c "import pathlib, shutil; p = pathlib.Path('.env'); print('OK .env ya existe') if p.exists() else (shutil.copyfile('.env.example', p), print('OK .env creado desde .env.example: rellena las credenciales'))"
	$(VENV_PY) -c "import shutil; m = [t for t in ('java', 'sbt', 'docker') if not shutil.which(t)]; print('AVISO: no estan en el PATH (necesarios para Scala/Docker): ' + ', '.join(m) if m else 'OK java, sbt y docker disponibles')"
	@echo "Entorno listo. Activa el venv con:"
	@echo "  Linux/macOS: source $(VENV)/bin/activate"
	@echo "  Windows:     $(VENV)\Scripts\Activate.ps1"

$(VENV)/pyvenv.cfg:
	$(VENV_BASE_PYTHON) -c "import sys; sys.exit(0 if (3, 9) <= sys.version_info[:2] <= (3, 12) else 'Se necesita Python 3.9-3.12 para el venv (encontrado %d.%d). Usa VENV_BASE_PYTHON=...' % sys.version_info[:2])"
	$(VENV_BASE_PYTHON) -m venv $(VENV)

$(VENV)/.deps-installed: $(VENV)/pyvenv.cfg requirements.txt
	$(VENV_PY) -m pip install --upgrade pip
	$(VENV_PY) -m pip install -r requirements.txt
	$(VENV_PY) -c "import pathlib; pathlib.Path('$@').touch()"

install-hooks:
	$(INSTALL_HOOKS_CMD)

test: test-python test-scala

# Usa directamente el python del venv: equivale a tenerlo activado, sin
# depender de `activate` (que es distinto en cada shell/SO).
test-python: $(VENV)/.deps-installed
	$(VENV_PY) -m pytest -q

test-scala:
	$(SBT) -batch test

# Levanta Ollama y dispara el DAG de ingesta con la predicción de demanda encadenada al final (conf predecir=true).
# TRIMESTRE=2026-T4 fija el trimestre a predecir (por defecto, el trimestre en curso); PRED_PROMPT elige la plantilla.
# Las ejecuciones programadas del DAG (@daily) solo ingestan. Orden de las variables: ver 07_prediccion.
00_ingest: llm-up
	$(COMPOSE) exec airflow-scheduler airflow dags trigger ingesta_data_sources --conf "{\"predecir\": true, \"trimestre\": \"$(TRIMESTRE)\", \"prompt\": \"$(PRED_PROMPT)\"}"

01_raw-uploader:
	$(SBT) -batch "runMain raillytics.ingesta.l1.RawUploaderApp"

02_parquet-converter:
	$(SBT) -batch "runMain raillytics.ingesta.l2.ParquetConverterApp"

# Silver sintético: corre en el host con el python del venv (como test-python)
# y habla con MinIO con las variables MINIO_* del .env.
03_silver-sample: $(VENV)/.deps-installed
	$(VENV_PY) -m raillytics.procesamiento.silver_sample

# Silver real en streaming: lee lo que L2 deja en Bronze (l2/<fuente>/) y construye las tablas Silver que declara cada
# fuente de config/data_sources.yml (clave `silver`), con quality gates y cuarentena. Se queda en primer plano, como
# 01_raw-uploader y 02_parquet-converter: lánzalo en una terminal aparte.
04_silver:
	$(SBT) -batch "runMain raillytics.silver.SilverBuilderApp"

# Gold: app Spark batch en Scala (misma configuración s3a que L1/L2). Aplica los
# quality gates de config/quality_gates.yml a Silver (entrada) y a Gold (salida,
# antes de escribir): si falla uno bloqueante, termina con error y no toca Gold.
05_gold:
	$(SBT) -batch "runMain raillytics.gold.GoldBuilderApp"

06_superset-import:
	$(COMPOSE) exec superset bash /app/raillytics/docker/superset-import-dashboards.sh

# Predicción diaria de demanda del corredor AVE Madrid-Barcelona: el código fija el total del
# trimestre y un LLM de Ollama reparte ese total entre los días. Escribe un CSV en
# PREDICCIONES_ROOT (resultados/predicciones por defecto, un directorio del repo que está en git). Necesita Ollama arriba: make llm-up.
TRIMESTRE ?=
PRED_PROMPT ?= eventos_v1
PRED_ARGS ?=
# Fuentes SINTETICAS (no reales) de la prediccion en Bronze L2, en las rutas que espera config/prediccion.yml, para
# probar 07_prediccion y 00_ingest mientras Airflow no ingiera las reales. make prediccion-sample MUESTRA_ARGS="--hasta 2026-T3"
MUESTRA_ARGS ?=
prediccion-sample: $(VENV)/.deps-installed
	$(VENV_PY) -m raillytics.prediccion.muestra $(MUESTRA_ARGS)

# Antes de predecir comprueba que Silver tiene los datos de la CNMC que exige el trimestre (el mismo trimestre del año
# anterior, el último publicado y su gemelo) y, si faltan, los descarga (DAG de descarga -> L1 -> L2 -> Silver): usa el stack
# de docker, así que lo levanta si está parado. PRED_CNMC=no se salta la comprobación (p. ej. con --total-esperado y sin Docker).
PRED_CNMC ?= auto

07_prediccion: $(VENV)/.deps-installed
	$(if $(TRIMESTRE),,$(error Falta TRIMESTRE: make 07_prediccion TRIMESTRE=2026-T4))
	$(if $(filter no,$(PRED_CNMC)),,$(VENV_PY) scripts/carga_e2e.py --make "$(E2E_MAKE)" --compose "$(COMPOSE)" --asegurar-cnmc --trimestre $(TRIMESTRE))
	$(VENV_PY) -m raillytics.prediccion --trimestre $(TRIMESTRE) --prompt $(PRED_PROMPT) $(PRED_ARGS)


# LLM local (Ollama) para la predicción: perfil `llm` del compose. Con LLM_GPU=1 (p. ej. en el .env) se añade
# la reserva de GPU NVIDIA. `llm-up` espera a que Ollama esté sano y descarga OLLAMA_MODEL si falta.
LLM_COMPOSE = docker compose -f docker/docker-compose.yml $(if $(LLM_GPU),-f docker/docker-compose.gpu.yml) --env-file .env --profile llm

llm-up:
	$(LLM_COMPOSE) up -d --wait ollama
	$(LLM_COMPOSE) run --rm ollama-init

llm-down:
	$(LLM_COMPOSE) rm -sf ollama ollama-init

# Quality gate independiente sobre el lake: no escribe datos, solo evalúa y
# registra. Termina con error si falla algún gate bloqueante, así sirve de
# barrera entre pasos (p. ej. tras 03_silver-sample y antes de 05_gold).
QG_ARGS ?=
quality-gates:
	$(SBT) -batch "runMain raillytics.calidad.QualityGatesApp $(QG_ARGS)"

# Carga end-to-end (scripts/carga_e2e.py): como 01_raw-uploader, 02_parquet-converter y 04_silver no terminan solos, el
# script los lanza uno a uno, espera a que se queden sin trabajo y los para; el resto de pasos son los targets de arriba.
# E2E_ARGS: --sin-prediccion (no necesita Ollama), --desde <infra|descarga|l1|l2|silver|gold|prediccion> para reanudar,
# --timeout-fase N, --silencio-silver N. Los logs de los streams quedan en data/logs/carga_e2e/.
# E2E_MAKE evita escribir $(MAKE) en la receta: make ejecuta SIEMPRE las líneas que lo contienen, y `make -n carga-e2e`
# lanzaría la carga de verdad.
E2E_ARGS ?=
E2E_MAKE := $(MAKE)
carga-e2e: $(VENV)/.deps-installed
	$(VENV_PY) scripts/carga_e2e.py --make "$(E2E_MAKE)" --compose "$(COMPOSE)" $(if $(TRIMESTRE),--trimestre $(TRIMESTRE)) $(E2E_ARGS)

# Dashboard «Lineage de cargas»: sus datasets se GENERAN del repositorio (fuentes, SQL de Silver y Gold, gates, predicción y dashboards),
# así que al cambiar cualquiera de ellos hay que regenerarlos; un test comprueba que lo versionado está al día.
# AIRFLOW_UI_URL (en el .env) fija la dirección de Airflow que usan los enlaces a los logs (por defecto http://localhost:8080).
lineage: $(VENV)/.deps-installed
	$(VENV_PY) -m raillytics.lineage

cargas: $(VENV)/.deps-installed
	$(VENV_PY) -m raillytics.utils.cargas

calidad: $(VENV)/.deps-installed
	$(VENV_PY) -m raillytics.calidad

clean:
	$(PYTHON) -c "import shutil; [shutil.rmtree(p, ignore_errors=True) for p in ['data/bronze_l1_done', 'data/bronze_processed', 'data/bronze_rejected', 'data/checkpoints', 'target', 'project/target']]"
