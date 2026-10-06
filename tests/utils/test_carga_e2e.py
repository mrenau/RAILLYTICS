"""Tests de las piezas puras de scripts/carga_e2e.py (la orquestación con Docker, sbt y Airflow se prueba a mano: make carga-e2e)."""
import importlib.util
import subprocess
import sys
import time
from pathlib import Path

import pytest

RUTA = Path(__file__).resolve().parents[2] / "scripts" / "carga_e2e.py"
spec = importlib.util.spec_from_file_location("carga_e2e", RUTA)
e2e = importlib.util.module_from_spec(spec)
sys.modules["carga_e2e"] = e2e   # dataclasses resuelve las anotaciones por sys.modules
spec.loader.exec_module(e2e)


def tocar(ruta: Path) -> Path:
    ruta.parent.mkdir(parents=True, exist_ok=True)
    ruta.write_text("x")
    return ruta


def checkpoint(raiz: Path, offsets: list[str], commits: list[str]) -> Path:
    for n in offsets:
        tocar(raiz / "offsets" / n)
    for n in commits:
        tocar(raiz / "commits" / n)
    return raiz


# --------------------------------------------------------------------------------------------- ficheros_pendientes

def test_ficheros_pendientes_ignora_ocultos_y_cuenta_los_de_subcarpetas(tmp_path):
    tocar(tmp_path / "crtm" / ".gitkeep")
    tocar(tmp_path / "crtm" / ".fichero.zip.crc")
    assert e2e.ficheros_pendientes(tmp_path) == []

    real = tocar(tmp_path / "crtm" / "20261006_gtfs.zip")
    assert e2e.ficheros_pendientes(tmp_path) == [real]


def test_ficheros_pendientes_de_una_carpeta_que_no_existe(tmp_path):
    assert e2e.ficheros_pendientes(tmp_path / "no_existe") == []


# --------------------------------------------------------------------------------------------- checkpoints

def test_checkpoint_asentado_si_cada_offset_tiene_su_commit(tmp_path):
    assert e2e.checkpoint_asentado(checkpoint(tmp_path / "a", ["0", "1"], ["0", "1"]))


def test_checkpoint_con_un_batch_sin_confirmar_no_esta_asentado(tmp_path):
    assert not e2e.checkpoint_asentado(checkpoint(tmp_path / "a", ["0", "1"], ["0"]))


def test_checkpoint_inexistente_no_tiene_nada_a_medias(tmp_path):
    assert e2e.checkpoint_asentado(tmp_path / "no_existe")


def test_los_crc_de_hadoop_no_cuentan_como_batches(tmp_path):
    ck = checkpoint(tmp_path / "a", ["0"], ["0"])
    tocar(ck / "offsets" / ".0.crc")
    assert e2e.checkpoint_asentado(ck)
    assert e2e.lotes_confirmados(ck) == 1


def test_checkpoints_de_solo_devuelve_las_queries_con_offsets(tmp_path):
    checkpoint(tmp_path / "crtm", ["0"], ["0"])
    (tmp_path / "vacia").mkdir()
    assert [p.name for p in e2e.checkpoints_de(tmp_path)] == ["crtm"]


# --------------------------------------------------------------------------------------------- Sostenida

class Reloj:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t


def test_sostenida_exige_que_la_condicion_dure_el_tiempo_pedido():
    reloj = Reloj()
    s = e2e.Sostenida(10, reloj)
    assert not s.actualizar(True)
    reloj.t = 9
    assert not s.actualizar(True)
    reloj.t = 10
    assert s.actualizar(True)


def test_sostenida_se_reinicia_si_la_condicion_se_rompe():
    reloj = Reloj()
    s = e2e.Sostenida(10, reloj)
    s.actualizar(True)
    reloj.t = 8
    assert not s.actualizar(False)
    reloj.t = 12
    assert not s.actualizar(True)       # la cuenta empieza de nuevo en t=12
    reloj.t = 22
    assert s.actualizar(True)


def test_sostenida_se_reinicia_si_cambia_la_clave():
    reloj = Reloj()
    s = e2e.Sostenida(10, reloj)
    s.actualizar(True, clave=1)
    reloj.t = 9
    assert not s.actualizar(True, clave=2)   # un batch nuevo reinicia el silencio de Silver
    reloj.t = 18
    assert not s.actualizar(True, clave=2)
    reloj.t = 19
    assert s.actualizar(True, clave=2)


# --------------------------------------------------------------------------------------------- json_de

def test_json_de_ignora_el_ruido_anterior():
    salida = 'WARNING: algo [no es json]\n[{"dag_id": "a", "is_paused": "True"}]\n'
    assert e2e.json_de(salida) == [{"dag_id": "a", "is_paused": "True"}]


def test_json_de_acepta_una_lista_vacia():
    assert e2e.json_de("[]\n") == []


def test_json_de_sin_json_falla_con_la_salida_en_el_mensaje():
    with pytest.raises(e2e.CargaError, match="hola"):
        e2e.json_de("hola mundo")


# --------------------------------------------------------------------------------------------- varios

@pytest.mark.parametrize("dia, esperado", [
    ("2026-01-01", "2026-T1"), ("2026-03-31", "2026-T1"), ("2026-04-01", "2026-T2"),
    ("2026-10-06", "2026-T4"), ("2026-12-31", "2026-T4"),
])
def test_trimestre_en_curso(dia, esperado):
    from datetime import date
    assert e2e.trimestre_en_curso(date.fromisoformat(dia)) == esperado


def test_las_tablas_silver_esperadas_salen_del_registro_de_fuentes(monkeypatch):
    monkeypatch.chdir(RUTA.parents[1])
    assert {"cnmc_trimestral", "cnmc_precio_trimestral", "cnmc_precio_mensual"} <= set(e2e.tablas_silver_esperadas())


# --------------------------------------------------------------------------------------------- correr_stream

def contexto(tmp_path, **kw):
    base = dict(make=[sys.executable, "-c"], compose=[], trimestre="2026-T4", logs=tmp_path / "logs",
                timeout_fase=20, silencio_silver=0, estable=0, sondeo=0.05)
    base.update(kw)
    return e2e.Contexto(**base)


@pytest.mark.skipif(sys.platform == "win32", reason="el árbol de procesos se para con taskkill en Windows")
def test_correr_stream_para_el_proceso_cuando_esta_terminado(tmp_path):
    # `make` es aquí un python que se duerme: «objetivo» es el código que ejecuta.
    ctx = contexto(tmp_path)
    inicio = time.monotonic()
    e2e.correr_stream(ctx, "demo", "import time; time.sleep(60)", lambda: True)
    assert time.monotonic() - inicio < 15
    assert (ctx.logs / "demo.log").exists()


def test_correr_stream_falla_si_el_proceso_muere_antes_de_terminar(tmp_path):
    ctx = contexto(tmp_path)
    with pytest.raises(e2e.CargaError, match="código 3"):
        e2e.correr_stream(ctx, "demo", "import sys; print('boom'); sys.exit(3)", lambda: False)


def test_correr_stream_incluye_la_cola_del_log_en_el_error(tmp_path):
    ctx = contexto(tmp_path)
    with pytest.raises(e2e.CargaError, match="boom"):
        e2e.correr_stream(ctx, "demo", "import sys; print('boom'); sys.exit(1)", lambda: False)


def test_correr_stream_falla_por_timeout_y_no_deja_el_proceso_vivo(tmp_path):
    ctx = contexto(tmp_path, timeout_fase=0.3)
    with pytest.raises(e2e.CargaError, match="tiempo agotado"):
        e2e.correr_stream(ctx, "demo", "import time; time.sleep(60)", lambda: False)


def test_la_fase_se_salta_si_no_hay_nada_pendiente(tmp_path, capsys):
    ctx = contexto(tmp_path)
    e2e._fase_ingesta(ctx, "l1", "import sys; sys.exit(9)", lambda: True)   # si arrancara, fallaría
    assert "se salta" in capsys.readouterr().out


def test_el_script_no_usa_make_en_la_receta_del_makefile():
    # make -n ejecuta las líneas con $(MAKE): una carga real no puede colgar de ahí (ver E2E_MAKE en el Makefile).
    receta = [l for l in (RUTA.parents[1] / "Makefile").read_text(encoding="utf-8").splitlines()
              if "carga_e2e.py" in l and l.startswith("\t")]
    assert receta and all("$(MAKE)" not in l and "${MAKE}" not in l for l in receta)


def test_el_script_se_puede_importar_sin_efectos_secundarios():
    resultado = subprocess.run([sys.executable, str(RUTA), "--help"], capture_output=True, text=True)
    assert resultado.returncode == 0
    assert "--sin-prediccion" in resultado.stdout


# --------------------------------------------------------------------------------------------- asegurar_cnmc

from types import SimpleNamespace


def cobertura(suficiente, motivo="", descripcion="2025-T4, 2025-T2 y 2026-T2 publicados"):
    return SimpleNamespace(suficiente=suficiente, motivo=motivo, descripcion=descripcion)


def test_si_el_lake_ya_tiene_los_datos_no_se_descarga_nada(capsys):
    llamadas = []

    codigo = e2e.asegurar_cnmc("2026-T4", lambda: cobertura(True), lambda: llamadas.append("descarga"))

    assert codigo == 0 and llamadas == []
    assert "2026-T4" in capsys.readouterr().out


def test_si_faltan_se_descargan_y_se_vuelve_a_comprobar(capsys):
    estados = iter([cobertura(False, "faltan los trimestres 2025-T4"), cobertura(True)])
    llamadas = []

    codigo = e2e.asegurar_cnmc("2026-T4", lambda: next(estados), lambda: llamadas.append("descarga"))

    assert codigo == 0 and llamadas == ["descarga"]
    salida = capsys.readouterr().out
    assert "faltan los trimestres 2025-T4" in salida and "descargan" in salida


def test_si_tras_descargar_la_cnmc_sigue_sin_cubrir_el_trimestre_avisa_pero_no_bloquea(capsys):
    # La CNMC puede no haber publicado aún: el predictor decide con lo que haya (y avisa o pide --total-esperado).
    codigo = e2e.asegurar_cnmc("2027-T1", lambda: cobertura(False, "último publicado 2026-T2"), lambda: None)

    assert codigo == 0
    assert "AVISO" in capsys.readouterr().out


def test_si_la_descarga_falla_se_corta_con_error(capsys):
    def descargar():
        raise e2e.CargaError("silver: tiempo agotado")

    codigo = e2e.asegurar_cnmc("2026-T4", lambda: cobertura(False, "no hay datos"), descargar)

    assert codigo == 1
    assert "silver: tiempo agotado" in capsys.readouterr().out


def test_la_cli_asegurar_cnmc_exige_el_trimestre(capsys):
    assert e2e.main(["--asegurar-cnmc"]) == 2
    assert "--trimestre" in capsys.readouterr().err
