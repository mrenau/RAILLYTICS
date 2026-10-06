package raillytics.silver

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import raillytics.common.calidad.QualityGates
import raillytics.testutil.TestSpark

import java.time.LocalDate
import scala.jdk.CollectionConverters._

class SilverSqlSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private implicit var spark: SparkSession = _

  override def beforeAll(): Unit = spark = TestSpark.session("SilverSqlSpec")
  override def afterAll(): Unit = spark.stop()

  private val ColsIndicadores = Seq("Trimestre", "Tipo de producto", "Corredor", "Empresa", "Tipo de servicio",
    "Ingresos por venta de billetes (€)", "Plazas Ofertadas (Núm)", "Plazas.km Ofertadas (Plazas.km)", "Tren.km (Tren.km)",
    "Viajeros (Núm)", "Viajeros.km (Viajeros.km)")
  private val ColsPrecioTrimestral = Seq("Trimestre", "Empresa", "Tipo de servicio", "Tipo de producto", "Trayecto", "Precio (€)")
  private val ColsPrecioMensual = Seq("Mes", "Empresa", "Tipo de servicio", "Tipo de producto", "Trayecto", "Precio (€)")
  private val ColsViajerosProducto = Seq("Trimestre", "Tipo de producto", "Plazas Ofertadas (Mill.)", "Viajeros (Mill.)")
  private val ColsViajerosCorredor = Seq("Trimestre", "Corredor", "Empresa", "Plazas Ofertadas (Mill.)", "Viajeros (Mill.)")

  private def entrada(columnas: Seq[String], filas: Seq[Seq[String]]): DataFrame = {
    val schema = StructType((columnas :+ "_source_file").map(StructField(_, StringType)))
    spark.createDataFrame(filas.map(Row.fromSeq).asJava, schema)
  }

  // Una fila de indicadores; los volúmenes por defecto valen para casi todos los casos.
  private def ind(trimestre: String, corredor: String, empresa: String, viajeros: String = "100", ingresos: String = "",
                  producto: String = "LD AV", fichero: String = "f1") =
    Seq(trimestre, producto, corredor, empresa, "Comercial", ingresos, "1000", "100000", "500", viajeros, "200000", fichero)

  private def silverDe(tabla: String, df: DataFrame): DataFrame = {
    df.createOrReplaceTempView("entrada")
    spark.sql(SilverSql.cargar(tabla))
  }

  "cnmc_trimestral.sql" should "keep LD AV commercial rows, type the numbers, map the company and set the quarter start" in {
    val df = entrada(ColsIndicadores, Seq(
      ind("2026T2", "Madrid-Barcelona", "Renfe Viajeros", viajeros = "2042504"),
      ind("2026T2", "Madrid-Barcelona", "Iryo", viajeros = "849519"),
      ind("2026T2", "Madrid-Barcelona", "Total", viajeros = "", ingresos = "168413384"),
      ind("2026T2", "Madrid-Barcelona", "Renfe Viajeros", producto = "Cercanías"),      // otro producto: fuera
      ind("2026T2", "Madrid-Barcelona", "Nueva SA", viajeros = "n/d")                    // empresa desconocida y dato no numérico
    ))

    val filas = silverDe("cnmc_trimestral", df)
      .select("anio", "trimestre", "fecha_inicio", "corredor", "empresa", "operador_id", "viajeros", "ingresos_eur")
      .collect().map(_.toSeq).toSet

    filas shouldBe Set(
      Seq(2026, 2, java.sql.Date.valueOf(LocalDate.of(2026, 4, 1)), "Madrid-Barcelona", "Renfe Viajeros", "RENFE", 2042504L, null),
      Seq(2026, 2, java.sql.Date.valueOf(LocalDate.of(2026, 4, 1)), "Madrid-Barcelona", "Iryo", "IRYO", 849519L, null),
      Seq(2026, 2, java.sql.Date.valueOf(LocalDate.of(2026, 4, 1)), "Madrid-Barcelona", "Total", "TOTAL", null, 168413384L),
      Seq(2026, 2, java.sql.Date.valueOf(LocalDate.of(2026, 4, 1)), "Madrid-Barcelona", "Nueva SA", "OTRO", null, null)
    )
  }

  it should "keep the row of the newest file when the same key comes twice" in {
    val df = entrada(ColsIndicadores, Seq(
      ind("2026T1", "Madrid-Barcelona", "Iryo", viajeros = "10", fichero = "l2/2026-09-30/a.parquet"),
      ind("2026T1", "Madrid-Barcelona", "Iryo", viajeros = "11", fichero = "l2/2026-10-01/a.parquet")
    ))

    silverDe("cnmc_trimestral", df).select("viajeros").collect().map(_.getLong(0)) shouldBe Array(11L)
  }

  it should "leave a bad quarter as NULL keys instead of failing the query, so the gates can catch it" in {
    val df = entrada(ColsIndicadores, Seq(ind("2026T9", "Madrid-Barcelona", "Iryo"), ind("sin-trimestre", "Madrid-Barcelona", "Iryo")))

    val filas = silverDe("cnmc_trimestral", df).select("anio", "trimestre", "fecha_inicio").collect().map(_.toSeq).toSet

    filas shouldBe Set(Seq(2026, 9, null), Seq(null, null, null))   // 2026T9: fecha_inicio NULL (el mes 25 no existe)
  }

  "cnmc_precio_trimestral.sql" should "parse the decimal comma and map Renfe-AVE and Renfe-AVLO to different operators" in {
    val df = entrada(ColsPrecioTrimestral, Seq(
      Seq("2026T2", "Renfe-AVE", "Comercial", "LD AV", "Madrid-Barcelona", "71,6", "f"),
      Seq("2026T2", "Renfe-AVLO", "Comercial", "LD AV", "Madrid-Barcelona", "39,9", "f"),
      Seq("2026T2", "Total", "Comercial", "LD AV", "Madrid-Barcelona", "66,06", "f"),
      Seq("2026T2", "Renfe-AVE", "Comercial", "Cercanías", "Madrid-Barcelona", "5,0", "f")
    ))

    val filas = silverDe("cnmc_precio_trimestral", df).select("anio", "trimestre", "operador_id", "precio_medio_eur")
      .collect().map(r => (r.getString(2), r.getDecimal(3).toPlainString)).toSet

    filas shouldBe Set(("RENFE", "71.60"), ("AVLO", "39.90"), ("TOTAL", "66.06"))
  }

  "cnmc_precio_mensual.sql" should "split the month and set the first day of the month" in {
    val df = entrada(ColsPrecioMensual, Seq(Seq("2026-06", "Iryo", "Comercial", "LD AV", "Madrid-Barcelona", "66,56", "f")))

    val fila = silverDe("cnmc_precio_mensual", df).select("anio", "mes", "fecha_inicio", "operador_id").collect().head.toSeq

    fila shouldBe Seq(2026, 6, java.sql.Date.valueOf(LocalDate.of(2026, 6, 1)), "IRYO")
  }

  "cnmc_viajeros_producto.sql" should "parse the decimal comma into millions, leave missing seats as NULL and set the quarter start" in {
    val df = entrada(ColsViajerosProducto, Seq(
      Seq("2026T2", "LD AV", "11,5374", "10,612", "f"),
      Seq("2026T2", "Cercanías", "108,4298", "112,2463", "f"),
      Seq("2016T1", "MD AV", "", "1,753", "f"),                 // la CNMC no da plazas antes de 2018
      Seq("2016T1", "MD Convencional", "", "n/d", "f")           // dato no numérico: NULL, lo cazan los gates
    ))

    val filas = silverDe("cnmc_viajeros_producto", df)
      .select("anio", "trimestre", "fecha_inicio", "tipo_producto", "viajeros_millones", "plazas_millones")
      .collect().map(r => (r.getInt(0), r.getInt(1), r.getDate(2).toString, r.getString(3),
        Option(r.getDecimal(4)).map(_.toPlainString), Option(r.getDecimal(5)).map(_.toPlainString))).toSet

    filas shouldBe Set(
      (2026, 2, "2026-04-01", "LD AV", Some("10.6120"), Some("11.5374")),
      (2026, 2, "2026-04-01", "Cercanías", Some("112.2463"), Some("108.4298")),
      (2016, 1, "2016-01-01", "MD AV", Some("1.7530"), None),
      (2016, 1, "2016-01-01", "MD Convencional", None, None)
    )
  }

  it should "keep the row of the newest file when the same key comes twice" in {
    val df = entrada(ColsViajerosProducto, Seq(
      Seq("2026T1", "LD AV", "1", "10", "l2/2026-09-30/a.parquet"),
      Seq("2026T1", "LD AV", "1", "11", "l2/2026-10-01/a.parquet")
    ))

    silverDe("cnmc_viajeros_producto", df).select("viajeros_millones").collect().map(_.getDecimal(0).intValue) shouldBe Array(11)
  }

  "cnmc_viajeros_corredor.sql" should "map the company, parse the decimal comma into millions and keep every corridor" in {
    val df = entrada(ColsViajerosCorredor, Seq(
      Seq("2026T2", "Madrid-Barcelona", "Renfe Viajeros", "1,7681", "2,0425", "f"),
      Seq("2026T2", "Madrid-Barcelona", "Iryo", "0,978", "0,8495", "f"),
      Seq("2026T2", "Madrid-Barcelona", "OUIGO", "0,5305", "0,5324", "f"),
      Seq("2026T2", "Madrid-Sevilla", "Nueva SA", "", "0,1", "f")
    ))

    val filas = silverDe("cnmc_viajeros_corredor", df)
      .select("corredor", "operador_id", "viajeros_millones", "plazas_millones")
      .collect().map(r => (r.getString(0), r.getString(1), r.getDecimal(2).toPlainString, Option(r.getDecimal(3)).map(_.toPlainString))).toSet

    filas shouldBe Set(
      ("Madrid-Barcelona", "RENFE", "2.0425", Some("1.7681")),
      ("Madrid-Barcelona", "IRYO", "0.8495", Some("0.9780")),
      ("Madrid-Barcelona", "OUIGO", "0.5324", Some("0.5305")),
      ("Madrid-Sevilla", "OTRO", "0.1000", None)
    )
  }

  it should "keep the row of the newest file when the same key comes twice" in {
    val df = entrada(ColsViajerosCorredor, Seq(
      Seq("2026T1", "Madrid-Barcelona", "Iryo", "1", "10", "l2/2026-09-30/a.parquet"),
      Seq("2026T1", "Madrid-Barcelona", "Iryo", "1", "11", "l2/2026-10-01/a.parquet")
    ))

    silverDe("cnmc_viajeros_corredor", df).select("viajeros_millones").collect().map(_.getDecimal(0).intValue) shouldBe Array(11)
  }

  "SilverSql.cargar" should "say which resource is missing" in {
    val e = the[IllegalArgumentException] thrownBy SilverSql.cargar("no_existe")
    e.getMessage should include("/silver/no_existe.sql")
  }

  // ---- los gates reales de config/quality_gates.yml sobre un resultado verosímil ----

  private val gates = QualityGates.load("config/quality_gates.yml")

  // 6 corredores × 26 trimestres × 4 empresas = 624 filas (el gate de filas mínimas exige 300).
  private def indicadoresVerosimiles(): Seq[Seq[String]] =
    for {
      corredor <- Seq("Madrid-Barcelona", "Madrid-Sevilla", "Madrid-Valencia", "Madrid-Alicante", "Madrid-Málaga/Granada", "Resto")
      anio <- 2020 to 2026
      t <- 1 to 4 if anio < 2026 || t <= 2
      empresa <- Seq("Renfe Viajeros", "Iryo", "OUIGO", "Total")
    } yield {
      val fila = ind(s"${anio}T$t", corredor, empresa, viajeros = "1000", ingresos = if (empresa == "Total") "5000" else "")
      // Como en los datos reales (2026-10-01): el Total de un corredor solo trae ingresos (columnas 6 a 10 = volúmenes, vacías),
      // pero el de «Resto» (la red que no es esos corredores) también trae volúmenes.
      if (empresa == "Total" && corredor != "Resto") fila.patch(6, Seq.fill(5)(""), 5) else fila
    }

  private def evaluar(vista: String, df: DataFrame) = {
    df.createOrReplaceTempView(vista)
    QualityGates.evaluar(vista, gates(vista))
  }

  "the silver_cnmc_trimestral gates" should "pass on a realistic snapshot (no blocking failures)" in {
    val resultado = silverDe("cnmc_trimestral", entrada(ColsIndicadores, indicadoresVerosimiles()))

    evaluar("silver_cnmc_trimestral", resultado).filter(_.bloquea) shouldBe empty
  }

  it should "not warn about the volumes of the Total row of the Resto pseudo-corridor (real CNMC data), but still warn for a real corridor" in {
    def avisos(filas: Seq[Seq[String]]) =
      evaluar("silver_cnmc_trimestral", silverDe("cnmc_trimestral", entrada(ColsIndicadores, filas))).filter(r => !r.pasa).map(_.gate)
    val gate = "volumen_en_operadores_e_ingresos_en_total"

    avisos(indicadoresVerosimiles()) should not contain gate

    val totalConViajeros = indicadoresVerosimiles().map(f => if (f(2) == "Madrid-Barcelona" && f(3) == "Total" && f(0) == "2026T2") f.updated(9, "123") else f)
    avisos(totalConViajeros) should contain(gate)
  }

  it should "block a snapshot with negative volumes" in {
    val filas = indicadoresVerosimiles().map(f => if (f(2) == "Madrid-Sevilla" && f(3) == "Iryo" && f(0) == "2026T2") f.updated(9, "-5") else f)

    val bloqueantes = evaluar("silver_cnmc_trimestral", silverDe("cnmc_trimestral", entrada(ColsIndicadores, filas))).filter(_.bloquea)

    bloqueantes.map(_.gate) shouldBe Seq("volumenes_no_negativos")
  }

  it should "block a snapshot without the Madrid-Barcelona corridor, and warn (not block) about an unknown company" in {
    val sinCorredor = indicadoresVerosimiles().filterNot(_(2) == "Madrid-Barcelona") :+ ind("2026T2", "Madrid-Sevilla", "Nueva SA")

    val res = evaluar("silver_cnmc_trimestral", silverDe("cnmc_trimestral", entrada(ColsIndicadores, sinCorredor)))

    res.filter(_.bloquea).map(_.gate) shouldBe Seq("corredor_madrid_barcelona_presente")
    res.filter(r => !r.pasa && !r.bloquea).map(_.gate) should contain("empresas_conocidas")
  }

  it should "block a snapshot with a missing quarter key" in {
    val filas = indicadoresVerosimiles() :+ ind("sin-trimestre", "Madrid-Barcelona", "Iryo")

    evaluar("silver_cnmc_trimestral", silverDe("cnmc_trimestral", entrada(ColsIndicadores, filas))).filter(_.bloquea).map(_.gate) should contain("claves_sin_nulos")
  }

  "the silver price gates" should "pass on realistic prices and block an out-of-range price" in {
    // 5 trayectos × 14 trimestres × 5 empresas = 350 filas (mínimo 150) y × 42 meses = 1050 (mínimo 500).
    val trayectos = Seq("Madrid-Barcelona", "Madrid-Sevilla", "Madrid-Valencia", "Madrid-Alicante", "Madrid-Málaga")
    def precios(valor: String) = for {
      trayecto <- trayectos
      anio <- 2023 to 2026
      t <- 1 to 4 if anio < 2026 || t <= 2
      empresa <- Seq("Renfe-AVE", "Renfe-AVLO", "Iryo", "OUIGO", "Total")
    } yield Seq(s"${anio}T$t", empresa, "Comercial", "LD AV", trayecto, valor, "f")
    val mensual = for {
      trayecto <- trayectos
      anio <- 2023 to 2026
      m <- 1 to 12 if anio < 2026 || m <= 6
      empresa <- Seq("Renfe-AVE", "Renfe-AVLO", "Iryo", "OUIGO", "Total")
    } yield Seq(f"$anio-$m%02d", empresa, "Comercial", "LD AV", trayecto, "60,5", "f")

    evaluar("silver_cnmc_precio_trimestral", silverDe("cnmc_precio_trimestral", entrada(ColsPrecioTrimestral, precios("60,5")))).filter(_.bloquea) shouldBe empty
    evaluar("silver_cnmc_precio_mensual", silverDe("cnmc_precio_mensual", entrada(ColsPrecioMensual, mensual))).filter(_.bloquea) shouldBe empty
    evaluar("silver_cnmc_precio_trimestral", silverDe("cnmc_precio_trimestral", entrada(ColsPrecioTrimestral, precios("9999,0")))).filter(_.bloquea).map(_.gate) shouldBe Seq("precio_razonable")
  }

  // 5 productos × 26 trimestres = 130 filas (mínimo 120); 5 corredores × 26 trimestres × 3 empresas = 390 (mínimo 190).
  private def viajerosProducto(viajeros: String = "10,5") = for {
    producto <- Seq("Cercanías", "LD AV", "LD Convencional", "MD AV", "MD Convencional")
    anio <- 2020 to 2026
    t <- 1 to 4 if anio < 2026 || t <= 2
  } yield Seq(s"${anio}T$t", producto, "2,5", viajeros, "f")

  private def viajerosCorredor(corredores: Seq[String] = Seq("Madrid-Barcelona", "Madrid-Sevilla", "Madrid-Valencia", "Madrid-Alicante", "Madrid-Málaga/Granada")) = for {
    corredor <- corredores
    anio <- 2020 to 2026
    t <- 1 to 4 if anio < 2026 || t <= 2
    empresa <- Seq("Renfe Viajeros", "Iryo", "OUIGO")
  } yield Seq(s"${anio}T$t", corredor, empresa, "0,5", "0,4", "f")

  "the silver_cnmc_viajeros_producto gates" should "pass on realistic data, and block negative volumes or a missing LD AV" in {
    def bloqueantes(filas: Seq[Seq[String]]) =
      evaluar("silver_cnmc_viajeros_producto", silverDe("cnmc_viajeros_producto", entrada(ColsViajerosProducto, filas))).filter(_.bloquea).map(_.gate)

    bloqueantes(viajerosProducto()) shouldBe empty
    bloqueantes(viajerosProducto("-3,0")) shouldBe Seq("volumenes_no_negativos")
    bloqueantes(viajerosProducto().filterNot(_(1) == "LD AV")) should contain("ld_av_presente")
  }

  it should "warn (not block) about a product the project does not know" in {
    val filas = viajerosProducto() :+ Seq("2026T2", "Hyperloop", "1", "1", "f")

    val res = evaluar("silver_cnmc_viajeros_producto", silverDe("cnmc_viajeros_producto", entrada(ColsViajerosProducto, filas)))

    res.filter(_.bloquea) shouldBe empty
    res.filter(r => !r.pasa).map(_.gate) should contain("productos_conocidos")
  }

  "the silver_cnmc_viajeros_corredor gates" should "pass on realistic data, and block negative volumes or a missing Madrid-Barcelona" in {
    def bloqueantes(filas: Seq[Seq[String]]) =
      evaluar("silver_cnmc_viajeros_corredor", silverDe("cnmc_viajeros_corredor", entrada(ColsViajerosCorredor, filas))).filter(_.bloquea).map(_.gate)

    bloqueantes(viajerosCorredor()) shouldBe empty
    bloqueantes(viajerosCorredor().map(f => if (f(1) == "Madrid-Sevilla" && f(0) == "2026T2" && f(2) == "Iryo") f.updated(4, "-1") else f)) shouldBe Seq("volumenes_no_negativos")
    bloqueantes(viajerosCorredor(Seq("Madrid-Sevilla", "Madrid-Valencia", "Madrid-Alicante", "Madrid-Málaga/Granada", "Madrid-Galicia"))) shouldBe Seq("corredor_madrid_barcelona_presente")
  }

  it should "warn (not block) about an unknown company" in {
    val filas = viajerosCorredor() :+ Seq("2026T2", "Madrid-Sevilla", "Nueva SA", "0,1", "0,1", "f")

    val res = evaluar("silver_cnmc_viajeros_corredor", silverDe("cnmc_viajeros_corredor", entrada(ColsViajerosCorredor, filas)))

    res.filter(_.bloquea) shouldBe empty
    res.filter(r => !r.pasa).map(_.gate) should contain("empresas_conocidas")
  }
}
