# Guía de Ejercicios — Cap.14: Scala — el Lenguaje Nativo de Spark

> Spark está escrito en Scala. No es un detalle histórico — es una decisión
> de diseño que todavía importa en 2024.
>
> Cuando escribes un job en PySpark, el plan de ejecución pasa por Py4J,
> se serializa, cruza el boundary JVM-Python, y Spark ejecuta código Scala/JVM.
> Cuando escribes el mismo job en Scala, el plan es código JVM nativo desde el inicio.
> No hay boundary. No hay serialización. No hay traducción.
>
> Esto no significa que siempre debas escribir en Scala.
> Significa que debes entender cuándo el boundary importa —
> y cuándo la productividad de Python supera ampliamente la diferencia de rendimiento.
>
> Para el 80% de los jobs, PySpark es la respuesta correcta.
> Para el 20% donde el rendimiento o la integración profunda con Spark importa,
> entender Scala te da acceso a las APIs que PySpark no expone.

---

## Por qué Scala para Spark: la historia en tres párrafos

Spark nació en el AMPLab de Berkeley en 2009, escrito en Scala. La elección
no fue arbitraria: Scala corre en la JVM (aprovechando el ecosistema Java),
tiene un sistema de tipos expresivo que permite APIs como el Dataset API,
y sus funciones de orden superior hacen que el código de transformación
de datos sea natural y conciso.

Cuando Spark añadió Python (PySpark) en 2010 y R (SparkR) en 2015,
lo hizo a través de un bridge. La JVM sigue siendo el runtime principal.
Las APIs de Python y R son traducciones de las APIs de Scala/Java.

En 2024, la mayoría de los proyectos nuevos usan PySpark — el ecosistema
PyData, la curva de aprendizaje menor, y la productividad de Python pesan
más que la diferencia de rendimiento para la mayoría de los casos.
Pero entender Scala te hace un mejor usuario de PySpark: entiendes por qué
el API se comporta como se comporta, qué hay "debajo" de cada operación,
y cuándo vale la pena ir a la API nativa.

---

## El modelo mental: Scala como JVM tipada con funciones de orden superior

```
Java + tipos estáticos + funciones como valores + inferencia de tipos
= Scala

Spark aprovecha:
  - Tipos en tiempo de compilación: Dataset[Transaccion] vs DataFrame (sin tipos)
  - Funciones de orden superior: df.map(fila => ...)
  - Pattern matching: match/case para el plan de ejecución del optimizer
  - Traits: la forma en que Spark define sus interfaces (Encoder, etc.)
  - Implicits (Scala 2) / Given/Using (Scala 3): cómo Spark registra Encoders
```

---

## Tabla de contenidos

- [Sección 14.1 — Scala para ingenieros de datos: lo esencial](#sección-141--scala-para-ingenieros-de-datos-lo-esencial)
- [Sección 14.2 — Dataset API: tipos en tiempo de compilación](#sección-142--dataset-api-tipos-en-tiempo-de-compilación)
- [Sección 14.3 — Implicits y Encoders: la magia detrás del Dataset](#sección-143--implicits-y-encoders-la-magia-detrás-del-dataset)
- [Sección 14.4 — UDFs en Scala: sin el overhead de Python](#sección-144--udfs-en-scala-sin-el-overhead-de-python)
- [Sección 14.5 — Spark internals desde Scala: acceso a APIs no expuestas](#sección-145--spark-internals-desde-scala-acceso-a-apis-no-expuestas)
- [Sección 14.6 — Testing de jobs Spark en Scala](#sección-146--testing-de-jobs-spark-en-scala)
- [Sección 14.7 — Cuándo Scala y cuándo Python: la decisión final](#sección-147--cuándo-scala-y-cuándo-python-la-decisión-final)

---

## Sección 14.1 — Scala para Ingenieros de Datos: lo Esencial

### Ejercicio 14.1.1 — Leer: Scala en 30 minutos para un Python developer

**Tipo: Leer**

```scala
// Scala: lo que un Python developer necesita saber para entender código de Spark

// 1. val vs var (equivalente a const vs let en JS, o simplemente la distinción mutable/inmutable)
val nombre = "Alice"          // inmutable — no se puede reasignar
var contador = 0              // mutable
// nombre = "Bob"             // Error: reasignación de val

// 2. Tipos (inferidos, pero presentes)
val n: Int = 42               // tipo explícito
val m = 42                    // tipo inferido: Int
val x = 3.14                  // Double
val s = "hola"                // String

// 3. Colecciones inmutables (lo que Spark usa internamente)
val lista = List(1, 2, 3, 4, 5)
val mapa = Map("a" -> 1, "b" -> 2)
val conjunto = Set(1, 2, 3)

// 4. Funciones como valores (fundamental para Spark)
val duplicar: Int => Int = x => x * 2
val sumar: (Int, Int) => Int = (a, b) => a + b

lista.map(x => x * 2)         // List(2, 4, 6, 8, 10)
lista.filter(x => x > 2)     // List(3, 4, 5)
lista.reduce((a, b) => a + b) // 15

// 5. Case classes (lo que más usarás para definir schemas)
case class Transaccion(
  id: String,
  userId: String,
  monto: Double,
  region: String,
  timestamp: Long,
)
// Las case classes tienen: equals, hashCode, toString, copy, y pattern matching gratis

val t = Transaccion("t1", "alice", 150.0, "norte", 1700000000L)
val tCopy = t.copy(monto = 200.0)  // nueva instancia con monto cambiado

// 6. Option: alternativa a null (Scala evita null cuando es posible)
val quizas: Option[String] = Some("valor")
val nada: Option[String] = None

quizas.getOrElse("default")  // "valor"
nada.getOrElse("default")    // "default"
quizas.map(s => s.toUpperCase)  // Some("VALOR")

// 7. Pattern matching (más potente que switch/case)
val resultado = t.region match {
  case "norte" => t.monto * 0.9
  case "sur"   => t.monto * 0.95
  case _       => t.monto
}

// 8. For comprehensions (como list comprehensions de Python)
val montosMayores = for {
  t <- lista  // itera sobre lista
  if t > 2    // filtro
} yield t * 10  // transforma

// Equivalente Python:
// [x * 10 for x in lista if x > 2]
```

**Preguntas:**

1. ¿Las `case class` de Scala son equivalentes a los `dataclass` de Python?
   ¿Qué tienen de más?

2. ¿Por qué Scala prefiere `Option[T]` sobre `null`?
   ¿Cómo se relaciona con el manejo de nulls en DataFrames de Spark?

3. ¿La función `lista.map(x => x * 2)` en Scala es lazy o eager?
   ¿Y si usas `lista.toStream.map(x => x * 2)`?

4. ¿Los `val` en Scala son inmutables pero pueden contener colecciones mutables?
   ¿Por qué es importante esta distinción?

5. ¿Qué ventaja da el pattern matching de Scala sobre el `if/elif/else`
   de Python para el código del optimizer de Spark?

**Pista:** Las `case class` de Scala tienen structural equality automática:
`Transaccion("t1", "alice", 150.0, "norte", 0L) == Transaccion("t1", "alice", 150.0, "norte", 0L)`
es `true` sin necesitar `__eq__`. Los `dataclass` de Python añaden esto con
`@dataclass(eq=True)`, pero en Scala es el comportamiento por defecto.
El pattern matching es clave en el optimizer de Spark: cada regla de optimización
hace pattern matching sobre el plan lógico (que es un árbol de case classes)
y lo transforma si encuentra el patrón.

---

### Ejercicio 14.1.2 — El primer job de Spark en Scala

```scala
import org.apache.spark.sql.{SparkSession, Dataset, DataFrame}
import org.apache.spark.sql.functions._

object MiPrimerJob {
  
  // Case class para el schema tipado:
  case class Transaccion(
    id: String,
    userId: String,
    monto: Double,
    region: String,
  )
  
  def main(args: Array[String]): Unit = {
    
    // Crear SparkSession:
    val spark = SparkSession.builder()
      .appName("mi-primer-job")
      .config("spark.sql.shuffle.partitions", "200")
      .getOrCreate()
    
    // Importar implicits para la conversión DataFrame <-> Dataset:
    import spark.implicits._
    
    // Leer como DataFrame (sin tipos en tiempo de compilación):
    val df: DataFrame = spark.read.parquet("datos/transacciones.parquet")
    
    // Leer como Dataset[Transaccion] (con tipos):
    val ds: Dataset[Transaccion] = spark.read
      .parquet("datos/transacciones.parquet")
      .as[Transaccion]
    
    // Transformaciones con DataFrame API (misma que PySpark):
    val resultado: DataFrame = df
      .filter(col("monto") > 100)
      .groupBy("region")
      .agg(
        sum("monto").alias("revenue"),
        count("*").alias("transacciones"),
      )
      .orderBy(desc("revenue"))
    
    // Transformaciones con Dataset API (tipado):
    val resultadoTipado = ds
      .filter(t => t.monto > 100)  // lambda Scala — sin magia de Catalyst
      .groupByKey(t => t.region)
      .mapValues(t => t.monto)
      .reduceGroups((a, b) => a + b)
    
    // Guardar:
    resultado.write
      .format("delta")
      .mode("overwrite")
      .save("resultados/revenue_por_region")
    
    spark.stop()
  }
}
```

**Preguntas:**

1. ¿Por qué se necesita `import spark.implicits._` para usar `.as[Transaccion]`?

2. ¿La transformación `ds.filter(t => t.monto > 100)` genera un plan de
   ejecución diferente al `df.filter(col("monto") > 100)`?
   ¿Cuál es más eficiente?

3. ¿`Dataset[Transaccion]` y `DataFrame` son el mismo tipo en la JVM?
   ¿Cuál es la relación entre ellos?

4. ¿El `spark.stop()` es necesario en producción?
   ¿Qué pasa si no se llama?

5. ¿Este código puede empaquetarse como un JAR y enviarse a un cluster
   sin cambios? ¿Qué necesita el cluster para ejecutarlo?

---

### Ejercicio 14.1.3 — sbt: el sistema de build de Spark

```scala
// build.sbt — el equivalente de pyproject.toml para proyectos Scala

name := "pipeline-ecommerce"
version := "1.0.0"
scalaVersion := "2.13.12"

// Dependencias:
libraryDependencies ++= Seq(
  // Spark — "provided" significa que no incluirlo en el JAR final
  // (el cluster ya tiene Spark instalado)
  "org.apache.spark" %% "spark-core"         % "3.5.0" % "provided",
  "org.apache.spark" %% "spark-sql"          % "3.5.0" % "provided",
  "org.apache.spark" %% "spark-streaming"    % "3.5.0" % "provided",
  
  // Delta Lake:
  "io.delta"         %% "delta-spark"        % "3.0.0" % "provided",
  
  // Para testing:
  "org.scalatest"    %% "scalatest"          % "3.2.17" % "test",
  "org.apache.spark" %% "spark-sql"          % "3.5.0" % "test",
  
  // Librerías que SÍ van en el JAR:
  "com.typesafe"     % "config"              % "1.4.3",
  "ch.qos.logback"   % "logback-classic"     % "1.4.11",
)

// Resolver conflictos de versiones (frecuente con el ecosistema Spark):
dependencyOverrides ++= Seq(
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.15.2",
)

// Configuración de assembly (para crear un fat JAR):
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _                         => MergeStrategy.first
}
```

**Preguntas:**

1. ¿Por qué Spark se marca como `"provided"` en las dependencias?
   ¿Qué pasa si no lo marcas así?

2. ¿El `%%` en `"org.apache.spark" %% "spark-core"` es diferente al `%`?
   ¿Qué hace?

3. ¿`sbt assembly` y `spark-submit` son equivalentes a `python setup.py` + ejecutar?

4. ¿Cómo gestionas que el cluster tiene Scala 2.12 pero tu código usa Scala 2.13?

5. ¿Por qué los conflictos de versiones de Jackson son tan frecuentes en proyectos Spark?

**Pista:** El `%%` añade automáticamente el sufijo de versión de Scala al artifact ID.
`"org.apache.spark" %% "spark-core" % "3.5.0"` es equivalente a
`"org.apache.spark" % "spark-core_2.13" % "3.5.0"` en Scala 2.13.
Los conflictos de Jackson ocurren porque Spark incluye Jackson para serialización,
y muchas librerías también incluyen Jackson — a veces versiones distintas.
`dependencyOverrides` fuerza una versión específica para resolver el conflicto.

---

### Ejercicio 14.1.4 — Funciones de orden superior: el vocabulario de Spark

```scala
// Las funciones de orden superior son el vocabulario del código de Spark.
// Entenderlas hace que el código de Spark sea legible.

val numeros = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

// map: transformar cada elemento
val dobles = numeros.map(n => n * 2)
// List(2, 4, 6, 8, 10, 12, 14, 16, 18, 20)

// flatMap: transformar y aplanar (como explode en Spark)
val conVecinos = numeros.flatMap(n => List(n - 1, n, n + 1))
// List(0, 1, 2, 1, 2, 3, 2, 3, 4, ...)

// filter: seleccionar elementos
val pares = numeros.filter(n => n % 2 == 0)
// List(2, 4, 6, 8, 10)

// reduce: combinar todos los elementos en uno
val suma = numeros.reduce((a, b) => a + b)  // 55
val maximo = numeros.reduce((a, b) => if (a > b) a else b)  // 10

// fold: como reduce pero con un valor inicial
val sumaConInicio = numeros.foldLeft(100)((acc, n) => acc + n)  // 155

// groupBy: agrupar por clave (equivalente al GroupBy de Spark)
val porParidad: Map[String, List[Int]] = numeros.groupBy(n =>
  if (n % 2 == 0) "par" else "impar"
)
// Map("par" -> List(2,4,6,8,10), "impar" -> List(1,3,5,7,9))

// zip y zipWithIndex:
val conIndice = numeros.zipWithIndex
// List((1,0), (2,1), (3,2), ...)

// partition: dividir en dos listas según predicado
val (mayores, menores) = numeros.partition(n => n > 5)
// mayores = List(6,7,8,9,10), menores = List(1,2,3,4,5)
```

**Preguntas:**

1. ¿`flatMap` en Scala es equivalente al `explode()` de Spark?
   ¿O al `flatMap` del RDD API?

2. ¿`reduce` y `foldLeft` son equivalentes?
   ¿Cuándo `reduce` puede fallar donde `foldLeft` no?

3. ¿El `groupBy` de Scala collections es paralelo?
   ¿Cómo se diferencia del `groupBy` de Spark?

4. ¿Por qué `foldLeft` es más comúnmente usado que `foldRight` en práctica?

5. ¿Las funciones de orden superior de Scala collections son lazy o eager?
   ¿Y si usas `LazyList` en lugar de `List`?

---

### Ejercicio 14.1.5 — Leer: código Spark real en Scala

**Tipo: Leer y analizar**

Leer y explicar este fragmento de código Spark nativo en Scala:

```scala
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

def calcularMetricas(
  spark: SparkSession,
  rutaVentas: String,
  rutaClientes: String,
): DataFrame = {
  import spark.implicits._

  val ventas = spark.read.parquet(rutaVentas)
  val clientes = spark.read.parquet(rutaClientes)
    .select("id", "region", "segmento")
    .cache()

  val ventanaUsuario = Window
    .partitionBy("userId")
    .orderBy("timestamp")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

  ventas
    .filter($"monto" > 0 && $"tipo" === "compra")
    .join(clientes, ventas("userId") === clientes("id"), "left")
    .withColumn("gastoAcumulado", sum($"monto").over(ventanaUsuario))
    .withColumn("esClientePremium", $"gastoAcumulado" > 10000)
    .groupBy("region", "segmento")
    .agg(
      sum("monto").alias("revenue"),
      countDistinct("userId").alias("usuariosUnicos"),
      avg("gastoAcumulado").alias("gastoPromedioAcumulado"),
      sum(when($"esClientePremium", $"monto").otherwise(0)).alias("revenuePremium"),
    )
    .withColumn("pctRevenuePremium",
      $"revenuePremium" / $"revenue" * 100)
}
```

**Preguntas:**

1. ¿Qué hace `$"monto"` en Scala? ¿Es equivalente a `F.col("monto")` en PySpark?

2. ¿El `.cache()` en `clientes` tiene el mismo efecto que en PySpark?
   ¿Cuándo se materializa el caché?

3. ¿La `Window` de Scala y la `Window` de PySpark generan el mismo plan físico?

4. ¿`sum(when($"esClientePremium", $"monto").otherwise(0))` es más eficiente
   que hacer `filter` primero y luego `sum`?

5. ¿Cómo se comporta este código si `clientes` tiene filas duplicadas por `id`?

---

## Sección 14.2 — Dataset API: Tipos en Tiempo de Compilación

### Ejercicio 14.2.1 — DataFrame vs Dataset[T]: cuándo cada uno

```scala
import org.apache.spark.sql.{SparkSession, Dataset, DataFrame}

val spark = SparkSession.builder().getOrCreate()
import spark.implicits._

// DataFrame: columnas tipadas dinámicamente
// El schema se conoce en tiempo de ejecución, no de compilación
val df: DataFrame = spark.read.parquet("ventas.parquet")

// Esto COMPILA pero puede FALLAR en runtime:
val resultadoMalo = df.filter(col("monto_typo") > 100)
// Error en runtime: "AnalysisException: Column monto_typo not found"

// Dataset[T]: el schema es la case class — errores en tiempo de compilación
case class Venta(id: String, monto: Double, region: String)

val ds: Dataset[Venta] = spark.read.parquet("ventas.parquet").as[Venta]

// Esto FALLA EN COMPILACIÓN:
// val resultadoBueno = ds.filter(v => v.monto_typo > 100)
// Error: value monto_typo is not a member of Venta

// Esto COMPILA y es correcto:
val resultadoBueno: Dataset[Venta] = ds.filter(v => v.monto > 100)

// Transformaciones tipadas:
case class ResumenRegion(region: String, revenue: Double, numVentas: Long)

val resumen: Dataset[ResumenRegion] = ds
  .groupByKey(v => v.region)
  .agg(
    typed.sum[Venta](v => v.monto),
    typed.count[Venta](),
  )
  .map { case (region, revenue, count) =>
    ResumenRegion(region, revenue, count)
  }
```

**Preguntas:**

1. ¿El Dataset API es siempre más rápido que el DataFrame API?
   ¿Por qué puede ser más lento en algunos casos?

2. Si el Dataset API detecta errores en compilación, ¿por qué la mayoría
   de los proyectos de Spark en Scala usan DataFrame API?

3. ¿`ds.filter(v => v.monto > 100)` y `df.filter(col("monto") > 100)`
   generan el mismo plan físico en Spark?

4. ¿Puedes hacer `.as[Venta]` si el Parquet tiene columnas adicionales
   que no están en la case class `Venta`?

5. ¿El `Dataset[T]` elimina la necesidad de schema validation en los tests?

**Pista:** El Dataset API puede ser más lento que el DataFrame API porque
las lambdas de Scala (`v => v.monto > 100`) no son transparentes para el
optimizador de Catalyst — Spark no puede "ver dentro" de la función Scala
para optimizarla. El DataFrame API usa `Column` objects que Catalyst puede
analizar, reordenar, y optimizar (predicate pushdown, constant folding, etc.).
La regla práctica: usar DataFrame API para transformaciones que Catalyst puede
optimizar, Dataset API para lógica compleja que no cabe en SQL/Column expressions.

---

### Ejercicio 14.2.2 — Encoders: cómo Spark serializa tus tipos

```scala
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

// Un Encoder convierte entre objetos JVM (tus case classes) y
// la representación interna de Spark (InternalRow).

// Para case classes simples, el encoder se genera automáticamente:
case class Producto(id: String, nombre: String, precio: Double)
val encoder: Encoder[Producto] = Encoders.product[Producto]
// Generado automáticamente via macros de Scala

// Para tipos primitivos:
val encoderInt = Encoders.scalaInt
val encoderString = Encoders.STRING

// Inspeccionar el schema que Spark infiere de la case class:
println(encoder.schema)
// StructType(
//   StructField(id, StringType, true),
//   StructField(nombre, StringType, true),
//   StructField(precio, DoubleType, true)
// )

// Encoder personalizado para tipos que Spark no conoce:
// (ej: una clase de Java sin case class)
case class Wrapper(valor: java.time.Instant)
// java.time.Instant no tiene Encoder automático en Spark 3.x
// Necesitas mapear a un tipo que Spark conoce:
implicit val encoderInstant: Encoder[Wrapper] =
  Encoders.product[Wrapper]
  // o mapear a Long primero:
  // df.withColumn("valor", col("valor").cast(LongType))
  //   .as[Long].map(ms => Wrapper(java.time.Instant.ofEpochMilli(ms)))
```

**Preguntas:**

1. ¿El Encoder de Spark es equivalente a la serialización de Kryo en Spark RDD?
   ¿Cuál es más eficiente y por qué?

2. ¿Si una case class tiene un campo de tipo `Option[Double]`, cómo lo
   representa el schema de Spark?

3. ¿Los Encoders son generados en tiempo de compilación o en tiempo de ejecución?
   ¿Qué implica para el rendimiento?

4. ¿Qué pasa cuando haces `.as[MiCaseClass]` y el schema del Parquet
   tiene tipos incompatibles con la case class?

5. ¿Un Encoder personalizado para un tipo complejo (ej: un grafo) es factible?
   ¿Cómo lo implementarías?

---

### Ejercicio 14.2.3 — Typed transformations: map, flatMap, groupByKey

```scala
import org.apache.spark.sql.{Dataset, SparkSession}

val spark = SparkSession.builder().getOrCreate()
import spark.implicits._

case class Evento(userId: String, tipo: String, valor: Double, ts: Long)
case class Sesion(userId: String, numEventos: Int, valorTotal: Double)

val eventos: Dataset[Evento] = spark.read.parquet("eventos.parquet").as[Evento]

// map: transforma un Dataset[Evento] en Dataset[OtraCosa]
val valores: Dataset[Double] = eventos.map(e => e.valor)

// flatMap: un evento → múltiples elementos
val etiquetas: Dataset[String] = eventos.flatMap { e =>
  if (e.valor > 1000) List(e.userId, s"${e.userId}-premium")
  else List(e.userId)
}

// groupByKey + reduceGroups: agrupar y reducir de forma tipada
val sesiones: Dataset[(String, Double)] = eventos
  .filter(e => e.tipo == "compra")
  .groupByKey(e => e.userId)
  .reduceGroups((a, b) => a.copy(valor = a.valor + b.valor))
  .map { case (userId, eventoReducido) =>
    (userId, eventoReducido.valor)
  }

// mapGroups: acceso completo al grupo (todas las filas del grupo)
val sesionesDet: Dataset[Sesion] = eventos
  .groupByKey(e => e.userId)
  .mapGroups { (userId, grupoIter) =>
    val grupo = grupoIter.toSeq
    Sesion(userId, grupo.size, grupo.map(_.valor).sum)
  }
// Cuidado: mapGroups carga todo el grupo en memoria (OOM si el grupo es grande)

// flatMapGroups: como mapGroups pero puede emitir 0, 1, o N elementos por grupo
val alertas = eventos
  .groupByKey(e => e.userId)
  .flatMapGroups { (userId, grupoIter) =>
    val compras = grupoIter.filter(_.tipo == "compra").toSeq
    if (compras.size > 5 && compras.map(_.valor).sum > 5000)
      Seq(s"ALERTA: usuario $userId con ${compras.size} compras")
    else
      Seq.empty
  }
```

**Preguntas:**

1. ¿`mapGroups` carga todo el grupo en memoria de la JVM?
   ¿Qué pasa si un grupo tiene 10M filas?

2. ¿`groupByKey` en Dataset API hace shuffle? ¿Cuándo?

3. ¿`reduceGroups` es equivalente al combiner del Cap.03?
   ¿Puede aplicarse parcialmente antes del shuffle?

4. ¿`flatMapGroups` es equivalente al `flatMapGroupsWithState` de Spark SS?
   ¿Cuál es la diferencia?

5. ¿Para el mismo cómputo, `groupByKey.mapGroups` vs `groupBy.agg`
   en DataFrame API — cuál es más eficiente?

---

### Ejercicio 14.2.4 — Dataset y null safety: el problema de los nulls

```scala
// El sistema de tipos de Scala no puede eliminar los nulls de Spark completamente.
// Los nulls en Spark vienen del formato de datos (Parquet, JSON) —
// no del sistema de tipos de Scala.

case class Venta(id: String, userId: String, monto: Double, region: String)

// Si el Parquet tiene nulls en "region", esto compila pero puede fallar:
val ds = spark.read.parquet("ventas.parquet").as[Venta]
ds.filter(v => v.region.length > 3)
// NullPointerException si region es null — aunque String no es Option[String]

// Forma segura 1: Option en la case class
case class VentaSegura(id: String, userId: String, monto: Double, region: Option[String])

val dsSeguro = spark.read.parquet("ventas.parquet").as[VentaSegura]
dsSeguro.filter(v => v.region.exists(_.length > 3))  // no NPE

// Forma segura 2: usar DataFrame API para el filtro y Dataset para el resto
val dsLimpio = spark.read.parquet("ventas.parquet")
  .na.fill(Map("region" -> "desconocida"))  // rellenar nulls con DataFrame API
  .as[Venta]                                // ahora es seguro convertir
dsLimpio.filter(v => v.region.length > 3)  // sin riesgo de NPE

// Forma segura 3: validar schema antes de convertir
val schema = spark.read.parquet("ventas.parquet").schema
val camposNullables = schema.fields.filter(_.nullable).map(_.name)
println(s"Campos nullable: ${camposNullables.mkString(", ")}")
```

**Preguntas:**

1. ¿Por qué Spark permite nulls en campos `String` aunque Scala
   no tenga `String?` (nullable strings)?

2. ¿Usar `Option[String]` en la case class afecta el rendimiento
   comparado con `String`?

3. ¿Hay una forma de que el compilador de Scala garantice que
   un Dataset[T] no contiene nulls?

4. ¿El problema de nulls es más o menos severo en Dataset API
   vs DataFrame API?

---

### Ejercicio 14.2.5 — Leer: Dataset API en producción — un análisis honesto

**Tipo: Analizar**

Evaluar esta afirmación del arquitecto de un equipo:

> "Siempre usamos Dataset[T] en lugar de DataFrame porque detectamos
> los errores de schema en compilación. Esto nos ahorra horas de debugging."

Analizar con ejemplos concretos:

```scala
// Caso 1: el schema del archivo Parquet cambió en producción
// El archivo ahora tiene "amount" en lugar de "monto"
case class Venta(id: String, monto: Double)  // el código no cambió

val ds = spark.read.parquet("ventas_nuevo.parquet").as[Venta]
// ¿El compilador detecta esto? ¿O falla en runtime?

// Caso 2: el Parquet tiene una columna nueva "descuento" que no está en la case class
case class Venta2(id: String, monto: Double)

val ds2 = spark.read.parquet("ventas_con_descuento.parquet").as[Venta2]
// ¿Qué pasa con la columna "descuento"? ¿Error o silencio?

// Caso 3: transformación que el Dataset API hace más verbose
// Con DataFrame:
df.groupBy("region").agg(sum("monto").alias("revenue"), countDistinct("userId"))

// Con Dataset API (tipado):
ds.groupByKey(_.region)
  .agg(
    typed.sum[Venta](_.monto),
    typed.count[Venta](),
  )
  .toDF("region", "revenue", "count")
// ¿Vale la pena la verbosidad extra por la seguridad de tipos?
```

**Preguntas:**

1. ¿El Dataset API detecta cambios de nombre de columna en el Parquet
   en tiempo de compilación?

2. ¿La afirmación del arquitecto es correcta? ¿Exagerada?

3. ¿Dónde el Dataset API SÍ ayuda a detectar errores en compilación?

4. ¿El tradeoff verbosidad/seguridad del Dataset API vale la pena?
   ¿Para qué tipos de proyectos?

**Pista:** El Dataset API NO detecta cambios en el schema del archivo Parquet
en compilación — el Parquet se lee en runtime. Lo que detecta es errores
en las transformaciones sobre el tipo: `v.monto_typo` falla en compilación,
`col("monto_typo")` falla en runtime. Para detectar cambios de schema del archivo,
necesitas schema validation explícita — Dataset API o no.
La principal ventaja real del Dataset API: en código de transformación compleja,
el compilador detecta que accedes a un campo inexistente en la case class.

---

## Sección 14.3 — Implicits y Encoders: la Magia Detrás del Dataset

### Ejercicio 14.3.1 — Implicits de Scala: el mecanismo que hace funcionar `.as[T]`

```scala
// Los implicits de Scala 2 (given/using en Scala 3) permiten que el compilador
// pase argumentos automáticamente cuando están "en scope".

// Sin implicits (lo que pasaría si no existieran):
val ds = spark.read.parquet("ventas.parquet")
            .as[Venta](Encoders.product[Venta])  // pasar el Encoder explícitamente

// Con implicits (lo que realmente ocurre):
import spark.implicits._  // esto inyecta Encoders implícitos para case classes

val ds = spark.read.parquet("ventas.parquet").as[Venta]
// El compilador busca un Encoder[Venta] implícito en scope — lo encuentra via implicits._

// Definir tu propio implicit (para tipos no soportados por defecto):
case class Precio(centavos: Long) {
  def euros: Double = centavos / 100.0
}

// Sin esto, .as[Precio] falla con "could not find implicit value for parameter evidence$1: Encoder[Precio]"
implicit val encoderPrecio: Encoder[Precio] =
  Encoders.product[Precio]

val dsPrecio = spark.range(100).map(n => Precio(n * 99)).as[Precio]

// Scala 3 usa "given" en lugar de "implicit val":
// given encoderPrecio: Encoder[Precio] = Encoders.product[Precio]
```

**Preguntas:**

1. ¿`import spark.implicits._` importa Encoders para todos los tipos posibles
   o solo para los más comunes?

2. ¿Por qué el `import spark.implicits._` debe ir DESPUÉS de crear la SparkSession
   y no al principio del archivo?

3. ¿Los implicits de Scala son equivalentes a los decorators de Python
   o a algo diferente?

4. ¿Cómo el compilador decide qué implicit usar si hay dos en scope
   para el mismo tipo?

5. ¿Scala 3 (con `given`/`using`) resuelve los problemas de los implicits de Scala 2?

**Pista:** `import spark.implicits._` importa Encoders para tipos primitivos
(Int, String, Double, etc.) y para case classes que solo contienen tipos soportados.
No funciona para tipos arbitrarios de Java como `java.time.LocalDate` sin un
Encoder personalizado. Debe ir después de crear la SparkSession porque los
implicits son métodos de la instancia de SparkSession — no pueden importarse
antes de que exista la instancia.

---

### Ejercicio 14.3.2 — El sistema de tipos de Scala en el contexto de Spark

```scala
// Scala tiene un sistema de tipos más expresivo que Java o Python.
// En Spark, esto se usa para garantizar en compilación que ciertas operaciones son válidas.

// Tipos genéricos (type parameters):
def filtrarPorMonto[T <: { def monto: Double }](ds: Dataset[T], minMonto: Double): Dataset[T] =
  ds.filter(_.monto > minMonto)

// Type bounds: T debe tener un campo "monto: Double"
// Structural typing en Scala — similar a los Protocols de Python

// Tipos de función (Function types):
type TransformadorDF = DataFrame => DataFrame

def aplicarTransformaciones(df: DataFrame, pasos: List[TransformadorDF]): DataFrame =
  pasos.foldLeft(df)((dfActual, transformacion) => transformacion(dfActual))

// Uso:
val pipeline: List[TransformadorDF] = List(
  df => df.filter(col("monto") > 0),
  df => df.withColumn("monto_iva", col("monto") * 1.19),
  df => df.dropDuplicates("id"),
)
val resultado = aplicarTransformaciones(spark.read.parquet("ventas.parquet"), pipeline)

// Either: resultado que puede ser éxito o error
def leerVentas(ruta: String): Either[String, DataFrame] =
  try Right(spark.read.parquet(ruta))
  catch { case e: Exception => Left(s"Error leyendo $ruta: ${e.getMessage}") }

leerVentas("ventas.parquet") match {
  case Right(df) => df.show()
  case Left(error) => println(s"Error: $error")
}
```

**Preguntas:**

1. ¿El tipo `TransformadorDF = DataFrame => DataFrame` es equivalente
   al `Callable[[DataFrame], DataFrame]` de Python? ¿Con qué diferencias?

2. ¿El patrón `foldLeft(df)(transformacion)` para componer transformaciones
   es más expresivo que una función que aplica transformaciones en un loop?

3. ¿`Either[String, DataFrame]` es preferible a lanzar excepciones?
   ¿Cuándo cada enfoque es más apropiado?

4. ¿El structural typing de Scala (`{ def monto: Double }`) es equivalente
   a los Protocols de Python? ¿Es verificado en compilación?

---

### Ejercicio 14.3.3 — Traits: composición de comportamiento en Spark

```scala
// Los traits de Scala son como interfaces de Java pero con implementación.
// Se usan mucho en el código de Spark para definir comportamientos reutilizables.

// Trait para validación de DataFrames:
trait ValidadorSchema {
  def validar(df: DataFrame): Either[List[String], DataFrame] = {
    val errores = columnasFaltantes(df) ++ tiposIncorrectos(df)
    if (errores.isEmpty) Right(df) else Left(errores)
  }

  def columnasFaltantes(df: DataFrame): List[String]
  def tiposIncorrectos(df: DataFrame): List[String] = Nil  // implementación default
}

// Trait para caching:
trait CacheInteligente {
  def cacheConMetrica(df: DataFrame, nombre: String): DataFrame = {
    val cached = df.cache()
    val count = cached.count()  // materializar y medir
    println(s"Cacheado '$nombre': $count filas")
    cached
  }
}

// Combinar traits en una clase:
class PipelineVentas(spark: SparkSession)
    extends ValidadorSchema
    with CacheInteligente {

  override def columnasFaltantes(df: DataFrame): List[String] =
    List("id", "monto", "userId").filter(c => !df.columns.contains(c))

  def ejecutar(ruta: String): DataFrame = {
    val df = spark.read.parquet(ruta)
    validar(df) match {
      case Left(errores) => throw new IllegalStateException(errores.mkString(", "))
      case Right(dfValido) =>
        val dfCached = cacheConMetrica(dfValido, "ventas")
        dfCached.filter(col("monto") > 0)
    }
  }
}
```

**Preguntas:**

1. ¿Los traits de Scala son equivalentes a las clases abstractas de Python
   o a las ABC (Abstract Base Classes)?

2. ¿Puedes combinar traits que tienen el mismo método con implementaciones
   distintas? ¿Cómo resuelve Scala el conflicto?

3. ¿El patrón de traits para pipeline steps es mejor que una lista de funciones?
   ¿Para qué casos?

---

### Ejercicio 14.3.4 — Pattern matching avanzado: analizar el plan de Spark

```scala
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.expressions._

// Pattern matching sobre el plan lógico de Spark:
def analizarPlan(plan: LogicalPlan): String = plan match {
  case Project(exprs, child) =>
    s"Proyección de ${exprs.length} columnas sobre: ${analizarPlan(child)}"

  case Filter(cond, child) =>
    s"Filtro [${cond.sql}] sobre: ${analizarPlan(child)}"

  case Aggregate(groupBy, agg, child) =>
    s"Agregación por [${groupBy.map(_.sql).mkString(", ")}] sobre: ${analizarPlan(child)}"

  case Join(left, right, joinType, condition, _) =>
    s"Join ${joinType} entre (${analizarPlan(left)}) y (${analizarPlan(right)})"
    + condition.map(c => s" ON ${c.sql}").getOrElse("")

  case Relation(table, _) =>
    s"Tabla: ${table}"

  case other =>
    s"Nodo desconocido: ${other.getClass.getSimpleName}"
}

// Usar:
val df = spark.read.parquet("ventas.parquet")
  .filter(col("monto") > 100)
  .groupBy("region")
  .agg(sum("monto"))

println(analizarPlan(df.queryExecution.analyzed))
```

**Preguntas:**

1. ¿Esta función es equivalente al `explain()` de Spark?
   ¿Qué información adicional da?

2. ¿Las reglas de optimización de Catalyst también usan pattern matching?
   Describe cómo funciona una regla simple.

3. ¿Desde PySpark, puedes acceder al plan lógico con el mismo nivel de
   detalle que desde Scala?

---

### Ejercicio 14.3.5 — Implementar: una regla de optimización personalizada en Scala

```scala
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.expressions._

// Una regla de optimización personalizada para Spark Catalyst.
// Este ejemplo: "constant folding" para expresiones de negocio específicas.
// Si hay una expresión `monto * 1.0`, reemplazarla por `monto` (multiplicar por 1 es no-op).

object EliminarMultiplicacionPorUno extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformAllExpressions {
    case Multiply(left, Literal(1.0, _)) => left   // expr * 1.0 → expr
    case Multiply(Literal(1.0, _), right) => right  // 1.0 * expr → expr
  }
}

// Registrar la regla en el optimizer de Spark:
spark.experimental.extraOptimizations = Seq(EliminarMultiplicacionPorUno)

// Verificar que funciona:
val df = spark.range(100)
  .withColumn("valor", col("id") * 1.0 * 2.5)  // 1.0 debería eliminarse

println("Plan ANTES de la regla:")
df.queryExecution.optimizedPlan.prettyJson

// Si la regla funciona, el plan debería mostrar col("id") * 2.5
// en lugar de col("id") * 1.0 * 2.5
```

**Restricciones:**
1. Implementar la regla y verificar que elimina la multiplicación por 1
2. Implementar una segunda regla: `monto + 0` → `monto`
3. ¿Cómo testeas que la regla se aplica correctamente?
4. ¿Esta regla es más eficiente que la misma optimización en el nivel SQL?

---

## Sección 14.4 — UDFs en Scala: Sin el Overhead de Python

### Ejercicio 14.4.1 — UDFs Scala vs UDFs Python: la diferencia de rendimiento

```scala
import org.apache.spark.sql.functions.udf

// UDF en Scala: sin el overhead de cruce JVM-Python
// La función lambda se ejecuta directamente en la JVM
val categorizarMonto = udf((monto: Double, region: String) => {
  val umbral = if (region == "norte") 500.0 else 300.0
  if (monto > umbral) "alto" else if (monto > umbral / 2) "medio" else "bajo"
})

val df = spark.read.parquet("ventas.parquet")
val resultado = df.withColumn(
  "categoria",
  categorizarMonto(col("monto"), col("region"))
)

// Diferencia de rendimiento vs Python UDF:
// Python UDF: JVM → pickle fila → Python → unpickle → Python eval → pickle resultado → JVM
// Scala UDF:  JVM → desreferenciar → ejecutar lambda → resultado (todo en JVM)
// La diferencia puede ser 10-50× según la complejidad de la función

// Nota: las UDFs Scala tampoco son transparentes para Catalyst
// (el optimizador no puede ver "dentro" de la lambda)
// Para predicados simples, es mejor usar Column expressions:
val resultadoNativo = df.withColumn(
  "categoria",
  when(col("region") === "norte" && col("monto") > 500, "alto")
  .when(col("region") === "norte" && col("monto") > 250, "medio")
  .when(col("region") =!= "norte" && col("monto") > 300, "alto")
  .when(col("region") =!= "norte" && col("monto") > 150, "medio")
  .otherwise("bajo")
)
// Esto SÍ puede ser optimizado por Catalyst (predicate pushdown, etc.)
```

**Preguntas:**

1. ¿Las UDFs Scala son más rápidas que las Pandas UDFs de Python?
   ¿Y que las Arrow UDFs?

2. ¿Las UDFs Scala pueden acceder al estado entre filas?
   (ej: un contador acumulado)

3. ¿Una UDF Scala que llama a código Rust via JNI es posible?
   ¿Cuándo tiene sentido?

4. ¿El plan de `explain()` muestra las UDFs Scala de la misma forma
   que las UDFs Python?

---

### Ejercicio 14.4.2 — UDAF en Scala: funciones de agregación personalizadas

```scala
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.{Encoder, Encoders, Row}
import org.apache.spark.sql.types._

// UDAF con la API de Aggregator (tipada, Spark 3.0+):
case class Buffer(suma: Double, count: Long)

object MediaGeometrica extends Aggregator[Double, Buffer, Double] {
  // Valor inicial del buffer:
  override def zero: Buffer = Buffer(0.0, 0L)

  // Añadir un elemento al buffer:
  override def reduce(buffer: Buffer, entrada: Double): Buffer =
    if (entrada > 0)
      Buffer(buffer.suma + Math.log(entrada), buffer.count + 1)
    else
      buffer  // ignorar valores no positivos

  // Combinar dos buffers (para el merge después del shuffle):
  override def merge(b1: Buffer, b2: Buffer): Buffer =
    Buffer(b1.suma + b2.suma, b1.count + b2.count)

  // Calcular el resultado final:
  override def finish(buffer: Buffer): Double =
    if (buffer.count > 0) Math.exp(buffer.suma / buffer.count)
    else 0.0

  // Encoders para el buffer y el resultado:
  override def bufferEncoder: Encoder[Buffer] = Encoders.product[Buffer]
  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

// Registrar y usar:
val mediaGeometrica = udaf(MediaGeometrica)
val resultado = df.groupBy("region")
  .agg(mediaGeometrica(col("monto")).alias("media_geometrica"))
```

**Preguntas:**

1. ¿El método `merge()` en el UDAF de Scala es el combiner del Cap.03?
   ¿Cuándo Spark lo llama?

2. ¿La media geométrica puede implementarse con funciones nativas de Spark?
   ¿Cuál sería la implementación?

3. ¿Un UDAF en Scala es más eficiente que un Pandas UDF de agregación en Python?

4. ¿Cómo testearías este UDAF de forma unitaria sin SparkSession?

---

### Ejercicio 14.4.3 — Higher-order functions en Spark SQL: evitar UDFs para arrays

```scala
// Spark 2.4+ tiene funciones de orden superior nativas para arrays y maps.
// Son más eficientes que UDFs porque son visibles para Catalyst.

import org.apache.spark.sql.functions._

val df = spark.createDataFrame(Seq(
  (1, Array(1.0, 2.0, 3.0, -1.0, 5.0)),
  (2, Array(10.0, -5.0, 20.0)),
)).toDF("id", "valores")

// transform: aplicar una función a cada elemento del array
val dobles = df.withColumn(
  "valores_dobles",
  transform(col("valores"), v => v * 2)
)

// filter: filtrar elementos del array
val soloPositivos = df.withColumn(
  "solo_positivos",
  filter(col("valores"), v => v > 0)
)

// aggregate: reducir el array a un valor
val suma = df.withColumn(
  "suma",
  aggregate(col("valores"), lit(0.0), (acc, v) => acc + v)
)

// exists y forall:
val tieneNegativos = df.withColumn(
  "tiene_negativos",
  exists(col("valores"), v => v < 0)
)
```

**Preguntas:**

1. ¿`transform()` de Spark SQL es más rápido que `.withColumn()` +
   `explode()` + transformación + `collect_list()`?

2. ¿Las higher-order functions de Spark son visibles para el optimizer?
   ¿Puede Catalyst optimizarlas?

3. ¿Cuándo necesitas una UDF en lugar de `transform`/`filter`/`aggregate`?

---

### Ejercicio 14.4.4 — Acceder a APIs privadas de Spark desde Scala

```scala
// Spark expone ciertas APIs solo desde Scala/Java (no via PySpark).
// Algunas son experimentales o internas — pero útiles en producción.

import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

// Acceder al plan de ejecución detallado:
val df = spark.read.parquet("ventas.parquet").filter(col("monto") > 100)
val qe: QueryExecution = df.queryExecution

// Plan lógico analizado (con tipos resueltos):
val planAnalizado: LogicalPlan = qe.analyzed

// Plan lógico optimizado (después de Catalyst):
val planOptimizado: LogicalPlan = qe.optimizedPlan

// Plan físico:
val planFisico = qe.executedPlan

// Estadísticas del plan:
println(planOptimizado.stats.sizeInBytes)

// Acceder al RDD subyacente del DataFrame:
val rdd = df.rdd  // disponible en PySpark también, pero con más overhead

// Acceder a las métricas de ejecución (TaskMetrics):
spark.sparkContext.addSparkListener(new SparkListener {
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val metrics = taskEnd.taskMetrics
    println(s"Bytes leídos: ${metrics.inputMetrics.bytesRead}")
    println(s"Registros leídos: ${metrics.inputMetrics.recordsRead}")
  }
})
```

**Preguntas:**

1. ¿`queryExecution.analyzed` vs `queryExecution.optimizedPlan`:
   cuál es la diferencia concreta?

2. ¿`planOptimizado.stats.sizeInBytes` es exacto o una estimación?
   ¿De dónde vienen esas estadísticas?

3. ¿Puedes acceder a `queryExecution` desde PySpark?
   ¿Con qué limitaciones?

4. ¿El SparkListener de Scala tiene equivalente en PySpark?

---

### Ejercicio 14.4.5 — Leer: cuándo una UDF Scala supera a las expresiones nativas

**Tipo: Analizar**

Para cada caso, evaluar si una UDF Scala es apropiada o si hay una solución
con expresiones nativas de Spark:

```scala
// Caso 1: Parsear un string con formato propietario
// Formato: "REGION:SEGMENTO:CODIGO" → extraer los tres campos
// UDF:
val parsearCodigo = udf((codigo: String) => {
  val partes = codigo.split(":")
  if (partes.length == 3) (partes(0), partes(1), partes(2))
  else ("", "", "")
})

// ¿Hay alternativa nativa de Spark?

// Caso 2: Aplicar una regla de negocio compleja
// El descuento depende de la región, segmento, día de semana, y monto
// con una tabla de lookup de 200 combinaciones
val descuento = udf((region: String, segmento: String, 
                     diaSemana: Int, monto: Double) => {
  tablaDescuentos.getOrElse((region, segmento, diaSemana), 0.0) * monto
})
// tablaDescuentos es un Map[(...), Double] en el driver

// Caso 3: Calcular la distancia entre dos puntos geográficos
val distanciaKm = udf((lat1: Double, lon1: Double, 
                        lat2: Double, lon2: Double) => {
  val R = 6371.0  // radio de la Tierra en km
  val dLat = Math.toRadians(lat2 - lat1)
  val dLon = Math.toRadians(lon2 - lon1)
  val a = Math.sin(dLat/2) * Math.sin(dLat/2) +
          Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
          Math.sin(dLon/2) * Math.sin(dLon/2)
  2 * R * Math.asin(Math.sqrt(a))
})
```

---

## Sección 14.5 — Spark Internals desde Scala: Acceso a APIs No Expuestas

### Ejercicio 14.5.1 — Catalyst: el optimizer en detalle

```scala
// Catalyst es el optimizer de Spark SQL.
// Está implementado en Scala y usa pattern matching extensivamente.

// Las fases de Catalyst:
// 1. Analysis: resolver nombres y tipos
// 2. Logical optimization: aplicar reglas de optimización lógica
// 3. Physical planning: elegir algoritmos físicos (BroadcastHashJoin vs SortMergeJoin)
// 4. Code generation: generar bytecode JVM específico para la query

// Ver qué reglas de optimización se aplicaron:
val df = spark.read.parquet("ventas.parquet")
  .filter(col("monto") > 100)
  .filter(col("region") === "norte")  // dos filtros separados

// El optimizer debería combinarlos en un solo filtro:
println(df.queryExecution.optimizedPlan)
// Debería mostrar: Filter((monto > 100) AND (region = norte), ...)
// en lugar de dos Filter anidados

// Ver las reglas aplicadas (requiere logging de Spark):
// spark.conf.set("spark.sql.optimizer.excludedRules", "...")
// Para añadir reglas: spark.experimental.extraOptimizations = Seq(...)
```

**Preguntas:**

1. ¿El optimizer de Catalyst es determinista?
   ¿La misma query siempre produce el mismo plan optimizado?

2. ¿Cuándo NO combina Catalyst dos filtros consecutivos?

3. ¿Puedes desactivar una regla de optimización específica?
   ¿Cuándo querrías hacerlo?

4. ¿La generación de código (Tungsten) es transparente para el usuario?
   ¿O hay que activarla explícitamente?

---

### Ejercicio 14.5.2 — Tungsten: ejecución eficiente en la JVM

```scala
// Tungsten es el motor de ejecución física de Spark.
// Mejoras sobre la ejecución "normal" de JVM:
// 1. Gestión de memoria off-heap (evita GC para datos)
// 2. Generación de código (bytecode JVM específico para la query)
// 3. Caché-consciente: organiza los datos para aprovechar el cache de CPU

// Verificar que Tungsten está activo:
spark.conf.get("spark.sql.tungsten.enabled")  // "true" por defecto

// Whole-stage code generation: un solo stage de código para múltiples operadores
spark.conf.set("spark.sql.codegen.wholeStage", "true")

// Ver el código generado (para debugging):
val df = spark.range(1000)
  .filter(col("id") > 500)
  .select(col("id") * 2)

df.queryExecution.debug.codegen()
// Imprime el código Java/Scala generado para esta query
```

**Preguntas:**

1. ¿La gestión de memoria off-heap de Tungsten requiere configuración adicional?

2. ¿El código generado por Tungsten es mejor que código Scala equivalente
   escrito a mano? ¿Por qué?

3. ¿Tungsten afecta a las UDFs Python? ¿O solo al código JVM?

4. ¿Cuándo desactivarías Tungsten? ¿Hay casos donde es contraproducente?

---

### Ejercicio 14.5.3 — Implementar una fuente de datos personalizada en Scala

```scala
import org.apache.spark.sql.sources._
import org.apache.spark.sql.{DataFrame, SQLContext}

// Una fuente de datos personalizada para Spark SQL.
// Permite usar `spark.read.format("mi-formato").load("ruta")`.

class MiFuenteDeDatos extends RelationProvider with SchemaRelationProvider {

  override def createRelation(
    sqlContext: SQLContext,
    parameters: Map[String, String],
  ): BaseRelation = {
    val ruta = parameters.getOrElse("path", throw new IllegalArgumentException("path es requerido"))
    new MiRelacion(ruta)(sqlContext)
  }

  override def createRelation(
    sqlContext: SQLContext,
    parameters: Map[String, String],
    schema: StructType,
  ): BaseRelation = {
    val ruta = parameters("path")
    new MiRelacion(ruta, Some(schema))(sqlContext)
  }
}

class MiRelacion(ruta: String, userSchema: Option[StructType] = None)
    (@transient val sqlContext: SQLContext)
    extends BaseRelation with TableScan {

  override def schema: StructType = userSchema.getOrElse(
    StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("valor", DoubleType, nullable = true),
    ))
  )

  override def buildScan(): RDD[Row] = {
    // Leer los datos en el formato propietario:
    sqlContext.sparkContext.textFile(ruta)
      .map(linea => {
        val partes = linea.split(",")
        Row(partes(0), partes(1).toDouble)
      })
  }
}

// Registrar y usar:
spark.read.format("com.mi.empresa.MiFuenteDeDatos")
  .option("path", "datos/archivo.csv")
  .load()
  .show()
```

**Preguntas:**

1. ¿Esta fuente de datos soporta predicate pushdown?
   ¿Cómo añadirías soporte para `PrunedFilteredScan`?

2. ¿`buildScan()` devuelve un `RDD[Row]` — cuándo se ejecuta este código?

3. ¿Las fuentes de datos personalizadas en Scala pueden usarse desde PySpark?

4. ¿Esta implementación escala para datasets de 100 GB en S3?
   ¿Qué cambiarías?

---

### Ejercicio 14.5.4 — Acceder a métricas de ejecución: SparkListener

```scala
import org.apache.spark.scheduler._
import scala.collection.mutable

// Un SparkListener personalizado para recolectar métricas de ejecución.
// Más detallado que las métricas de Spark UI.

class MetricasPersonalizadas extends SparkListener {
  private val metricasPorStage = mutable.Map[Int, StageMetrics]()

  case class StageMetrics(
    stageId: Int,
    nombre: String,
    duracionMs: Long,
    bytesLeidos: Long,
    registrosLeidos: Long,
    bytesEscritos: Long,
    tiempoCPUMs: Long,
  )

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val info = stageCompleted.stageInfo
    val taskMetrics = info.taskMetrics

    metricasPorStage(info.stageId) = StageMetrics(
      stageId = info.stageId,
      nombre = info.name,
      duracionMs = info.completionTime.getOrElse(0L) - info.submissionTime.getOrElse(0L),
      bytesLeidos = taskMetrics.inputMetrics.bytesRead,
      registrosLeidos = taskMetrics.inputMetrics.recordsRead,
      bytesEscritos = taskMetrics.outputMetrics.bytesWritten,
      tiempoCPUMs = taskMetrics.executorCpuTime / 1_000_000,
    )
  }

  def resumen(): String = metricasPorStage.values
    .toSeq
    .sortBy(_.stageId)
    .map(m => s"Stage ${m.stageId} (${m.nombre}): ${m.duracionMs}ms, " +
              s"${m.bytesLeidos/1e6:.1f}MB leídos, CPU: ${m.tiempoCPUMs}ms")
    .mkString("\n")
}

// Usar:
val metricas = new MetricasPersonalizadas()
spark.sparkContext.addSparkListener(metricas)

// Ejecutar el job...
spark.read.parquet("ventas.parquet")
  .filter(col("monto") > 100)
  .groupBy("region").agg(sum("monto"))
  .count()

println(metricas.resumen())
```

**Restricciones:**
1. Implementar el listener completo con métricas de shuffle
2. ¿Puedes implementar un listener equivalente en PySpark?
   ¿Con qué limitaciones?
3. Exportar las métricas a Prometheus o StatsD

---

### Ejercicio 14.5.5 — Leer: APIs de Scala que PySpark no expone

**Tipo: Investigar**

Identificar APIs de Spark disponibles en Scala pero NO en PySpark:

```
1. Dataset[T] tipado
   - ¿Existe un equivalente en PySpark?
   - ¿Es una limitación fundamental o podría añadirse?

2. SparkListener programático
   - ¿PySpark expone SparkListeners?
   - ¿Cómo monitorizarías lo mismo desde Python?

3. Fuentes de datos personalizadas (RelationProvider)
   - ¿PySpark puede definir fuentes de datos custom?
   - ¿Qué usa PySpark cuando necesitas una fuente custom?

4. Reglas de optimización de Catalyst
   - ¿PySpark puede añadir reglas custom a Catalyst?
   - ¿Cuándo es necesario?

5. Acceso directo a InternalRow (representación interna de Spark)
   - ¿Por qué es más eficiente que Row?
   - ¿Está accesible desde PySpark?
```

---

## Sección 14.6 — Testing de Jobs Spark en Scala

### Ejercicio 14.6.1 — SparkSession para testing: SharedSparkSession

```scala
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

// Trait para compartir SparkSession entre tests (evita iniciar una nueva por test):
trait SharedSparkSession extends BeforeAndAfterAll { self: Suite =>

  @transient private var _spark: SparkSession = _

  implicit def spark: SparkSession = _spark

  override def beforeAll(): Unit = {
    super.beforeAll()
    _spark = SparkSession.builder()
      .master("local[2]")  // 2 threads para tests
      .appName(this.getClass.getSimpleName)
      .config("spark.sql.shuffle.partitions", "4")  // menos particiones para tests
      .config("spark.ui.enabled", "false")           // desactivar UI en tests
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    try _spark.stop()
    finally super.afterAll()
  }
}

// Test que usa la SparkSession compartida:
class PipelineVentasTest extends SharedSparkSession {
  import spark.implicits._

  test("calcular_revenue agrupa por región correctamente") {
    val ventasTest = Seq(
      ("t1", "alice", 100.0, "norte"),
      ("t2", "bob",   200.0, "sur"),
      ("t3", "carol", 150.0, "norte"),
    ).toDF("id", "userId", "monto", "region")

    val resultado = calcularRevenue(ventasTest)

    val revenueNorte = resultado.filter($"region" === "norte")
      .select("revenue").as[Double].head()

    assert(revenueNorte == 250.0, s"Revenue norte esperado 250.0, obtenido $revenueNorte")
  }
}
```

**Preguntas:**

1. ¿Por qué usar `local[2]` en lugar de `local[*]` para tests?
   ¿Qué problema resuelve?

2. ¿`spark.sql.shuffle.partitions = 4` en tests puede causar que
   un test pase pero el job falle en producción con 200 particiones?

3. ¿`@transient` en `_spark` es necesario? ¿Para qué sirve?

4. ¿Los tests de Spark son lentos por definición?
   ¿Cuánto tiempo debería tardar un test unitario de Spark?

---

### Ejercicio 14.6.2 — Property-based testing para transformaciones de Spark

```scala
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll

// Property-based testing: generar datos aleatorios y verificar propiedades.
// Más robusto que ejemplos manuales para encontrar edge cases.

class PropiedadesPipeline extends SharedSparkSession with ScalaCheckSuite {
  import spark.implicits._

  // Generador de ventas aleatorias:
  val genVenta = for {
    id     <- Gen.uuid.map(_.toString)
    userId <- Gen.alphaNumStr.suchThat(_.nonEmpty)
    monto  <- Gen.choose(0.01, 100000.0)
    region <- Gen.oneOf("norte", "sur", "este", "oeste")
  } yield (id, userId, monto, region)

  property("el revenue total siempre es positivo") {
    forAll(Gen.listOf(genVenta).suchThat(_.nonEmpty)) { ventas =>
      val df = ventas.toDF("id", "userId", "monto", "region")
      val resultado = calcularRevenue(df)
      resultado.filter($"revenue" < 0).count() == 0
    }
  }

  property("el número de regiones en el resultado ≤ número de regiones en el input") {
    forAll(Gen.listOf(genVenta)) { ventas =>
      val df = ventas.toDF("id", "userId", "monto", "region")
      val regionesInput = df.select("region").distinct().count()
      val regionesResultado = calcularRevenue(df).count()
      regionesResultado <= regionesInput
    }
  }

  property("el revenue total agregado = suma de todos los montos") {
    forAll(Gen.listOf(genVenta).suchThat(_.nonEmpty)) { ventas =>
      val df = ventas.toDF("id", "userId", "monto", "region")
      val sumaTotal = df.agg(sum("monto")).as[Double].head()
      val revenueTotalAgregado = calcularRevenue(df).agg(sum("revenue")).as[Double].head()
      Math.abs(sumaTotal - revenueTotalAgregado) < 0.001  // tolerancia de punto flotante
    }
  }
}
```

**Preguntas:**

1. ¿Property-based testing es práctico para jobs de Spark dado que
   cada test puede tardar segundos?

2. ¿`Gen.listOf` puede generar listas vacías? ¿Cómo maneja el pipeline
   un DataFrame vacío?

3. ¿Qué propiedades son más valiosas de testear en un pipeline de datos?
   (invariantes de datos, idempotencia, monotonía, etc.)

---

### Ejercicio 14.6.3 — Schema contracts: testear que el schema no cambia

```scala
// Un test de schema contract garantiza que el output del pipeline
// siempre tiene el schema esperado — independientemente de los datos de entrada.

import org.apache.spark.sql.types._

object SchemaContratos {
  val SCHEMA_VENTAS_PROCESADAS = StructType(Seq(
    StructField("id",          StringType,    nullable = false),
    StructField("revenue",     DoubleType,    nullable = false),
    StructField("region",      StringType,    nullable = false),
    StructField("timestamp",   TimestampType, nullable = false),
    StructField("es_premium",  BooleanType,   nullable = false),
  ))
}

class SchemaContractTest extends SharedSparkSession {

  test("pipeline produce el schema correcto") {
    val datosTest = spark.createDataFrame(Seq(
      ("t1", "alice", 100.0, "norte"),
    )).toDF("id", "userId", "monto", "region")

    val resultado = miPipeline(datosTest)

    // Verificar que cada campo del contrato está presente con el tipo correcto:
    SchemaContratos.SCHEMA_VENTAS_PROCESADAS.fields.foreach { campoEsperado =>
      val campoActual = resultado.schema.fields.find(_.name == campoEsperado.name)
      assert(campoActual.isDefined, s"Columna '${campoEsperado.name}' no encontrada en el resultado")
      assert(
        campoActual.get.dataType == campoEsperado.dataType,
        s"Columna '${campoEsperado.name}': tipo esperado ${campoEsperado.dataType}, " +
        s"obtenido ${campoActual.get.dataType}"
      )
    }
  }
}
```

**Restricciones:**
1. Implementar el schema contract test completo
2. ¿Cómo lo integras en el CI/CD del proyecto?
3. ¿Los schema contracts son más valiosos en Scala o en Python? ¿Por qué?

---

### Ejercicio 14.6.4 — Testear sin SparkSession: lógica pura de Scala

```scala
// El mejor código de pipeline es el que puede testearse sin SparkSession.
// Extraer la lógica pura a funciones de Scala — testearlas sin Spark.

// En lugar de:
def categorizarMonto(df: DataFrame): DataFrame =
  df.withColumn("categoria",
    when(col("monto") > 1000, "alto")
    .when(col("monto") > 100, "medio")
    .otherwise("bajo")
  )

// Extraer la lógica pura:
def categoriaDeMonto(monto: Double): String =
  if (monto > 1000) "alto"
  else if (monto > 100) "medio"
  else "bajo"

// Testear sin Spark:
class LogicaPuraTest {
  test("categoría alta para montos > 1000") {
    assert(categoriaDeMonto(1500.0) == "alto")
    assert(categoriaDeMonto(1001.0) == "alto")
    assert(categoriaDeMonto(1000.0) != "alto")
  }

  test("categoría baja para montos <= 100") {
    assert(categoriaDeMonto(100.0) == "bajo")
    assert(categoriaDeMonto(0.0) == "bajo")
    assert(categoriaDeMonto(-50.0) == "bajo")
  }
}

// El DataFrame wrapper solo llama a la función pura:
def categorizarMonto(df: DataFrame): DataFrame =
  df.withColumn("categoria", udf(categoriaDeMonto _)(col("monto")))
```

**Preguntas:**

1. ¿Esta separación entre lógica pura y código Spark es siempre posible?
   ¿Cuándo no?

2. ¿Los tests de lógica pura en Scala son significativamente más rápidos
   que los tests con SparkSession?

3. ¿Cómo defines los límites entre "lógica pura" y "código Spark"?

---

### Ejercicio 14.6.5 — Leer: la pirámide de tests para pipelines de datos

**Tipo: Diseñar**

Diseñar la estrategia de testing para un pipeline de datos en Scala:

```
Unit tests (base de la pirámide):
  - Lógica pura (sin Spark): rápidos, muchos
  - Schema validation: verificar contratos de datos

Integration tests (medio):
  - Tests con SparkSession local: más lentos, menos
  - Verificar que las transformaciones producen resultados correctos

End-to-end tests (cima):
  - Tests con datos reales (o realistas): lentos, pocos
  - Verificar el pipeline completo de extremo a extremo

Preguntas:
1. ¿Qué proporción de cada tipo es razonable para un pipeline de datos?
2. ¿Cómo distingues entre "lógica que pertenece en unit test" y
   "lógica que necesita integration test"?
3. ¿Los tests de schema contract son unit tests o integration tests?
4. ¿Cómo testeas que un pipeline produce los mismos resultados
   después de una refactorización mayor?
```

---

## Sección 14.7 — Cuándo Scala y Cuándo Python: la Decisión Final

### Ejercicio 14.7.1 — El árbol de decisión: Scala vs Python para Spark

```
¿Tu equipo ya sabe Scala?
  No → ¿el overhead de Python importa para este job?
    No → usar Python (menor curva de aprendizaje, mayor productividad inicial)
    Sí → evaluar: ¿cuánto importa? ¿1.5× o 10×?
      1.5× → Python con Pandas UDFs (mitiga la mayoría del overhead)
      10× → considerar Scala (UDFs, lógica compleja que Catalyst no puede optimizar)

¿Necesitas APIs no expuestas en PySpark?
  Sí → Scala (SparkListener, reglas de Catalyst, fuentes de datos custom)
  No → Python es suficiente

¿El job tiene lógica compleja que no cabe en SQL/Column expressions?
  Sí → ¿cuánta?
    Poca → Pandas UDFs en Python (batch vectorizado, poco overhead)
    Mucha → Scala UDFs (sin boundary JVM-Python)

¿El equipo escribe tests extensivos y valora los tipos estáticos?
  Sí → Scala Dataset API (errores en compilación, schema validado)
  No → Python con schema validation explícita (más flexible, menos garantías)
```

**Preguntas:**

1. ¿Existe un tercer camino: PySpark + Spark Connect (Spark 3.4+)?
   ¿Cómo cambia el tradeoff?

2. ¿Un equipo debería usar ambos (Scala y Python) en el mismo proyecto?
   ¿Cuándo tiene sentido?

3. ¿La tendencia en la industria es hacia más Python o más Scala?
   ¿Qué factores lo determinan?

---

### Ejercicio 14.7.2 — Benchmark: Python vs Scala para un job real

```scala
// El mismo pipeline en Scala y Python — medir la diferencia de rendimiento

// Scala:
val resultadoScala = spark.read.parquet("ventas_10gb.parquet")
  .filter(col("monto") > 100 && col("tipo") === "compra")
  .groupBy("region", "mes")
  .agg(
    sum("monto").alias("revenue"),
    countDistinct("userId").alias("usuariosUnicos"),
    avg("monto").alias("ticketPromedio"),
  )
  .orderBy(desc("revenue"))
```

```python
# Python equivalente:
resultado_python = spark.read.parquet("ventas_10gb.parquet") \
    .filter((F.col("monto") > 100) & (F.col("tipo") == "compra")) \
    .groupBy("region", "mes") \
    .agg(
        F.sum("monto").alias("revenue"),
        F.countDistinct("userId").alias("usuariosUnicos"),
        F.avg("monto").alias("ticketPromedio"),
    ) \
    .orderBy(F.desc("revenue"))
```

**Preguntas:**

1. Para este pipeline específico (sin UDFs), ¿cuánto más rápido es Scala?
   ¿Por qué?

2. ¿El plan físico generado es el mismo para ambas versiones?

3. ¿Añadir una UDF Python compleja cambia significativamente el resultado
   del benchmark? ¿En qué medida?

4. ¿El startup time del job (iniciar SparkSession, leer el Parquet) es el mismo?

---

### Ejercicio 14.7.3 — Scala en el stack de datos de 2024

```
El estado actual (2024):

  Scala dominaba (2015-2020):
  - Spark 1.x y 2.x tenían mejor soporte nativo en Scala
  - PySpark era más limitado y menos maduro
  - Los datos scientists que usaban Spark preferían Scala

  Python tomó el liderazgo (2020+):
  - PySpark 3.x está en paridad de features con Scala para la mayoría de casos
  - Pandas UDFs y Arrow redujeron el overhead de Python
  - El ecosistema PyData (ML, datos, visualización) es incomparable
  - La mayoría de los nuevos proyectos eligen Python

  Scala sigue relevante para (2024):
  - Proyectos que requieren APIs internas de Spark
  - Equipos con experiencia Scala existente
  - Jobs con mucha lógica de transformación no expresable en SQL
  - Testing riguroso con tipos estáticos
```

**Preguntas:**

1. ¿Apache Spark continuará siendo relevante con el auge de DuckDB, Polars,
   y DataFusion para análisis de datos?

2. ¿Databricks ha influido en la dirección de Python vs Scala en Spark?
   (Databricks es el principal contribuidor de Spark y tiene incentivos propios)

3. ¿Aprender Scala en 2024 para data engineering es una inversión con buen ROI?

---

### Ejercicio 14.7.4 — Integrar Scala y Python en el mismo proyecto

```scala
// Patrón: el core lógico en Scala, la orquestación en Python

// jar/core-logic.jar (Scala):
// - Transformaciones complejas
// - UDFs de rendimiento crítico
// - Reglas de optimización custom
// - Tests unitarios y de integración

// pipeline/orchestration.py (Python):
// - Leer datos desde fuentes externas
// - Llamar al JAR de Scala via spark-submit
// - Gestionar dependencias de Airflow
// - Notificaciones y alertas

// Llamar al JAR de Scala desde Python:
import subprocess

def ejecutar_job_scala(
    jar_path: str,
    clase_principal: str,
    args: list[str],
    spark_conf: dict[str, str] = None,
) -> int:
    """Ejecutar un job Scala desde Python."""
    cmd = ["spark-submit",
           "--class", clase_principal,
           "--master", "yarn",
           "--deploy-mode", "cluster"]

    for k, v in (spark_conf or {}).items():
        cmd += ["--conf", f"{k}={v}"]

    cmd += [jar_path] + args

    proceso = subprocess.run(cmd, capture_output=True, text=True)
    if proceso.returncode != 0:
        raise RuntimeError(f"Job Scala falló: {proceso.stderr}")
    return proceso.returncode
```

**Preguntas:**

1. ¿Este patrón (orquestación Python + lógica Scala) es mantenible a largo plazo?
   ¿Qué problemas crea?

2. ¿Hay una forma más integrada de combinar Scala y Python en el mismo job?
   (ej: PySpark llamando a código Scala via Py4J directamente)

3. ¿Cuándo un equipo debería migrar de "todo en Python" a "híbrido Scala+Python"?

---

### Ejercicio 14.7.5 — El sistema de e-commerce en Scala: refactorizar el pipeline

**Tipo: Implementar**

Tomar el pipeline del sistema de e-commerce que desarrollaste en Python
(Cap.13, Ejercicio 13.7.4) y reimplementar la parte de lógica de negocio
más compleja en Scala:

```scala
package com.ecommerce.pipeline

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object MetricasEcommerce {

  case class ConfigMetricas(
    rutaEventos: String,
    rutaClientes: String,
    rutaSalida: String,
    fecha: String,
    minMonto: Double = 0.0,
  )

  def calcularRevenuePorSegmento(
    ventas: DataFrame,
    clientes: DataFrame,
  ): DataFrame = {
    // TODO: implementar
    // - Join ventas con clientes
    // - Calcular revenue por segmento y región
    // - Incluir: revenue, usuarios únicos, ticket promedio, % revenue premium
    ???
  }

  def detectarClientesPremium(
    ventas: DataFrame,
    umbralGasto: Double = 10000.0,
  ): DataFrame = {
    // TODO: implementar
    // - Calcular gasto acumulado por usuario con window function
    // - Marcar como premium si gasto acumulado > umbral
    // - Emitir solo el momento en que cruzan el umbral
    ???
  }

  def main(args: Array[String]): Unit = {
    val config = ConfigMetricas(
      rutaEventos = args(0),
      rutaClientes = args(1),
      rutaSalida = args(2),
      fecha = args(3),
    )

    val spark = SparkSession.builder()
      .appName("metricas-ecommerce")
      .getOrCreate()

    // TODO: implementar el pipeline completo
  }
}
```

**Restricciones:**
1. Implementar `calcularRevenuePorSegmento` y `detectarClientesPremium`
2. Añadir tests unitarios para ambas funciones
3. Empaquetar como JAR y ejecutar con `spark-submit`
4. Comparar el rendimiento con la versión Python del Cap.13

---

## Resumen del capítulo

**Cuándo Scala añade valor real en 2024:**

```
✓ UDFs complejas sin overhead JVM-Python
  → La función ejecuta directamente en JVM — 10-50× más rápido que Python UDF

✓ APIs internas de Spark no expuestas en PySpark
  → SparkListener, Catalyst rules, fuentes de datos custom, InternalRow

✓ Dataset[T]: errores de schema en compilación para las transformaciones
  → El compilador detecta accesos a campos inexistentes
  → No detecta cambios en el schema del archivo — eso requiere validation explícita

✓ Testing con tipos estáticos
  → El compilador elimina una clase de bugs antes de ejecutar
  → Las case classes hacen el schema auto-documentado

✗ No vale la pena cuando:
  → El pipeline usa principalmente SQL/Column expressions (Catalyst los optimiza igual)
  → El equipo no conoce Scala (la curva de aprendizaje supera el beneficio)
  → El overhead de Python es < 2× y los jobs son cortos
```

**La frase que resume el capítulo:**

```
Scala elimina el boundary JVM-Python.
Ese boundary importa cuando:
  (1) La lógica de transformación es compleja y no cabe en SQL
  (2) Necesitas APIs internas de Spark
  (3) El volumen de datos hace que incluso un pequeño overhead se amplifique

Para el resto: PySpark con Pandas UDFs y Arrow es la respuesta correcta.
```

**Conexión con el Cap.15 (Java):**

> Java es el predecesor de Scala en el ecosistema JVM.
> Para Beam y Kafka Streams — los frameworks del Cap.10 y el Cap.09 §6 —
> Java no es una elección sino el ecosistema nativo.
> El Cap.15 explora cuándo la verbosidad de Java es una ventaja,
> no un obstáculo.
