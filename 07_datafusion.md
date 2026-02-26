# Guía de Ejercicios — Cap.07: DataFusion — SQL Distribuido en Rust

> DataFusion ocupa un nicho diferente a Polars y Spark.
> No es un DataFrame API (como Polars) ni un cluster distribuido (como Spark).
> Es un motor de queries SQL embebible, escrito en Rust, construido sobre Arrow.
>
> Su propuesta: llevar la potencia de un motor analítico al interior de tu aplicación —
> sin proceso externo, sin JVM, sin cluster. Una librería que puedes incluir en
> cualquier programa Rust o Python y que ejecuta SQL sobre Arrow con rendimiento
> comparable a DuckDB o Spark local.
>
> Cuándo es relevante: cuando construyes herramientas de datos, no cuando
> procesas datos con herramientas existentes.

---

## El nicho de DataFusion

```
Polars:    "proceso datos en Python/Rust con DataFrame API"
           → para data engineers y data scientists

Spark:     "proceso datos distribuidos en un cluster"
           → para data engineers con datasets grandes

DataFusion: "construyo una herramienta que procesa datos"
            → para ingenieros de software que construyen:
              - motores de queries personalizados
              - herramientas de analytics embebidas
              - backends de SQL para aplicaciones
              - nuevos frameworks de datos (como Delta Lake, Ballista)
```

DataFusion es el motor detrás de herramientas como:
- **Ballista**: motor distribuido (Spark-like) construido sobre DataFusion
- **InfluxDB IOx**: base de datos de series temporales
- **GlareDB**: base de datos analítica serverless
- **Cube.js**: semántica layer para analytics

Si construyes una herramienta de datos, DataFusion te da el motor SQL
gratuitamente y tú te concentras en la capa de valor añadido.

---

## Apache Arrow y el ecosistema Rust de datos

```
El stack Rust para data engineering en 2024:

                    Tu aplicación
                         │
          ┌──────────────┼──────────────┐
          │              │              │
       Polars        DataFusion     delta-rs
   (DataFrame API)  (SQL engine)  (Delta Lake)
          │              │              │
          └──────────────┼──────────────┘
                         │
                    Apache Arrow
                  (formato en memoria)
                         │
                    Parquet / IPC
                   (formato en disco)
```

Los tres comparten Arrow como formato de memoria — pueden intercambiar datos
sin copia. DataFusion puede leer resultados de Polars directamente como
RecordBatches de Arrow. Polars puede procesar resultados de DataFusion.

---

## Tabla de contenidos

- [Sección 7.1 — DataFusion como librería](#sección-71--datafusion-como-librería)
- [Sección 7.2 — SQL sobre Arrow: el modelo de ejecución](#sección-72--sql-sobre-arrow-el-modelo-de-ejecución)
- [Sección 7.3 — Fuentes de datos personalizadas](#sección-73--fuentes-de-datos-personalizadas)
- [Sección 7.4 — Optimización del plan de queries](#sección-74--optimización-del-plan-de-queries)
- [Sección 7.5 — DataFusion desde Python](#sección-75--datafusion-desde-python)
- [Sección 7.6 — Extensibilidad: UDFs y reglas de optimización](#sección-76--extensibilidad-udfs-y-reglas-de-optimización)
- [Sección 7.7 — Cuándo DataFusion, cuándo Polars, cuándo Spark](#sección-77--cuándo-datafusion-cuándo-polars-cuándo-spark)

---

## Sección 7.1 — DataFusion como Librería

### Ejercicio 7.1.1 — Leer: la diferencia entre motor y herramienta

**Tipo: Leer/analizar**

La distinción más importante del capítulo:

```
Polars como herramienta:
  import polars as pl
  df = pl.read_parquet("datos.parquet")
  resultado = df.group_by("region").agg(pl.sum("monto"))
  # Polars ES la herramienta. Tú eres el usuario.

DataFusion como motor:
  // En Rust, dentro de tu aplicación:
  let ctx = SessionContext::new();
  ctx.register_parquet("ventas", "datos.parquet", ParquetReadOptions::default()).await?;
  let df = ctx.sql("SELECT region, SUM(monto) FROM ventas GROUP BY region").await?;
  // DataFusion es el MOTOR de tu herramienta. Tu aplicación es la herramienta.
```

**Preguntas:**

1. ¿Por qué alguien querría "embeber" un motor SQL en su aplicación
   en lugar de usar una base de datos separada?

2. Da tres ejemplos de aplicaciones reales que se beneficiarían
   de tener DataFusion embebido.

3. ¿Qué ventajas tiene DataFusion sobre SQLite para consultas analíticas?

4. ¿Qué ventajas tiene DuckDB sobre DataFusion para el mismo propósito?
   ¿Por qué existen ambos?

5. ¿La distinción "motor vs herramienta" es exclusiva de DataFusion,
   o también aplica a otros proyectos del ecosistema?

**Pista:** SQLite es row-oriented y está diseñado para OLTP (muchas transacciones
pequeñas). DataFusion es columnar (Arrow) y está diseñado para OLAP (queries
analíticas sobre muchas filas, pocas columnas). Para una query como
`SELECT region, SUM(monto) FROM ventas GROUP BY region` sobre 100M filas,
DataFusion puede ser 100× más rápido que SQLite porque lee solo la columna
`region` y `monto` (columnar), no todas las columnas.
DuckDB tiene un modelo similar a DataFusion pero es más maduro como producto
terminado — incluye su propio almacenamiento, herramientas CLI, etc.
DataFusion es más una librería de building blocks.

---

### Ejercicio 7.1.2 — Primer contacto: DataFusion desde Rust

```rust
// Cargo.toml:
// [dependencies]
// datafusion = "35.0"
// tokio = { version = "1", features = ["rt-multi-thread"] }

use datafusion::prelude::*;
use datafusion::arrow::util::pretty::print_batches;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Crear un contexto de sesión (equivalente a SparkSession):
    let ctx = SessionContext::new();

    // Registrar un archivo Parquet como tabla:
    ctx.register_parquet(
        "ventas",
        "transacciones.parquet",
        ParquetReadOptions::default(),
    ).await?;

    // Ejecutar SQL:
    let df = ctx.sql(
        "SELECT region, SUM(monto) as total, COUNT(*) as transacciones
         FROM ventas
         WHERE monto > 100
         GROUP BY region
         ORDER BY total DESC"
    ).await?;

    // Recoger resultados como RecordBatches de Arrow:
    let resultados = df.collect().await?;
    print_batches(&resultados)?;

    // Alternativamente, usar la DataFrame API de DataFusion:
    let df2 = ctx.table("ventas").await?
        .filter(col("monto").gt(lit(100.0)))?
        .aggregate(
            vec![col("region")],
            vec![sum(col("monto")).alias("total")],
        )?
        .sort(vec![col("total").sort(false, false)])?;

    let resultados2 = df2.collect().await?;
    print_batches(&resultados2)?;

    Ok(())
}
```

**Preguntas:**

1. ¿Por qué `collect().await?` — por qué es async?

2. ¿Qué es un `RecordBatch` de Arrow y cómo se relaciona con un DataFrame?

3. ¿`SessionContext::new()` tiene overhead similar al `SparkSession` de Spark?

4. ¿DataFusion puede ejecutar queries en paralelo dentro del mismo `SessionContext`?

5. ¿Qué pasa si el archivo Parquet tiene 100 GB y la máquina tiene 16 GB de RAM?

**Pista:** `collect().await?` es async porque DataFusion usa tokio para
paralelismo asíncrono — puede ejecutar múltiples streams de datos en paralelo
sin bloquear threads. El overhead del `SessionContext::new()` es mínimo
comparado con Spark — no hay JVM que arrancar, no hay proceso externo,
no hay conexión de red. Para datos más grandes que la RAM: DataFusion procesa
en streaming de RecordBatches (similar al modo streaming de Polars) — no carga
todo en memoria.

---

### Ejercicio 7.1.3 — DataFusion desde Python: datafusion-python

```python
# pip install datafusion

import datafusion
from datafusion import SessionContext
import pyarrow as pa
import pyarrow.parquet as pq

# El mismo contexto, desde Python:
ctx = SessionContext()

# Registrar una tabla Parquet:
ctx.register_parquet("ventas", "transacciones.parquet")

# Ejecutar SQL:
resultado = ctx.sql("""
    SELECT region,
           SUM(monto) as total,
           COUNT(*) as transacciones,
           AVG(monto) as ticket_promedio
    FROM ventas
    WHERE monto > 100
    GROUP BY region
    ORDER BY total DESC
""")

# Obtener como Arrow Table:
tabla_arrow = resultado.collect()
# tabla_arrow es una lista de RecordBatches

# Convertir a Polars o Pandas:
import polars as pl
df_polars = pl.from_arrow(pa.Table.from_batches(tabla_arrow))

import pandas as pd
df_pandas = pa.Table.from_batches(tabla_arrow).to_pandas()

# Registrar un DataFrame de Polars como tabla en DataFusion:
df_polars_fuente = pl.DataFrame({
    "id": [1, 2, 3],
    "valor": [10.0, 20.0, 30.0],
})
ctx.register_record_batches(
    "mi_tabla",
    [df_polars_fuente.to_arrow().to_batches()]
)
```

**Preguntas:**

1. ¿`ctx.sql()` ejecuta inmediatamente o devuelve un plan lazy?

2. ¿Por qué `collect()` retorna una lista de RecordBatches en lugar de
   un solo DataFrame?

3. ¿DataFusion-python usa GIL cuando ejecuta queries?

4. ¿Cuál es el overhead de registrar una tabla vía `register_parquet`
   vs `register_record_batches`?

5. ¿Puedes ejecutar múltiples queries concurrentes en el mismo `SessionContext`?

**Pista:** `ctx.sql()` devuelve un `DataFrame` de DataFusion que representa
el plan lógico — no ejecuta hasta que llamas a `.collect()`, `.show()`, o
`.write_parquet()`. Esto es equivalente al `LazyFrame` de Polars.
`collect()` retorna una lista de RecordBatches porque DataFusion procesa
en streams de batches — el resultado puede ser muy grande para un solo batch.
GIL: DataFusion-python lanza el cómputo a threads de Rust que no tienen GIL —
puedes ejecutar múltiples queries concurrentes desde Python.

---

### Ejercicio 7.1.4 — Comparar la API: DataFusion vs Polars vs Spark SQL

```python
# La misma query en los tres sistemas:
query = """
    SELECT
        c.segmento,
        DATE_TRUNC('month', v.fecha) as mes,
        SUM(v.monto) as revenue,
        COUNT(DISTINCT v.user_id) as usuarios_unicos,
        SUM(v.monto) / COUNT(DISTINCT v.user_id) as revenue_por_usuario
    FROM ventas v
    JOIN clientes c ON v.user_id = c.id
    WHERE v.fecha >= '2024-01-01'
      AND v.monto > 0
    GROUP BY c.segmento, DATE_TRUNC('month', v.fecha)
    ORDER BY mes, revenue DESC
"""

# DataFusion:
import datafusion
ctx = datafusion.SessionContext()
ctx.register_parquet("ventas", "ventas.parquet")
ctx.register_parquet("clientes", "clientes.parquet")
resultado_df = ctx.sql(query)
tabla = resultado_df.collect()

# Polars (SQL interface):
import polars as pl
ctx_polars = pl.SQLContext(
    ventas=pl.scan_parquet("ventas.parquet"),
    clientes=pl.scan_parquet("clientes.parquet"),
)
resultado_polars = ctx_polars.execute(query).collect()

# Spark SQL:
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark.read.parquet("ventas.parquet").createOrReplaceTempView("ventas")
spark.read.parquet("clientes.parquet").createOrReplaceTempView("clientes")
resultado_spark = spark.sql(query).collect()
```

**Restricciones:**
1. Ejecutar la misma query en los tres sistemas y medir el tiempo
2. ¿Los tres producen el mismo resultado? ¿Hay diferencias de semántica SQL?
3. ¿Polars tiene una interfaz SQL? ¿Cuándo es preferible SQL sobre DataFrame API?
4. Para datos de 1 GB, 10 GB, y 100 GB, ¿cuál sistema es más rápido?

**Pista:** Polars sí tiene una interfaz SQL (`pl.SQLContext`) — puede ejecutar
las mismas queries SQL que DataFusion, con el mismo Arrow bajo el capó.
La diferencia está en la extensibilidad: DataFusion está diseñado para ser
extendido con nuevas fuentes de datos, funciones, y optimizaciones desde Rust.
Polars está más orientado a ser una herramienta terminal. Para la misma query SQL
sobre datos locales, el rendimiento es comparable porque ambos usan Arrow.

---

### Ejercicio 7.1.5 — Leer: por qué Arrow es la clave de la interoperabilidad

**Tipo: Leer/analizar**

```
Sin Arrow (pre-2016):
  Herramienta A tiene su formato de memoria propio.
  Herramienta B tiene su formato propio.
  Para pasar datos de A a B: serializar → transmitir → deserializar.
  Costo: proporcional al tamaño de los datos.

Con Arrow (post-2016):
  DataFusion, Polars, DuckDB, Pandas (2.0), Spark (con Arrow habilitado):
  todos usan Arrow como formato de memoria.
  Para pasar datos entre ellos: pasar un puntero.
  Costo: O(1) — una asignación de puntero, sin copia de datos.

DataFusion como "glue":
  DataFusion puede actuar como motor SQL que consume datos de Polars,
  devuelve resultados a Polars, y escribe a Delta Lake —
  todo sin serialización intermedia.
```

**Preguntas:**

1. ¿Por qué el estándar Arrow tarda tanto en adoptarse si tiene ventajas claras?

2. ¿Hay tipos de datos de Polars que no tienen representación en Arrow?
   ¿Y al revés?

3. "Pasar un puntero" entre DataFusion y Polars: ¿es realmente O(1)?
   ¿Qué consideraciones de seguridad de memoria hay en Rust?

4. Si DataFusion y Polars comparten el mismo buffer de Arrow,
   ¿qué pasa si uno modifica los datos mientras el otro los lee?

5. ¿El mismo zero-copy funciona entre Python y Rust (DataFusion-python + Polars)?

**Pista:** Arrow garantiza que los buffers son inmutables por defecto —
no puedes modificar un RecordBatch existente, solo crear uno nuevo.
Esto es lo que permite compartir buffers de forma segura: si nadie puede
modificar, no hay data races. En Rust, el sistema de ownership garantiza
esto en tiempo de compilación — el compilador rechaza código que intentaría
modificar un buffer compartido. En Python, la inmutabilidad es por convención
(nadie te impide hacer `arr[0] = 5` sobre un numpy array compartido — hazlo
y tendrás resultados incorrectos silenciosamente).

---

## Sección 7.2 — SQL sobre Arrow: el Modelo de Ejecución

### Ejercicio 7.2.1 — Del SQL al plan físico de Arrow

DataFusion traduce SQL a un árbol de operadores que trabajan sobre
RecordBatches de Arrow:

```
SQL:
  SELECT region, SUM(monto) FROM ventas WHERE monto > 100 GROUP BY region

Plan lógico (árbol de operaciones abstractas):
  Projection [region, sum_monto]
  └─ Aggregation [group_by=region, agg=SUM(monto)]
     └─ Filter [monto > 100]
        └─ TableScan [ventas]

Plan físico (operadores concretos sobre RecordBatches):
  ProjectionExec
  └─ AggregateExec [mode=FinalPartitioned]
     └─ CoalesceBatchesExec
        └─ AggregateExec [mode=Partial]
           └─ FilterExec [monto > 100]
              └─ ParquetExec [ventas.parquet, projection=[region, monto]]
```

```python
import datafusion
ctx = datafusion.SessionContext()
ctx.register_parquet("ventas", "transacciones.parquet")

df = ctx.sql("""
    SELECT region, SUM(monto) as total
    FROM ventas
    WHERE monto > 100
    GROUP BY region
""")

# Ver el plan lógico:
print(df.logical_plan())

# Ver el plan físico optimizado:
print(df.execution_plan())
```

**Preguntas:**

1. ¿Por qué hay dos `AggregateExec` en el plan físico (Partial y FinalPartitioned)?

2. ¿`ParquetExec` lee solo las columnas `region` y `monto`?
   ¿Cómo lo sabe?

3. ¿El `FilterExec [monto > 100]` puede ejecutarse antes de leer
   todos los datos del Parquet?

4. ¿`CoalesceBatchesExec` qué hace y por qué es necesario?

5. ¿Cómo se diferencia este plan del que generaría Spark para la misma query?

**Pista:** `AggregateExec Partial` es el combiner (agrega localmente dentro
de cada thread). `AggregateExec FinalPartitioned` combina los resultados
parciales de todos los threads. Esto es exactamente el `partial_sum` +
`sum` que vimos en los planes de Spark en los Cap.03-05 — el mismo modelo
Map/Reduce, implementado en Rust sobre Arrow en lugar de Scala sobre JVM.
`CoalesceBatchesExec` combina RecordBatches pequeños en uno más grande
para reducir el overhead de procesamiento por batch.

---

### Ejercicio 7.2.2 — Streams de RecordBatches: el modelo de ejecución

```rust
use datafusion::prelude::*;
use datafusion::arrow::record_batch::RecordBatch;
use futures::stream::StreamExt;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();
    ctx.register_parquet("ventas", "datos.parquet", ParquetReadOptions::default()).await?;

    let df = ctx.sql(
        "SELECT region, SUM(monto) FROM ventas GROUP BY region"
    ).await?;

    // En lugar de collect() (que carga todo en memoria),
    // procesar el stream de RecordBatches:
    let mut stream = df.execute_stream().await?;

    let mut total_filas = 0usize;
    while let Some(batch) = stream.next().await {
        let batch: RecordBatch = batch?;
        total_filas += batch.num_rows();
        println!("Batch recibido: {} filas, {} bytes",
                 batch.num_rows(),
                 batch.get_array_memory_size());
    }
    println!("Total: {} filas", total_filas);
    Ok(())
}
```

**Preguntas:**

1. ¿Por qué procesar el stream de RecordBatches es mejor que `.collect()`
   para datasets grandes?

2. ¿Cuánto tiempo tarda el primer RecordBatch en aparecer en el stream?
   ¿Es proporcional al tamaño total del dataset?

3. ¿DataFusion puede ejecutar múltiples streams en paralelo?
   ¿Cómo se coordinan los threads?

4. Si el consumidor del stream es más lento que el productor,
   ¿qué mecanismo controla la velocidad?

5. ¿El modelo de RecordBatch streams tiene equivalente en Spark o Polars?

**Pista:** El primer RecordBatch aparece en cuanto el primer chunk del
Parquet es procesado — no hay que esperar a que todo el dataset sea leído.
Esto es "streaming execution" (no confundir con streaming de eventos como Kafka).
En Spark, el equivalente es `foreach`/`foreachPartition` — procesar resultados
mientras llegan en lugar de acumular todo. DataFusion usa backpressure:
si el consumidor es lento, el stream pausa la producción automáticamente.

---

### Ejercicio 7.2.3 — Predicate pushdown a nivel de Parquet

```python
import datafusion
import time

ctx = datafusion.SessionContext()
ctx.register_parquet("ventas", "transacciones.parquet")

# Sin predicate pushdown explícito — DataFusion aplica automáticamente:
inicio = time.perf_counter()
resultado = ctx.sql("""
    SELECT COUNT(*) as total
    FROM ventas
    WHERE monto > 5000
      AND region = 'norte'
      AND fecha >= '2024-01-01'
""").collect()
tiempo = time.perf_counter() - inicio
print(f"Tiempo: {tiempo:.2f}s")

# Ver en el plan si el pushdown ocurrió:
df = ctx.sql("""
    SELECT COUNT(*) FROM ventas
    WHERE monto > 5000 AND region = 'norte'
""")
plan = df.execution_plan()
# Buscar en el plan: ParquetExec debería mostrar los filtros pusheados
print(str(plan))
```

**Preguntas:**

1. ¿El predicate `monto > 5000` se pushea al nivel de row group de Parquet?
   ¿Cómo verificarlo en el plan?

2. ¿El predicate `region = 'norte'` es más eficiente si la columna
   `region` usa dictionary encoding en el Parquet?

3. ¿Hay predicates que DataFusion NO puede pushear al Parquet?
   Da un ejemplo.

4. ¿DataFusion puede usar las statistics de Parquet (min/max por row group)
   para saltar row groups?

5. ¿Cómo compara el predicate pushdown de DataFusion con el de Spark SQL
   sobre los mismos archivos Parquet?

**Pista:** DataFusion sí usa las estadísticas de Parquet (min/max) para
saltar row groups — es uno de los beneficios de estar construido sobre Arrow
y tener integración nativa con Parquet. Los predicates que NO pueden pushearse:
funciones no-deterministas (`RANDOM()`), predicates sobre columnas calculadas
(`WHERE monto * 1.19 > 1000` — puede calcularse pero no tiene statistics),
y subqueries correlacionadas. Para `region = 'norte'` con dictionary encoding,
la comparación es especialmente eficiente: compara el índice del diccionario
(un entero), no el string completo.

---

### Ejercicio 7.2.4 — Joins en DataFusion: algoritmos y estrategias

```rust
use datafusion::prelude::*;

// DataFusion soporta múltiples algoritmos de join:
// Hash Join (default para la mayoría de los casos)
// Sort-Merge Join
// Cross Join
// Nested Loop Join (para non-equi joins)

let ctx = SessionContext::new();
ctx.register_parquet("ventas", "ventas.parquet", ParquetReadOptions::default()).await?;
ctx.register_parquet("clientes", "clientes.parquet", ParquetReadOptions::default()).await?;

// Hash join (automático):
let df = ctx.sql(
    "SELECT v.*, c.segmento
     FROM ventas v
     JOIN clientes c ON v.user_id = c.id"
).await?;

// Ver qué algoritmo eligió DataFusion:
println!("{}", df.execution_plan().await?);
// HashJoinExec si una tabla es pequeña
// SortMergeJoinExec si ambas son grandes

// Hint para forzar broadcast (si DataFusion no lo detecta automáticamente):
// En DataFusion, el broadcast se configura via session config:
let config = SessionConfig::new()
    .with_target_partitions(8)
    .with_collect_statistics(true);  // habilitar estadísticas para decisiones de join
let ctx = SessionContext::new_with_config(config);
```

**Preguntas:**

1. ¿DataFusion tiene broadcast join automático como Spark?
   ¿Qué estadísticas necesita para decidir?

2. ¿Hash join vs Sort-Merge join: cuándo DataFusion elige cada uno?

3. ¿DataFusion puede hacer joins entre tablas en diferentes nodos?
   (pista: no — DataFusion por sí solo no es distribuido)

4. ¿Ballista (el framework distribuido sobre DataFusion) puede hacer
   joins distribuidos con shuffle?

5. ¿Cómo se compara el rendimiento del join de DataFusion vs DuckDB vs Polars
   para datos que caben en memoria?

**Pista:** DataFusion por sí solo NO es distribuido — todo el procesamiento
ocurre en una sola máquina con múltiples threads. Ballista añade la capa
de distribución: coordina múltiples nodos DataFusion, gestiona el shuffle
entre ellos (similar a cómo Spark coordina executors), y proporciona
tolerancia a fallos. DataFusion es al motor SQL lo que Arrow es al formato:
un bloque de construcción que otros sistemas usan.

---

### Ejercicio 7.2.5 — Medir: DataFusion vs Polars vs DuckDB para analytics locales

```python
import datafusion
import polars as pl
import duckdb
import pandas as pd
import time

# Dataset de referencia: 10M filas
# Generar con Polars (rápido) y guardar como Parquet
df = pl.DataFrame({
    "id": pl.arange(0, 10_000_000, eager=True),
    "monto": pl.Series([float(i % 10000) for i in range(10_000_000)]),
    "region": pl.Series(["norte", "sur", "este", "oeste"] * 2_500_000),
    "mes": pl.Series([i % 12 + 1 for i in range(10_000_000)]),
    "activo": pl.Series([i % 3 != 0 for i in range(10_000_000)]),
})
df.write_parquet("benchmark.parquet")

# La misma query en los cuatro sistemas:
QUERY = """
    SELECT
        region,
        mes,
        COUNT(*) as transacciones,
        SUM(monto) as revenue,
        AVG(monto) as ticket
    FROM {tabla}
    WHERE activo = true
      AND monto > 100
    GROUP BY region, mes
    ORDER BY revenue DESC
"""

# DataFusion:
ctx = datafusion.SessionContext()
ctx.register_parquet("ventas", "benchmark.parquet")
t_df = time.perf_counter()
ctx.sql(QUERY.format(tabla="ventas")).collect()
print(f"DataFusion: {time.perf_counter()-t_df:.3f}s")

# Polars:
lf = pl.scan_parquet("benchmark.parquet")
t_pl = time.perf_counter()
(lf.filter((pl.col("activo") == True) & (pl.col("monto") > 100))
    .group_by(["region", "mes"])
    .agg([pl.count(), pl.sum("monto"), pl.mean("monto")])
    .sort("monto", descending=True)
    .collect())
print(f"Polars:     {time.perf_counter()-t_pl:.3f}s")

# DuckDB:
con = duckdb.connect()
t_duck = time.perf_counter()
con.execute(QUERY.format(tabla="read_parquet('benchmark.parquet')")).fetchall()
print(f"DuckDB:     {time.perf_counter()-t_duck:.3f}s")
```

**Restricciones:**
1. Ejecutar el benchmark y completar los tiempos
2. ¿Hay diferencias significativas? ¿Por qué esperarías que sean similares?
3. ¿El resultado cambia con 100M filas? ¿Con 1B filas?
4. ¿Cuál usa más RAM? ¿Cuál menos?
5. ¿Qué factor diferencia realmente a estos tres motores si el rendimiento es similar?

**Pista:** Los tres (DataFusion, Polars, DuckDB) usan Arrow internamente
y tienen optimizadores similares. El rendimiento para queries analíticas
simples es comparable — las diferencias son más pronunciadas en:
(1) queries muy complejas donde el optimizador marca la diferencia,
(2) datos más grandes que la RAM (estrategias de streaming diferentes),
(3) integración con el ecosistema (DuckDB tiene mejor soporte para
distintas fuentes de datos, Polars tiene mejor DataFrame API).
El factor diferenciador real es la extensibilidad y el caso de uso:
DataFusion para construir herramientas, DuckDB como herramienta terminal.

---

## Sección 7.3 — Fuentes de Datos Personalizadas

Esta es la capacidad más diferenciadora de DataFusion: puedes enseñarle
a leer cualquier formato de datos, y el resto del motor (SQL, optimización,
paralelismo) funciona automáticamente.

### Ejercicio 7.3.1 — Cómo DataFusion lee datos: el trait TableProvider

```rust
use datafusion::datasource::TableProvider;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::context::ExecutionState;
use datafusion::physical_plan::ExecutionPlan;
use async_trait::async_trait;

// Para crear una fuente de datos personalizada, implementar TableProvider:
pub struct MiTablPersonalizada {
    schema: SchemaRef,
    datos: Vec<RecordBatch>,
}

#[async_trait]
impl TableProvider for MiTablaPersonalizada {
    fn as_any(&self) -> &dyn std::any::Any { self }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &ExecutionState,
        projection: Option<&Vec<usize>>,    // ← column pruning
        filters: &[Expr],                   // ← predicate pushdown
        limit: Option<usize>,               // ← limit pushdown
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        // DataFusion pasa los filtros y la proyección para que
        // la fuente de datos solo lea lo necesario.
        // Aquí implementas la lógica de lectura de tu formato.
        todo!()
    }
}
```

**Preguntas:**

1. ¿Qué son `projection`, `filters`, y `limit` en el método `scan()`?
   ¿Por qué DataFusion los pasa a la fuente de datos?

2. ¿Qué ocurre si tu fuente de datos ignora los `filters`?
   ¿El resultado es incorrecto?

3. ¿Por qué no simplemente hacer `scan()` que devuelve todos los datos
   y dejar que DataFusion filtre después?

4. ¿Cuántas veces puede llamar DataFusion al método `scan()` para una
   sola query?

5. Da tres ejemplos de formatos de datos donde implementar un
   `TableProvider` personalizado tiene sentido.

**Pista:** Si tu fuente ignora los `filters`, el resultado sigue siendo
CORRECTO — DataFusion aplica un `FilterExec` adicional después del scan
que garantiza la corrección. Lo que se pierde es la EFICIENCIA: sin pushdown,
la fuente devuelve todos los datos y DataFusion los filtra después.
Con pushdown, la fuente solo devuelve los datos relevantes — menos I/O,
menos memoria, más rápido. DataFusion diseña sus extensiones para que
"incorrecto no es posible, ineficiente sí".

---

### Ejercicio 7.3.2 — Implementar una fuente de datos para un API HTTP

```python
# En Python, es más fácil demostrar el concepto con datafusion-python:
import datafusion
from datafusion import SessionContext
import pyarrow as pa
import requests

class APIRestTableProvider:
    """
    Fuente de datos que lee de una API REST y expone los datos como tabla SQL.
    """

    def __init__(self, url: str, schema: pa.Schema):
        self.url = url
        self.schema = schema

    def scan(
        self,
        projection: list[int] | None = None,
        filters: list = None,
        limit: int | None = None,
    ) -> pa.RecordBatch:
        """
        Llama a la API y retorna los datos como RecordBatch.
        Aplica projection para no devolver columnas innecesarias.
        """
        # Construir parámetros de la query según filtros (si la API los soporta):
        params = {}
        if limit is not None:
            params["limit"] = limit

        response = requests.get(self.url, params=params)
        datos = response.json()

        # Convertir a Arrow:
        tabla = pa.Table.from_pylist(datos, schema=self.schema)

        # Aplicar projection (solo columnas necesarias):
        if projection is not None:
            campos = [self.schema.field(i).name for i in projection]
            tabla = tabla.select(campos)

        return tabla.to_batches()[0]
```

**Restricciones:**
1. Implementar el `APIRestTableProvider` completo
2. Registrarlo en DataFusion y ejecutar una query SQL sobre él
3. ¿Qué optimizaciones (projection, limit) puede aplicar tu provider?
4. ¿Cómo manejas la paginación si la API devuelve resultados en páginas?
5. ¿Cuándo es útil este patrón vs simplemente hacer fetch + procesar en Python?

**Pista:** El patrón "fuente de datos REST en SQL" es útil cuando tienes
múltiples APIs y quieres hacer queries que combinan datos de varias:
`SELECT * FROM api_usuarios JOIN api_pedidos ON usuario_id = id WHERE ...`.
Sin DataFusion, tendrías que hacer los fetches manualmente y hacer el join
en Python. Con DataFusion, puedes expresarlo como SQL y el motor hace
el join, filtro, y proyección automáticamente.

---

### Ejercicio 7.3.3 — Diseñar: una fuente de datos para logs en formato custom

**Tipo: Diseñar**

Una empresa tiene logs de aplicación en un formato propio (no JSON, no CSV):

```
[2024-01-15T14:23:01.123Z] INFO  req_id=abc123 user=u456 path=/api/checkout ms=234 status=200
[2024-01-15T14:23:02.456Z] ERROR req_id=def789 user=u456 path=/api/checkout ms=1203 status=500
[2024-01-15T14:23:03.789Z] INFO  req_id=ghi012 user=u789 path=/api/catalog ms=45 status=200
```

Tienen 10 TB de estos logs en S3 y quieren hacer queries SQL como:
```sql
SELECT
    path,
    COUNT(*) as requests,
    AVG(ms) as avg_latency,
    SUM(CASE WHEN status >= 500 THEN 1 ELSE 0 END) as errors
FROM logs
WHERE timestamp >= '2024-01-15'
  AND ms > 100
GROUP BY path
ORDER BY avg_latency DESC
```

Diseñar el `TableProvider` para esta fuente:
1. ¿Cuál es el schema (tipos de columnas)?
2. ¿Cómo aplicas el predicate pushdown para `timestamp >= '2024-01-15'`?
3. ¿Cómo paralelizas la lectura de 10 TB?
4. ¿Cómo gestionas el parsing del formato custom?
5. ¿Tiene sentido convertir a Parquet previamente? ¿O el `TableProvider` es mejor?

---

### Ejercicio 7.3.4 — Fuentes de datos en tiempo real: streaming en DataFusion

DataFusion también puede procesar streams de datos (no solo archivos estáticos):

```rust
use datafusion::prelude::*;
use datafusion::datasource::streaming::StreamingTable;
use datafusion::arrow::record_batch::RecordBatch;
use tokio::sync::mpsc;

// Crear una tabla de streaming que recibe datos por un channel:
async fn tabla_streaming(
    schema: SchemaRef,
    receiver: mpsc::Receiver<RecordBatch>,
) -> impl TableProvider {
    StreamingTable::try_new(
        schema,
        vec![Box::new(ChannelStream::new(receiver))],
    ).unwrap()
}

// El query se ejecuta sobre un stream continuo de batches:
// (cada vez que llega un RecordBatch al channel, se procesa)
```

**Preguntas:**

1. ¿DataFusion con streaming es equivalente a Flink o Spark Structured Streaming?
   ¿Qué capacidades tiene y cuáles faltan?

2. ¿DataFusion streaming soporta windowing (ventanas de tiempo)?

3. ¿Puede DataFusion streaming hacer joins entre un stream y una tabla estática?

4. ¿Cuándo usarías DataFusion streaming vs Flink para procesamiento de eventos?

**Pista:** DataFusion streaming es mucho más limitado que Flink o Spark Streaming.
No tiene soporte nativo de windowing basado en event-time, ni watermarks,
ni estado persistente entre batches. Es útil para casos simples de
"procesar un stream de RecordBatches y aplicar transformaciones SQL",
pero no para casos que requieren el modelo de ventanas de tiempo con
late data handling. Para esos casos, Flink (Cap.12) sigue siendo la
herramienta apropiada.

---

### Ejercicio 7.3.5 — Leer: Delta Lake y DataFusion

**Tipo: Leer/analizar**

Delta Lake usa DataFusion como uno de sus motores de lectura:

```rust
// delta-rs: la implementación de Delta Lake en Rust
use deltalake::DeltaTableBuilder;

// Leer una tabla Delta Lake:
let tabla = DeltaTableBuilder::from_uri("s3://mi-bucket/mi-tabla/")
    .with_storage_options(opciones_s3)
    .load()
    .await?;

// delta-rs internamente usa DataFusion para ejecutar queries:
let ctx = DeltaTableBuilder::build_datafusion_context(&tabla)?;

let resultado = ctx.sql(
    "SELECT * FROM delta_table WHERE fecha = '2024-01-15'"
).await?.collect().await?;
```

**Preguntas:**

1. ¿Por qué delta-rs usa DataFusion internamente en lugar de
   implementar su propio motor SQL?

2. ¿Qué ventaja tiene usar DataFusion en delta-rs respecto a
   simplemente leer los archivos Parquet directamente?

3. ¿delta-rs puede usar el time travel de Delta Lake con DataFusion?
   ¿Cómo se implementa?

4. ¿Qué otras herramientas del ecosistema Arrow/Rust usan DataFusion
   como motor embebido?

> 🔗 Ecosistema: `delta-rs` es la implementación de Delta Lake en Rust
> que permite a herramientas no-JVM (Python via Polars, Rust via DataFusion)
> leer y escribir tablas Delta Lake. Se cubre en el Cap.08 (El Lakehouse).
> El hecho de que delta-rs use DataFusion internamente es un ejemplo
> del patrón "motor embebible" que es el propósito central de DataFusion.

---

## Sección 7.4 — Optimización del Plan de Queries

### Ejercicio 7.4.1 — El pipeline de optimización de DataFusion

```python
import datafusion

ctx = datafusion.SessionContext()
ctx.register_parquet("ventas", "transacciones.parquet")
ctx.register_parquet("clientes", "clientes_grandes.parquet")  # 10 GB

df = ctx.sql("""
    SELECT v.region, c.segmento, SUM(v.monto) as revenue
    FROM ventas v
    JOIN clientes c ON v.user_id = c.id
    WHERE v.monto > 100
      AND c.activo = true
    GROUP BY v.region, c.segmento
""")

# Ver el plan en diferentes etapas:
print("=== Plan lógico inicial ===")
print(df.logical_plan())

print("\n=== Plan lógico optimizado ===")
print(df.optimized_logical_plan())
# Muestra las optimizaciones aplicadas:
# - Predicate pushdown
# - Projection pushdown
# - Filter reordering (filtros más selectivos primero)

print("\n=== Plan físico ===")
print(df.execution_plan())
# Muestra cómo se ejecutará en paralelo:
# - Número de particiones
# - Algoritmo de join elegido
# - Paralelismo por operador
```

**Preguntas:**

1. ¿DataFusion aplica predicate pushdown a través del join?
   (es decir, ¿el filtro `c.activo = true` se aplica antes del join?)

2. ¿El optimizador de DataFusion reordena los joins automáticamente
   (join reordering)?

3. ¿Cuántas "reglas de optimización" tiene DataFusion?
   ¿Puedes añadir las tuyas?

4. ¿El plan físico muestra el número de threads/particiones para cada
   operador?

5. ¿Cuándo el optimizador de DataFusion es mejor que el de Spark?
   ¿Cuándo es peor?

**Pista:** El join reordering es una de las optimizaciones más poderosas
y también más complejas. DataFusion tiene join reordering básico (basado en
heurísticas de tamaño) pero no tan sofisticado como el de Spark con AQE
que ajusta en runtime. Para queries con muchos joins, el orden puede cambiar
el tiempo de ejecución en 10-100×. DataFusion mejora esta área activamente —
verificar la versión actual para el estado de join reordering.

---

### Ejercicio 7.4.2 — Estadísticas de tablas: ayudar al optimizador

```python
import datafusion
from datafusion import SessionContext
from datafusion.catalog import MemTable
import pyarrow as pa

ctx = SessionContext()

# Sin estadísticas: el optimizador no sabe cuántos datos hay
ctx.register_parquet("ventas_sin_stats", "ventas.parquet")

# Con estadísticas: el optimizador puede tomar mejores decisiones de join
ctx.register_parquet(
    "ventas_con_stats",
    "ventas.parquet",
    # DataFusion puede inferir estadísticas del footer de Parquet:
    # row count, min/max por columna
)

# Registrar con estadísticas manuales:
schema = pa.schema([
    pa.field("user_id", pa.int64()),
    pa.field("monto", pa.float64()),
    pa.field("region", pa.string()),
])

# MemTable con datos en memoria (estadísticas exactas):
datos = pa.table({
    "user_id": [1, 2, 3],
    "monto": [100.0, 200.0, 300.0],
    "region": ["norte", "sur", "norte"],
})
tabla_mem = MemTable.try_new(schema, [datos.to_batches()])
ctx.register_table("dimensiones", tabla_mem)
```

**Preguntas:**

1. ¿DataFusion lee las estadísticas del footer de Parquet automáticamente?
   ¿Qué estadísticas usa para las decisiones de join?

2. ¿Cuándo las estadísticas pueden ser incorrectas y llevar a una
   decisión de join subóptima?

3. ¿DataFusion tiene algo equivalente a `ANALYZE TABLE` de Spark/Hive?

4. ¿Cómo afecta la falta de estadísticas al rendimiento de un join
   entre una tabla de 10 GB y una de 100 MB?

---

### Ejercicio 7.4.3 — Reglas de optimización personalizadas

Una de las capacidades más poderosas de DataFusion es poder añadir
reglas de optimización propias:

```rust
use datafusion::optimizer::{OptimizerRule, OptimizerConfig};
use datafusion::logical_expr::LogicalPlan;

// Implementar una regla que reemplaza COUNT(col) por COUNT(*) cuando
// la columna no puede ser null (optimización: COUNT(*) es más eficiente):
struct OptimizarCountNonNull;

impl OptimizerRule for OptimizarCountNonNull {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        config: &mut OptimizerConfig,
    ) -> datafusion::error::Result<LogicalPlan> {
        // Recorrer el plan buscando COUNT(col) donde col no puede ser null,
        // y reemplazarlo por COUNT(*)
        // TODO: implementar la transformación del árbol de plan lógico
        todo!()
    }

    fn name(&self) -> &str { "OptimizarCountNonNull" }
}

// Registrar la regla en el contexto:
let mut config = SessionConfig::new();
let optimizer_rules = config.optimizer.rules.clone();
// Añadir la regla al pipeline de optimización
```

**Preguntas:**

1. ¿Cuándo tiene sentido añadir una regla de optimización personalizada?
   Da tres casos de uso reales.

2. ¿Las reglas de optimización personalizadas son seguras?
   ¿Pueden cambiar el resultado de una query?

3. ¿Cómo testearías una regla de optimización personalizada?

4. ¿DataFusion tiene un mecanismo de "explain" que muestra qué reglas
   se aplicaron y en qué orden?

**Pista:** Las reglas de optimización que modifican el plan lógico (no el físico)
son seguras si preservan la semántica de la query — deben producir el mismo
resultado. Las reglas que cambian el plan físico (cómo se ejecuta) solo
cambian el rendimiento, no la corrección. El testing de reglas: crear un
plan de entrada, aplicar la regla, verificar que el plan de salida es
equivalente semánticamente y más eficiente. DataFusion tiene infraestructura
de testing para esto (`assert_optimized_plan_eq`).

---

### Ejercicio 7.4.4 — Pushdown de límites y agregaciones parciales

```python
import datafusion
import time

ctx = datafusion.SessionContext()
ctx.register_parquet("ventas", "transacciones.parquet")  # 10M filas

# Query 1: TOP 10 sin optimización de limit
t1 = time.perf_counter()
ctx.sql("""
    SELECT user_id, SUM(monto) as total
    FROM ventas
    GROUP BY user_id
    ORDER BY total DESC
    LIMIT 10
""").collect()
print(f"Sin optimización: {time.perf_counter()-t1:.2f}s")
# Problema: agrupa TODOS los user_ids, luego ordena TODOS, luego toma 10

# Query 2: Con LIMIT pushdown (si DataFusion lo hace):
# El optimizador puede calcular el TOP 10 local en cada thread
# y hacer merge de los 10 mejores — mucho más eficiente

# Ver el plan:
df = ctx.sql("""
    SELECT user_id, SUM(monto) as total
    FROM ventas
    GROUP BY user_id
    ORDER BY total DESC
    LIMIT 10
""")
print(df.execution_plan())
# ¿Hay un LocalLimitExec antes del GlobalLimitExec?
```

**Preguntas:**

1. ¿DataFusion aplica "limit pushdown" para `ORDER BY ... LIMIT N`?
   ¿Qué es el `TopK` optimization?

2. ¿Cuántos datos procesa DataFusion para `GROUP BY user_id ORDER BY total LIMIT 10`
   con y sin TopK optimization?

3. ¿Polars y DuckDB aplican la misma optimización?

4. ¿El TopK optimization funciona para `LIMIT 10` pero no para `OFFSET 1000 LIMIT 10`?
   ¿Por qué?

---

### Ejercicio 7.4.5 — Leer: el optimizador de DataFusion vs el de Spark

**Tipo: Comparar**

DataFusion y Spark SQL tienen optimizadores de queries, pero con filosofías distintas:

```
Spark SQL Catalyst (Scala, JVM):
  - Rule-based optimization (40+ reglas predefinidas)
  - Cost-based optimization con estadísticas de Hive Metastore
  - AQE: reoptimización en runtime con estadísticas reales
  - Integrado con el ecosistema Hive/Hadoop

DataFusion Optimizer (Rust):
  - Rule-based optimization (40+ reglas, extensible)
  - Cost-based optimization basada en estadísticas de Parquet
  - Sin AQE (en desarrollo para versiones futuras)
  - Más ligero, sin dependencias de ecosistema externo
```

**Preguntas:**

1. ¿Qué es el "Volcano model" de optimización de queries y lo usan
   DataFusion y Spark?

2. ¿Qué es "cost-based optimization" y por qué requiere estadísticas?

3. ¿Sin AQE, DataFusion es siempre menos eficiente que Spark para
   queries con skew?

4. ¿La extensibilidad del optimizador de DataFusion (añadir reglas custom)
   es una ventaja real o una complejidad innecesaria?

5. Para queries simples (filtro + groupby + sort), ¿cuál produce
   un plan físico más eficiente?

> 📖 Profundizar: el paper *Volcano — An Extensible and Parallel Query
> Evaluation System* (Graefe, 1994) describe el modelo de evaluación
> que usan prácticamente todos los motores SQL modernos incluyendo
> DataFusion y Spark. Aunque es de 1994, los conceptos son vigentes.

---

## Sección 7.5 — DataFusion desde Python

### Ejercicio 7.5.1 — Integración con el ecosistema PyData

```python
import datafusion
from datafusion import SessionContext
import pyarrow as pa
import polars as pl
import pandas as pd
import numpy as np

ctx = SessionContext()

# Registrar datos desde distintas fuentes Python:

# 1. Desde un numpy array:
arr_monto = np.random.uniform(10, 10000, 1_000_000)
arr_region = np.random.choice(["norte", "sur", "este", "oeste"], 1_000_000)
tabla_np = pa.table({
    "monto": arr_monto,
    "region": arr_region,
})
ctx.register_record_batches("numpy_tabla", [tabla_np.to_batches()])

# 2. Desde Polars (zero-copy):
df_polars = pl.DataFrame({"id": range(100), "valor": range(100)})
ctx.register_record_batches(
    "polars_tabla",
    [df_polars.to_arrow().to_batches()]
)

# 3. Desde Pandas:
df_pandas = pd.DataFrame({"categoria": ["A", "B", "C"] * 100,
                          "precio": range(300)})
ctx.register_record_batches(
    "pandas_tabla",
    [pa.Table.from_pandas(df_pandas).to_batches()]
)

# Query que une las tres fuentes:
resultado = ctx.sql("""
    SELECT n.region, p.categoria, SUM(n.monto) as revenue
    FROM numpy_tabla n
    CROSS JOIN pandas_tabla p
    WHERE n.monto > 1000
    GROUP BY n.region, p.categoria
""").collect()
```

**Preguntas:**

1. ¿Las registraciones desde Polars y Pandas son zero-copy?
   ¿O se hace una copia al pasar a Arrow?

2. ¿Qué tipos de Python no tienen representación directa en Arrow?
   (ej: objetos Python arbitrarios, sets, etc.)

3. ¿Puedes modificar el DataFrame de Polars después de registrarlo
   en DataFusion y que los cambios se reflejen en las queries?

4. ¿`ctx.register_record_batches()` materializa los datos en memoria
   o mantiene una referencia al original?

---

### Ejercicio 7.5.2 — Ejecutar queries desde Python con resultados en Arrow

```python
import datafusion
from datafusion import SessionContext
import pyarrow as pa
import pyarrow.parquet as pq

ctx = SessionContext()
ctx.register_parquet("ventas", "transacciones.parquet")

# Obtener resultados como Arrow Table (la forma más eficiente):
df = ctx.sql("SELECT region, SUM(monto) FROM ventas GROUP BY region")

# Opción 1: collect() → lista de RecordBatches
batches = df.collect()
tabla = pa.Table.from_batches(batches)

# Opción 2: to_arrow_table() (equivalente pero más directo)
tabla2 = df.collect()

# Opción 3: escribir directamente a Parquet (sin pasar por Python)
df.write_parquet("resultado.parquet")
# DataFusion escribe directamente desde Rust → muy eficiente

# Opción 4: escribir a CSV
df.write_csv("resultado.csv")

# Opción 5: procesar el stream de batches (para datasets grandes)
import asyncio

async def procesar_stream():
    async for batch in df.execute_stream():
        # Procesar cada RecordBatch a medida que llega
        print(f"Batch: {batch.num_rows()} filas")

asyncio.run(procesar_stream())
```

**Restricciones:**
1. Medir el tiempo de cada opción para un resultado de 10M filas
2. ¿Cuál tiene el menor uso de RAM?
3. ¿`write_parquet()` comprime los datos? ¿Cuál es el codec por defecto?
4. Implementar la opción "stream a S3" sin cargar todo en memoria del driver

---

### Ejercicio 7.5.3 — UDFs en Python: el precio del cruce de boundary

```python
import datafusion
from datafusion import SessionContext, udf
import pyarrow as pa
import pyarrow.compute as pc
import time

ctx = SessionContext()
ctx.register_parquet("ventas", "transacciones.parquet")

# UDF en Python (lento: cruce de boundary Rust → Python por cada batch):
def calcular_descuento_python(monto_array: pa.Array) -> pa.Array:
    """
    Calcula descuento por rangos. Se llama una vez por RecordBatch (batch de filas).
    """
    return pa.array([
        monto * 0.20 if monto > 5000
        else monto * 0.10 if monto > 1000
        else monto * 0.05
        for monto in monto_array.to_pylist()  # ← conversión a lista Python
    ])

# Registrar UDF:
descuento_udf = udf(
    calcular_descuento_python,
    [pa.float64()],     # tipos de input
    pa.float64(),       # tipo de output
    "stable",           # volatilidad
)
ctx.register_udf(descuento_udf)

# Alternativa eficiente: usar pyarrow.compute (sin conversión a Python):
def calcular_descuento_arrow(monto_array: pa.Array) -> pa.Array:
    """
    La misma lógica pero vectorizada con Arrow Compute.
    Sin conversión a lista Python → sin overhead de boxing.
    """
    return pc.if_else(
        pc.greater(monto_array, 5000),
        pc.multiply(monto_array, 0.20),
        pc.if_else(
            pc.greater(monto_array, 1000),
            pc.multiply(monto_array, 0.10),
            pc.multiply(monto_array, 0.05),
        )
    )

# Medir la diferencia:
t_lento = time.perf_counter()
ctx.sql("SELECT calcular_descuento(monto) FROM ventas").collect()
print(f"UDF Python lento: {time.perf_counter()-t_lento:.2f}s")

# ¿Hay una forma de expresar esto con SQL puro (sin UDF)?
t_sql = time.perf_counter()
ctx.sql("""
    SELECT CASE
        WHEN monto > 5000 THEN monto * 0.20
        WHEN monto > 1000 THEN monto * 0.10
        ELSE monto * 0.05
    END as descuento
    FROM ventas
""").collect()
print(f"SQL nativo: {time.perf_counter()-t_sql:.2f}s")
```

**Preguntas:**

1. ¿Por qué la UDF Python que usa `.to_pylist()` es más lenta que el SQL nativo?

2. ¿La versión con `pyarrow.compute` es más rápida que la versión Python pura?
   ¿Por qué?

3. ¿Para qué tipo de lógica necesitas una UDF que no puede expresarse en SQL?

4. ¿DataFusion puede llamar a UDFs escritas en Rust con rendimiento nativo?

**Pista:** `.to_pylist()` convierte cada valor de Arrow a un objeto Python —
boxing/unboxing de tipos, allocación por cada valor. Para 1M filas,
son 1M allocaciones de objetos Python. La versión `pyarrow.compute` opera
sobre el buffer de Arrow directamente con operaciones vectorizadas en C/Rust —
sin conversión a Python, sin allocaciones intermedias.
La regla: dentro de una UDF de DataFusion, usar `pyarrow.compute` cuando sea
posible, Python puro solo cuando sea inevitable.

---

### Ejercicio 7.5.4 — DataFusion como backend de Pandas: Ibis y similares

```python
# Ibis es una librería que provee una API de DataFrame "backend-agnostic"
# que puede compilar a DataFusion, DuckDB, Spark, BigQuery, etc.

# pip install ibis-framework[datafusion]
import ibis

# Conectar al backend DataFusion:
con = ibis.datafusion.connect({
    "ventas": "transacciones.parquet",
    "clientes": "clientes.parquet",
})

# Escribir la query con la API de Ibis (similar a Pandas/Polars):
ventas = con.table("ventas")
clientes = con.table("clientes")

resultado = (
    ventas
    .join(clientes, ventas.user_id == clientes.id)
    .filter(ventas.monto > 100)
    .group_by(["region", "segmento"])
    .aggregate(
        revenue=ventas.monto.sum(),
        clientes_unicos=ventas.user_id.nunique(),
    )
    .order_by(ibis.desc("revenue"))
)

# Ibis compila a SQL de DataFusion y ejecuta:
df = resultado.to_pandas()  # o .to_arrow(), .to_polars()
```

**Preguntas:**

1. ¿Qué ventaja tiene Ibis sobre escribir SQL directamente?

2. ¿El SQL que genera Ibis es eficiente? ¿O añade overhead?

3. ¿Puedes cambiar el backend de Ibis de DataFusion a Spark
   sin cambiar el código de análisis?

4. ¿Cuándo Ibis es una buena elección? ¿Cuándo es innecesaria complejidad?

> 🔗 Ecosistema: Ibis Project (ibis-project.org) es un framework que
> unifica múltiples backends de data analytics bajo una sola API.
> Soporta DataFusion, DuckDB, Spark, BigQuery, Snowflake, y más.
> Es relevante cuando necesitas código portátil entre backends o
> quieres una API más ergonómica que SQL raw.

---

### Ejercicio 7.5.5 — Leer: DataFusion en un servicio web de analytics

**Tipo: Diseñar/analizar**

Una empresa quiere construir un servicio web que permite a sus clientes
hacer queries SQL sobre sus propios datos (data tenancy):

```
Arquitectura:
  Cliente A → API HTTP → Motor de queries → Datos de A en S3
  Cliente B → API HTTP → Motor de queries → Datos de B en S3
  (aislamiento total: A nunca ve datos de B)

Requisitos:
  - < 1 segundo de latencia para queries simples
  - Hasta 100 queries concurrentes
  - Datos de cada cliente en S3, particionados por fecha
  - Clientes pueden hacer cualquier SELECT (no INSERT/DELETE)
```

¿Por qué DataFusion es una buena elección para este servicio?
¿Cómo diseñarías la arquitectura?

1. ¿Cómo aíslas los datos entre clientes usando `SessionContext`?
2. ¿Cómo manejas 100 queries concurrentes?
3. ¿Cómo limitas el tiempo de ejecución de cada query?
4. ¿Cómo controlas el uso de memoria por query?
5. ¿Cuándo esta arquitectura escalaría y cuándo necesitarías Spark en su lugar?

**Pista:** La clave del aislamiento: crear un `SessionContext` separado por
cliente (o por query). Cada `SessionContext` tiene su propio catálogo de tablas —
no hay forma de que el cliente A acceda a las tablas registradas del cliente B.
Para 100 queries concurrentes: DataFusion es async (tokio) — puede manejar
muchas queries concurrentes en un solo proceso con múltiples threads.
El límite de tiempo: tokio permite cancelar futures con timeout.
El límite de memoria: más difícil — DataFusion no tiene límites de memoria por
sesión en 2024. Para producción con clientes no confiables, necesitas
sandboxing adicional (procesos separados).

---

## Sección 7.6 — Extensibilidad: UDFs y Reglas de Optimización

### Ejercicio 7.6.1 — UDFs en Rust: rendimiento nativo

```rust
use datafusion::logical_expr::{create_udf, Volatility};
use datafusion::arrow::array::{ArrayRef, Float64Array};
use datafusion::arrow::datatypes::DataType;
use std::sync::Arc;

// UDF en Rust: rendimiento nativo, sin overhead de Python
fn descuento_tiered(args: &[ArrayRef]) -> datafusion::error::Result<ArrayRef> {
    let monto_array = args[0]
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("Expected float64 array");

    // Vectorizado en Rust — sin allocaciones por elemento
    let descuentos: Float64Array = monto_array
        .iter()
        .map(|monto| monto.map(|m| {
            if m > 5000.0 { m * 0.20 }
            else if m > 1000.0 { m * 0.10 }
            else { m * 0.05 }
        }))
        .collect();

    Ok(Arc::new(descuentos))
}

// Registrar en DataFusion:
let udf = create_udf(
    "descuento_tiered",                    // nombre en SQL
    vec![DataType::Float64],               // tipos de input
    Arc::new(DataType::Float64),           // tipo de output
    Volatility::Immutable,                 // misma entrada → mismo resultado
    Arc::new(descuento_tiered),            // la función
);

ctx.register_udf(udf);

// Usar en SQL:
// SELECT descuento_tiered(monto) FROM ventas
```

**Preguntas:**

1. ¿Cuánto más rápida es la UDF en Rust respecto a la UDF en Python
   del ejercicio anterior?

2. ¿Por qué declarar la UDF como `Immutable` importa para el optimizador?

3. ¿La UDF en Rust puede acceder a estado externo (una base de datos, un cache)?
   ¿Cómo?

4. ¿Puedes vectorizar la UDF usando instrucciones SIMD explícitas en Rust?

5. ¿DataFusion puede compilar (JIT) UDFs en Python a código nativo?
   ¿Existe algo equivalente?

---

### Ejercicio 7.6.2 — UDAFs: funciones de agregación personalizadas

```rust
use datafusion::logical_expr::{create_udaf, Accumulator, AggregateUDF};

// Implementar una función de agregación custom: mediana aproximada
// usando el algoritmo T-Digest
struct MedianaAprox {
    valores: Vec<f64>,  // simplificado — en producción usar T-Digest
}

impl Accumulator for MedianaAprox {
    fn state(&self) -> datafusion::error::Result<Vec<ScalarValue>> {
        // Serializar el estado para merges distribuidos
        Ok(vec![ScalarValue::List(/* serializar valores */)])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion::error::Result<()> {
        // Añadir nuevos valores al acumulador
        let arr = values[0].as_any().downcast_ref::<Float64Array>().unwrap();
        self.valores.extend(arr.iter().flatten());
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion::error::Result<()> {
        // Combinar con otro acumulador (para paralelismo)
        // (equivalente al combiner de Map/Reduce)
        todo!()
    }

    fn evaluate(&self) -> datafusion::error::Result<ScalarValue> {
        // Calcular el resultado final
        let mut sorted = self.valores.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let mediana = sorted[sorted.len() / 2];
        Ok(ScalarValue::Float64(Some(mediana)))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(&self.valores) + self.valores.len() * 8
    }
}
```

**Preguntas:**

1. ¿El `merge_batch` del UDAF es el equivalente al combiner de Map/Reduce?
   ¿Por qué es importante?

2. ¿Para calcular la mediana exacta, necesitas que todos los valores
   lleguen al mismo `Accumulator`? ¿O puede hacerse distribuido?

3. ¿DataFusion puede usar UDAFs de Python (no solo Rust)?

4. ¿Cuándo implementar un UDAF personalizado vs usar una función aproximada
   nativa como `approx_percentile_cont`?

---

### Ejercicio 7.6.3 — Table Functions: generar datos dinámicamente

```rust
// Table functions: funciones que generan tablas (no valores escalares)
// Ejemplo: una función que genera una tabla de series temporales

// En SQL: SELECT * FROM generate_series('2024-01-01', '2024-12-31', '1 day')
// Retorna una tabla con una columna de fechas

use datafusion::logical_expr::TableFunctionDefinition;

fn generate_series(args: &[Expr]) -> datafusion::error::Result<LogicalPlan> {
    // Implementar la lógica de generación de la serie
    // Retorna un plan lógico que genera los datos
    todo!()
}
```

**Preguntas:**

1. ¿Cuándo es útil una table function vs una CTE (Common Table Expression)?

2. ¿DataFusion tiene table functions predefinidas (como `generate_series`)?

3. Da tres ejemplos de table functions útiles para data engineering.

4. ¿Una table function puede leer de una fuente externa (API HTTP)?

---

### Ejercicio 7.6.4 — Catálogos y schemas: organizar múltiples fuentes

```python
import datafusion
from datafusion import SessionContext

ctx = SessionContext()

# Organizar tablas en catálogos y schemas (similar a databases/schemas en SQL):
# catalog.schema.tabla

# Registrar tablas con nombres completamente calificados:
ctx.register_parquet(
    "produccion.ventas.transacciones",  # catálogo.schema.tabla
    "s3://prod/ventas/transacciones.parquet"
)
ctx.register_parquet(
    "produccion.clientes.perfiles",
    "s3://prod/clientes/perfiles.parquet"
)
ctx.register_parquet(
    "staging.ventas.transacciones",
    "s3://staging/ventas/transacciones.parquet"
)

# Query que cruza entornos (producción vs staging):
resultado = ctx.sql("""
    SELECT
        p.region,
        SUM(p.monto) as produccion,
        SUM(s.monto) as staging,
        SUM(p.monto) - SUM(s.monto) as diferencia
    FROM produccion.ventas.transacciones p
    JOIN staging.ventas.transacciones s
        ON p.user_id = s.user_id
        AND p.fecha = s.fecha
    GROUP BY p.region
""")
```

**Preguntas:**

1. ¿DataFusion soporta catálogos y schemas por defecto?

2. ¿Puedes conectar DataFusion a un catálogo externo (Hive Metastore, Glue)?

3. ¿Para qué escenario es útil tener múltiples catálogos en el mismo contexto?

---

### Ejercicio 7.6.5 — Leer: el ecosistema que usa DataFusion

**Tipo: Leer/investigar**

DataFusion es usado como componente en muchos proyectos del ecosistema Arrow.
Para cada proyecto, entender qué parte de DataFusion usa y por qué:

```
1. delta-rs (Delta Lake en Rust)
   → Usa DataFusion para: ???
   → Por qué DataFusion y no DuckDB o SQLite: ???

2. InfluxDB IOx (base de datos de series temporales)
   → Usa DataFusion para: ???
   → Qué extensiones de DataFusion implementa: ???

3. Ballista (Spark-like sobre DataFusion)
   → Usa DataFusion para: ???
   → Qué añade Ballista sobre DataFusion: ???

4. Comet (acelerador de Spark con DataFusion)
   → Qué es Comet y cómo usa DataFusion: ???
   → Qué operadores de Spark acelera con DataFusion/Arrow: ???
```

> 🔗 Ecosistema: Apache Arrow Comet es especialmente interesante —
> es un plugin de Spark que reemplaza algunos operadores del plan físico
> de Spark con implementaciones de DataFusion/Arrow escritas en Rust.
> Para ciertas queries, puede acelerar Spark 2-5× sin cambiar el código.
> Su estado en 2024: incubating en Apache Software Foundation.

---

## Sección 7.7 — Cuándo DataFusion, Cuándo Polars, Cuándo Spark

### Ejercicio 7.7.1 — La matriz de decisión completa

**Tipo: Construir**

Completar la tabla con los criterios de decisión:

```
Criterio                 DataFusion  Polars  DuckDB  Spark
──────────────────────────────────────────────────────────────────
Datos > 1 TB              ✗           ✗       ✗       ✓
Datos < 10 GB             ✓           ✓       ✓       ✗ (overhead)
API DataFrame             ✗ (*)       ✓       ✗       ✓
Interfaz SQL              ✓           ✓(*)    ✓       ✓
Embebible en app          ✓           ✓       ✓       ✗
Extensible (nuevas        ✓           ✗       ✗       ✓
  fuentes/funciones)
Sin proceso externo       ✓           ✓       ✓       ✗
Streaming de eventos      ✗           ✗       ✗       ✓
Delta Lake nativo         ✓(delta-rs) ✓(*)    ✓(*)    ✓
Multi-tenancy             ✓           ✗       ✗       ✓
Fault tolerance           ✗           ✗       ✗       ✓
```

(*) Con limitaciones o extensiones de terceros

**Restricciones:**
1. Completar todas las celdas marcadas como ???
2. Añadir dos criterios más que consideres importantes
3. ¿Hay un caso donde todos los sistemas son igualmente válidos?

---

### Ejercicio 7.7.2 — Casos de uso concretos

Para cada caso, elegir la herramienta más apropiada y justificar en una oración:

```
1. Una startup quiere añadir "analytics en el producto":
   los usuarios pueden hacer queries SQL sobre sus propios datos.
   Dataset por usuario: 1-100 GB. Latencia requerida: < 2 segundos.
   Equipo: 3 engineers de backend (Rust/Python).

2. Un equipo de data science procesa datasets de 5-50 GB
   para feature engineering. Iteran rápido en notebooks.
   No tienen un cluster de Spark.

3. Un data engineer mantiene un pipeline batch diario
   que procesa 2 TB de logs para un reporte de negocio.
   El cluster de EMR ya existe.

4. Un equipo quiere construir un "semantic layer": una herramienta
   que permite definir métricas de negocio y queries pre-computadas
   que se sirven a múltiples herramientas de BI.

5. Un ingeniero quiere explorar un dataset de 200 GB de CSV
   desde su laptop con 16 GB de RAM para una investigación ad-hoc.
```

---

### Ejercicio 7.7.3 — Integración: DataFusion + Polars + Delta Lake

**Tipo: Implementar**

Implementar un pipeline que usa cada herramienta donde es más apropiada:

```python
import datafusion
from datafusion import SessionContext
import polars as pl
from deltalake import DeltaTable, write_deltalake
import pyarrow as pa

# Escenario: servicio de analytics que:
# 1. Recibe datos crudos de múltiples fuentes (Polars para preprocesar)
# 2. Almacena en Delta Lake (delta-rs)
# 3. Sirve queries SQL de usuarios (DataFusion)

# Paso 1: Polars preprocesa datos crudos (eficiente, ergonómico)
def preprocesar_datos(ruta_crudo: str) -> pa.Table:
    return (pl.scan_parquet(ruta_crudo)
        .filter(pl.col("monto") > 0)
        .with_columns([
            pl.col("region").str.to_lowercase(),
            (pl.col("monto") * 1.19).alias("monto_con_iva"),
        ])
        .collect()
        .to_arrow()
    )

# Paso 2: delta-rs escribe a Delta Lake (ACID, time travel)
def guardar_en_delta(tabla_arrow: pa.Table, ruta_delta: str):
    write_deltalake(
        ruta_delta,
        tabla_arrow,
        mode="append",
        partition_by=["region"],
    )

# Paso 3: DataFusion sirve queries (embebible, extensible)
class ServicioAnalytics:
    def __init__(self, ruta_delta: str):
        self.ctx = SessionContext()
        # Registrar la tabla Delta Lake en DataFusion:
        self.ctx.register_parquet(
            "ventas",
            f"{ruta_delta}/**/*.parquet",
        )

    def query(self, sql: str) -> pa.Table:
        return pa.Table.from_batches(
            self.ctx.sql(sql).collect()
        )

# Uso:
tabla = preprocesar_datos("datos_crudos/*.parquet")
guardar_en_delta(tabla, "s3://mi-delta/ventas/")

servicio = ServicioAnalytics("s3://mi-delta/ventas/")
resultado = servicio.query(
    "SELECT region, SUM(monto_con_iva) FROM ventas GROUP BY region"
)
```

**Restricciones:**
1. Implementar el pipeline completo y verificar que funciona
2. ¿El servicio puede actualizar la tabla Delta mientras hay queries activas?
3. ¿Cómo añadirías time travel al servicio (queries sobre versiones anteriores)?
4. ¿Cómo escalarías el servicio para 1,000 queries concurrentes?

---

### Ejercicio 7.7.4 — Leer: el futuro del ecosistema Arrow/Rust en data engineering

**Tipo: Analizar**

El ecosistema Arrow/Rust está ganando terreno en data engineering.
Analizar las tendencias y sus implicaciones:

```
Tendencia 1: El GIL de Python va a desaparecer (PEP 703, Python 3.13)
  → ¿Cómo cambia la ecuación Polars/DataFusion vs Pandas?

Tendencia 2: Apache Arrow Comet acelera Spark con DataFusion
  → ¿Spark JVM + DataFusion Rust: lo mejor de los dos mundos?

Tendencia 3: DuckDB se vuelve omnipresente como "SQLite para analytics"
  → ¿Cuál es el nicho de DataFusion si DuckDB cubre el mismo espacio?

Tendencia 4: Streaming y batch convergen (Flink, Spark Streaming)
  → ¿DataFusion puede añadir streaming y competir con Flink?
```

**Preguntas:**

1. ¿El fin del GIL en Python amenaza la propuesta de valor de Polars?

2. Si Comet madura, ¿tiene sentido migrar de Spark a DataFusion/Polars?

3. ¿Cuál es el nicho de DataFusion en un mundo donde DuckDB es popular?

4. ¿En 3-5 años, DataFusion podría reemplazar a Spark para workloads medianos?

> 📖 Profundizar: el post *"Apache Arrow: 5 Years of Development"*
> (Wes McKinney, 2021) y el estado del ecosistema Arrow en los blogs
> de Apache Software Foundation muestran la trayectoria del proyecto
> y hacia dónde va el ecosistema.

---

### Ejercicio 7.7.5 — El pipeline completo de la Parte 2

**Tipo: Integrar**

Este es el ejercicio de cierre de la Parte 2 del repositorio
(Cap.04-07: batch processing con Spark, Polars, DataFusion).

Diseñar el pipeline batch del sistema de e-commerce usando
la herramienta correcta para cada componente:

```
Datos de entrada:
  - 5 TB/día de eventos en S3 (Parquet, particionado por hora)
  - Catálogo de productos: 10 GB en PostgreSQL
  - Datos de clientes: 50 GB en S3 (actualización diaria)
  - Tipos de cambio: 1 MB, actualización cada hora via API REST

Pipeline diario:
  1. Leer y limpiar datos crudos de eventos (filtrar nulls, normalizar)
  2. Join con catálogo de productos y clientes
  3. Calcular métricas de negocio (revenue, conversión, top productos)
  4. Escribir resultados a Delta Lake para BI
  5. Exponer métricas via SQL endpoint para dashboards
```

Para cada paso, especificar:
- ¿Qué herramienta? (Spark, Polars, DataFusion, u otra)
- ¿Por qué esa herramienta para ese paso específico?
- ¿Qué formato de datos en la interfaz entre pasos?
- ¿Cuánto tiempo estimado?

---

## Resumen del capítulo

**DataFusion en una frase:**

> DataFusion es un motor SQL embebible y extensible en Rust,
> construido sobre Arrow — para construir herramientas de datos,
> no para ser una herramienta de datos.

**Las tres situaciones donde DataFusion es la respuesta correcta:**

```
1. Construyes una herramienta que necesita SQL
   (analytics embebido en un producto, semantic layer, servicio de queries)
   → DataFusion te da el motor; tú construyes la capa de valor.

2. Necesitas extensibilidad máxima del motor SQL
   (fuentes de datos custom, funciones de optimización propias, 
    integración profunda con el stack Rust/Arrow)
   → DataFusion es extensible hasta el core; DuckDB y Polars menos.

3. Construyes sobre el stack Arrow/Rust (delta-rs, Ballista, IOx)
   → El ecosistema ya usa DataFusion; integrarte es natural.
```

**Las tres situaciones donde DataFusion NO es la respuesta:**

```
1. Solo quieres procesar datos (no construir herramientas)
   → Polars o DuckDB son más simples y tienen mejor UX.

2. Tus datos son más grandes que una máquina
   → DataFusion no es distribuido. Ballista lo es, pero es menos maduro que Spark.

3. Necesitas streaming de eventos con estado y windowing
   → DataFusion no tiene estas capacidades. Flink (Cap.12) es la respuesta.
```

**La conexión con el Cap.08 (El Lakehouse):**

> DataFusion, Polars, y Spark son motores de procesamiento.
> El Cap.08 cubre el *almacenamiento*: Delta Lake, Iceberg, Hudi.
> La frontera entre procesamiento y almacenamiento se está difuminando —
> delta-rs usa DataFusion, Spark es el motor principal de Delta Lake,
> y Polars puede leer y escribir a ambos formatos.
> El lakehouse es la arquitectura que une procesamiento y almacenamiento
> en un modelo coherente.
