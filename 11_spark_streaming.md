# Guía de Ejercicios — Cap.11: Spark Structured Streaming

> Spark Structured Streaming resuelve el mismo problema que Flink —
> procesar streams de eventos con semántica correcta —
> pero lo hace desde la perspectiva de alguien que ya conoce Spark SQL.
>
> La apuesta de diseño: el mismo código que escribe un batch job
> debe poder ejecutarse como streaming job con cambios mínimos.
> Un DataFrame de ventas históricas y un DataFrame de ventas en tiempo real
> deben ser intercambiables, en la medida de lo posible.
>
> Esto simplifica el onboarding. Pero tiene un costo:
> el modelo de micro-batching introduce latencia que el streaming puro de Flink
> no tiene. Entender ese tradeoff es el centro de este capítulo.

---

## El modelo mental: streaming como batch infinito

Spark Structured Streaming trata un stream como una tabla que crece
indefinidamente. Cada micro-batch lee los nuevos registros y los procesa
como si fueran un batch pequeño:

```
t=0:  tabla = []
t=1s: tabla = [evento_1, evento_2, evento_3]          → procesar batch 1
t=2s: tabla = [evento_4, evento_5]                    → procesar batch 2
t=3s: tabla = [evento_6, evento_7, evento_8, evento_9] → procesar batch 3
...   tabla = [eventos infinitos]                     → procesar indefinidamente
```

```
Ventajas del modelo de micro-batching:
  + El mismo código SQL/DataFrame funciona en batch y streaming
  + Garantías de exactly-once con Kafka y sinks idempotentes
  + Checkpointing automático para recuperación de fallos
  + Integración natural con el resto del ecosistema Spark

Limitaciones:
  - Latencia mínima ≈ duración del micro-batch (típicamente segundos)
  - El "continuous mode" intenta reducir esto pero es experimental
  - No es apropiado para casos que requieren sub-segundo
```

---

## Tabla de contenidos

- [Sección 11.1 — El modelo de ejecución: micro-batching](#sección-111--el-modelo-de-ejecución-micro-batching)
- [Sección 11.2 — Fuentes y sinks: leer y escribir streams](#sección-112--fuentes-y-sinks-leer-y-escribir-streams)
- [Sección 11.3 — Estado y ventanas: más allá del batch](#sección-113--estado-y-ventanas-más-allá-del-batch)
- [Sección 11.4 — Watermarks: manejar datos tardíos](#sección-114--watermarks-manejar-datos-tardíos)
- [Sección 11.5 — Output modes: append, update, complete](#sección-115--output-modes-append-update-complete)
- [Sección 11.6 — Joins en streaming: stream-stream y stream-static](#sección-116--joins-en-streaming-stream-stream-y-stream-static)
- [Sección 11.7 — Operacionalizar: checkpoints, fallos, monitoreo](#sección-117--operacionalizar-checkpoints-fallos-monitoreo)

---

## Sección 11.1 — El Modelo de Ejecución: Micro-batching

### Ejercicio 11.1.1 — Leer: micro-batching vs streaming continuo

**Tipo: Leer/comparar**

```
Micro-batching (Spark Structured Streaming):
  El stream se divide en micro-batches.
  Cada micro-batch es un job de Spark normal.
  
  t=0ms:   llegan eventos   →  acumulados en buffer
  t=1000ms: trigger          →  procesar batch (job Spark)
  t=1500ms: batch completado →  resultados escritos, offset commiteado
  t=2000ms: trigger          →  procesar siguiente batch
  
  Latencia mínima: duración del micro-batch (típicamente 1-30 segundos)
  Throughput: alto (los batches amortizan el overhead de Spark)

Streaming continuo (Flink, Kafka Streams):
  Cada evento se procesa individualmente, sin acumulación.
  
  t=0ms:   llega evento_1   →  procesado inmediatamente
  t=1ms:   llega evento_2   →  procesado inmediatamente
  t=2ms:   llega evento_3   →  procesado inmediatamente
  
  Latencia: milisegundos (o menos)
  Throughput: menor overhead por evento (sin batch overhead)
              pero sin el beneficio de procesar en bulk
```

**Preguntas:**

1. Para un sistema de detección de fraude que necesita responder en < 100ms,
   ¿es Spark Structured Streaming adecuado? ¿Por qué?

2. Para calcular métricas de negocio cada 5 minutos sobre clicks de usuarios,
   ¿cuál tiene más sentido: micro-batching o streaming continuo?

3. El "continuous mode" experimental de Spark Structured Streaming
   promete sub-segundo. ¿Qué sacrifica para lograrlo?

4. Si el micro-batch tarda 800ms pero llegan eventos más rápido de lo que
   se procesan, ¿qué ocurre?

5. ¿Cuál es la diferencia entre `trigger(processingTime="1 second")`,
   `trigger(once=True)`, y `trigger(availableNow=True)`?

**Pista:** El "continuous mode" sacrifica algunas garantías de exactly-once
y soporte de operaciones con estado complejo. El processing time trigger de 1s
significa "intentar procesar un batch cada segundo" — si el batch tarda 2s,
el siguiente se retrasa. Si los eventos llegan más rápido que el procesamiento,
el lag de Kafka aumenta continuamente — symptoma de que el pipeline no puede
mantener el ritmo (`spark.streaming.backpressure.enabled=true` puede ayudar).

---

### Ejercicio 11.1.2 — El primer streaming job

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

spark = SparkSession.builder \
    .appName("mi-primer-streaming") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# Schema del evento (siempre explícito en streaming):
schema_evento = StructType([
    StructField("evento_id", StringType(), nullable=False),
    StructField("user_id", StringType(), nullable=False),
    StructField("tipo", StringType(), nullable=True),
    StructField("monto", DoubleType(), nullable=True),
    StructField("timestamp", TimestampType(), nullable=False),
    StructField("region", StringType(), nullable=True),
])

# Fuente: leer de Kafka
df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "eventos") \
    .option("startingOffsets", "latest") \
    .load()

# Los mensajes de Kafka llegan como (key: binary, value: binary).
# Parsear el valor JSON:
df_eventos = df_stream.select(
    F.from_json(
        F.col("value").cast("string"),
        schema_evento
    ).alias("evento")
).select("evento.*")

# Transformación: filtrar y enriquecer
df_transformado = df_eventos \
    .filter(F.col("tipo").isin(["compra", "devolucion"])) \
    .withColumn("monto_iva", F.col("monto") * 1.19) \
    .withColumn("hora_procesamiento", F.current_timestamp())

# Sink: escribir a consola (para desarrollo)
query = df_transformado.writeStream \
    .format("console") \
    .option("truncate", False) \
    .option("numRows", 20) \
    .trigger(processingTime="5 seconds") \
    .start()

query.awaitTermination()
```

**Preguntas:**

1. ¿Por qué el schema es obligatorio en streaming y Spark no puede inferirlo?

2. ¿`startingOffsets="latest"` vs `"earliest"` — cuándo usar cada uno?

3. ¿Qué contiene el `value` de un mensaje de Kafka antes del `from_json`?

4. ¿El `filter` en streaming es un narrow o wide transformation?
   ¿Genera algún shuffle?

5. Si el job se reinicia después de un fallo, ¿reprocesa los mensajes
   desde el principio o desde donde quedó?

**Pista:** Spark no puede inferir el schema en streaming porque inferirlo
requiere leer datos primero — y en streaming los datos no han llegado
cuando se construye el plan. Sin schema explícito, Spark no sabe cómo
parsear los bytes del `value` de Kafka. La respuesta a la pregunta 5
depende del checkpoint: con checkpoint habilitado, el job retoma desde
el último offset commitado. Sin checkpoint, depende de `startingOffsets`.

---

### Ejercicio 11.1.3 — Entender el checkpoint internamente

```python
# El checkpoint de Spark Structured Streaming guarda tres cosas:
# 1. Offsets: qué mensajes ya se procesaron (para no reprocesar)
# 2. State: el estado de operaciones stateful (aggregations, joins)
# 3. Commits: confirmación de que el batch N fue completado y escrito

# Estructura del directorio de checkpoint:
# checkpoint/
#   offsets/
#     0          ← batch 0: {kafka-topic-0: {0: 100, 1: 95, 2: 103}}
#     1          ← batch 1: {kafka-topic-0: {0: 247, 1: 201, 2: 198}}
#   commits/
#     0          ← batch 0 completado
#     1          ← batch 1 completado
#   state/
#     0/         ← estado del operator 0 (aggregation)
#       0.delta  ← delta de estado del batch 0
#       1.delta  ← delta de estado del batch 1

query = df_con_aggregacion.writeStream \
    .format("delta") \
    .option("checkpointLocation", "s3://checkpoints/job-metricas/") \
    .outputMode("update") \
    .trigger(processingTime="30 seconds") \
    .start()

# Después de un fallo:
# 1. Spark lee el checkpoint: "el último batch completado fue el 15"
# 2. Spark lee los offsets del batch 16 (que empezó pero no terminó)
# 3. Spark re-ejecuta el batch 16 desde los mismos offsets
# 4. Si el sink es idempotente → exactly-once
# 5. Si el sink no es idempotente → at-least-once
```

**Preguntas:**

1. Si el job falla justo después de escribir al sink pero antes de commitear
   el checkpoint, ¿qué pasa al reiniciar?

2. ¿El checkpoint puede corromperse? ¿Cuándo y cómo lo detectas?

3. Si cambias el código del streaming job y reinicias con el mismo checkpoint,
   ¿funciona siempre? ¿Qué cambios son compatibles y cuáles no?

4. ¿Cuánto espacio ocupa el checkpoint a lo largo del tiempo?
   ¿Se limpia automáticamente?

5. ¿Puedes mover el checkpoint de S3 a HDFS sin reiniciar desde cero?

**Pista:** Los cambios compatibles con el checkpoint existente:
añadir columnas computadas (narrow transformations), cambiar el sink,
cambiar el trigger interval. Cambios incompatibles: añadir o eliminar
operaciones stateful (groupBy, join), cambiar el schema de la fuente,
cambiar el número de particiones del shuffle. Para cambios incompatibles,
hay que borrar el checkpoint y reiniciar desde cero (perdiendo el estado acumulado).

---

### Ejercicio 11.1.4 — Medir la latencia end-to-end

```python
from pyspark.sql import functions as F

# Medir latencia: tiempo desde que el evento se originó hasta
# que aparece en el resultado del streaming job

schema = StructType([
    StructField("evento_id", StringType()),
    StructField("timestamp_origen", TimestampType()),  # cuando ocurrió el evento
    StructField("monto", DoubleType()),
])

df_stream = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "eventos") \
    .load() \
    .select(F.from_json(F.col("value").cast("string"), schema).alias("e")) \
    .select("e.*")

df_con_latencia = df_stream.withColumn(
    "timestamp_procesamiento", F.current_timestamp()
).withColumn(
    "latencia_segundos",
    F.col("timestamp_procesamiento").cast("double") -
    F.col("timestamp_origen").cast("double")
)

# Sink: calcular estadísticas de latencia por micro-batch
query = df_con_latencia \
    .writeStream \
    .foreachBatch(lambda df, batch_id: (
        df.select(
            F.min("latencia_segundos").alias("latencia_min"),
            F.avg("latencia_segundos").alias("latencia_avg"),
            F.max("latencia_segundos").alias("latencia_max"),
            F.percentile_approx("latencia_segundos", 0.99).alias("latencia_p99"),
            F.count("*").alias("num_eventos"),
        ).show()
    )) \
    .trigger(processingTime="10 seconds") \
    .start()
```

**Restricciones:**
1. Implementar el pipeline y medir la distribución de latencia
2. ¿Qué componente contribuye más a la latencia: producción en Kafka,
   tiempo en el buffer de Kafka, o procesamiento en Spark?
3. ¿Cómo afecta el trigger interval a la latencia P50 vs P99?
4. ¿Qué pasa con la latencia si aumentas el throughput de eventos 10×?

---

### Ejercicio 11.1.5 — Leer: el streaming job que no puede mantener el ritmo

**Tipo: Diagnosticar**

Un streaming job de Spark presenta este comportamiento en Spark UI:

```
Batch 100: procesamiento 2.1s, input 500 eventos
Batch 101: procesamiento 2.3s, input 520 eventos
Batch 102: procesamiento 4.1s, input 980 eventos  ← trigger cada 5s pero tardó más
Batch 103: procesamiento 6.2s, input 1,450 eventos ← empeorando
Batch 104: procesamiento 9.8s, input 2,100 eventos ← lag creciente
...
Batch 120: procesamiento 45s,  input 8,900 eventos ← !!

Kafka consumer lag (topic "eventos"):
  Partition 0: 45,000 mensajes de lag
  Partition 1: 43,200 mensajes de lag
  Partition 2: 47,100 mensajes de lag
```

**Preguntas:**

1. ¿Qué ocurrió alrededor del batch 102 para que el procesamiento se disparara?

2. ¿Por qué el input de cada batch crece con el tiempo aunque el throughput
   de producción de Kafka sea constante?

3. ¿Qué configuraciones de Spark Structured Streaming pueden mitigar esto?

4. ¿El backpressure de Spark (`spark.streaming.backpressure.enabled`) ayuda
   en este caso? ¿Cómo funciona?

5. ¿Cuál es la solución definitiva si el procesamiento es estructuralmente
   más lento que la producción?

**Pista:** El input crece porque el lag acumulado se suma al input del siguiente
batch: si hay 45,000 mensajes de lag y se producen 500 más por segundo,
el siguiente batch tiene 45,500 mensajes pendientes. Sin límite en el tamaño
del batch, el job nunca puede alcanzar el ritmo de producción — la situación
empeora con cada batch. La solución: `maxOffsetsPerTrigger` limita cuántos
mensajes por batch, dando tiempo para que otros mecanismos de backpressure
reduzcan el lag gradualmente. Pero si el throughput es fundamentalmente mayor
que la capacidad de procesamiento, la solución es escalar los executors.

---

## Sección 11.2 — Fuentes y Sinks: Leer y Escribir Streams

### Ejercicio 11.2.1 — Fuentes nativas de Spark Structured Streaming

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

# 1. Kafka (la fuente más frecuente en producción):
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "eventos")                    \
    .option("startingOffsets", "latest")               \
    .option("maxOffsetsPerTrigger", 10_000)            \
    .option("kafka.security.protocol", "SASL_SSL")     \
    .load()
# Columnas: key, value, topic, partition, offset, timestamp, timestampType

# 2. Directorio de archivos (file streaming):
# Lee archivos nuevos que aparecen en un directorio
df_archivos = spark.readStream \
    .format("parquet") \
    .schema(mi_schema) \
    .option("path", "s3://landing-zone/eventos/") \
    .option("maxFilesPerTrigger", 100) \
    .load()

# 3. Rate source (para testing):
# Genera N filas por segundo, útil para probar pipelines sin Kafka real
df_rate = spark.readStream \
    .format("rate") \
    .option("rowsPerSecond", 1000) \
    .option("numPartitions", 4) \
    .load()
# Columnas: timestamp, value (contador autoincremental)

# 4. Socket (solo para demos, nunca en producción):
df_socket = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()
```

**Preguntas:**

1. ¿Por qué el socket source "nunca debe usarse en producción"?
   ¿Qué garantías le faltan?

2. El file streaming (`format("parquet")`) lee archivos nuevos en un directorio.
   ¿Cómo sabe Spark qué archivos ya procesó?

3. `maxFilesPerTrigger` y `maxOffsetsPerTrigger` sirven para controlar el
   tamaño de cada micro-batch. ¿Cuándo es importante limitarlos?

4. Si un topic de Kafka tiene 12 particiones, ¿cuántos tasks crea Spark
   para leer ese topic en cada micro-batch?

5. ¿El file streaming puede leer archivos que se están escribiendo?
   ¿Cómo maneja archivos parcialmente escritos?

**Pista:** El file streaming usa el checkpoint para recordar qué archivos
ya procesó (guarda los paths). Solo procesa archivos que aparecen
**después** de que el streaming job empieza (a menos que uses `startingOffsets`
o directorios con datos históricos). Los archivos parcialmente escritos
son un problema: Spark puede leerlos antes de que estén completos.
La solución estándar: escribir a un directorio temporal (`_tmp_`) y hacer
un rename atómico al directorio final cuando el archivo está completo.

---

### Ejercicio 11.2.2 — Sinks: escribir resultados del stream

```python
from pyspark.sql import functions as F

# 1. Kafka (escribir resultados a otro topic):
query_kafka = df_procesado \
    .select(
        F.to_json(F.struct("*")).alias("value"),
        F.col("region").alias("key"),
    ) \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "metricas-procesadas") \
    .option("checkpointLocation", "s3://checkpoints/kafka-sink/") \
    .start()

# 2. Delta Lake (el sink más recomendado para persistencia):
query_delta = df_procesado \
    .writeStream \
    .format("delta") \
    .option("checkpointLocation", "s3://checkpoints/delta-sink/") \
    .outputMode("append") \
    .partitionBy("fecha", "region") \
    .start("s3://lakehouse/eventos-procesados/")

# 3. Console (solo para desarrollo):
query_console = df_procesado \
    .writeStream \
    .format("console") \
    .option("numRows", 10) \
    .option("truncate", False) \
    .outputMode("update") \
    .start()

# 4. foreachBatch: sink personalizado
def escribir_a_postgres(df, batch_id):
    """
    Sink personalizado usando foreachBatch.
    Recibe un DataFrame de Spark (batch pequeño) y lo escribe donde quieras.
    """
    # Convertir a Pandas para upsert a PostgreSQL:
    df_pandas = df.toPandas()
    # upsert_a_postgres(df_pandas, tabla="metricas", conflict_key="id")

query_custom = df_procesado \
    .writeStream \
    .foreachBatch(escribir_a_postgres) \
    .option("checkpointLocation", "s3://checkpoints/custom-sink/") \
    .trigger(processingTime="30 seconds") \
    .start()
```

**Preguntas:**

1. ¿Por qué `foreachBatch` recibe un `batch_id`?
   ¿Para qué sirve ese número en el contexto de exactly-once?

2. Si la función de `foreachBatch` falla a mitad, ¿Spark reintenta
   el batch completo? ¿Qué implica para la idempotencia del sink?

3. ¿Delta Lake como sink garantiza exactly-once?
   ¿Qué mecanismo usa para eso?

4. ¿Qué output mode es compatible con el sink de Kafka?

5. Si escribes a PostgreSQL con `foreachBatch` sin idempotencia,
   ¿qué garantía de delivery tienes?

**Pista:** `batch_id` sirve precisamente para idempotencia: si el batch
se reintenta (fallo tras escribir pero antes del checkpoint), tendrá
el mismo `batch_id`. Tu sink puede verificar si ese `batch_id` ya fue
procesado (ej: guardar `batch_id` en una tabla de control en PostgreSQL).
Delta Lake es exactly-once porque usa transacciones ACID: si el batch
ya fue escrito (el transaction log lo registra), la re-escritura con el
mismo `batch_id` es un no-op. PostgreSQL sin este control es at-least-once.

---

### Ejercicio 11.2.3 — Leer de múltiples topics de Kafka

```python
from pyspark.sql import functions as F
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()

# Opción A: suscripción a múltiples topics
df_multi = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "clicks,compras,devoluciones") \
    .load()

# Distinguir qué topic generó cada mensaje:
df_con_topic = df_multi.withColumn(
    "tipo_evento", F.col("topic")
)

# Opción B: patrón regex para suscribirse a múltiples topics
df_patron = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribePattern", "eventos-.*")  # regex: eventos-clicks, eventos-compras, etc.
    .load()

# Opción C: leer de un topic con múltiples tipos de eventos en el value:
schema_union = StructType([
    StructField("tipo", StringType()),
    StructField("id", StringType()),
    StructField("timestamp", TimestampType()),
    StructField("payload", StringType()),  # JSON anidado diferente por tipo
])

df_eventos = spark.readStream \
    .format("kafka") \
    .option("subscribe", "todos-los-eventos") \
    .load() \
    .select(
        F.from_json(F.col("value").cast("string"), schema_union).alias("e")
    ).select("e.*")

# Separar por tipo para procesamiento específico:
df_clicks = df_eventos.filter(F.col("tipo") == "click")
df_compras = df_eventos.filter(F.col("tipo") == "compra")
```

**Restricciones:**
1. Implementar el pipeline con múltiples topics y un schema union
2. ¿Cómo manejas el caso donde el schema varía entre topics?
3. ¿Hay implicaciones de rendimiento en suscribirse a muchos topics vs uno?
4. Si un topic tiene alta tasa y otro muy baja, ¿cómo afecta al micro-batch?

---

### Ejercicio 11.2.4 — foreachBatch para sinks complejos

```python
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import psycopg2
from contextlib import contextmanager

@contextmanager
def conexion_postgres(dsn: str):
    conn = psycopg2.connect(dsn)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def sink_postgres_idempotente(df: DataFrame, batch_id: int) -> None:
    """
    Sink idempotente a PostgreSQL usando foreachBatch.
    
    Idempotencia: si el mismo batch_id se procesa dos veces,
    el resultado final es el mismo que si se procesó una vez.
    
    Estrategia: UPSERT con batch_id en tabla de control.
    """
    # Verificar si este batch ya fue procesado:
    with conexion_postgres(DSN) as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM batch_control WHERE batch_id = %s",
            (batch_id,)
        )
        ya_procesado = cur.fetchone()[0] > 0

    if ya_procesado:
        print(f"Batch {batch_id} ya procesado, skipping")
        return

    # Convertir a Pandas y hacer upsert:
    df_pandas = df.toPandas()

    with conexion_postgres(DSN) as conn:
        cur = conn.cursor()

        # Upsert de los datos:
        for _, fila in df_pandas.iterrows():
            cur.execute("""
                INSERT INTO metricas (region, mes, revenue, batch_id)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (region, mes) DO UPDATE
                SET revenue = EXCLUDED.revenue,
                    batch_id = EXCLUDED.batch_id
            """, (fila["region"], fila["mes"], fila["revenue"], batch_id))

        # Registrar el batch como completado:
        cur.execute(
            "INSERT INTO batch_control (batch_id, timestamp) VALUES (%s, NOW())",
            (batch_id,)
        )

# Usar en el streaming job:
query = df_metricas \
    .writeStream \
    .foreachBatch(sink_postgres_idempotente) \
    .option("checkpointLocation", "s3://checkpoints/postgres/") \
    .trigger(processingTime="60 seconds") \
    .start()
```

**Restricciones:**
1. ¿El patrón de `batch_control` garantiza exactly-once bajo todos los escenarios
   de fallo? ¿Hay una condición de carrera?
2. ¿La conversión a Pandas dentro de `foreachBatch` escala para batches grandes?
3. Implementar una versión más eficiente usando `COPY` de PostgreSQL
4. ¿Qué pasa si PostgreSQL no está disponible cuando se ejecuta el batch?

---

### Ejercicio 11.2.5 — Leer: el sink que perdía datos

**Tipo: Diagnosticar**

Un pipeline escribe a S3 como archivos Parquet usando `foreachBatch`:

```python
def escribir_a_s3(df, batch_id):
    df.write \
      .mode("overwrite") \
      .parquet(f"s3://resultados/batch_{batch_id}/")

query = df_stream.writeStream \
    .foreachBatch(escribir_a_s3) \
    .option("checkpointLocation", "s3://checkpoints/s3-sink/") \
    .start()
```

Después de una semana en producción, el equipo nota que faltan datos
de algunos días. Al investigar, encuentran que hay días donde el directorio
`s3://resultados/` no tiene archivos.

**Preguntas:**

1. ¿Puede `mode("overwrite")` en `foreachBatch` causar pérdida de datos?
   ¿En qué escenario?

2. Si el job se reinicia y re-procesa el batch 42, ¿qué ocurre con
   los datos del batch 42 que ya estaban en S3?

3. ¿Por qué hay días completos sin archivos? ¿Qué tipo de fallo
   podría causar eso?

4. ¿Cómo rediseñarías el sink para evitar estos problemas?

5. ¿Por qué Delta Lake como sink es más seguro que archivos Parquet sueltos?

**Pista:** El problema con `mode("overwrite")`: si el batch se re-ejecuta
(por fallo + checkpoint), el `overwrite` borra los datos del intento anterior
y escribe los nuevos. Si el segundo intento también falla a mitad,
los datos del primero ya se borraron pero los del segundo están incompletos.
Para días sin archivos: si todos los batches de ese día fallaron después
del `overwrite` pero antes de completar la escritura, quedan directorios vacíos
o inexistentes. Delta Lake lo evita con transacciones ACID: o el batch se
escribe completamente o no se escribe nada (rollback atómico).

---

## Sección 11.3 — Estado y Ventanas: Más Allá del Batch

### Ejercicio 11.3.1 — Stateless vs stateful operations

```python
from pyspark.sql import functions as F

# Stateless: cada evento se procesa independientemente
# Sin memoria entre micro-batches

df_stateless = df_stream \
    .filter(F.col("monto") > 100) \
    .withColumn("monto_iva", F.col("monto") * 1.19)
# Cada batch: transforma sus propios eventos, sin depender de batches anteriores

# Stateful: el resultado depende de eventos previos
# Requiere mantener estado entre micro-batches

# Aggregation stateful (cuenta total de eventos por usuario):
df_conteo_acumulado = df_stream \
    .groupBy("user_id") \
    .count()
# El conteo de user_id="alice" en el batch 100 necesita saber cuántos
# eventos de "alice" llegaron en los batches 1-99

# Deduplicación stateful:
df_deduplicado = df_stream \
    .dropDuplicates(["evento_id"])
# Necesita recordar todos los evento_ids vistos para detectar duplicados
```

**Preguntas:**

1. ¿Las operaciones stateless pueden perder datos si el job falla?

2. ¿Por qué las operaciones stateful son más costosas en términos de
   memoria y disco?

3. ¿El estado de una aggregation stateful crece indefinidamente?
   ¿Cómo lo controlas?

4. ¿`dropDuplicates` sin watermark puede causar OOM eventualmente?
   ¿Por qué?

5. ¿Cuándo una operación que parece stateful puede implementarse como stateless?

**Pista:** El estado de una aggregation sin watermark crece indefinidamente:
para `groupBy("user_id").count()`, Spark guarda el conteo de cada `user_id`
que haya visto. Con 10M usuarios activos a lo largo de 30 días, el estado
puede tener 10M entradas, cada una con su conteo. Sin watermark ni TTL,
nunca se eliminan las entradas viejas. El watermark (§11.4) permite
decir "después de X tiempo, los datos de esta ventana no llegarán más"
y así limpiar el estado asociado.

---

### Ejercicio 11.3.2 — Ventanas de tiempo: tumbling, sliding, session

```python
from pyspark.sql import functions as F

# Ventana tumbling: intervalos fijos sin solapamiento
# [0s-60s], [60s-120s], [120s-180s]
conteo_por_minuto = df_stream \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(
        F.window("timestamp", "1 minute"),  # ventana de 1 minuto
        "region"
    ) \
    .agg(
        F.count("*").alias("eventos"),
        F.sum("monto").alias("revenue"),
    )

# Ventana sliding: intervalos con solapamiento
# [0s-5min], [1min-6min], [2min-7min], ...
# Cada evento puede estar en múltiples ventanas
media_movil_5min = df_stream \
    .withWatermark("timestamp", "15 minutes") \
    .groupBy(
        F.window("timestamp", "5 minutes", "1 minute"),  # ventana 5min, slide 1min
        "region"
    ) \
    .agg(F.avg("monto").alias("media_movil"))

# Ventana de sesión: ventana dinámica que se cierra cuando no hay actividad
# [evento1, evento2, evento3] [gap=5min] [evento4, evento5]
sesiones = df_stream \
    .withWatermark("timestamp", "30 minutes") \
    .groupBy(
        F.session_window("timestamp", "5 minutes"),  # sesión con gap de 5 min
        "user_id"
    ) \
    .agg(
        F.count("*").alias("eventos_por_sesion"),
        F.sum("monto").alias("valor_sesion"),
        F.min("timestamp").alias("inicio_sesion"),
        F.max("timestamp").alias("fin_sesion"),
    )
```

**Preguntas:**

1. Un evento puede caer en múltiples ventanas sliding. ¿Cuántas ventanas
   exactamente para una ventana de "5 minutos, slide 1 minuto"?

2. ¿Las ventanas tumbling generan más o menos estado que las sliding?
   ¿Por qué?

3. Para calcular una media móvil de 7 días de datos diarios,
   ¿usarías sliding window o algo diferente?

4. ¿La ventana de sesión puede tener longitud variable?
   ¿Cuál es la longitud máxima teórica?

5. ¿Qué output mode debes usar con ventanas de tiempo?

**Pista:** Para la ventana "5 minutos, slide 1 minuto": cada evento cae en
exactamente `ceil(5min / 1min) = 5` ventanas distintas. Un evento en t=7min
está en las ventanas [3-8], [4-9], [5-10], [6-11], [7-12].
Esto significa que el estado de una sliding window es 5× mayor que el de
una tumbling window del mismo tamaño — hay 5 ventanas activas simultáneamente
por cada evento en lugar de 1.

---

### Ejercicio 11.3.3 — Estado arbitrario con flatMapGroupsWithState

```python
from pyspark.sql import functions as F
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout
from pyspark.sql.types import *
from dataclasses import dataclass
from typing import Iterator

@dataclass
class EstadoSesion:
    """Estado de una sesión de usuario."""
    primer_evento: str
    ultimo_evento: str
    num_eventos: int
    valor_total: float
    inicio: float  # timestamp unix

def actualizar_sesion(
    key: tuple,           # (user_id,)
    eventos: Iterator,    # eventos del micro-batch para este user_id
    estado: GroupState,   # estado actual de este user_id
) -> Iterator:            # resultados a emitir
    """
    Función de estado arbitrario para calcular sesiones de usuario.
    Se llama una vez por user_id por micro-batch.
    """
    import time

    GAP_SESION = 5 * 60  # 5 minutos en segundos
    user_id = key[0]

    # Obtener el estado actual:
    if estado.exists:
        sesion = estado.get
    else:
        sesion = None

    # Procesar los nuevos eventos:
    for evento in eventos:
        ts = evento["timestamp"].timestamp()

        if sesion is None:
            # Nueva sesión:
            sesion = EstadoSesion(
                primer_evento=evento["tipo"],
                ultimo_evento=evento["tipo"],
                num_eventos=1,
                valor_total=evento["monto"] or 0.0,
                inicio=ts,
            )
        elif ts - sesion.inicio > GAP_SESION:
            # Sesión expirada: emitir la sesión actual y crear nueva
            yield (user_id, sesion.num_eventos, sesion.valor_total,
                   sesion.inicio, ts)
            sesion = EstadoSesion(
                primer_evento=evento["tipo"],
                ultimo_evento=evento["tipo"],
                num_eventos=1,
                valor_total=evento["monto"] or 0.0,
                inicio=ts,
            )
        else:
            # Continuar sesión:
            sesion.ultimo_evento = evento["tipo"]
            sesion.num_eventos += 1
            sesion.valor_total += evento["monto"] or 0.0

    # Verificar timeout (si no llegaron eventos):
    if estado.hasTimedOut:
        if sesion:
            yield (user_id, sesion.num_eventos, sesion.valor_total,
                   sesion.inicio, time.time())
        estado.remove()
        return

    # Actualizar estado:
    if sesion:
        estado.update(sesion)
        # Timeout: cerrar la sesión si no llegan eventos en 5 minutos
        estado.setTimeoutDuration("5 minutes")

# Schema del resultado:
schema_resultado = StructType([
    StructField("user_id", StringType()),
    StructField("num_eventos", IntegerType()),
    StructField("valor_total", DoubleType()),
    StructField("inicio_sesion", DoubleType()),
    StructField("fin_sesion", DoubleType()),
])

# Aplicar:
df_sesiones = df_stream \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy("user_id") \
    .flatMapGroupsWithState(
        outputMode="append",
        timeoutConf=GroupStateTimeout.ProcessingTimeTimeout,
        func=actualizar_sesion,
        outputStructType=schema_resultado,
    )
```

**Preguntas:**

1. ¿Qué diferencia hay entre `mapGroupsWithState` y `flatMapGroupsWithState`?
   ¿Cuándo usar cada uno?

2. ¿El estado de `flatMapGroupsWithState` se guarda en el checkpoint?
   ¿Qué pasa si cambias el schema del estado?

3. ¿`ProcessingTimeTimeout` vs `EventTimeTimeout` — cuándo usar cada uno?

4. ¿Puede `flatMapGroupsWithState` crear más filas de output que de input?
   ¿Y menos?

5. ¿Qué pasa con el estado si un `user_id` no tiene actividad durante
   6 horas pero el timeout está en 5 minutos?

---

### Ejercicio 11.3.4 — Deduplicación en streaming

```python
from pyspark.sql import functions as F

# Deduplicación en streaming: eliminar eventos duplicados
# Problema: si un productor reintenta, puede enviar el mismo evento_id dos veces

# Sin watermark: Spark recuerda TODOS los evento_ids vistos
# → estado crece indefinidamente → OOM eventual
df_dedup_sin_watermark = df_stream \
    .dropDuplicates(["evento_id"])  # ← PELIGROSO en producción

# Con watermark: Spark solo recuerda los evento_ids de la ventana activa
# → estado acotado, pero no detecta duplicados fuera de la ventana
df_dedup_con_watermark = df_stream \
    .withWatermark("timestamp", "1 hour") \
    .dropDuplicates(["evento_id", "timestamp"])

# La diferencia crucial:
# Sin watermark: detecta duplicados de todos los tiempos (estado infinito)
# Con watermark de 1 hora: detecta duplicados de la última hora solamente
# Si un duplicado llega con más de 1 hora de retraso → no se detecta
```

**Restricciones:**
1. Medir el crecimiento del estado con y sin watermark para 1M eventos/hora
2. ¿Cuánta memoria usa el estado de deduplicación para 1 día de datos
   si el evento_id es un UUID (36 chars)?
3. Implementar una deduplicación "exacta" que usa una tabla externa
   (Redis o PostgreSQL) como store de IDs vistos
4. ¿La deduplicación con watermark garantiza exactly-once en el resultado?

---

### Ejercicio 11.3.5 — Leer: el estado que no para de crecer

**Tipo: Diagnosticar**

Un streaming job de Spark empieza con 2 GB de estado en el checkpoint
y después de 30 días tiene 180 GB. El job empieza a ser lento:

```
Día 1:  estado = 2 GB,   tiempo por batch = 5s
Día 7:  estado = 14 GB,  tiempo por batch = 8s
Día 14: estado = 42 GB,  tiempo por batch = 18s
Día 30: estado = 180 GB, tiempo por batch = 75s
```

El código:

```python
df_conteos = df_stream \
    .groupBy("user_id", F.date_trunc("day", "timestamp")) \
    .count()

query = df_conteos.writeStream \
    .format("delta") \
    .outputMode("update") \
    .option("checkpointLocation", "s3://checkpoints/conteos/") \
    .start("s3://lakehouse/conteos/")
```

**Preguntas:**

1. ¿Por qué el estado crece 6 GB por día (2 GB × 30 = 180 GB)?

2. ¿El groupBy por `(user_id, day)` acumula estado indefinidamente?
   ¿Por qué?

3. ¿Cómo el watermark resolvería este problema?

4. Si añades `withWatermark("timestamp", "2 days")`, ¿cuánto estado
   se mantiene en equilibrio?

5. ¿El dato histórico (conteos de días anteriores) se pierde al añadir
   el watermark?

**Pista:** El groupBy por `(user_id, día)` sin watermark mantiene estado
para cada combinación `(user_id, día)` vista. Con 1M usuarios activos
y 30 días, hay hasta 30M entradas en el estado. Cada entrada ocupa
~100 bytes → 3 GB solo en las claves, más el valor (count). Pero hay
un dataframe que cambia: los usuarios no siempre son activos todos los días.
Con watermark de 2 días, Spark puede descartar el estado de días más
viejos que el watermark — el estado se estabiliza en ~2 días × usuarios_activos.
El dato histórico no se pierde porque ya fue escrito a Delta Lake antes de
que el estado fuera limpiado.

---

## Sección 11.4 — Watermarks: Manejar Datos Tardíos

### Ejercicio 11.4.1 — Por qué los datos llegan tarde

```
Fuentes de tardanza en sistemas distribuidos:

1. Latencia de red variable
   Un evento ocurrió a t=10:00:05 pero llegó a Kafka a t=10:00:08.
   El delay de red varió de 10ms normal a 3 segundos esta vez.

2. Reintentos del productor
   El productor intentó enviar el evento a t=10:00:00,
   falló, reintentó a t=10:00:30. El evento tiene timestamp=10:00:00
   pero llegó a las 10:00:30.

3. Particiones de red (brain split)
   Un datacenter estuvo desconectado 2 horas.
   Los eventos buffereados llegan todos a la vez cuando se reconecta.

4. Dispositivos móviles sin conectividad
   Un evento de IoT se originó a t=09:00:00 pero el dispositivo
   estaba offline y lo envió cuando recuperó señal a t=11:30:00.

El watermark es la respuesta de Spark a esta pregunta:
"¿Cuánto tiempo espero antes de considerar que una ventana está cerrada?"
```

```python
from pyspark.sql import functions as F

# Sin watermark: esperar indefinidamente (estado infinito, nunca cerrar ventanas)
df_sin_watermark = df_stream \
    .groupBy(F.window("timestamp", "1 minute")) \
    .count()
# Las ventanas de t=10:00-10:01 nunca se "cierran" — si llega un dato tardío
# a t=10:00 mañana, aún actualiza esa ventana

# Con watermark de 10 minutos: aceptar datos con hasta 10 minutos de retraso
df_con_watermark = df_stream \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(F.window("timestamp", "1 minute")) \
    .count()
# La ventana de t=10:00-10:01 se cierra cuando el watermark supera 10:01.
# El watermark avanza cuando llegan datos con timestamp > (max_timestamp - 10min).
# Datos que llegan con más de 10 minutos de retraso son descartados.
```

**Preguntas:**

1. ¿Cómo calcula Spark el watermark actual en cada micro-batch?

2. Si todos los eventos del siguiente micro-batch tienen timestamps
   del año pasado (un bug en el productor), ¿qué pasa con el watermark?

3. ¿El watermark puede retroceder? ¿Por qué es importante que no pueda?

4. Para un sistema IoT donde los dispositivos pueden estar offline hasta
   72 horas, ¿qué watermark configuras? ¿Cuánto estado acumulas?

5. ¿Hay un tradeoff entre el tamaño del watermark y la latencia del resultado?

**Pista:** El watermark es `max(event_timestamps_seen) - watermark_delay`.
Si llegan eventos con timestamps del año pasado, el `max` de eventos vistos
no disminuye (el watermark no retrocede — solo avanza monótonamente).
Esos eventos del año pasado con timestamp muy pequeño serán considerados
"muy tardíos" (su timestamp < watermark actual) y podrían ser descartados.
El tradeoff: watermark grande → acepta más datos tardíos → más estado acumulado,
más latencia antes de emitir resultados. Watermark pequeño → menos estado,
resultados más rápidos → descarta más datos tardíos.

---

### Ejercicio 11.4.2 — Implementar: verificar datos tardíos descartados

```python
from pyspark.sql import functions as F

# Para diagnosticar cuántos datos tardíos se descartan:
def monitorear_datos_tardios(df_stream, watermark_delay: str, ventana: str):
    """
    Envuelve un stream con monitoreo de datos tardíos.
    """
    # Añadir una columna con el timestamp actual de procesamiento:
    df_con_meta = df_stream.withColumn(
        "ts_procesamiento", F.current_timestamp()
    )

    # Calcular el retraso de cada evento:
    df_con_retraso = df_con_meta.withColumn(
        "retraso_segundos",
        F.col("ts_procesamiento").cast("double") -
        F.col("timestamp").cast("double")
    )

    # El watermark en segundos (para calcular el threshold):
    # (simplificado — el watermark real es más complejo de calcular)
    watermark_segundos = parse_duration_to_seconds(watermark_delay)

    df_con_flag = df_con_retraso.withColumn(
        "tardio",
        F.col("retraso_segundos") > watermark_segundos
    )

    # Contar tardíos por micro-batch en foreachBatch:
    def reportar_tardios(df, batch_id):
        stats = df.agg(
            F.count("*").alias("total"),
            F.sum(F.col("tardio").cast("int")).alias("tardios"),
            F.avg("retraso_segundos").alias("retraso_promedio"),
            F.max("retraso_segundos").alias("retraso_maximo"),
        ).collect()[0]

        pct_tardios = 100 * stats["tardios"] / max(stats["total"], 1)
        print(f"Batch {batch_id}: {stats['total']} eventos, "
              f"{stats['tardios']} tardíos ({pct_tardios:.1f}%), "
              f"retraso promedio: {stats['retraso_promedio']:.1f}s, "
              f"máximo: {stats['retraso_maximo']:.1f}s")

    return df_con_flag, reportar_tardios
```

**Restricciones:**
1. Implementar `parse_duration_to_seconds` para strings como "10 minutes", "1 hour"
2. Integrar el monitoreo con el pipeline del ejercicio anterior
3. Simular datos tardíos y verificar que el porcentaje coincide con lo esperado

---

### Ejercicio 11.4.3 — Watermark y output mode: la combinación correcta

```python
# Las combinaciones válidas de watermark y output mode:

# 1. Watermark + ventana + output=update: emite actualizaciones
df.withWatermark("ts", "10 min") \
  .groupBy(F.window("ts", "1 min"), "region") \
  .count() \
  .writeStream.outputMode("update")  # ✓ emite la fila cuando cambia

# 2. Watermark + ventana + output=append: espera al cierre de ventana
df.withWatermark("ts", "10 min") \
  .groupBy(F.window("ts", "1 min"), "region") \
  .count() \
  .writeStream.outputMode("append")  # ✓ emite solo cuando la ventana cierra

# 3. Sin watermark + groupBy + output=complete: emite todo el estado
df.groupBy("region") \
  .count() \
  .writeStream.outputMode("complete")  # ✓ pero estado crece infinitamente

# 4. Sin watermark + ventana + output=append: ERROR
df.groupBy(F.window("ts", "1 min")) \
  .count() \
  .writeStream.outputMode("append")  # ✗ AnalysisException: requires watermark
```

**Preguntas:**

1. ¿Cuál es la diferencia en latencia entre `outputMode("update")` y
   `outputMode("append")` para una ventana de 1 minuto con watermark de 10 minutos?

2. ¿Con `outputMode("append")`, una ventana puede emitirse más de una vez?

3. ¿Con `outputMode("update")`, una ventana puede emitirse más de una vez?

4. ¿Qué output mode necesitas si el sink es append-only (como un log)?

5. Si combinas `outputMode("append")` con un sink de Delta Lake,
   ¿las ventanas ya emitidas pueden volver a cambiar en Delta?

---

### Ejercicio 11.4.4 — Múltiples watermarks: el mínimo global

```python
# Cuando haces JOIN entre dos streams, cada uno tiene su propio watermark.
# Spark usa el mínimo de los dos watermarks para determinar el progreso global.

df_clicks = spark.readStream \
    .format("kafka").option("subscribe", "clicks").load() \
    .withWatermark("timestamp", "10 minutes")

df_compras = spark.readStream \
    .format("kafka").option("subscribe", "compras").load() \
    .withWatermark("timestamp", "10 minutes")

# JOIN de dos streams (ver §11.6 para detalle):
df_conversion = df_clicks.join(
    df_compras,
    on=["user_id"],
    how="inner"
)
# El watermark del resultado = min(watermark_clicks, watermark_compras)
```

**Preguntas:**

1. Si el topic de `clicks` recibe datos con 30 minutos de retraso
   y el de `compras` recibe datos en tiempo real, ¿cómo afecta al watermark global?

2. ¿Qué pasa con la latencia del resultado si uno de los dos streams
   se queda sin datos nuevos durante 1 hora?

3. ¿Por qué Spark usa el mínimo y no el máximo de los watermarks?

4. ¿Cómo puedes "avanzar" artificialmente el watermark de un stream lento?

**Pista:** Si el topic de clicks se retrasa 30 minutos, su watermark
está 30 minutos detrás del de compras. El watermark global del JOIN
es el mínimo: 30 minutos de retraso. Esto significa que el JOIN no puede
emitir resultados hasta que hayan pasado 30 minutos del event time más reciente.
Si el stream de clicks se "congela" (sin datos nuevos), su watermark no avanza
y el JOIN bloquea indefinidamente — el resultado más reciente emitido tendrá
30 minutos de antigüedad mínimo, y crecerá con el tiempo de congelamiento.

---

### Ejercicio 11.4.5 — Watermark en producción: configurar para el caso real

**Tipo: Diseñar**

Para cada sistema, recomendar el watermark apropiado con justificación:

```
Sistema 1: Aplicación móvil
  - Los usuarios pueden estar offline hasta 48 horas
  - Se procesan 500M eventos/día
  - Los resultados se usan para dashboards que se actualizan cada hora

Sistema 2: Sensores IoT industriales
  - Conectados por red local, latencia < 100ms en condiciones normales
  - Fallos de red: hasta 15 minutos sin conexión
  - Los resultados deben estar disponibles en < 5 minutos

Sistema 3: Sistema de pagos bancario
  - Todas las transacciones llegan en < 2 segundos en condiciones normales
  - No hay datos tardíos en circunstancias normales
  - La exactitud es crítica: no se pueden descartar eventos

Sistema 4: Logs de servidor web
  - Logs generados localmente, enviados a Kafka cada 10 segundos en batch
  - Los servidores pueden retrasarse hasta 5 minutos en condiciones de carga alta
  - El pipeline calcula métricas de error rate por minuto
```

---

## Sección 11.5 — Output Modes: append, update, complete

### Ejercicio 11.5.1 — La semántica de cada output mode

```python
# Demostración visual de los tres output modes con datos concretos:

# Dataset: eventos de compras que llegan a lo largo del tiempo

# Batch 1: ["alice compró $100", "bob compró $200"]
# Batch 2: ["alice compró $150", "carol compró $300"]
# Batch 3: ["bob compró $50"]

# GroupBy user_id, SUM(monto):

# OUTPUT MODE: complete
# Emite TODA la tabla de resultados en cada batch
# Batch 1: [alice=100, bob=200]
# Batch 2: [alice=250, bob=200, carol=300]   ← toda la tabla
# Batch 3: [alice=250, bob=250, carol=300]   ← toda la tabla

# OUTPUT MODE: update
# Emite solo las filas que CAMBIARON en este batch
# Batch 1: [alice=100, bob=200]
# Batch 2: [alice=250, carol=300]             ← solo alice y carol cambiaron
# Batch 3: [bob=250]                          ← solo bob cambió

# OUTPUT MODE: append
# Emite solo filas NUEVAS (inmutables)
# Solo válido para operaciones que producen filas definitivas (con watermark)
# Batch 1: []                                 ← ninguna ventana cerrada aún
# Batch 2: []
# Batch N (cuando el watermark supera la ventana): [ventana=10:00-10:01, alice=250, bob=200]
```

**Preguntas:**

1. ¿El `complete` mode es escalable para un aggregation con millones de claves?
   ¿Por qué?

2. ¿Con `update` mode, si escribes a Delta Lake, cómo gestiona Delta
   las actualizaciones de filas existentes?

3. ¿Con `append` mode, una fila emitida puede cambiar posteriormente?

4. ¿Qué output mode elegirías para un dashboard de métricas en tiempo real
   que muestra los totales actualizados?

5. ¿Qué output mode elegirías para un data lakehouse donde el historial
   debe ser inmutable?

---

### Ejercicio 11.5.2 — Complete mode con Delta Lake: merge vs overwrite

```python
from pyspark.sql import functions as F

# Complete mode + Delta Lake: escribir toda la tabla en cada batch
# Problema: la tabla de dimensiones de Spark tiene todas las claves,
# pero Delta Lake necesita hacer merge o overwrite

# Opción A: overwrite completo (simple pero costoso para tablas grandes)
def sink_complete_overwrite(df, batch_id):
    df.write \
      .format("delta") \
      .mode("overwrite") \
      .save("s3://lakehouse/metricas-acumuladas/")

# Opción B: merge (más eficiente para tablas grandes)
def sink_complete_merge(df, batch_id):
    from delta.tables import DeltaTable
    
    if DeltaTable.isDeltaTable(spark, "s3://lakehouse/metricas-acumuladas/"):
        tabla_delta = DeltaTable.forPath(spark, "s3://lakehouse/metricas-acumuladas/")
        tabla_delta.alias("existente") \
            .merge(
                df.alias("nuevo"),
                "existente.user_id = nuevo.user_id"
            ) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
    else:
        df.write.format("delta").save("s3://lakehouse/metricas-acumuladas/")

query = df_acumulado \
    .writeStream \
    .foreachBatch(sink_complete_merge) \
    .option("checkpointLocation", "s3://checkpoints/metricas/") \
    .outputMode("complete") \
    .trigger(processingTime="60 seconds") \
    .start()
```

**Restricciones:**
1. Comparar el rendimiento de `overwrite` vs `merge` para una tabla
   que crece de 1M a 10M filas
2. ¿El `merge` es idempotente? ¿Qué pasa si el mismo batch se aplica dos veces?
3. ¿Cuándo `overwrite` es más eficiente que `merge`?

---

### Ejercicio 11.5.3 — Leer: elegir el output mode correcto para un pipeline

**Tipo: Analizar**

Para cada caso de uso, elegir el output mode correcto y justificar:

```
Caso 1: Calcular el total de ventas del día actual (para un dashboard live).
  La cifra debe actualizarse con cada nueva venta.
  El dashboard se conecta a Delta Lake.

Caso 2: Generar un log de auditoría de todas las transacciones.
  Cada transacción debe aparecer exactamente una vez.
  El log es inmutable (no se modifican registros una vez escritos).

Caso 3: Calcular el ranking de top-10 productos por ventas de la semana.
  El ranking cambia continuamente.
  Los consumidores del resultado siempre quieren el top-10 actual completo.

Caso 4: Detectar usuarios con más de 5 compras en la última hora.
  Los usuarios detectados se añaden a una lista de "clientes frecuentes".
  Una vez en la lista, no se eliminan.

Caso 5: Calcular métricas por ventana de 15 minutos para Grafana.
  Cada ventana debe emitirse solo cuando está completa.
  Grafana lee los puntos de datos de InfluxDB o Prometheus.
```

---

## Sección 11.6 — Joins en Streaming: Stream-Stream y Stream-Static

### Ejercicio 11.6.1 — Stream-static join: enriquecer con datos de referencia

```python
from pyspark.sql import functions as F

# El caso más frecuente y simple: enriquecer el stream con datos estáticos
spark = SparkSession.builder.getOrCreate()

# Datos estáticos: tabla de clientes (actualizada cada hora en S3)
df_clientes = spark.read.parquet("s3://dimensiones/clientes/")
# → 5 millones de filas, ~500 MB en Parquet, ~3 GB en memoria

# Stream de eventos:
df_eventos = spark.readStream \
    .format("kafka") \
    .option("subscribe", "eventos") \
    .load() \
    .select(F.from_json(F.col("value").cast("string"), schema).alias("e")) \
    .select("e.*")

# Join stream-static:
df_enriquecido = df_eventos.join(
    df_clientes,
    on="user_id",
    how="left"
)
# El df_clientes se lee una vez por micro-batch (o se cachea)
# No requiere estado adicional — es solo un broadcast join de facto

# Para que df_clientes se refresque automáticamente:
# No es posible directamente en Spark SS. Opciones:
# 1. Usar foreachBatch y re-leer df_clientes en cada batch
# 2. Usar Delta Lake para clientes y activar la lectura incremental

def enriquecer_con_clientes_frescos(df_batch, batch_id):
    """Re-leer clientes en cada batch para tener datos frescos."""
    df_clientes_fresco = spark.read.parquet("s3://dimensiones/clientes/")
    return df_batch.join(df_clientes_fresco, on="user_id", how="left")

query = df_eventos \
    .writeStream \
    .foreachBatch(lambda df, bid: (
        enriquecer_con_clientes_frescos(df, bid)
        .write.format("delta").mode("append")
        .save("s3://lakehouse/eventos-enriquecidos/")
    )) \
    .option("checkpointLocation", "s3://checkpoints/enrich/") \
    .start()
```

**Preguntas:**

1. ¿El stream-static join genera un shuffle? ¿Por qué no necesita shuffle?

2. Si `df_clientes` es de 3 GB y tienes 20 executors con 8 GB cada uno,
   ¿cabe en memoria para un broadcast join?

3. ¿Cuántas veces se lee `df_clientes` de S3 si el trigger es cada 30 segundos
   y el job corre 24 horas? ¿Es ese I/O un problema?

4. ¿Cómo sabes que el stream-static join no está generando un shuffle
   innecesario? ¿Cómo lo verificas en el plan?

5. Si `df_clientes` está en Delta Lake y se actualiza continuamente,
   ¿el join stream-static lee los datos frescos o una snapshot fija?

---

### Ejercicio 11.6.2 — Stream-stream join: correlacionar dos streams

```python
from pyspark.sql import functions as F

# El caso complejo: correlacionar eventos de dos streams
# Ejemplo: match clicks con compras para calcular conversión

df_clicks = spark.readStream \
    .format("kafka").option("subscribe", "clicks").load() \
    .select(F.from_json(F.col("value").cast("string"), schema_click).alias("c")) \
    .select("c.*") \
    .withWatermark("timestamp_click", "1 hour")

df_compras = spark.readStream \
    .format("kafka").option("subscribe", "compras").load() \
    .select(F.from_json(F.col("value").cast("string"), schema_compra).alias("cp")) \
    .select("cp.*") \
    .withWatermark("timestamp_compra", "1 hour")

# Join stream-stream: usuario hizo click Y compró en los siguientes 30 minutos
df_conversion = df_clicks.join(
    df_compras,
    on=[
        df_clicks["user_id"] == df_compras["user_id"],
        df_compras["timestamp_compra"] >= df_clicks["timestamp_click"],
        df_compras["timestamp_compra"] <= df_clicks["timestamp_click"] + F.expr("INTERVAL 30 MINUTES"),
    ],
    how="inner"
)
```

**Preguntas:**

1. ¿Por qué el stream-stream join requiere watermark en ambos streams?

2. ¿Cuánto estado acumula este join? ¿Qué datos se guardan?

3. ¿El inner join del ejemplo puede producir falsos negativos?
   (clicks que deberían matchearse con compras pero no se matchean)

4. ¿Un outer join entre dos streams es posible en Spark SS?
   ¿Cuándo se emiten las filas sin match?

5. Si un usuario hace click pero compra 2 horas después, ¿se detecta
   la conversión con el watermark de 1 hora y la ventana de 30 minutos?

**Pista:** El stream-stream join guarda en estado los eventos de ambos streams
que aún están "dentro del watermark" — es decir, eventos que podrían
aún tener un match futuro. Con watermark de 1 hora, el estado contiene
todos los clicks y compras de la última hora. Para el inner join:
si un click llega y no encuentra compra en 30 minutos, una vez que
el watermark supera `timestamp_click + 30min`, ese click se descarta
del estado sin emitir nada. Esto es un "falso negativo" de conversión —
el usuario pudo haber comprado pero fuera de la ventana de 30 minutos.

---

### Ejercicio 11.6.3 — Stream-stream join para detección de anomalías

```python
# Caso real: detectar anomalías comparando el stream de eventos
# con el estado "normal" calculado de un stream histórico

# Stream 1: eventos en tiempo real
df_tiempo_real = spark.readStream \
    .format("kafka").option("subscribe", "metricas-realtime").load() \
    .withWatermark("timestamp", "5 minutes")

# Stream 2: baseline histórico (calculado en batch, servido como stream lento)
# (en la práctica, esto suele ser un stream-static join con tabla Delta)
df_baseline = spark.readStream \
    .format("kafka").option("subscribe", "baseline-metricas").load() \
    .withWatermark("timestamp", "1 hour")

# Join: comparar métrica actual vs baseline
df_anomalias = df_tiempo_real.join(
    df_baseline,
    on=["metrica_id", F.window("tiempo_real.timestamp", "5 minutes")],
    how="left"
).withColumn(
    "es_anomalia",
    F.col("valor_actual") > F.col("baseline_promedio") * 3  # 3× el baseline
).filter(F.col("es_anomalia"))
```

**Restricciones:**
1. Implementar el pipeline de detección de anomalías con un caso real
2. ¿Qué output mode es apropiado para emitir alertas?
3. ¿Cómo evitar alertas duplicadas para la misma anomalía?
4. ¿Cómo el watermark afecta la latencia de las alertas?

---

### Ejercicio 11.6.4 — Leer: el join que nunca emite resultados

**Tipo: Diagnosticar**

Un stream-stream join lleva 20 minutos corriendo sin emitir ningún resultado,
aunque ambos streams tienen datos:

```python
df_a = stream_a.withWatermark("ts_a", "1 minute")
df_b = stream_b.withWatermark("ts_b", "1 minute")

df_join = df_a.join(
    df_b,
    on=df_a["key"] == df_b["key"],
    how="inner"
)

query = df_join.writeStream.format("console").outputMode("append").start()
```

Spark UI muestra:
```
Batch 1-20: input=500 filas de stream_a + 500 filas de stream_b
            output=0 filas
            State size: creciendo
```

**Preguntas:**

1. ¿Por qué el join no emite resultados aunque hay matches entre los streams?

2. ¿Qué columnas deben incluirse en la condición de join para que
   Spark pueda calcular el watermark del join?

3. ¿Cómo arreglarías el pipeline?

4. ¿Cuándo empezaría a emitir resultados después del arreglo?

**Pista:** El problema clásico: el join de dos streams en `outputMode("append")`
solo emite resultados cuando puede garantizar que una fila ya no cambiará.
Eso ocurre cuando el watermark supera el tiempo máximo que podría afectar esa fila.
Sin una condición de tiempo en el join (`ts_b BETWEEN ts_a - 5min AND ts_a + 5min`),
Spark no puede determinar cuándo una fila es "definitiva" y no emite nada.
La condición de tiempo es obligatoria para que el watermark pueda avanzar
y el join emita resultados en modo `append`.

---

### Ejercicio 11.6.5 — Implementar: pipeline completo de atribución de conversiones

```python
# Pipeline que atribuye cada compra al último click que la precedió
# (modelo de atribución last-click)

def pipeline_atribucion_conversiones(spark: SparkSession) -> StreamingQuery:
    """
    Para cada compra, encontrar el último click del mismo usuario
    en los últimos 30 minutos y atribuirle la conversión.
    
    Emite: (user_id, campaign_id del último click, monto de compra, tiempo de conversión)
    """
    schema_click = StructType([
        StructField("user_id", StringType()),
        StructField("campaign_id", StringType()),
        StructField("timestamp", TimestampType()),
    ])

    schema_compra = StructType([
        StructField("user_id", StringType()),
        StructField("monto", DoubleType()),
        StructField("timestamp", TimestampType()),
    ])

    df_clicks = (spark.readStream
        .format("kafka")
        .option("subscribe", "clicks")
        .load()
        .select(F.from_json(F.col("value").cast("string"), schema_click).alias("c"))
        .select("c.*")
        .withWatermark("timestamp", "35 minutes")
    )

    df_compras = (spark.readStream
        .format("kafka")
        .option("subscribe", "compras")
        .load()
        .select(F.from_json(F.col("value").cast("string"), schema_compra).alias("cp"))
        .select("cp.*")
        .withWatermark("timestamp", "35 minutes")
    )

    # TODO: implementar el join de atribución
    # Condición: mismo user_id, click ocurrió antes de la compra,
    # y la compra ocurrió dentro de los 30 minutos posteriores al click
    df_atribucion = ...

    return df_atribucion.writeStream \
        .format("delta") \
        .option("checkpointLocation", "s3://checkpoints/atribucion/") \
        .outputMode("append") \
        .start("s3://lakehouse/atribucion/")
```

**Restricciones:**
1. Implementar el join de atribución con la condición temporal correcta
2. ¿Qué pasa si un usuario hace 3 clicks antes de comprar?
   ¿Cómo atribuyes la conversión solo al último click?
3. Verificar con datos de prueba que el pipeline emite correctamente

---

## Sección 11.7 — Operacionalizar: Checkpoints, Fallos, Monitoreo

### Ejercicio 11.7.1 — Gestión del ciclo de vida de una StreamingQuery

```python
from pyspark.sql.streaming import StreamingQuery

# Iniciar el job:
query: StreamingQuery = df_procesado.writeStream \
    .format("delta") \
    .option("checkpointLocation", "s3://checkpoints/produccion/") \
    .trigger(processingTime="30 seconds") \
    .start("s3://lakehouse/metricas/")

# Monitorear el estado:
print(query.status)
# {'message': 'Processing new data', 'isDataAvailable': True, 'isTriggerActive': True}

print(query.lastProgress)
# {'id': 'xxx', 'runId': 'yyy', 'numInputRows': 5000, 'inputRowsPerSecond': 166.7,
#  'processedRowsPerSecond': 500.0, 'durationMs': {...}, 'stateOperators': [...]}

# Esperar a que termine (block):
query.awaitTermination()

# Detener limpiamente:
query.stop()

# Múltiples queries en el mismo SparkSession:
query_1 = stream_1.writeStream.format("delta").start("s3://path1/")
query_2 = stream_2.writeStream.format("kafka").start()

# Esperar a que cualquiera falle:
spark.streams.awaitAnyTermination()

# Para reiniciar después de un fallo:
if query.exception():
    print(f"Query falló: {query.exception()}")
    # Relanzar con el mismo checkpoint:
    query_nuevo = df_procesado.writeStream \
        .format("delta") \
        .option("checkpointLocation", "s3://checkpoints/produccion/") \
        .trigger(processingTime="30 seconds") \
        .start("s3://lakehouse/metricas/")
```

**Preguntas:**

1. ¿`query.lastProgress["processedRowsPerSecond"]` mide el throughput real
   o el throughput del último micro-batch?

2. Si el job falla y se reinicia con el mismo checkpoint,
   ¿el `runId` cambia? ¿Y el `id`?

3. ¿Cuántos streaming queries puede manejar una sola SparkSession?

4. ¿`query.stop()` es graceful (espera a que termine el batch actual)
   o abrupto?

---

### Ejercicio 11.7.2 — Monitoreo con métricas y alertas

```python
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql import SparkSession
import json
import time

class MonitoreoStreaming(StreamingQueryListener):
    """
    Listener de eventos de streaming para monitoreo y alertas.
    """
    
    def __init__(self, umbral_lag_segundos: float = 60.0):
        self.umbral_lag = umbral_lag_segundos
        self.metricas_historico = []
    
    def onQueryStarted(self, event):
        print(f"[{time.strftime('%H:%M:%S')}] Query iniciada: {event.id}")
    
    def onQueryProgress(self, event):
        progress = event.progress
        
        # Métricas de throughput:
        input_rps = progress.inputRowsPerSecond
        process_rps = progress.processedRowsPerSecond
        num_filas = progress.numInputRows
        
        # Métricas de latencia:
        batch_duration = sum(progress.durationMs.values()) / 1000
        
        # Métricas de Kafka:
        if progress.sources:
            for fuente in progress.sources:
                if "kafka" in fuente.get("description", ""):
                    # Consumer lag (diferencia entre endOffset y processedOffset):
                    end_offset = fuente.get("endOffset", "{}")
                    start_offset = fuente.get("startOffset", "{}")
                    # Calcular lag total...
        
        # Alertas:
        if batch_duration > self.umbral_lag:
            self._alertar(f"ALERTA: batch tardó {batch_duration:.1f}s (umbral: {self.umbral_lag}s)")
        
        if process_rps < input_rps * 0.8:
            self._alertar(f"ALERTA: procesando más lento que entrada "
                         f"({process_rps:.0f} rps < {input_rps:.0f} rps)")
        
        self.metricas_historico.append({
            "timestamp": time.time(),
            "input_rps": input_rps,
            "process_rps": process_rps,
            "batch_duration": batch_duration,
            "num_filas": num_filas,
        })
    
    def onQueryTerminated(self, event):
        if event.exception:
            self._alertar(f"ERROR: Query terminada con excepción: {event.exception}")
        else:
            print(f"Query terminada limpiamente: {event.id}")
    
    def _alertar(self, mensaje: str):
        print(f"[ALERTA] {mensaje}")
        # En producción: enviar a PagerDuty, Slack, etc.

# Registrar el listener:
monitor = MonitoreoStreaming(umbral_lag_segundos=30.0)
spark.streams.addListener(monitor)
```

**Restricciones:**
1. Implementar el cálculo del consumer lag de Kafka desde las métricas de progress
2. Añadir métricas de estado (tamaño del estado, número de grupos en groupBy)
3. Exportar métricas a Prometheus o StatsD para visualización en Grafana
4. Implementar auto-reinicio del job cuando detecta un error conocido

---

### Ejercicio 11.7.3 — Estrategia de checkpoint: cuándo reiniciar desde cero

```python
# Hay situaciones donde el checkpoint es compatible con el nuevo código
# y situaciones donde hay que reiniciar desde cero:

# COMPATIBLE (puedes reusar el checkpoint):
# - Añadir columnas computadas (narrow transformations)
# - Cambiar el trigger interval
# - Cambiar el sink (de console a Delta Lake)
# - Añadir filtros que no cambian el schema
# - Actualizar la lógica de una UDF sin cambiar su firma

# INCOMPATIBLE (hay que borrar el checkpoint y reiniciar):
# - Añadir o eliminar operaciones stateful (groupBy, join, deduplicación)
# - Cambiar el schema de la fuente (nuevas columnas en el topic de Kafka)
# - Cambiar el número de shuffle partitions
# - Cambiar el watermark de una operación stateful
# - Cambiar el tipo de datos de columnas clave

def verificar_compatibilidad_checkpoint(
    checkpoint_path: str,
    nueva_version_codigo: str,
) -> dict:
    """
    Analiza si la nueva versión del código es compatible con el checkpoint existente.
    
    Retorna: {'compatible': bool, 'razon': str, 'accion_recomendada': str}
    """
    # Leer el plan del checkpoint:
    import json
    from pathlib import Path
    
    # El checkpoint guarda el plan serializado:
    plan_path = Path(checkpoint_path) / "metadata"
    if not plan_path.exists():
        return {"compatible": False, "razon": "Checkpoint no encontrado"}
    
    with open(plan_path) as f:
        metadata = json.load(f)
    
    plan_guardado = metadata.get("operatorId", {})
    
    # TODO: comparar el plan guardado con el plan del nuevo código
    # Verificar cambios en operators stateful
    
    return {"compatible": True, "razon": "Sin cambios en operators stateful"}
```

**Preguntas:**

1. ¿Cómo migras datos de estado de un checkpoint incompatible?
   ¿Es posible migrar el estado o siempre se pierde?

2. ¿Existe una forma de versionar los cambios de estado para
   hacer migraciones incrementales?

3. Para un job con 30 días de estado acumulado (10 GB), reiniciar
   desde cero tiene un costo real. ¿Cómo lo minimizas?

4. ¿Qué estrategia de despliegue reduce el riesgo de incompatibilidad?

---

### Ejercicio 11.7.4 — Escalado dinámico de un streaming job

```python
# Spark en Kubernetes o YARN con autoescalado:
# Cuando el lag aumenta, añadir más executors automáticamente

spark = SparkSession.builder \
    .appName("job-streaming") \
    .config("spark.dynamicAllocation.enabled", "true") \
    .config("spark.dynamicAllocation.minExecutors", "2") \
    .config("spark.dynamicAllocation.maxExecutors", "20") \
    .config("spark.dynamicAllocation.schedulerBacklogTimeout", "1s") \
    .config("spark.dynamicAllocation.sustainedSchedulerBacklogTimeout", "5s") \
    .config("spark.streaming.backpressure.enabled", "true") \
    .config("spark.streaming.kafka.maxRatePerPartition", "10000") \
    .getOrCreate()
```

**Preguntas:**

1. ¿El autoescalado de Spark funciona bien con Structured Streaming?
   ¿Hay configuraciones específicas que hay que tener en cuenta?

2. ¿El backpressure y el autoescalado se contradicen?
   (uno reduce el input, el otro añade capacidad)

3. ¿Cuándo escalar horizontalmente (más executors) vs verticalmente
   (más memoria por executor) para un streaming job?

4. ¿El estado de streaming se redistribuye cuando se añaden/quitan executors?

---

### Ejercicio 11.7.5 — El pipeline de streaming production-ready

**Tipo: Diseñar + Implementar**

Diseñar e implementar un streaming job de producción completo:

```
Requisitos:
  - Fuente: topic de Kafka con 100K mensajes/segundo
  - Procesamiento: filtrar, enriquecer con tabla de clientes, calcular
    métricas por ventana de 5 minutos
  - Sink: Delta Lake + Kafka (para downstream consumers)
  - SLA: resultados disponibles en < 2 minutos del event time
  - Tolerancia a fallos: exactly-once, recuperación automática
  - Monitoreo: alertas si el lag supera 5 minutos
  
Restricciones de infraestructura:
  - Kubernetes con autoescalado
  - Checkpoint en S3
  - 10 executors × 8 GB de base, hasta 50 con autoescalado
```

El diseño debe incluir:
1. Configuración de SparkSession optimizada para este workload
2. Watermark apropiado para el SLA de 2 minutos
3. Número de particiones de Kafka y shuffle apropiados
4. Estrategia de checkpoint y manejo de fallos
5. Monitoreo y alertas
6. Plan de escalado y degradación

---

## Resumen del capítulo

**El tradeoff fundamental de Spark Structured Streaming:**

```
Micro-batching:
  + Exactly-once con fuentes y sinks idempotentes
  + El mismo código SQL/DataFrame funciona en batch y streaming
  + Integración natural con el ecosistema Spark (Delta Lake, MLlib, etc.)
  + Checkpointing automático y recovery de fallos
  - Latencia mínima de segundos (no milisegundos)
  - No apropiado para casos de uso en tiempo real estricto
  - El estado crece en el checkpoint si no se gestiona con watermarks
```

**Los cinco conceptos que distinguen a un data engineer que entiende SSS
de uno que solo copia ejemplos:**

```
1. Watermark
   No es solo "cuánto tiempo esperar" — es el mecanismo que permite
   que el estado sea acotado. Sin watermark, el estado crece indefinidamente.

2. Output mode
   append/update/complete no son estilos de escritura — son contratos
   sobre qué filas se emiten y cuándo. Elegir mal = datos incorrectos o pérdida.

3. Idempotencia en el sink
   Exactly-once en Spark requiere que el sink sea idempotente.
   Delta Lake lo es. PostgreSQL sin batch_id no lo es.

4. Estado del checkpoint
   El checkpoint guarda offsets + estado + plan. Cambiar el código
   puede invalidar el checkpoint — saber cuándo es compatible es clave.

5. Latencia vs throughput
   El trigger interval controla el tradeoff. Trigger de 1s = latencia baja,
   overhead alto. Trigger de 5min = latencia alta, throughput máximo.
   El default no es siempre el óptimo.
```

**La cadena que conecta Spark SS con el Cap.12 (Flink):**

> Spark SS y Flink resuelven el mismo problema con filosofías distintas.
> Spark SS: micro-batching, API SQL/DataFrame, latencia de segundos.
> Flink: streaming nativo, API Java/Python con estado explícito, latencia de milisegundos.
> El Cap.12 explora cuándo los milisegundos de Flink justifican la complejidad adicional.
