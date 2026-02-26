# Guía de Ejercicios — Cap.01: De Concurrencia a Datos Distribuidos

> Este capítulo es el puente entre el repositorio de concurrencia y este.
> No tiene código de Spark ni de Kafka — tiene el modelo mental que hace
> que los siguientes 19 capítulos tengan sentido.
>
> Si vienes del repo de concurrencia, este capítulo te mostrará qué
> cambia y qué se conserva. Si no, te dará el contexto mínimo necesario.

---

## El cambio de perspectiva

En el repositorio de concurrencia, el problema central era:

> *Tengo múltiples unidades de ejecución (goroutines, threads) que
> necesitan coordinarse. ¿Cómo evito races, deadlocks, y starvation?*

En este repositorio, el problema es diferente:

> *Tengo más datos de los que caben en una máquina, o datos que llegan
> más rápido de lo que puedo procesar. ¿Cómo proceso todo sin perder
> corrección ni rendimiento?*

Son problemas relacionados pero no iguales. La diferencia más importante:

```
Concurrencia (repo anterior):
  Tú controlas el paralelismo.
  Decides cuántos goroutines, cuándo se comunican, cómo se sincronizan.
  Los bugs son: races, deadlocks, starvation.

Data engineering a escala (este repo):
  El framework controla el paralelismo.
  Tú describes QUÉ procesar; el framework decide CÓMO.
  Los bugs son: skew, shuffles innecesarios, estado que crece sin límite.
```

Esta inversión de control es el concepto más importante del capítulo.
Ignorarla — e intentar controlar el paralelismo de Spark manualmente —
es el origen de la mayoría de los problemas de rendimiento que verás.

---

## Por qué el paralelismo es diferente a escala

Considera este programa en Go que suma una lista de números:

```go
// Concurrencia manual: tú decides cómo particionar y combinar
func sumarParalelo(nums []int, goroutines int) int {
    tamaño := len(nums) / goroutines
    resultados := make(chan int, goroutines)

    for i := 0; i < goroutines; i++ {
        inicio := i * tamaño
        fin := inicio + tamaño
        go func(segmento []int) {
            suma := 0
            for _, n := range segmento { suma += n }
            resultados <- suma
        }(nums[inicio:fin])
    }

    total := 0
    for i := 0; i < goroutines; i++ { total += <-resultados }
    return total
}
```

Ahora el mismo problema en PySpark:

```python
# Paralelismo declarativo: describes qué, el framework decide cómo
df = spark.read.parquet("s3://bucket/numeros/")
total = df.agg(F.sum("valor")).collect()[0][0]
```

En el código de Go, tú decides:
- Cuántos goroutines (explícito: `goroutines`)
- Cómo dividir el trabajo (explícito: `tamaño = len(nums) / goroutines`)
- Cómo combinar los resultados (explícito: el canal)

En el código de Spark, Spark decide:
- Cuántas tasks (basado en el número de particiones del archivo)
- Cómo dividir el trabajo (una task por partición)
- Cómo combinar los resultados (internamente, con un shuffle si es necesario)

Ninguno de los dos es "mejor" — son apropiados para contextos distintos.
Go es correcto para un array en memoria en un proceso. Spark es correcto
para datos en S3 distribuidos en cientos de archivos, procesados en
un cluster de 50 máquinas.

---

## La tabla de correspondencias

Esta tabla aparece en el README y merece expandirse:

| Concepto en concurrencia | Equivalente en data engineering | Diferencia clave |
|---|---|---|
| Thread / Goroutine | Task de Spark, Flink operator | El framework los crea y gestiona, no tú |
| Canal de Go | Kafka topic, Beam PCollection | Puede ser persistente y distribuido |
| Mutex / Lock | OCC en Delta Lake | No bloquea — detecta conflictos al commitear |
| Race condition | Inconsistencia en replicación eventual | Se manifiesta en datos, no en crashes |
| Deadlock | Shuffle deadlock en Spark | Menos frecuente, pero ocurre |
| Goroutine leak | Consumer lag creciente en Kafka | El "leak" son mensajes sin procesar |
| Circuit breaker | Backpressure en streaming | Ralentiza el productor en lugar de fallar |
| Exactly-once | Exactly-once en Kafka/Flink | Mucho más difícil de garantizar |

---

## Tabla de contenidos

- [Sección 1.1 — La inversión de control](#sección-11--la-inversión-de-control)
- [Sección 1.2 — El problema del tamaño: cuando los datos no caben](#sección-12--el-problema-del-tamaño-cuando-los-datos-no-caben)
- [Sección 1.3 — El problema del tiempo: cuando los datos no terminan](#sección-13--el-problema-del-tiempo-cuando-los-datos-no-terminan)
- [Sección 1.4 — El tradeoff central: latencia vs throughput](#sección-14--el-tradeoff-central-latencia-vs-throughput)
- [Sección 1.5 — El ecosistema: qué herramienta para qué problema](#sección-15--el-ecosistema-qué-herramienta-para-qué-problema)

---

## Sección 1.1 — La Inversión de Control

### Ejercicio 1.1.1 — Leer: el mismo algoritmo, dos paradigmas

**Tipo: Leer/comparar**

El algoritmo: contar las palabras más frecuentes en un corpus de texto.

**Implementación 1 — concurrencia manual (Python con multiprocessing):**

```python
from multiprocessing import Pool
from collections import Counter
import os

def contar_en_archivo(ruta: str) -> Counter:
    with open(ruta) as f:
        return Counter(f.read().split())

def top_palabras_paralelo(directorio: str, top_n: int = 10) -> list:
    archivos = [
        os.path.join(directorio, f)
        for f in os.listdir(directorio)
        if f.endswith('.txt')
    ]

    # El programador decide: cuántos workers, cómo dividir, cómo combinar
    with Pool(processes=os.cpu_count()) as pool:
        conteos_por_archivo = pool.map(contar_en_archivo, archivos)

    total = Counter()
    for conteo in conteos_por_archivo:
        total.update(conteo)

    return total.most_common(top_n)
```

**Implementación 2 — declarativa (PySpark):**

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

top_palabras = (spark
    .read.text("s3://bucket/corpus/*.txt")
    .select(F.explode(F.split("value", r"\s+")).alias("palabra"))
    .filter(F.col("palabra") != "")
    .groupBy("palabra")
    .count()
    .orderBy(F.col("count").desc())
    .limit(10)
)

top_palabras.show()
```

**Preguntas:**

1. En la implementación con `multiprocessing`, ¿quién decide cuántos workers?
   ¿Quién decide cómo dividir el trabajo entre ellos?

2. En la implementación con Spark, ¿quién decide cuántas tasks paralelas
   se ejecutan? ¿Puedes verlo en el código?

3. La implementación con `multiprocessing` falla si el corpus no cabe
   en la memoria del proceso que llama a `pool.map`. ¿Por qué?
   ¿Falla igual la implementación de Spark?

4. Si tienes 1,000 archivos de texto y 8 cores, ¿cómo distribuye el trabajo
   cada implementación? ¿Cuál es más eficiente?

5. Si un archivo tiene 100× el tamaño de los demás, ¿cuál implementación
   maneja mejor ese desbalance? ¿Por qué?

**Pista:** La pregunta 5 apunta al concepto de *data skew* que será central
en el Cap.04. Con `multiprocessing`, un archivo de 100× el tamaño ocupa
un worker completo durante 100× más tiempo — los otros 7 workers terminan
y esperan. Spark puede subdividir ese archivo en múltiples particiones
(si el formato lo permite) y distribuir las particiones entre workers.

---

### Ejercicio 1.1.2 — Leer: qué pierdes y qué ganas con la inversión de control

**Tipo: Analizar**

La inversión de control tiene costos reales. No es solo una simplificación.

```python
# Lo que puedes hacer con multiprocessing que NO puedes con Spark:

# 1. Control fino del scheduling:
with Pool(8) as pool:
    # Primero procesar los archivos pequeños (para tener resultados rápido)
    archivos_ordenados = sorted(archivos, key=os.path.getsize)
    resultados = pool.map(procesar, archivos_ordenados)

# 2. Estado compartido en memoria (con cuidado):
from multiprocessing import Manager
with Manager() as manager:
    caché = manager.dict()  # compartida entre workers
    pool.starmap(procesar_con_cache, [(archivo, caché) for archivo in archivos])

# 3. Cancelación granular:
result = pool.apply_async(procesar_lento, [archivo])
try:
    resultado = result.get(timeout=5.0)  # cancelar si tarda más de 5s
except TimeoutError:
    pool.terminate()
```

```python
# Lo que Spark hace automáticamente que multiprocessing NO hace:

# 1. Tolerancia a fallos:
# Si un worker de Spark falla a mitad de una task, Spark reintenta
# automáticamente en otro worker. Con multiprocessing: excepción, job muerto.

# 2. Escalar a múltiples máquinas:
# Spark distribuye las tasks en 50 máquinas sin cambiar el código.
# multiprocessing: limitado a los cores de una sola máquina.

# 3. Leer datos distribuidos:
# Spark lee directamente de S3, HDFS, Delta Lake con locality awareness.
# multiprocessing: necesitas que los datos estén en el sistema de archivos local.

# 4. Optimización del plan:
# Spark puede reordenar operaciones, aplicar predicate pushdown,
# elegir el tipo de join óptimo.
# multiprocessing: tú eres el optimizador.
```

**Preguntas:**

1. Para cada uno de los tres ejemplos de "lo que puedes hacer con
   multiprocessing pero no con Spark", ¿existe un equivalente en Spark?
   Si sí, ¿cómo se ve?

2. La "tolerancia a fallos" de Spark tiene un costo.
   ¿Cuál es ese costo y cuándo es significativo?

3. Imagina que necesitas procesar 100 archivos de 1 GB cada uno.
   Tienes una máquina con 32 GB de RAM y 16 cores.
   ¿Cuál de los dos enfoques elegiría y por qué?

4. Misma pregunta pero los archivos son 10,000 archivos de 10 GB cada uno,
   distribuidos en un cluster de 20 máquinas.

5. ¿En qué escenario `multiprocessing` sería claramente la elección
   correcta sobre Spark, incluso si el dataset es grande?

**Pista:** Para la pregunta 5: cuando el costo de arrancar y coordinar
un cluster de Spark supera el beneficio del paralelismo. Un job que tarda
30 segundos en un laptop con `multiprocessing` puede tardar 2 minutos
en Spark solo por el overhead de arrancar el SparkSession, el scheduler,
y la coordinación. Para jobs frecuentes sobre datos pequeños o medianos
(< ~50 GB en una máquina con suficiente RAM), `multiprocessing` o Polars
pueden ser más prácticos.

---

### Ejercicio 1.1.3 — El modelo mental de la ejecución distribuida

**Tipo: Leer**

Antes de ver código de Spark o Flink, vale la pena tener el modelo mental
de cómo un framework distribuido ejecuta trabajo.

```
El modelo de tres capas:

┌─────────────────────────────────────────────────────────┐
│  TU CÓDIGO (el Driver)                                  │
│  Define el plan de trabajo.                             │
│  "Leer estos archivos, filtrar estas filas, agrupar     │
│   por esta columna, escribir aquí."                     │
│  NO procesa datos — solo describe el trabajo.           │
└────────────────────────┬────────────────────────────────┘
                         │ plan de trabajo
                         ▼
┌─────────────────────────────────────────────────────────┐
│  EL SCHEDULER (Spark Master / Flink JobManager)         │
│  Traduce el plan en tasks.                              │
│  Asigna tasks a workers considerando locality.          │
│  Reintenta tasks fallidas.                              │
│  Monitorea el progreso.                                 │
└────────────────────────┬────────────────────────────────┘
                         │ tasks asignadas
                         ▼
┌─────────────────────────────────────────────────────────┐
│  LOS WORKERS (Spark Executors / Flink TaskManagers)     │
│  Ejecutan las tasks.                                    │
│  Leen datos (desde S3, HDFS, Kafka).                    │
│  Procesan y producen resultados parciales.              │
│  Comunican datos entre sí (shuffle).                    │
└─────────────────────────────────────────────────────────┘
```

**Preguntas:**

1. En el modelo de tres capas, ¿qué pasa si el Driver muere a mitad
   de un job? ¿Y si muere un Worker?

2. "Locality awareness" significa que el scheduler intenta asignar
   una task al worker que tiene los datos más cerca.
   ¿Por qué esto importa en un cluster distribuido?

3. ¿Qué tipo de comunicación entre workers no puede evitarse en un
   GroupBy (aggregation)? ¿Por qué?

4. Si tienes 100 archivos de Parquet en S3 y 10 workers,
   ¿cuántas tasks se crean para leer esos archivos?
   ¿Depende de algo más que el número de archivos?

5. El Driver "no procesa datos — solo describe el trabajo".
   ¿Qué pasa si haces `df.collect()` en el Driver?
   ¿Viola esta propiedad?

**Pista:** Para la pregunta 4: el número de tasks depende del número de
particiones, no solo del número de archivos. Un archivo de Parquet grande
puede dividirse en múltiples particiones (típicamente de 128 MB cada una).
Un archivo de 10 GB → ~80 tasks si el tamaño de partición es 128 MB.
La "locality" en S3 es diferente a HDFS: en S3 los datos no tienen
una ubicación física fija, así que la locality es menos relevante —
todos los workers tienen el mismo costo de acceso a S3.

---

### Ejercicio 1.1.4 — Diseñar: cuándo NO usar un framework distribuido

**Tipo: Diseñar**

El instinto de usar Spark para todo en data engineering es comprensible
pero costoso. Para cada caso, decidir si un framework distribuido
es necesario o si hay una solución más simple:

```
Caso 1:
  Dataset: 500 MB de CSV con datos de ventas mensuales.
  Operación: calcular el total de ventas por región.
  Frecuencia: una vez al mes, ejecutado en un laptop con 16 GB RAM.

Caso 2:
  Dataset: 50 TB de logs de acceso web, creciendo 100 GB/día.
  Operación: contar URLs únicas visitadas en el último año.
  Frecuencia: reporte diario, necesario en menos de 2 horas.

Caso 3:
  Dataset: 20 GB de datos de sensores IoT.
  Operación: detectar anomalías usando un modelo de ML ya entrenado.
  Frecuencia: cada hora, en una máquina con 64 GB RAM y 32 cores.

Caso 4:
  Dataset: 200 GB de transacciones bancarias.
  Operación: join con una tabla de 1 GB de clientes,
             luego agregar por segmento de cliente.
  Frecuencia: cada noche.

Caso 5:
  Dataset: stream de eventos de clickstream, ~50,000 eventos/segundo.
  Operación: calcular el número de usuarios activos en los últimos 5 minutos.
  Latencia requerida: resultado actualizado cada 10 segundos.
```

Para cada caso, indica:
- Herramienta recomendada (Polars, Pandas, Spark, Flink, otro)
- Justificación en términos de: tamaño del dato, frecuencia, latencia, equipo
- Costo aproximado de usar Spark vs la alternativa

**Pista:** La regla práctica para Spark: si el dato cabe en memoria de una
máquina razonablemente equipada (digamos, 128 GB RAM), Polars o DuckDB
son frecuentemente más rápidos y más simples. Spark paga dividendos cuando
los datos genuinamente no caben en una máquina o cuando el cluster ya existe
y el overhead de arranque es amortizado por jobs frecuentes.

> 🔗 Ecosistema: DuckDB es otra herramienta relevante para el Caso 1 y 3 —
> un motor SQL analítico en memoria, sin cluster, extremadamente eficiente
> para datasets de hasta ~100 GB. No se cubre en profundidad en este repo
> pero vale tenerlo en el radar.

---

### Ejercicio 1.1.5 — Leer: el costo del overhead de coordinación

**Tipo: Medir/analizar**

El siguiente experimento mide el overhead de distintos frameworks
para el mismo job sobre distintos tamaños de datos:

```
Job: leer un archivo Parquet, filtrar filas donde monto > 100,
     agrupar por región, sumar montos.

Hardware: laptop, 16 GB RAM, 8 cores, SSD NVMe.

Resultados (tiempo en segundos):

Tamaño    Pandas    Polars    Spark (local)    Spark (cluster 4 nodos)
──────────────────────────────────────────────────────────────────────
100 MB     0.8s      0.3s         8.2s               12.4s
1 GB       7.4s      1.9s        11.3s               14.1s
10 GB    OOM (*)    18.4s        42.1s               31.2s
100 GB     N/A       N/A        380s                 89s
1 TB       N/A       N/A         N/A                920s

(*) Pandas cargó el archivo completo en memoria y se quedó sin RAM
```

**Preguntas:**

1. ¿Por qué Spark local es más lento que Polars para 100 MB y 1 GB?
   ¿Qué explica los 8.2 segundos de Spark vs 0.3 de Polars para 100 MB?

2. ¿A qué tamaño de datos Spark cluster empieza a ser más rápido
   que Polars (single-node)?

3. El Spark local para 100 GB tarda 380 segundos vs 89 segundos
   en el cluster de 4 nodos. ¿El speedup es proporcional al número de nodos?
   ¿Por qué no?

4. ¿Qué información falta en esta tabla para tomar una decisión
   de arquitectura completa?

5. Si el job se ejecuta 100 veces al día sobre datos de 1 GB cada vez,
   ¿qué herramienta elegirías y por qué?

**Pista:** El overhead de Spark para datos pequeños incluye:
arrancar la JVM (~1-2s), inicializar el SparkSession (~2-3s),
planificar el job, y comunicar el plan a los executors.
Este overhead es fijo — no escala con el tamaño del dato.
Para datos de 100 MB, el overhead es 8× el tiempo de procesamiento.
Para datos de 1 TB, el overhead es < 0.1% del tiempo total.

---

## Sección 1.2 — El Problema del Tamaño: Cuando los Datos No Caben

### Ejercicio 1.2.1 — La aritmética del escalado

**Tipo: Calcular/razonar**

Antes de escribir código, vale la pena hacer la aritmética:

```
Dato: tabla de transacciones de e-commerce.
  1 fila = 1 transacción
  Campos: id (8B), user_id (8B), producto_id (8B), monto (8B),
          timestamp (8B), descripcion (avg 50B), región (4B) = ~94 bytes/fila

Volumen:
  1,000 transacciones/segundo
  = 86,400,000 transacciones/día
  = 86,400,000 × 94 bytes ≈ 8 GB/día (sin comprimir)
  = ~2.9 TB/año (sin comprimir)
  Con Parquet (compresión ~4:1): ~730 GB/año
```

**Preguntas:**

1. ¿Cuántas filas de transacciones caben en 16 GB de RAM?
   ¿Eso cuántos días de datos representa?

2. Si quieres hacer una query sobre "transacciones del último año",
   ¿cuántos GB leerías en el peor caso (sin particionamiento)?
   ¿Y con particionamiento por día?

3. La compresión de Parquet es ~4:1 en disco. Al leer en memoria,
   los datos se descomprimen. ¿Cuánta RAM necesitas para procesar
   una semana de datos descomprimidos?

4. Tienes un cluster de 10 workers con 32 GB RAM cada uno.
   ¿Cuántos días de datos puedes procesar en memoria simultáneamente
   (sin spill a disco)?

5. Con ese cluster, ¿qué pasa si intentas hacer un JOIN entre un año
   de transacciones y una tabla de usuarios de 1 GB?

**Pista:** Para la pregunta 3: el factor de expansión de Parquet en memoria
depende del tipo de dato. Para strings, puede ser 10:1 o más (en disco,
están comprimidos con dictionary encoding; en memoria, son punteros a strings completos).
Para números, 2:1 es más típico. La práctica: asumir que los datos en memoria
ocupan 3-5× el tamaño del archivo Parquet.

---

### Ejercicio 1.2.2 — Particionamiento: la solución al problema del tamaño

**Tipo: Leer**

El particionamiento es la técnica fundamental para trabajar con datos
que no caben en memoria. En lugar de cargar todo, carga solo lo que necesitas.

```
Sin particionamiento:
  s3://bucket/transacciones/data.parquet  (730 GB)
  Query: "dame las transacciones de enero 2024"
  → Spark debe leer los 730 GB para filtrar ~60 GB

Con particionamiento por año/mes:
  s3://bucket/transacciones/año=2024/mes=01/part-*.parquet  (60 GB)
  s3://bucket/transacciones/año=2024/mes=02/part-*.parquet  (58 GB)
  ...
  Query: "dame las transacciones de enero 2024"
  → Spark lee solo los 60 GB de año=2024/mes=01/
  → El resto (670 GB) ni se toca: "partition pruning"
```

La pregunta clave del particionamiento: **¿cuáles son las queries más frecuentes?**

El particionamiento óptimo para una query puede ser subóptimo para otra:

```
Particionamiento por fecha → óptimo para queries por rango de fecha
Particionamiento por región → óptimo para queries por región específica
Particionamiento por (región, fecha) → óptimo para queries que filtran ambos

No existe el "particionamiento universal".
```

**Preguntas:**

1. Para las siguientes queries, indica qué particionamiento sería óptimo:
   - "Todas las transacciones de hoy"
   - "Todas las transacciones del usuario U"
   - "Todas las transacciones de más de $1,000 en la región Norte"
   - "El top 10 de productos más vendidos en el último mes"

2. ¿Por qué NO usar `user_id` como partition key si hay 10 millones de usuarios?
   (pista: número de archivos)

3. ¿Cuál es el "small files problem" y cómo se relaciona con el particionamiento?

4. Si una partición tiene 500 GB y otra tiene 1 MB, ¿qué impacto tiene
   en el rendimiento de un job de Spark?

5. Un ingeniero propone: "particionemos por (año, mes, día, hora, región)
   para máxima flexibilidad". ¿Qué problemas tiene esta propuesta?

**Pista:** Para la pregunta 2: con 10 millones de usuarios, una partición
por `user_id` crea 10 millones de directorios en S3. El costo de listar
esos directorios puede ser mayor que el costo de leer los datos.
S3 cobra por operación de listing y tiene límites de throughput por prefijo.
Además, cada partición tendría muy pocos archivos — el "small files problem".

---

### Ejercicio 1.2.3 — Formatos de archivo: la decisión antes del framework

**Tipo: Comparar**

El formato del archivo determina cuánto se lee, cuánto se comprime,
y qué operaciones son eficientes — antes de que Spark, Polars, o cualquier
framework entre en juego.

```
CSV (texto plano, row-oriented):
  - Sin schema: cada herramienta puede inferirlo (o equivocarse)
  - Sin compresión nativa (puede comprimirse externamente con gzip)
  - Row-oriented: para leer solo 2 columnas de 100, debes leer todas
  - Sin estadísticas: no hay forma de saber el min/max sin leer todo
  - Universal: cualquier herramienta lo puede leer
  - Tamaño ejemplo (1M filas, 10 cols): ~1 GB

Parquet (binario, column-oriented):
  - Schema embebido en el archivo
  - Compresión por columna (dict encoding, RLE, snappy/zstd)
  - Column-oriented: para leer 2 columnas de 100, solo lees esas 2
  - Estadísticas por row group: min, max, null count → predicate pushdown
  - Requiere librería para leer (pyarrow, spark, polars)
  - Tamaño ejemplo (1M filas, 10 cols): ~80 MB (12:1 vs CSV)

ORC (binario, column-oriented):
  - Similar a Parquet, preferido en el ecosistema Hive/Hadoop
  - Mejor compresión para datos con alta cardinalidad
  - Menos adoptado fuera del ecosistema Hadoop

Avro (binario, row-oriented):
  - Schema evolution: puede leer datos escritos con versiones anteriores
  - Bueno para streaming (Kafka usa Avro frecuentemente)
  - Row-oriented: no ideal para analytics
  - Bueno para: CDC, event sourcing, inter-service communication
```

**Preguntas:**

1. Una query necesita leer solo la columna `monto` de un archivo con 50 columnas.
   ¿Cuántos bytes lee en CSV vs Parquet (orden de magnitud)?

2. ¿Qué es el "predicate pushdown" y cómo lo habilitan las estadísticas de Parquet?

3. Si tienes datos de sensores IoT que llegan por Kafka,
   ¿qué formato usarías para: (a) transportar los datos por Kafka,
   (b) almacenarlos en S3 para analytics?

4. ¿Por qué el column-oriented es mejor para analytics pero el row-oriented
   es mejor para transacciones (bases de datos OLTP)?

5. Tienes 10 TB de CSV histórico que quieres migrar a Parquet.
   ¿Cuánto espacio ahorras? ¿Qué ganas en velocidad de lectura?

**Pista:** Para la pregunta 4: las bases de datos OLTP (PostgreSQL, MySQL) son
row-oriented porque la operación más frecuente es leer o escribir una fila completa
("dame todos los campos del usuario 12345"). Las bases de datos analíticas son
column-oriented porque la operación frecuente es agregar una columna sobre
muchas filas ("el promedio de monto de todas las transacciones").
En column-oriented, esta query lee solo la columna `monto`, no todas las columnas.

> 📖 Profundizar: el paper *Dremel: Interactive Analysis of Web-Scale Datasets*
> (Melnik et al., Google, 2010) explica el modelo de datos columnar anidado
> que inspiró Parquet. Es corto (~10 páginas) y explica por qué la codificación
> de estructuras anidadas en columnar es más compleja de lo que parece.

---

### Ejercicio 1.2.4 — Leer: diagnosticar un pipeline lento por formato

**Tipo: Diagnosticar**

Un data engineer reporta que su job de Spark tarda 45 minutos
cuando esperaba 5 minutos. El código:

```python
df = spark.read.csv("s3://bucket/ventas/*.csv.gz")  # 500 GB comprimidos

resultado = (df
    .filter(df["_c3"] == "norte")      # columna 3 = región
    .filter(df["_c4"].cast("double") > 1000)  # columna 4 = monto
    .groupBy("_c2")                    # columna 2 = producto
    .count()
)

resultado.write.parquet("s3://bucket/resultado/")
```

Métricas del job en Spark UI:
```
Stage 0: Read CSV
  Input: 500 GB
  Output: 500 GB
  Duration: 38 min

Stage 1: Filter + GroupBy
  Input: 500 GB
  Output: 2 MB
  Duration: 5 min

Stage 2: Write Parquet
  Input: 2 MB
  Duration: 2 min
```

**Preguntas:**

1. ¿Por qué el Stage 0 lee 500 GB si el resultado final es 2 MB?

2. El archivo está comprimido con gzip (`.csv.gz`).
   ¿Por qué esto es un problema específico para Spark (no para otras herramientas)?

3. ¿El filtro `región == "norte"` se aplica durante la lectura del CSV?
   ¿Por qué no?

4. Si los mismos datos estuvieran en Parquet, particionado por región,
   ¿cuántos GB leería el Stage 0?

5. Propón tres cambios al pipeline que reducirían el tiempo de 45 a ~5 minutos.

**Pista:** `.csv.gz` (gzip sobre CSV) es un formato que Spark no puede dividir
en particiones. El archivo completo debe leerse por un solo worker antes de
distribuirlo. Gzip no es "splittable". Parquet sí es splittable — múltiples
workers pueden leer diferentes row groups del mismo archivo en paralelo.
Para CSV comprimido que sí es splittable, usar bzip2 (pero es más lento para comprimir/descomprimir)
o, mejor, migrar a Parquet.

---

### Ejercicio 1.2.5 — El cálculo de "cuánto cluster necesito"

**Tipo: Diseñar/calcular**

Antes de lanzar un cluster de Spark, vale la pena estimar el tamaño necesario.
Para el siguiente workload, calcular los requerimientos mínimos:

```
Workload:
  Dataset: 3 TB de datos de transacciones en Parquet (particionado por día)
  Job: JOIN entre transacciones y una tabla de clientes (50 GB),
       luego GROUP BY cliente + mes, SUM(monto)
  SLA: el job debe completarse en menos de 30 minutos
  Frecuencia: una vez al día
```

Usando estas reglas prácticas:
- Tamaño de partición óptimo: 128–256 MB
- Overhead de Spark en memoria: ~3× el tamaño de datos procesados simultáneamente
- Throughput de procesamiento de Spark: ~100 GB/hora por core (estimación conservadora)

Calcular:
1. Número de particiones para el dataset de 3 TB
2. Número de cores necesarios para completar en 30 minutos
3. Memoria RAM por executor (asumiendo executors de 5 cores)
4. Número de máquinas (asumiendo VMs de 16 cores, 64 GB RAM)
5. ¿Cómo cambia el cálculo si el JOIN con la tabla de 50 GB
   puede hacerse con broadcast?

**Pista:** Los cálculos de sizing son estimaciones, no garantías. Los factores
que los hacen imprecisos: data skew (algunas particiones tardan más),
overhead del shuffle (el JOIN genera tráfico de red), y la naturaleza
del dato (columnas de texto comprimen mucho más que columnas numéricas).
El approach práctico: estimar, lanzar un cluster, medir, ajustar.
Los proveedores cloud permiten escalar el cluster sin reescribir el job.

---

## Sección 1.3 — El Problema del Tiempo: Cuando los Datos No Terminan

### Ejercicio 1.3.1 — Leer: batch vs streaming como decisión de negocio

**Tipo: Analizar**

La elección entre batch y streaming no es solo técnica — es una decisión
sobre qué pregunta estás respondiendo.

```
Pregunta batch: "¿Cuánto vendimos el mes pasado?"
  Los datos son finitos (el mes pasado ya terminó).
  Puedes esperar — el resultado se necesita mañana, no en 1 segundo.
  La corrección importa más que la velocidad.
  Si el job falla, lo vuelves a correr.

Pregunta streaming: "¿Estamos vendiendo bien AHORA MISMO?"
  Los datos llegan continuamente — no hay un "final".
  El valor del resultado decae con el tiempo: saber que el sistema
  está caído hace 2 horas es menos útil que saberlo en 30 segundos.
  La latencia importa tanto como la corrección.
  Si el job falla, los datos siguen llegando — necesitas recuperarte.
```

**Preguntas:**

Para cada una de las siguientes preguntas de negocio, determina:
- ¿Batch o streaming?
- ¿Cuál es la latencia aceptable?
- ¿Qué pasa si el sistema falla 10 minutos?

```
1. "¿Cuál fue el producto más vendido en Q3 2023?"
2. "¿Hay alguna transacción que parece fraude en este momento?"
3. "¿Cuántos usuarios activos tenemos hoy?"
4. "¿Qué productos debería recomendar al usuario que está viendo
    la página ahora mismo?"
5. "¿Cuál es la tasa de conversión de este experimento A/B
    que lanzamos hace 2 semanas?"
6. "¿El servidor de pagos está respondiendo con alta latencia?"
7. "¿Cuánto revenue generamos en el último año, por país?"
8. "¿Cuántos usuarios se registraron en los últimos 5 minutos?"
```

**Pista:** Algunas preguntas tienen respuesta obvia (1 y 7 son claramente batch;
2 y 6 son claramente streaming). Las más interesantes son las del medio:
la pregunta 3 ("usuarios activos hoy") podría responderse con batch si "hoy"
significa "hasta el cierre del día", o con streaming si significa "ahora mismo".
La pregunta 4 (recomendaciones en tiempo real) parece streaming pero el modelo
de recomendaciones frecuentemente se recalcula en batch — solo la consulta final
es en tiempo real.

---

### Ejercicio 1.3.2 — El problema de los datos tardíos

**Tipo: Leer**

Este es el problema más importante y específico del streaming:

```
Situación: procesas eventos de clicks de usuarios.
Cada evento tiene un timestamp (cuándo ocurrió el click).

El sistema recibe los eventos en este orden:

Processing time (cuándo llega al sistema):
  14:00:01 → click del usuario A (event time: 14:00:00)
  14:00:03 → click del usuario B (event time: 14:00:02)
  14:00:15 → click del usuario C (event time: 14:00:14)
  14:00:47 → click del usuario D (event time: 13:59:58) ← !! llegó 49s tarde
  14:01:02 → click del usuario E (event time: 14:00:59)
  14:01:58 → click del usuario F (event time: 13:59:45) ← !! llegó 2m 13s tarde
```

Si calculas "clicks en la ventana 14:00–14:01":
- Con processing-time: incluyes todos los eventos que llegaron entre 14:00 y 14:01
  (A, B, C) — fácil, pero incorrecto: D y F ocurrieron antes de 14:01 pero no se incluyen
- Con event-time: incluyes todos los eventos que OCURRIERON entre 14:00 y 14:01
  (A, B, C, D, F) — correcto, pero ¿cuándo "cierras" la ventana?

```
El dilema:
  Si cierras la ventana a las 14:01 (processing-time):
    Pierdes D y F que llegan después pero ocurrieron antes.
  
  Si esperas para siempre por todos los tardíos:
    El resultado nunca llega.
  
  Si esperas 5 minutos:
    El resultado llega a las 14:06 — 5 minutos de latencia.
    Y aún así podrías perder eventos que tardan más de 5 minutos.
```

**Preguntas:**

1. ¿Por qué los eventos llegan tarde? Propón tres causas técnicas reales.

2. Para cada una de estas aplicaciones, determina cuánto tiempo de espera
   por datos tardíos es razonable:
   - Detección de fraude en pagos
   - Analytics de uso de una aplicación móvil
   - Monitoreo de infraestructura (CPU, latencia)
   - Revenue reporting para el equipo de finanzas

3. ¿Qué es un "watermark" en el contexto de stream processing?
   (No se espera conocimiento previo — razonar desde el problema.)

4. Si un evento llega 3 días tarde (por ejemplo, un dispositivo móvil
   que estuvo sin conexión 3 días), ¿dónde debería "ir" ese evento?

5. ¿El problema de los datos tardíos existe en batch processing?
   ¿O es exclusivo del streaming?

**Pista:** Para la pregunta 3: un watermark es esencialmente una declaración
de "todos los eventos con timestamp anterior a T ya llegaron".
Es un compromiso entre completitud (esperar a todos) y latencia (emitir pronto).
Los sistemas de streaming concretos (Beam, Flink, Spark Streaming) tienen
mecanismos para configurar este watermark — los veremos en los Cap.10–12.

---

### Ejercicio 1.3.3 — Micro-batching: el puente entre batch y streaming

**Tipo: Leer**

Spark Structured Streaming usa micro-batching: en lugar de procesar
evento a evento (streaming puro), procesa pequeños batches continuamente.

```
Streaming puro (Flink, Kafka Streams):
  Evento 1 llega → procesar Evento 1 → resultado
  Evento 2 llega → procesar Evento 2 → resultado
  ...
  Latencia: milisegundos por evento
  Throughput: limitado por la latencia de cada evento

Micro-batching (Spark Structured Streaming):
  Eventos 1–1000 llegan → procesar 1000 eventos → resultado
  Eventos 1001–2000 llegan → procesar 1000 eventos → resultado
  ...
  Latencia: el tamaño del micro-batch (típicamente 100ms–1s)
  Throughput: más alto (procesamiento en batch es más eficiente)
```

```python
# Configurar el trigger en Spark Structured Streaming:

# Trigger cada 10 segundos (micro-batch):
query = df.writeStream.trigger(processingTime='10 seconds').start()

# Trigger continuo (intenta latencia baja, experimental):
query = df.writeStream.trigger(continuous='1 second').start()

# Trigger una vez (procesar lo que hay ahora y parar):
query = df.writeStream.trigger(once=True).start()
```

**Preguntas:**

1. Si el micro-batch interval es 10 segundos y llegan 10,000 eventos/segundo,
   ¿cuántos eventos procesa cada micro-batch?

2. ¿Cuál es la latencia mínima posible con micro-batching de 10 segundos?
   (el tiempo entre que un evento llega y el resultado está disponible)

3. ¿Para qué caso de uso preferirías streaming puro (Flink) sobre
   micro-batching (Spark)?

4. ¿Para qué caso de uso preferirías micro-batching sobre streaming puro?

5. Si el micro-batch tarda más en procesarse que el interval configurado,
   ¿qué pasa? ¿Qué pasa con los eventos que siguen llegando?

**Pista:** Para la pregunta 5: si el procesamiento tarda más que el interval,
Spark simplemente ejecuta el siguiente micro-batch en cuanto termina el anterior.
No hay un mecanismo que reduzca la frecuencia automáticamente. Los eventos que
llegan mientras se procesa el micro-batch anterior se acumulan en Kafka
(o en el buffer de la fuente). Esto causa "consumer lag" — el primer síntoma
de que el sistema no puede mantener el ritmo de llegada de datos.

---

### Ejercicio 1.3.4 — El estado en streaming: el problema que no tiene batch

**Tipo: Leer**

En batch processing, el "estado" es trivial: los datos están en un archivo,
los lees, los procesas, los escribes. El estado entre runs está en los archivos.

En streaming, necesitas mantener estado entre eventos que llegan en el tiempo:

```
Query: "número de compras por usuario en las últimas 24 horas"

En batch: fácil.
  df.filter(fecha > hace_24h).groupBy("user_id").count()
  Los datos están todos en el archivo.

En streaming: ¿cómo mantienes el conteo?
  Evento: usuario A compra a las 14:00
  Evento: usuario A compra a las 15:30
  Evento: usuario A compra a las 16:00
  
  En cada momento necesitas saber: "¿cuántas veces compró A en las últimas 24h?"
  Esto requiere recordar las compras anteriores de A.
  
  El "estado" = la memoria de eventos pasados que afectan a eventos futuros.
```

**Preguntas:**

1. Para la query "número de compras en las últimas 24 horas",
   ¿qué información mínima necesitas guardar en el estado?

2. ¿Cuánto crece el estado si tienes 10 millones de usuarios activos?

3. ¿Qué pasa con el estado de un usuario que lleva 6 meses sin comprar?
   ¿Debería permanecer en el estado indefinidamente?

4. Si el sistema de streaming falla y reinicia, ¿qué pasa con el estado?
   ¿Cómo lo recuperas?

5. En batch, si el job falla, simplemente lo vuelves a ejecutar.
   ¿Por qué es más complicado "volver a ejecutar" en streaming?

**Pista:** El estado en streaming es el recurso más crítico.
Sin gestión del estado, el sistema eventualmente se queda sin memoria —
el estado acumula todas las compras de todos los usuarios desde el inicio.
Los frameworks modernos (Flink, Beam, Spark Streaming) tienen mecanismos
de "state TTL": el estado de un usuario que no ha tenido actividad en N días
se elimina automáticamente. También tienen checkpointing: guardar el estado
periódicamente para poder recuperarlo ante un fallo.

---

### Ejercicio 1.3.5 — Diseñar: el mismo pipeline en batch y en streaming

**Tipo: Diseñar**

El sistema de e-commerce necesita calcular, por producto:
- Número de vistas en el último día
- Número de compras en el último día
- Tasa de conversión (compras / vistas)

Diseñar dos versiones del pipeline:

**Versión batch:**
- Frecuencia: una vez por hora
- Latencia aceptable: los datos del reporte tienen hasta 1 hora de antigüedad
- Los datos viven en S3 como archivos Parquet, particionados por hora

**Versión streaming:**
- Latencia aceptable: el dashboard se actualiza cada 5 minutos
- Los datos llegan como eventos de Kafka en tiempo real
- Los datos tardíos son posibles (hasta 10 minutos)

Para cada versión, especificar:
1. La fuente de datos y cómo se lee
2. Cómo se calcula el "último día" (rolling window vs fixed window)
3. Dónde y cómo se guarda el estado (para la versión streaming)
4. Qué pasa si el pipeline falla 2 horas
5. La herramienta recomendada

**Pista:** El "último día" es más simple en batch (siempre es las últimas 24h desde ahora)
que en streaming. En streaming, una "sliding window de 24 horas que avanza cada 5 minutos"
requiere mantener en estado todos los eventos de las últimas 24 horas — un estado muy grande.
La alternativa práctica: calcular el acumulado del día (desde medianoche) — un estado
más pequeño que se reinicia a medianoche.

---

## Sección 1.4 — El Tradeoff Central: Latencia vs Throughput

### Ejercicio 1.4.1 — Medir: el tradeoff con números reales

**Tipo: Medir**

El tradeoff latencia/throughput se puede medir directamente.
Para el mismo job de suma de columnas sobre un DataFrame de 10M filas:

```python
import polars as pl
import time

df = pl.DataFrame({"valor": range(10_000_000)})

# Enfoque 1: procesar una fila a la vez (máxima latencia, mínimo throughput)
def sumar_fila_a_fila(df):
    total = 0
    for row in df.iter_rows():
        total += row[0]
    return total

# Enfoque 2: procesar en batches de 1000 (balance)
def sumar_en_batches(df, batch_size=1000):
    total = 0
    for i in range(0, len(df), batch_size):
        batch = df[i:i+batch_size]
        total += batch["valor"].sum()
    return total

# Enfoque 3: procesar todo de una vez (máximo throughput, latencia del total)
def sumar_vectorizado(df):
    return df["valor"].sum()
```

**Restricciones:**
1. Medir el tiempo de cada enfoque para 10M filas
2. Calcular el throughput (filas/segundo) de cada enfoque
3. Calcular la "latencia hasta el primer resultado parcial" de cada enfoque
4. Graficar el tradeoff: eje X = latencia, eje Y = throughput

**Pista:** La latencia del enfoque 1 para el primer resultado es 0 (procesa
una fila y ya tiene un resultado parcial). La latencia del enfoque 3 para
el primer resultado es el tiempo total del job (no hay resultados parciales).
Este es exactamente el tradeoff entre streaming puro (bajo latencia, procesa
evento a evento) y batch (alto throughput, procesa todo de una vez).

---

### Ejercicio 1.4.2 — El tradeoff en los frameworks

**Tipo: Analizar**

Cada framework de este repositorio elige una posición diferente en el
espectro latencia/throughput. Completar la tabla con los valores
aproximados y justificar:

```
Framework               Latencia típica    Throughput típico    Overhead fijo
──────────────────────────────────────────────────────────────────────────────
Pandas (local)          ???                ???                  muy bajo
Polars (local)          ???                ???                  muy bajo
Spark (local mode)      ???                ???                  ???
Spark (cluster)         ???                ???                  ???
Kafka Streams           ???                ???                  ???
Spark Structured        ???                ???                  ???
  Streaming
Apache Flink            ???                ???                  ???
Apache Beam / Flink     ???                ???                  ???
```

Donde:
- "Latencia típica" = tiempo entre que un dato está disponible y el resultado
- "Throughput típico" = GB/hora que puede procesar por core
- "Overhead fijo" = tiempo de arranque antes de empezar a procesar

**Pista:** Los frameworks de streaming puro (Flink, Kafka Streams) tienen
baja latencia pero más overhead de estado y coordinación que batch.
Spark tiene alto overhead fijo (arranque de la JVM y el SparkSession) pero
alto throughput una vez que arrancó. Polars tiene overhead mínimo y throughput
muy alto para datos que caben en memoria, pero no escala a múltiples máquinas.

---

### Ejercicio 1.4.3 — Leer: el sistema que eligió el tradeoff incorrecto

**Tipo: Diagnosticar**

El equipo de un banco implementó detección de fraude con Spark batch:

```python
# Corre cada 15 minutos
df_transacciones = spark.read.parquet("s3://transacciones/")
df_fraude = detectar_fraude(df_transacciones)
df_fraude.write.parquet("s3://alertas-fraude/")
# Tiempo de ejecución: 12 minutos
```

El director de seguridad pregunta:
"¿Por qué el banco tardó 14 minutos en detectar que la tarjeta de Juan
estaba siendo usada fraudulentamente esta mañana? Para entonces ya se
habían procesado 7 transacciones fraudulentas por un total de $4,200."

**Preguntas:**

1. ¿Por qué el sistema tardó hasta 14 minutos en detectar el fraude?
   (no 12 ni 15, sino hasta 14)

2. ¿El tiempo de detección depende de cuándo ocurre la transacción
   dentro del ciclo de 15 minutos?

3. ¿Cuál es el tiempo promedio de detección con este diseño?

4. ¿Qué cambio en la arquitectura reduciría el tiempo de detección
   a menos de 30 segundos?

5. ¿Ese cambio tiene un costo? ¿Cuál?

**Pista:** El tiempo máximo de detección con batch de 15 minutos y 12 minutos
de procesamiento es: la transacción ocurre justo después de que arranca un batch
(momento 0), el batch no la incluye (ya empezó a correr), el próximo batch
empieza a los 15 minutos, y termina a los 27 minutos. Pero el batch que sí
la incluye dura 12 minutos → la alerta llega a los 15+12=27 minutos en el
peor caso. El caso más típico es ~15+12/2 = ~21 minutos. Algo no cuadra con
el enunciado de 14 minutos — ¿puedes explicar cómo podría ser 14?

---

### Ejercicio 1.4.4 — El costo de la consistencia en streaming

**Tipo: Leer**

En streaming, hay un tradeoff adicional: consistencia de los resultados.

```
At-most-once (puede perder datos):
  El procesador recibe el evento, procesa, luego confirma la recepción.
  Si falla después de procesar pero antes de confirmar → el evento se pierde.
  Ventaja: sin overhead de deduplicación.
  Cuándo es aceptable: métricas donde perder el 0.1% de eventos es aceptable.

At-least-once (puede duplicar datos):
  El procesador confirma la recepción, luego procesa.
  Si falla después de confirmar pero antes de procesar → el evento se repite.
  La mayoría de los frameworks garantizan esto por defecto.
  Cuándo es problemático: operaciones no idempotentes (cobrar una tarjeta).

Exactly-once (sin pérdida ni duplicación):
  El procesamiento y la confirmación son atómicos.
  Requiere checkpointing del estado + sinks idempotentes.
  Cuándo es necesario: transacciones financieras, inventario.
  Costo: latencia adicional (transacción distribuida).
```

**Preguntas:**

1. Para cada caso, determina qué garantía es apropiada y por qué:
   - Contador de vistas de página
   - Procesamiento de pagos con tarjeta
   - Pipeline de logs para debugging
   - Actualización de inventario (stock disponible)
   - Notificaciones push a usuarios

2. ¿Por qué "exactly-once" es difícil de garantizar cuando el sink
   (el destino de los datos) es un sistema externo (una API, una BD)?

3. ¿"At-least-once" + "idempotencia en el sink" equivale a "exactly-once"?
   ¿Qué significa que el sink sea idempotente?

4. En Kafka, ¿cómo se implementa el "at-least-once" a nivel del consumer?

5. ¿Qué overhead de latencia añade "exactly-once" respecto a "at-least-once"?
   (Orden de magnitud: ¿10%? ¿2×? ¿10×?)

**Pista:** "At-least-once + idempotencia en el sink" sí equivale a exactly-once
en cuanto al resultado final — si procesar el mismo evento dos veces produce
el mismo resultado que procesarlo una vez, los duplicados son inocuos.
La clave es diseñar el sink correctamente: una operación INSERT con UPSERT
(insertar o actualizar si ya existe) es idempotente. Un INSERT que falla si
la clave ya existe también lo es (el segundo intento falla inofensivamente).

---

### Ejercicio 1.4.5 — Diseñar: el tradeoff para un sistema específico

**Tipo: Diseñar**

Una plataforma de e-commerce necesita:

```
1. Recomendaciones de productos en la página principal
   (el usuario está viendo la página ahora mismo)

2. Email de "carrito abandonado" 
   (el usuario añadió productos pero no compró, enviar email en 1 hora)

3. Reporte de revenue para el equipo de finanzas
   (revenue diario, semanal, mensual)

4. Alerta de "producto sin stock"
   (cuando el inventario llega a 0, notificar al equipo de compras)

5. Dashboard de métricas de negocio en tiempo real
   (GMV, conversión, usuarios activos — actualizado cada minuto)
```

Para cada uno, especificar:
- Batch o streaming (y si streaming, micro-batching o streaming puro)
- Latencia aceptable
- Garantía de consistencia necesaria (at-most-once, at-least-once, exactly-once)
- Herramienta recomendada del ecosistema de este repositorio

---

## Sección 1.5 — El Ecosistema: Qué Herramienta para Qué Problema

### Ejercicio 1.5.1 — El mapa de decisión

**Tipo: Leer/memorizar**

```
                    ¿Los datos caben en una máquina?
                              │
                 ┌────────────┴────────────┐
                 Sí                        No
                 │                         │
    ¿Necesitas SQL o DataFrame API?      ¿Batch o Streaming?
         │                                 │
    ┌────┴────┐                   ┌────────┴────────┐
    SQL   DataFrame            Batch             Streaming
    │         │                  │                   │
  DuckDB   Polars             Spark          ¿Latencia < 100ms?
                                                │
                                       ┌────────┴────────┐
                                       Sí                No
                                       │                 │
                                    Flink/          Spark SS /
                                  Kafka Streams     Beam/Flink

¿Necesitas el mismo código para batch Y streaming?
  → Apache Beam

¿Los datos viven en un lakehouse (Delta/Iceberg)?
  → Spark (integración más madura)

¿El equipo conoce Rust y la eficiencia es crítica?
  → Polars / DataFusion
```

**Preguntas:**

1. Siguiendo el árbol de decisión, ¿qué herramienta elegiría para:
   - 500 GB de Parquet, query SQL ad-hoc desde un notebook
   - 10 TB de eventos en Kafka, calcular métricas cada minuto
   - 100 GB de CSV, transformación compleja, equipo conoce PySpark
   - 2 TB en Delta Lake, join complejo, escribe a Delta Lake

2. ¿Hay casos donde el árbol lleva a la herramienta equivocada?
   ¿Cuáles son sus limitaciones?

3. "Los datos caben en una máquina" depende de cuánta RAM tiene la máquina.
   Una máquina de 1 TB de RAM cambia el árbol. ¿Cómo?

---

### Ejercicio 1.5.2 — Leer: la arquitectura del sistema de e-commerce

**Tipo: Leer**

Este es el sistema que construiremos a lo largo del repositorio.
Antes de empezar, entender la arquitectura completa:

```
FUENTES DE DATOS:
  ┌─────────────────────────────────────────────────┐
  │  PostgreSQL: catálogo de productos (1M productos)│
  │  Kafka: eventos de clicks (100K/s)              │
  │  Kafka: eventos de compras (1K/s)               │
  │  API externa: tipos de cambio (actualización c/hora)│
  └─────────────────────────────────────────────────┘
                          │
                          ▼
INGESTION Y PROCESAMIENTO:
  ┌─────────────────────────────────────────────────┐
  │  Batch (Spark + Delta Lake):                    │
  │    - ETL diario del catálogo PostgreSQL → Delta │
  │    - Agregaciones históricas                    │
  │                                                 │
  │  Streaming (Flink / Spark Streaming):           │
  │    - Enriquecer clicks con datos del catálogo   │
  │    - Detectar sesiones de usuario               │
  │    - Métricas por ventana de 5 minutos          │
  └─────────────────────────────────────────────────┘
                          │
                          ▼
ALMACENAMIENTO:
  ┌─────────────────────────────────────────────────┐
  │  Delta Lake (S3): datos históricos y agregados  │
  │  Redis: métricas en tiempo real (TTL corto)     │
  │  Elasticsearch: búsqueda y analytics ad-hoc     │
  └─────────────────────────────────────────────────┘
                          │
                          ▼
CONSUMIDORES:
  ┌─────────────────────────────────────────────────┐
  │  Dashboard de BI (Tableau / Superset)           │
  │  API de recomendaciones (< 50ms)                │
  │  Alertas de fraude (< 30s)                      │
  │  Reportes para finanzas (diarios)               │
  └─────────────────────────────────────────────────┘
```

**Preguntas:**

1. ¿Qué parte del sistema tiene el requisito de latencia más estricto?
   ¿Qué herramienta es más adecuada para ese componente?

2. ¿Por qué el catálogo de productos va de PostgreSQL a Delta Lake
   en lugar de consultarse directamente en PostgreSQL desde Spark?

3. El tipo de cambio se actualiza cada hora. ¿Cómo se propaga
   esa actualización al pipeline de streaming sin reiniciarlo?

4. ¿Hay algún componente del sistema donde el "exactly-once" es obligatorio?

5. Si el sistema de Kafka falla 2 horas, ¿qué partes del sistema
   pueden seguir funcionando? ¿Cuáles fallan?

---

### Ejercicio 1.5.3 — El glosario del repositorio

**Tipo: Construir**

A lo largo del repositorio se usarán muchos términos técnicos.
Antes de continuar, construir un glosario personal con definiciones
en tus propias palabras para los siguientes términos.
No buscar la definición formal — razonar desde lo que sabes:

```
1.  Partición (en el contexto de Spark)
2.  Shuffle
3.  Data skew
4.  Predicate pushdown
5.  Watermark (en streaming)
6.  Consumer lag (en Kafka)
7.  Checkpoint (en streaming)
8.  Exactly-once
9.  Event-time vs processing-time
10. Columnar format
11. Broadcast join
12. State store
13. Micro-batching
14. Compaction (en Delta Lake)
15. Schema evolution
```

Después de completar el repositorio, volver a este ejercicio y revisar
si las definiciones cambiaron.

---

### Ejercicio 1.5.4 — Conectar con el repositorio de concurrencia

**Tipo: Analizar**

El repositorio de concurrencia cubrió estos conceptos. Para cada uno,
describe cuál es el concepto equivalente en data engineering y cómo difiere:

```
1.  Goroutine leak (Cap.02 del repo de concurrencia)
    → Equivalente en data engineering: ???
    → Diferencia clave: ???

2.  Race condition en un mapa compartido (Cap.04)
    → Equivalente: ???
    → Diferencia: ???

3.  Circuit breaker (Cap.21)
    → Equivalente: ???
    → Diferencia: ???

4.  Event sourcing (Cap.23)
    → Equivalente: ???
    → Diferencia: ???

5.  Consistent hashing (Cap.23)
    → Equivalente: ???
    → Diferencia: ???
```

---

### Ejercicio 1.5.5 — El contrato del repositorio

**Tipo: Reflexionar**

Antes de continuar, hacer explícito lo que este repositorio puede
y no puede enseñar:

**Lo que este repositorio enseña:**
- Cómo diagnosticar pipelines lentos o incorrectos
- Cuándo usar cada herramienta y por qué
- Los patrones de diseño que aparecen repetidamente en data engineering
- Cómo razonar sobre tradeoffs antes de escribir código

**Lo que este repositorio NO enseña:**
- La configuración específica de tu cluster (depende del proveedor)
- Los detalles de administración de Kafka o Flink en producción
- Machine learning o feature engineering (son temas propios)
- SQL avanzado (se asume conocimiento previo)

**Una pregunta abierta para cerrar el capítulo:**

Un data engineer experimentado dice:
*"El 80% de los problemas de rendimiento que he visto en producción
son una de estas tres cosas: data skew, shuffles innecesarios, o formatos
de archivo incorrectos. Si dominas esos tres conceptos, dominas la mayoría
de los problemas reales."*

¿Estás de acuerdo con esa afirmación?
¿Qué problema de rendimiento crees que falta en esa lista?

---

## Resumen del capítulo

**Los tres cambios de mentalidad que hace falta interiorizar:**

```
1. De control a descripción
   En concurrencia: tú controlas el paralelismo.
   En data engineering: describes qué quieres, el framework decide cómo.
   El error más común: intentar controlar el paralelismo de Spark manualmente.

2. De memoria a escala
   Los datos no caben en memoria — ni en la de una máquina ni en la del cluster.
   El particionamiento es la solución: procesar solo los datos que necesitas.
   El formato es la fundación: Parquet + particionamiento = 80% de los problemas resueltos.

3. De finito a continuo
   En batch: los datos tienen un principio y un final.
   En streaming: los datos llegan continuamente, y pueden llegar tarde.
   El tradeoff es inevitable: más latencia = resultados más completos.
```

**La pregunta para evaluar cualquier decisión de arquitectura:**

> ¿Cuánta latencia estoy dispuesto a pagar por cuánta completitud de datos?

Esa pregunta tiene respuestas diferentes para cada componente de un sistema.
Los siguientes 19 capítulos exploran las herramientas que corresponden
a distintas respuestas de esa pregunta.
