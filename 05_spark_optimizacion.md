# Guía de Ejercicios — Cap.05: Spark — Optimización Avanzada

> El Cap.04 enseñó a diagnosticar: encontrar dónde se pierde el tiempo.
> Este capítulo enseña a resolver: qué hacer una vez encontrado el problema.
>
> Las técnicas de este capítulo no son trucos de performance —
> son consecuencias directas de entender cómo Spark ejecuta trabajo.
> Cada optimización tiene una causa clara. Memorizarlas sin entenderlas
> lleva a aplicarlas en el contexto incorrecto, con resultados peores.

---

## El mapa de problemas y soluciones

Antes de los ejercicios, el mapa completo:

```
SÍNTOMA                          CAUSA                      SOLUCIÓN
────────────────────────────────────────────────────────────────────────────────
Una task tarda 100× más          Data skew                  Salting, AQE skew hints,
que las demás                                               repartición manual

Join muy lento,                  Shuffle join innecesario   Broadcast join
shuffle de GBs                   (tabla pequeña)            si una tabla < threshold

Job lento aunque                 Demasiadas particiones     coalesce() antes de write,
los datos son pequeños           pequeñas                   AQE coalesce automático

Stage lento con                  Spill a disco              Aumentar memoria executor,
"spill" en UI                                               reducir particiones,
                                                            evitar groupByKey

Driver OOM al leer               .collect() sobre dataset   Usar .write() o .show(N),
resultados                       grande en el driver        nunca collect() en producción

UDFs Python lentos               GIL + JVM-Python boundary  Reemplazar con funciones
                                 por cada fila              nativas o Pandas UDFs

Job correcto pero caro           Sin caché en datos         .cache() / .persist() en
en queries repetidas             reutilizados               datasets reutilizados

Lecturas de S3 leen              Predicates no se           Particionamiento correcto,
demasiados datos                 pushdown a disco           columnas de filtro como
                                                            partition keys
```

---

## Tabla de contenidos

- [Sección 5.1 — Data skew: el problema más frecuente en producción](#sección-51--data-skew-el-problema-más-frecuente-en-producción)
- [Sección 5.2 — Broadcast join: eliminar el shuffle cuando es posible](#sección-52--broadcast-join-eliminar-el-shuffle-cuando-es-posible)
- [Sección 5.3 — Particionamiento y coalesce: el tamaño correcto](#sección-53--particionamiento-y-coalesce-el-tamaño-correcto)
- [Sección 5.4 — Caché y persistencia: no recalcular lo ya calculado](#sección-54--caché-y-persistencia-no-recalcular-lo-ya-calculado)
- [Sección 5.5 — AQE: dejar que Spark se optimice solo](#sección-55--aqe-dejar-que-spark-se-optimice-solo)
- [Sección 5.6 — Memoria y spill: cuando los datos no caben](#sección-56--memoria-y-spill-cuando-los-datos-no-caben)
- [Sección 5.7 — El pipeline de optimización completo](#sección-57--el-pipeline-de-optimización-completo)

---

## Sección 5.1 — Data Skew: el Problema más Frecuente en Producción

El data skew ocurre cuando los datos no están uniformemente distribuidos
entre particiones. Una partición con 10× más datos que las demás
bloquea todo el stage mientras los otros 99 executors esperan.

```
Sin skew: job de 100 tasks, cada una tarda 10s → stage completo en 10s
Con skew: 99 tasks tardan 10s, 1 task tarda 1000s → stage completo en 1000s
          Los 99 executors esperan 990 segundos haciendo nada.
```

### Ejercicio 5.1.1 — Leer: identificar skew en la Spark UI

**Tipo: Diagnosticar**

Un stage tiene estos tiempos de task (extraído de la Spark UI):

```
Stage 3 — ShuffleMapStage
  Tasks: 200
  Median task duration: 8s
  75th percentile:      12s
  Max task duration:    847s      ← !!
  
  Shuffle Read Size:
    Median:  45 MB
    Max:     4.2 GB               ← la task lenta lee 93× más datos
  
  GC Time:
    Median:  0.3s
    Max:     42s                  ← la task lenta pasa el 5% en GC
```

**Preguntas:**

1. ¿Cuánto tarda el stage completo? ¿Cuánto tardaría sin la task lenta?

2. ¿Qué columna es probablemente la clave del shuffle? ¿Cómo lo verificarías?

3. El max de shuffle read es 4.2 GB. Si el executor tiene 8 GB de memoria
   de ejecución, ¿hay riesgo de spill?

4. ¿La métrica de GC alto (42s sobre 847s total = 5%) es síntoma o causa?

5. Propón las dos primeras acciones de diagnóstico antes de aplicar
   cualquier optimización.

**Pista:** La regla práctica: si `max_task_duration / median_task_duration > 3×`,
hay skew significativo. En este caso es 847/8 = 105× — skew severo.
El primer paso siempre es identificar la clave: hacer
`df.groupBy("clave_sospechosa").count().orderBy(F.col("count").desc()).show(10)`
y ver si hay valores con millones de registros. El segundo paso: confirmar
que esa clave es la del shuffle del Stage 3 mirando el plan en Spark UI → SQL.

---

### Ejercicio 5.1.2 — Implementar: detectar skew antes de que falle el job

```python
from pyspark.sql import DataFrame, functions as F
from pyspark.sql import SparkSession

def diagnosticar_skew(
    df: DataFrame,
    columna: str,
    threshold_ratio: float = 10.0,
) -> dict:
    """
    Analiza la distribución de una columna para detectar skew potencial.
    
    Args:
        df: el DataFrame a analizar
        columna: la columna que se usará como clave de join o groupBy
        threshold_ratio: si max_count / median_count > threshold, hay skew
    
    Returns:
        dict con métricas de distribución y flag de skew detectado
    """
    conteos = df.groupBy(columna).count()
    
    stats = conteos.agg(
        F.count("*").alias("valores_unicos"),
        F.sum("count").alias("total_filas"),
        F.max("count").alias("max_count"),
        F.min("count").alias("min_count"),
        F.avg("count").alias("avg_count"),
        F.expr("percentile(count, 0.5)").alias("median_count"),
        F.expr("percentile(count, 0.95)").alias("p95_count"),
        F.expr("percentile(count, 0.99)").alias("p99_count"),
    ).collect()[0]
    
    top_valores = (conteos
        .orderBy(F.col("count").desc())
        .limit(10)
        .collect()
    )
    
    ratio = stats["max_count"] / stats["median_count"] if stats["median_count"] > 0 else float("inf")
    
    return {
        "columna": columna,
        "valores_unicos": stats["valores_unicos"],
        "total_filas": stats["total_filas"],
        "max_count": stats["max_count"],
        "median_count": stats["median_count"],
        "p99_count": stats["p99_count"],
        "ratio_max_median": ratio,
        "skew_detectado": ratio > threshold_ratio,
        "top_10_valores": [(r[columna], r["count"]) for r in top_valores],
    }

# Uso:
reporte = diagnosticar_skew(df_ventas, "region")
if reporte["skew_detectado"]:
    print(f"⚠ Skew en '{reporte['columna']}':")
    print(f"  Ratio max/median: {reporte['ratio_max_median']:.1f}×")
    print(f"  Top valores: {reporte['top_10_valores'][:3]}")
```

**Restricciones:**
1. Extender la función para analizar múltiples columnas a la vez
2. Añadir una recomendación automática: "usar salting", "broadcast join",
   o "ninguna acción necesaria" según el tipo y severidad del skew
3. Implementar una versión que estima el impacto en tiempo de un join
   dado el skew detectado

---

### Ejercicio 5.1.3 — Salting: romper las claves calientes artificialmente

El salting añade un sufijo aleatorio a las claves hot para distribuirlas
entre múltiples particiones:

```python
import random
from pyspark.sql import functions as F

def aplicar_salting(
    df: DataFrame,
    columna_clave: str,
    num_buckets: int,
) -> DataFrame:
    """
    Añade una columna 'clave_salted' = clave original + "_" + bucket_aleatorio.
    Distribuye los valores de claves hot entre num_buckets particiones.
    """
    return df.withColumn(
        "clave_salted",
        F.concat(
            F.col(columna_clave),
            F.lit("_"),
            (F.rand() * num_buckets).cast("int").cast("string"),
        )
    )

def join_con_salting(
    df_grande: DataFrame,
    df_pequeno: DataFrame,
    columna_join: str,
    num_buckets: int = 10,
) -> DataFrame:
    """
    Join entre df_grande (con skew) y df_pequeno usando salting.
    
    Estrategia:
      1. Expandir df_pequeno: duplicar cada fila N veces (una por bucket)
      2. Añadir salt a df_grande: clave_salted = clave + "_" + rand(0..N)
      3. Hacer el join en clave_salted (distribución uniforme)
    """
    # Paso 1: expandir df_pequeno para todos los buckets
    buckets = spark.range(num_buckets).withColumnRenamed("id", "bucket")
    df_pequeno_expandido = df_pequeno.crossJoin(buckets).withColumn(
        "clave_salted",
        F.concat(F.col(columna_join), F.lit("_"), F.col("bucket").cast("string"))
    ).drop("bucket")
    
    # Paso 2: añadir salt aleatorio a df_grande
    df_grande_salted = df_grande.withColumn(
        "clave_salted",
        F.concat(
            F.col(columna_join),
            F.lit("_"),
            (F.rand() * num_buckets).cast("int").cast("string"),
        )
    )
    
    # Paso 3: join por clave_salted
    resultado = df_grande_salted.join(
        df_pequeno_expandido,
        on="clave_salted",
        how="inner"
    ).drop("clave_salted")
    
    return resultado
```

**Restricciones:**
1. Verificar que `join_con_salting` produce el mismo resultado que el join normal
2. Medir el tiempo y el shuffle read del join original vs con salting
   para un dataset con skew 100× en una clave
3. ¿El salting funciona para todos los tipos de join (inner, left, right, full)?
4. ¿Cuál es el costo del salting en términos de tamaño de `df_pequeno_expandido`?
5. ¿Qué pasa si `num_buckets` es demasiado grande?

**Pista:** El costo del salting: `df_pequeno_expandido` tiene `num_buckets` veces
más filas que `df_pequeno` original. Si `df_pequeno` tiene 1M filas y `num_buckets=10`,
el DataFrame expandido tiene 10M filas. Esto es aceptable si `df_pequeno` es pequeño,
pero si tiene 1 GB, el expandido tiene 10 GB — puede no ser viable.
Para ese caso, la alternativa es el broadcast join directo (§5.2) o el AQE skew hint.

---

### Ejercicio 5.1.4 — AQE Skew Join: el salting automático de Spark 3.x

Spark 3.0 introdujo AQE (Adaptive Query Execution) con detección automática
de skew en joins:

```python
# Habilitar AQE (activo por defecto en Spark 3.2+):
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# Configurar los umbrales de detección:
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
# Una partición es "skewed" si su tamaño > 5× el tamaño mediano

spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256m")
# Y su tamaño es > 256 MB

# El join que antes necesitaba salting manual:
resultado = df_grande.join(df_otro_grande, on="user_id")
# Con AQE activo: Spark detecta particiones skewed y las subdivide automáticamente

# Ver si AQE intervino:
resultado.explain()
# Si AQE actuó, el plan muestra "AQE: true" y las particiones subdivided
```

**Restricciones:**
1. Comparar el tiempo de un join con skew con y sin AQE habilitado
2. Configurar los umbrales de AQE para un dataset específico y justificar
3. ¿AQE puede detectar skew en groupBy además de en joins?
4. ¿Qué diferencia hay entre el salting manual y el AQE automático?
   ¿Cuándo preferiría el salting manual?

**Pista:** La diferencia clave: el salting manual transforma los datos
(añade la columna de salt) y es visible en el schema resultante. AQE actúa
en el plan de ejecución sin cambiar los datos ni el schema. El salting manual
puede ser más predecible (sabes exactamente cuántos buckets hay) y puede
aplicarse en situaciones donde AQE no detecta automáticamente el skew
(por ejemplo, skew en la fase de map, no en el join).

---

### Ejercicio 5.1.5 — Diagnosticar: el skew oculto en datos temporales

**Tipo: Diagnosticar**

Un pipeline de analytics falla con OOM cada lunes por la mañana
pero funciona el resto de la semana. El stack trace:

```
ExecutorLostFailure: Container killed by YARN for exceeding memory limits.
Container used: 12.5 GB of 8 GB physical memory.
Consider boosting spark.yarn.executor.memoryOverhead.

Stage 7 / Task 143 of 200
  Shuffle Read: 7.8 GB (task lenta)
  Other tasks: median 38 MB shuffle read
```

El código relevante:

```python
df = spark.read.parquet("s3://eventos/fecha=*/")

resultado = (df
    .withColumn("hora", F.hour("timestamp"))
    .groupBy("hora", "user_id")
    .agg(F.count("*").alias("eventos"),
         F.sum("monto").alias("revenue"))
)
```

Datos adicionales:
- El dataset tiene `fecha` como partition key
- Los lunes incluyen datos del fin de semana (viernes noche, sábado, domingo)
  que se reprocesaron porque el pipeline de ingesta falla los fines de semana

**Preguntas:**

1. ¿Por qué el skew ocurre los lunes y no el resto de la semana?

2. La clave del groupBy es `(hora, user_id)`. ¿Qué valor de `hora`
   probablemente está causando el skew?

3. ¿El problema es del código o del proceso de ingesta?

4. Propón dos soluciones: una que arregla el síntoma (el OOM)
   y otra que arregla la causa raíz.

5. Si aumentar la memoria del executor de 8 GB a 16 GB "resuelve" el problema,
   ¿eso es una solución correcta? ¿Por qué sí o por qué no?

**Pista:** Los datos del fin de semana llegan todos el lunes, concentrados
en la misma fecha de procesamiento. Si los viernes noche tienen alta actividad
(digamos, hora=22 y hora=23 con muchos usuarios), y esos datos llegan todos
juntos el lunes, la clave `(hora=22, user_id_popular)` puede acumular 3 días
de datos en una sola partición. Aumentar la memoria del executor pospone
el problema hasta que haya 4 días acumulados en lugar de 3.

---

## Sección 5.2 — Broadcast Join: Eliminar el Shuffle Cuando Es Posible

El broadcast join evita el shuffle enviando la tabla pequeña a todos
los executors en lugar de mover la tabla grande:

```
Shuffle Join (default para tablas grandes):
  Tabla A (1 TB) → particionar por user_id → shuffle → joinear con partición de B
  Tabla B (1 TB) → particionar por user_id → shuffle → joinear con partición de A
  Datos shuffleados: ~2 TB

Broadcast Join (cuando una tabla es pequeña):
  Tabla A (1 TB) → no se mueve
  Tabla B (500 MB) → broadcast a todos los executors
  Datos shuffleados: 500 MB × num_executors (pero en paralelo, sin red bottleneck)
  
  Resultado: el join es tan rápido como un map (narrow transformation)
```

### Ejercicio 5.2.1 — Leer: cuándo Spark hace broadcast automáticamente

```python
from pyspark.sql import functions as F

# Spark hace broadcast automáticamente si la tabla es más pequeña que el umbral:
print(spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))
# Default: 10MB (Spark 2.x) o 10MB en Spark 3.x
# Nota: se refiere al tamaño estimado de los datos, no del archivo

# Ver si el join es broadcast en el plan:
df_ventas.join(df_productos, on="producto_id").explain()

# Plan con shuffle join:
# == Physical Plan ==
# SortMergeJoin  ← shuffle join
# :- Exchange hashpartitioning(producto_id)
# +- Exchange hashpartitioning(producto_id)

# Plan con broadcast join:
# == Physical Plan ==
# BroadcastHashJoin  ← no hay Exchange!
# :- [df_ventas]
# +- BroadcastExchange
#    +- [df_productos]

# Forzar broadcast cuando Spark no lo hace automáticamente:
from pyspark.sql.functions import broadcast

resultado = df_ventas.join(
    broadcast(df_productos),  # hint explícito
    on="producto_id"
)
```

**Preguntas:**

1. El umbral default de `autoBroadcastJoinThreshold` es 10 MB.
   Si `df_productos` tiene 100 MB comprimido en Parquet pero 800 MB
   en memoria, ¿Spark lo broadcastea automáticamente?

2. ¿Por qué hay un límite en el tamaño del broadcast?
   ¿Qué pasa si broadcasteas una tabla de 10 GB?

3. El broadcast envía los datos a todos los executors.
   Si tienes 50 executors, ¿cuántas copias de `df_productos` existen?

4. ¿El broadcast join funciona para left join, right join, y full outer join?
   ¿Cuáles sí y cuáles no?

5. Si `df_productos` cambia frecuentemente, ¿el broadcast tiene algún
   problema de consistencia?

**Pista:** Para la pregunta 1: Spark estima el tamaño en memoria del dataset
leyendo las estadísticas del archivo Parquet (row count × row size estimado).
Si la estimación es incorrecta (dataset muy comprimido o con strings grandes),
Spark puede decidir no broadcastear aunque quepa en memoria.
El hint explícito `broadcast()` fuerza el broadcast independientemente
de las estadísticas.

---

### Ejercicio 5.2.2 — Medir: broadcast join vs shuffle join

```python
from pyspark.sql import SparkSession, functions as F
import time

spark = SparkSession.builder.getOrCreate()

# Crear datasets de prueba:
n_ventas = 10_000_000
n_productos = 100_000  # "pequeño" — 100K productos

df_ventas = spark.range(n_ventas).select(
    F.col("id").alias("venta_id"),
    (F.rand() * n_productos).cast("long").alias("producto_id"),
    (F.rand() * 10000).alias("monto"),
)

df_productos = spark.range(n_productos).select(
    F.col("id").alias("producto_id"),
    F.when(F.rand() < 0.33, "electronico")
     .when(F.rand() < 0.66, "ropa")
     .otherwise("hogar").alias("categoria"),
    (F.rand() * 100).alias("margen"),
)

df_ventas.cache().count()
df_productos.cache().count()

# Deshabilitar broadcast automático para comparar:
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

inicio = time.perf_counter()
(df_ventas
    .join(df_productos, on="producto_id")
    .groupBy("categoria")
    .agg(F.sum("monto"))
    .collect()
)
tiempo_shuffle = time.perf_counter() - inicio

# Habilitar broadcast:
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "100mb")

inicio = time.perf_counter()
(df_ventas
    .join(df_productos, on="producto_id")
    .groupBy("categoria")
    .agg(F.sum("monto"))
    .collect()
)
tiempo_broadcast = time.perf_counter() - inicio

print(f"Shuffle join: {tiempo_shuffle:.1f}s")
print(f"Broadcast join: {tiempo_broadcast:.1f}s")
print(f"Speedup: {tiempo_shuffle/tiempo_broadcast:.1f}×")
```

**Restricciones:**
1. Ejecutar y medir el speedup real
2. Verificar en Spark UI los bytes shuffleados en cada caso
3. ¿A qué tamaño de `df_productos` el broadcast deja de ser beneficioso?
   Encontrar el punto de quiebre experimentalmente (10K, 100K, 1M, 10M, 100M filas)
4. ¿El speedup depende del número de executors? ¿Por qué?

---

### Ejercicio 5.2.3 — Bucket join: pre-particionar para evitar shuffles recurrentes

El bucket join es la solución para joins frecuentes entre tablas grandes
donde el broadcast no es posible:

```python
# Escribir las tablas con bucketing:
df_ventas.write \
    .bucketBy(200, "user_id") \
    .sortBy("user_id") \
    .saveAsTable("ventas_bucketed")

df_clientes.write \
    .bucketBy(200, "user_id") \
    .sortBy("user_id") \
    .saveAsTable("clientes_bucketed")

# El join sin shuffle:
resultado = spark.table("ventas_bucketed") \
    .join(spark.table("clientes_bucketed"), on="user_id")
# NO hay Exchange en el plan → sin shuffle
```

```
Sin bucketing:
  Cada join genera shuffle → partitions de ambas tablas
  10 joins al día × 500 GB shuffleado = 5 TB de tráfico de red

Con bucketing:
  Primera vez: escritura con bucketing (shuffle al escribir)
  Joins posteriores: sin shuffle → solo I/O local
  10 joins al día × 0 shuffle = 0 tráfico de red extra
```

**Preguntas:**

1. ¿Por qué el número de buckets debe ser el mismo en ambas tablas?

2. ¿Qué pasa si `df_ventas` tiene 200 buckets pero `df_clientes` tiene 100?

3. Si los datos de `df_ventas` se actualizan diariamente (append),
   ¿el bucketing se mantiene correctamente?

4. ¿El bucket join funciona con Parquet en S3 o solo con tablas en el Hive Metastore?

5. ¿Cuándo vale la pena pagar el costo inicial del bucketing?

**Pista:** Para la pregunta 3: si los nuevos datos de `df_ventas` se escriben
con el mismo bucketing (misma columna, mismo número de buckets), el join
sigue funcionando sin shuffle. Si los nuevos datos se escriben sin bucketing
(por ejemplo, una tabla Parquet sin metadatos de bucket), Spark no puede
aprovechar el bucketing y hace el shuffle de todas formas. El metadato
de bucket está en el Hive Metastore, no en el archivo Parquet.

---

### Ejercicio 5.2.4 — Leer: el join que Spark eligió mal

**Tipo: Diagnosticar**

Un job de Spark tarda 45 minutos. El plan físico muestra:

```
== Physical Plan ==
SortMergeJoin [producto_id]
:- Sort [producto_id ASC]
:  +- Exchange hashpartitioning(producto_id, 200)
:     +- Scan parquet ventas (500 GB)
+- Sort [producto_id ASC]
   +- Exchange hashpartitioning(producto_id, 200)
      +- Scan parquet productos (450 MB)  ← 450 MB!
```

`spark.sql.autoBroadcastJoinThreshold` está en `10MB`.

**Preguntas:**

1. ¿Por qué Spark no broadcasteó `productos` si tiene solo 450 MB?

2. El threshold es 10 MB pero el archivo es 450 MB. ¿El threshold se compara
   con el tamaño en disco o con el tamaño estimado en memoria?

3. ¿Qué tamaño en memoria estimarías para un Parquet de 450 MB
   con strings y números mixtos?

4. ¿Cómo arreglarías el job sin cambiar el threshold globalmente?

5. ¿Hay riesgos en aumentar `autoBroadcastJoinThreshold` a 1 GB globalmente?

**Pista:** El threshold se compara con el tamaño estimado **en memoria**,
no en disco. Parquet con compresión puede tener un factor de expansión de 3-10×.
Un archivo de 450 MB en Parquet puede ser 1.5-4 GB en memoria.
Si Spark estima 1.5 GB, no lo broadcastea con threshold de 10 MB.
La solución más segura: usar el hint `broadcast()` explícitamente en este join
sin cambiar la configuración global.

---

### Ejercicio 5.2.5 — Implementar: elegir automáticamente el tipo de join

```python
from pyspark.sql import DataFrame
from pyspark.sql.functions import broadcast
from typing import Union

def join_optimo(
    df_izq: DataFrame,
    df_der: DataFrame,
    on: Union[str, list],
    how: str = "inner",
    threshold_broadcast_mb: int = 500,
) -> DataFrame:
    """
    Elige automáticamente el tipo de join más eficiente:
    - Broadcast join si una tabla es pequeña
    - Bucket join si ambas están pre-particionadas compatiblemente
    - Sort-merge join en otros casos
    
    Registra qué estrategia eligió y por qué.
    """
    # Estimar tamaños (en MB):
    size_izq = estimar_tamano_mb(df_izq)
    size_der = estimar_tamano_mb(df_der)
    
    estrategia = None
    
    if size_der <= threshold_broadcast_mb:
        estrategia = "broadcast_derecha"
        resultado = df_izq.join(broadcast(df_der), on=on, how=how)
    elif size_izq <= threshold_broadcast_mb and how in ("inner", "right"):
        estrategia = "broadcast_izquierda"
        resultado = broadcast(df_izq).join(df_der, on=on, how=how)
    else:
        estrategia = "sort_merge"
        resultado = df_izq.join(df_der, on=on, how=how)
    
    print(f"Join strategy: {estrategia}")
    print(f"  Left size: ~{size_izq} MB, Right size: ~{size_der} MB")
    
    return resultado

def estimar_tamano_mb(df: DataFrame) -> float:
    """
    Estima el tamaño en memoria del DataFrame en MB.
    Usa las estadísticas del plan si están disponibles.
    """
    # TODO: implementar usando df._jdf.queryExecution().analyzed().stats()
    # o df.count() × avg_row_size como fallback
    pass
```

**Restricciones:**
1. Implementar `estimar_tamano_mb` usando las estadísticas del plan
2. Añadir soporte para bucket join cuando ambas tablas están pre-particionadas
3. Escribir tests que verifican que se elige la estrategia correcta para cada caso

---

## Sección 5.3 — Particionamiento y Coalesce: el Tamaño Correcto

El número de particiones determina el paralelismo y el overhead.
Demasiadas particiones → overhead de scheduling.
Muy pocas → bottleneck en pocos executors.

```
La regla general: particiones de ~128 MB en memoria.
  Si tienes 10 GB de datos: ~80 particiones
  Si tienes 1 TB de datos: ~8,000 particiones

spark.sql.shuffle.partitions (default: 200):
  - Demasiado grande para jobs pequeños (200 tasks de 1 KB cada una)
  - Demasiado pequeño para jobs grandes (200 tasks de 5 GB cada una → spill)
```

### Ejercicio 5.3.1 — Repartition vs Coalesce: cuándo usar cada uno

```python
# repartition(N): shuffle completo → distribución uniforme
# Cuándo usar: antes de un join o groupBy si las particiones actuales
#               están muy desequilibradas. También para aumentar particiones.
df_reparticionado = df.repartition(100)
df_por_clave = df.repartition(100, "region")  # particionar por columna

# coalesce(N): solo combina particiones locales → sin shuffle
# Cuándo usar: SOLO para reducir el número de particiones (no aumentar).
# Útil antes de escribir a disco para evitar small files.
df_coalescido = df.coalesce(10)

# La diferencia crítica:
# repartition(10) sobre 100 particiones → shuffle de todos los datos
# coalesce(10) sobre 100 particiones → combina 10 particiones localmente
#   Cada nueva partición = 10 particiones antiguas combinadas
#   Sin red → mucho más rápido, pero distribución potencialmente no uniforme

# Verificar el plan:
df.repartition(10).explain()
# == Physical Plan ==
# Exchange RoundRobinPartitioning(10)  ← shuffle

df.coalesce(10).explain()
# == Physical Plan ==
# Coalesce 10  ← no hay Exchange → sin shuffle
```

**Preguntas:**

1. Si tienes 1,000 particiones de 100 KB cada una (total 100 MB),
   ¿usarías `coalesce` o `repartition`? ¿Por qué?

2. Si tienes 10 particiones con distribución muy desequilibrada
   (una tiene 90% de los datos), ¿`coalesce(5)` mejora la distribución?

3. ¿Por qué hacer `coalesce(1)` antes de escribir es peligroso
   incluso si el resultado es pequeño?

4. El "small files problem": ¿cómo afectan miles de archivos de 10 KB
   al rendimiento de las lecturas futuras de Spark?

5. Implementar una función que detecta el problema de small files
   y recomienda el coalesce apropiado:

```python
def recomendar_coalesce(df: DataFrame, target_mb: int = 128) -> int:
    """
    Calcula el número óptimo de particiones para escribir a disco.
    Objetivo: particiones de aproximadamente target_mb MB.
    """
    # TODO: estimar el tamaño total y calcular num_particiones
    pass
```

**Pista:** `coalesce(1)` crea un solo archivo de salida — conveniente para
archivos pequeños que los usuarios descargan, pero elimina el paralelismo
de escritura. Si el dataset tiene 10 GB, `coalesce(1)` hace que un solo
executor escriba 10 GB mientras el resto espera. La alternativa para
"pocos archivos grandes": `repartition(N)` donde N es small pero > 1,
seguido de `.write`. Para resultados pequeños destinados a descarga,
`coalesce(1)` es aceptable — con conciencia del tradeoff.

---

### Ejercicio 5.3.2 — Configurar shuffle.partitions correctamente

```python
# El default de spark.sql.shuffle.partitions es 200.
# Para datos pequeños: demasiado (overhead).
# Para datos grandes: demasiado poco (spill).

# Regla práctica: ~2-4× el número de cores disponibles,
# con particiones de ~128 MB de datos shuffleados.

def calcular_shuffle_partitions(
    datos_shuffleados_gb: float,
    cores_total: int,
    target_mb_por_particion: int = 128,
) -> int:
    """
    Calcula el número óptimo de spark.sql.shuffle.partitions.
    """
    particiones_por_tamano = int((datos_shuffleados_gb * 1024) / target_mb_por_particion)
    particiones_por_cores = cores_total * 3  # 3 particiones por core
    
    # El mayor de los dos asegura suficiente paralelismo
    # y particiones manejables
    return max(particiones_por_tamano, particiones_por_cores, 1)

# Ejemplo:
# Job que shufflea 1 TB, cluster de 100 cores:
n = calcular_shuffle_partitions(1024, 100)
print(f"Recomendado: {n} particiones")
# particiones_por_tamano = 1024*1024/128 = 8192
# particiones_por_cores = 100*3 = 300
# Recomendado: 8192

# Con AQE (Spark 3.x) esto se puede dejar en automático:
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128mb")
# AQE ajusta el número de particiones después del shuffle según el tamaño real
```

**Restricciones:**
1. Medir el impacto de `shuffle.partitions = 10, 50, 200, 1000, 5000`
   para un job que shufflea 50 GB
2. Encontrar el valor óptimo experimentalmente y comparar con la fórmula
3. Con AQE habilitado, ¿la configuración manual sigue siendo necesaria?

---

### Ejercicio 5.3.3 — El problema del shuffle excesivo por particionamiento incorrecto

**Tipo: Diagnosticar**

Un pipeline lee datos de S3, aplica algunas transformaciones, y escribe:

```python
# Los datos están particionados en S3 por fecha: 365 archivos de ~2 GB cada uno
# = 730 GB totales, con ~6 particiones de 128 MB por archivo
# = ~2,190 particiones iniciales

df = spark.read.parquet("s3://ventas/fecha=*/")
# Spark crea ~2,190 particiones

resultado = (df
    .filter(F.col("region") == "norte")     # elimina ~75% de los datos
    .withColumn("monto_iva", F.col("monto") * 1.19)
    .groupBy("mes", "producto_id")
    .agg(F.sum("monto_iva"))
)
# El groupBy con shuffle.partitions=200 crea 200 particiones
# Pero los datos después del filtro son ~180 GB → 180GB/200 = ~900 MB/partición
# Demasiado grande → spill potencial

resultado.write.parquet("s3://resultado/")
# Escribe 200 archivos de ~900 MB → aceptable pero grande
```

**Preguntas:**

1. ¿Cuántos datos entran al groupBy después del filtro?
   ¿Cuántas particiones son apropiadas para ese volumen?

2. ¿El filtro `region == "norte"` reduce el número de particiones de lectura?
   ¿Por qué no?

3. ¿Con AQE habilitado, Spark ajusta automáticamente las 200 particiones del shuffle?

4. Reescribir el pipeline con el número de shuffle.partitions correcto
   y un coalesce apropiado antes de la escritura.

5. Si el filtro `region` estuviera como partition key en S3
   (`s3://ventas/fecha=*/region=*/`), ¿cambia algún aspecto del análisis?

---

### Ejercicio 5.3.4 — Particionamiento al escribir: evitar el small files problem

```python
# Problema: escribir 1,000 particiones de 1 MB cada una a S3
df.repartition(1000).write.parquet("s3://resultado/")
# Resultado: 1,000 archivos de 1 MB → leer más tarde es lento
# (1,000 requests a S3 para listar + 1,000 requests para leer)

# Solución 1: coalesce antes de escribir
df.coalesce(50).write.parquet("s3://resultado/")
# Resultado: 50 archivos de ~20 MB cada uno → mejor

# Solución 2: repartición por columna de particionamiento de negocio
df.repartition(200, "mes").write \
    .partitionBy("mes") \
    .parquet("s3://resultado/")
# Resultado: 1 archivo por (mes × partición) → organizado para queries futuras

# Solución 3: maxRecordsPerFile (Spark 2.2+)
df.write \
    .option("maxRecordsPerFile", 1_000_000) \
    .parquet("s3://resultado/")
# Limita el tamaño de cada archivo sin controlar el número exacto
```

**Restricciones:**
1. Para cada solución, medir: número de archivos generados, tamaño promedio,
   y tiempo de escritura
2. Medir la diferencia en tiempo de lectura de los resultados escritos
   con cada estrategia (con un job de lectura + filtro + count)
3. ¿Cuándo usar cada estrategia?

---

### Ejercicio 5.3.5 — Leer: el diagnóstico de particionamiento de un job complejo

**Tipo: Diagnosticar**

Un job de analytics tarda 3 horas. El análisis de Spark UI muestra:

```
Stage 1 (Scan + Filter):
  Tasks: 8,500  ← !!
  Median task duration: 0.02s
  Input: 120 GB
  
Stage 2 (Shuffle Write para GroupBy):
  Tasks: 8,500
  Median task duration: 0.08s
  Shuffle Write: 45 GB
  
Stage 3 (Shuffle Read + Aggregate):
  Tasks: 200
  Median task duration: 847s  ← !!
  Shuffle Read: 45 GB
  Spill: 38 GB  ← casi todo en disco
```

**Preguntas:**

1. ¿Por qué hay 8,500 tasks en el Stage 1? ¿Es eso un problema?

2. El Stage 1 tiene 8,500 tasks de 0.02s cada una.
   ¿Cuánto tiempo se pierde en overhead de scheduling?
   (asumiendo 50ms de overhead por task)

3. El Stage 3 tiene 200 tasks pero 45 GB de datos shuffleados.
   ¿Cuánto datos por task? ¿Por qué hay spill?

4. ¿Qué configuraciones cambiarías y por qué?

5. Con los cambios propuestos, estima el nuevo tiempo del job.

**Pista:** El problema tiene dos capas:
(1) Stage 1 tiene 8,500 tasks demasiado pequeñas (120 GB / 8,500 = ~14 MB/task —
demasiado pequeño, lo óptimo sería 128 MB). El overhead de 8,500 × 50ms = 425s solo
en scheduling, más el overhead de arranque de cada task.
(2) Stage 3 tiene 200 tasks demasiado grandes (45 GB / 200 = 225 MB/task —
el doble del óptimo, y con overhead de shuffle read el spill es casi inevitable).
La solución: reducir las particiones de lectura (coalesce o configurar el tamaño
de split) y aumentar `shuffle.partitions` para el Stage 3.

---

## Sección 5.4 — Caché y Persistencia: No Recalcular lo Ya Calculado

### Ejercicio 5.4.1 — Cuándo caché tiene sentido

```python
from pyspark import StorageLevel

# El DataFrame df_base se usa en múltiples lugares:
df_base = spark.read.parquet("s3://datos/") \
    .filter(F.col("activo") == True) \
    .withColumn("monto_iva", F.col("monto") * 1.19)

# Sin caché: df_base se recalcula dos veces
resultado_1 = df_base.groupBy("region").agg(F.sum("monto_iva"))
resultado_2 = df_base.groupBy("mes").agg(F.count("*"))

# Con caché: df_base se calcula una vez y se reutiliza
df_base.cache()          # = persist(StorageLevel.MEMORY_AND_DISK)
df_base.count()          # IMPORTANTE: materializar la caché (acción)

resultado_1 = df_base.groupBy("region").agg(F.sum("monto_iva"))
resultado_2 = df_base.groupBy("mes").agg(F.count("*"))

df_base.unpersist()      # liberar memoria cuando ya no se necesita
```

```
Los niveles de persistencia:
  MEMORY_ONLY:       solo en RAM. Si no cabe → recalcular (no spill)
  MEMORY_AND_DISK:   RAM primero, disco si no cabe
  DISK_ONLY:         solo en disco (más lento pero no ocupa RAM)
  MEMORY_ONLY_SER:   serializado en RAM (más compacto, más lento de deserializar)
  MEMORY_AND_DISK_SER: serializado en RAM + disco
  OFF_HEAP:          en memoria off-heap (evita GC del JVM)
```

**Preguntas:**

1. ¿Cuándo hacer `.cache()` es contraproducente?
   Da tres ejemplos concretos donde caché empeora el rendimiento.

2. ¿Por qué es importante llamar a una acción (`.count()`) después de `.cache()`?

3. ¿Qué pasa si haces `df.cache()` pero el DataFrame es más grande
   que la memoria disponible de todos los executors?

4. ¿`df.cache()` es thread-safe si múltiples queries usan el mismo DataFrame
   simultáneamente en un SparkSession compartido?

5. ¿Cuál es la diferencia entre `.cache()` y `.persist(StorageLevel.MEMORY_AND_DISK)`?

**Pista:** La caché es contraproducente cuando:
(1) el dataset se usa solo una vez — el overhead de caché supera el ahorro,
(2) el dataset es demasiado grande para caber en memoria — causa eviction de
otras particiones cacheadas o spill, degradando el rendimiento general,
(3) el dataset está cambiando entre los usos (stale cache) — Spark no invalida
la caché automáticamente si el origen de datos cambió.

---

### Ejercicio 5.4.2 — Implementar: el patrón de caché para pipelines iterativos

```python
def pipeline_con_cache_inteligente(
    spark: SparkSession,
    config: dict,
) -> dict[str, DataFrame]:
    """
    Patrón para pipelines donde múltiples outputs comparten un dataset base.
    Evita recalcular el mismo dato costoso múltiples veces.
    """
    resultados = {}
    dfs_cacheados = []

    try:
        # Lectura y transformación costosa (se usa en múltiples branches):
        df_base = (spark.read.parquet(config["ruta_entrada"])
            .filter(F.col("fecha") >= config["fecha_inicio"])
            .join(
                spark.read.parquet(config["ruta_clientes"]),
                on="cliente_id"
            )
        )
        df_base.persist(StorageLevel.MEMORY_AND_DISK)
        df_base.count()  # materializar
        dfs_cacheados.append(df_base)

        # Branch 1: métricas por región
        resultados["por_region"] = (df_base
            .groupBy("region")
            .agg(F.sum("monto").alias("revenue"),
                 F.countDistinct("cliente_id").alias("clientes_unicos"))
        )

        # Branch 2: métricas por segmento de cliente
        resultados["por_segmento"] = (df_base
            .groupBy("segmento")
            .agg(F.avg("monto").alias("ticket_promedio"),
                 F.count("*").alias("transacciones"))
        )

        # Branch 3: top clientes
        resultados["top_clientes"] = (df_base
            .groupBy("cliente_id")
            .agg(F.sum("monto").alias("gasto_total"))
            .orderBy(F.col("gasto_total").desc())
            .limit(1000)
        )

        # Materializar todos los resultados:
        for nombre, df in resultados.items():
            df.write.mode("overwrite").parquet(f"s3://resultados/{nombre}/")

    finally:
        # Liberar caché siempre, incluso si hay error:
        for df in dfs_cacheados:
            df.unpersist()

    return resultados
```

**Restricciones:**
1. Medir el tiempo con y sin caché para el pipeline
2. Verificar en Spark UI que el df_base se lee una sola vez con caché
3. Implementar la lógica de "caché parcial": si solo dos de los tres branches
   se ejecutan frecuentemente juntos, ¿vale la pena cachear?
4. ¿El `finally` garantiza que la caché se libera incluso con errores?

---

### Ejercicio 5.4.3 — Caché vs Broadcast: cuándo uno reemplaza al otro

```python
# Escenario: tabla de dimensiones usada en múltiples joins
df_productos = spark.read.parquet("s3://dimensiones/productos/")
# 500 MB en Parquet → ~2 GB en memoria

# Opción A: caché
df_productos.cache().count()
resultado_1 = df_ventas_enero.join(df_productos, on="producto_id")
resultado_2 = df_ventas_febrero.join(df_productos, on="producto_id")
resultado_3 = df_ventas_marzo.join(df_productos, on="producto_id")
df_productos.unpersist()

# Opción B: broadcast
from pyspark.sql.functions import broadcast
resultado_1 = df_ventas_enero.join(broadcast(df_productos), on="producto_id")
resultado_2 = df_ventas_febrero.join(broadcast(df_productos), on="producto_id")
resultado_3 = df_ventas_marzo.join(broadcast(df_productos), on="producto_id")
# Sin caché explícita — pero ¿Spark reconstruye df_productos 3 veces?
```

**Preguntas:**

1. Con la Opción A (caché), ¿cuántas veces se lee `df_productos` de S3?

2. Con la Opción B (broadcast sin caché), ¿cuántas veces se lee `df_productos` de S3?
   ¿Y cuántas veces se broadcastea por la red?

3. ¿Cómo combinarías caché y broadcast para leer `df_productos` una sola vez
   y broadcastearlo eficientemente en los tres joins?

4. Si `df_productos` tiene 2 GB en memoria, ¿el broadcast cabe en la memoria
   de cada executor si cada uno tiene 8 GB de heap?

5. ¿Spark cachea automáticamente los resultados de broadcasts o los recalcula?

**Pista:** Spark no cachea automáticamente los broadcasts — si `df_productos`
se broadcastea en tres joins y no hay caché explícita, Spark puede leerlo
de S3 tres veces (una por join). La combinación óptima:
```python
df_productos.cache().count()  # leer una vez, cachear
resultado_1 = df_ventas_enero.join(broadcast(df_productos), on="producto_id")
# La segunda vez que se broadcastea, Spark lee de caché local, no de S3
```

---

### Ejercicio 5.4.4 — Evitar caché accidental: el problema del DataFrame reusado

**Tipo: Diagnosticar**

Un pipeline está más lento de lo esperado y el uso de memoria del cluster
está al 95% constantemente:

```python
def calcular_metricas_diarias(spark, fecha):
    df = (spark.read.parquet(f"s3://eventos/fecha={fecha}/")
        .join(spark.read.parquet("s3://clientes/"), on="cliente_id")
        .withColumn("hora", F.hour("timestamp"))
    )
    df.cache()  # ← el ingeniero añadió esto "por si acaso"

    metricas = {}
    metricas["por_hora"] = df.groupBy("hora").count().collect()
    metricas["por_region"] = df.groupBy("region").count().collect()

    return metricas

# Este pipeline se llama 365 veces (una por día del año):
for fecha in fechas_del_año:
    metricas = calcular_metricas_diarias(spark, fecha)
    guardar_metricas(metricas, fecha)
```

**Preguntas:**

1. ¿Cuántos DataFrames están en caché al terminar el loop?

2. ¿Por qué el uso de memoria está al 95%?

3. ¿Es correcto el `.cache()` en este pipeline? ¿Se reutiliza `df`?

4. Arreglar el pipeline.

5. ¿Cómo verificarías en Spark UI si hay DataFrames cacheados que no deberían estarlo?

**Pista:** La función `calcular_metricas_diarias` cachea `df` pero nunca
llama a `.unpersist()`. Después de 365 llamadas, hay 365 DataFrames diferentes
en caché (uno por fecha). El GC del JVM no los elimina porque Spark mantiene
referencias fuertes a los DataFrames cacheados. En Spark UI → Storage,
puedes ver todos los RDDs/DataFrames en caché con su tamaño.

---

### Ejercicio 5.4.5 — Caché en streaming: checkpoint vs caché

En streaming, la caché tiene semántica diferente:

```python
# En streaming, la caché no persiste entre micro-batches por defecto.
# El estado se gestiona con checkpoint:

query = (df_stream
    .withWatermark("timestamp", "10 minutes")
    .groupBy(F.window("timestamp", "5 minutes"), "region")
    .count()
    .writeStream
    .option("checkpointLocation", "s3://checkpoints/job1/")
    .outputMode("update")
    .format("delta")
    .start()
)

# El checkpoint guarda:
# - El offset de Kafka (qué mensajes se procesaron)
# - El estado del groupBy (conteos acumulados por ventana)
# - El plan de ejecución (para recuperar después de un restart)
```

**Preguntas:**

1. ¿Por qué el checkpoint de streaming es diferente a `.cache()` en batch?

2. Si un streaming job falla y reinicia, ¿qué recupera del checkpoint?

3. ¿El checkpoint puede crecer indefinidamente? ¿Cómo se gestiona?

4. ¿Qué pasa si cambias el código del job de streaming y reinicias
   con el mismo checkpoint? ¿Funciona?

---

## Sección 5.5 — AQE: Dejar que Spark se Optimice Solo

AQE (Adaptive Query Execution) fue la mejora más importante en Spark 3.0.
Permite a Spark reoptimizar el plan de ejecución usando estadísticas reales
de los datos, en lugar de solo estimaciones pre-ejecución.

### Ejercicio 5.5.1 — Las tres optimizaciones de AQE

```python
# Habilitar AQE (default en Spark 3.2+):
spark.conf.set("spark.sql.adaptive.enabled", "true")

# Las tres optimizaciones principales:

# 1. Coalescing shuffle partitions:
# Después del shuffle, si hay particiones vacías o muy pequeñas, AQE las fusiona
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128mb")

# 2. Skew join handling:
# Si detecta particiones skewed en un join, las subdivide automáticamente
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")

# 3. Dynamic join selection:
# Cambia de SortMergeJoin a BroadcastHashJoin si una tabla es pequeña
# (incluso si no lo era antes del shuffle)
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")
```

**Restricciones:**
1. Crear un dataset donde AQE hace cada una de las tres optimizaciones
2. Verificar que AQE intervino mirando el plan en Spark UI
3. Comparar los tiempos con y sin AQE para cada caso
4. ¿Hay casos donde AQE empeora el rendimiento?

**Pista:** AQE puede empeorar el rendimiento en casos edge:
(1) jobs muy rápidos donde el overhead de AQE (replanificar) supera el beneficio,
(2) cuando las estadísticas reales son muy similares a las estimadas — AQE no hace
nada útil pero añade latencia de planificación,
(3) cuando AQE elige un broadcast join inapropiado porque una partición
post-shuffle es accidentalmente pequeña pero el dataset completo es grande.

---

### Ejercicio 5.5.2 — AQE y el problema de las estadísticas desactualizadas

```python
# Sin AQE: Spark estima el tamaño de los datos antes de ejecutar
# Problema: si los datos están muy comprimidos o filtrados, la estimación es incorrecta

df_ventas = spark.read.parquet("s3://ventas/")
# Spark estima: 10 GB (basado en estadísticas del Parquet)
# Realidad después del filtro: 200 MB (solo región norte, que es el 2%)

df_filtrado = df_ventas.filter(F.col("region") == "norte")
# Sin AQE: Spark asume 10 GB → elige SortMergeJoin
# Con AQE: Spark ve 200 MB reales → cambia a BroadcastHashJoin

resultado = df_filtrado.join(df_otros, on="user_id")

# Ver las estadísticas que usa Spark:
df_filtrado.explain(True)
# Sin AQE: Stats{sizeInBytes=10.0 GiB, rowCount=100000000}  ← estimación pre-ejecución
# Con AQE: Stats ajustadas en runtime según el shuffle output real
```

**Preguntas:**

1. ¿Por qué las estadísticas pre-ejecución pueden ser incorrectas?
   Da tres casos concretos.

2. ¿Cómo `ANALYZE TABLE` de Hive/Spark ayuda a AQE?

3. ¿AQE puede corregir estimaciones incorrectas en el primer stage?
   ¿O solo en stages posteriores al primero?

4. Para un pipeline con 5 stages, ¿en cuántos puede intervenir AQE?

---

### Ejercicio 5.5.3 — Cuándo desactivar AQE

**Tipo: Analizar**

Un equipo reporta que después de actualizar a Spark 3.2 (con AQE por defecto),
algunos jobs que eran deterministas ahora producen resultados diferentes
en distintas ejecuciones:

```python
# Job que antes era determinista:
resultado = (df_a
    .join(df_b, on="key")
    .orderBy("timestamp")
    .limit(1000)
)
resultado.show()
# Ejecución 1: [fila_1, fila_2, fila_3, ...]
# Ejecución 2: [fila_1, fila_5, fila_3, ...]  ← diferente!
```

**Preguntas:**

1. ¿Cómo puede AQE cambiar el resultado de una query que usa `orderBy`?

2. ¿`orderBy().limit()` en Spark garantiza determinismo?
   ¿Qué garantiza exactamente?

3. ¿Cuándo es correcto desactivar AQE?

4. ¿Existe una forma de mantener AQE pero garantizar determinismo
   en este caso específico?

**Pista:** `orderBy().limit()` garantiza que el resultado está ordenado por
la columna especificada, pero no garantiza el orden de las filas con el mismo
valor si hay empates. AQE puede cambiar el número de particiones del shuffle,
lo que cambia el orden en que se procesan los empates.
La solución: usar un `orderBy` con columnas que garanticen unicidad
(por ejemplo, añadir el `id` como desempate: `.orderBy("timestamp", "id")`).

---

### Ejercicio 5.5.4 — AQE en producción: configurar el advisor

```python
# Configuración de AQE para un cluster de producción:
def configurar_aqe_produccion(spark: SparkSession, perfil: str):
    """
    Perfil "small_data": jobs frecuentes con datos < 1 GB
    Perfil "medium_data": jobs con datos 1-100 GB  
    Perfil "large_data": jobs con datos > 100 GB
    """
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    
    if perfil == "small_data":
        spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1")
        spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64mb")
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "100mb")
        
    elif perfil == "medium_data":
        spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionNum", "10")
        spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128mb")
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "50mb")
        
    elif perfil == "large_data":
        spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionNum", "50")
        spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "256mb")
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10mb")
        spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "3")
```

**Restricciones:**
1. Verificar que cada perfil mejora el rendimiento para su tipo de job
2. ¿Existe un perfil "universalmente bueno"? ¿Por qué no?
3. Implementar detección automática del perfil basada en el tamaño del job

---

### Ejercicio 5.5.5 — Leer: AQE vs optimización manual

**Tipo: Comparar**

Para un pipeline con skew severo en un join:

```python
# Versión manual (optimización explícita):
resultado_manual = (df_grande
    .withColumn("key_salted",
        F.concat(F.col("user_id"), F.lit("_"),
                 (F.rand() * 20).cast("int").cast("string")))
    .join(
        df_medio.withColumn("key_salted",
            F.concat(F.col("user_id"), F.lit("_"),
                     F.explode(F.array([F.lit(str(i)) for i in range(20)]))))
        .drop("user_id"),
        on="key_salted"
    )
    .drop("key_salted")
)

# Versión AQE (automática):
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
resultado_aqe = df_grande.join(df_medio, on="user_id")
```

**Preguntas:**

1. ¿El resultado de la versión manual y la versión AQE son idénticos?

2. ¿Cuál es más fácil de mantener si los datos cambian?

3. ¿Hay casos donde la versión manual es mejor que AQE?

4. ¿Hay casos donde AQE funciona pero la versión manual falla?

5. ¿Qué estrategia adoptarías en un equipo con data engineers de distintos niveles?

---

## Sección 5.6 — Memoria y Spill: Cuando los Datos No Caben

### Ejercicio 5.6.1 — La arquitectura de memoria de un executor

```
Spark executor JVM heap (ej: 8 GB configurados):

┌──────────────────────────────────────────────────────────┐
│  Reserved Memory: 300 MB (mínimo fijo para Spark interno)│
├──────────────────────────────────────────────────────────┤
│  User Memory: ~2.4 GB (40% del heap - reserved)         │
│  Para estructuras de datos de usuario (maps, listas)     │
│  Para UDFs, estado interno de la aplicación              │
├──────────────────────────────────────────────────────────┤
│  Spark Memory: ~3.5 GB (60% del heap - reserved)        │
│  ┌────────────────────────────────────────────────────┐  │
│  │  Storage Memory: dinámica                          │  │
│  │  Para .cache(), .persist(), broadcast vars         │  │
│  ├────────────────────────────────────────────────────┤  │
│  │  Execution Memory: dinámica                        │  │
│  │  Para shuffle, sort, aggregation, join             │  │
│  │  Si no cabe → SPILL a disco                        │  │
│  └────────────────────────────────────────────────────┘  │
│  (Storage y Execution compiten por el mismo pool)        │
└──────────────────────────────────────────────────────────┘

Parámetros de configuración:
  spark.executor.memory = 8g           (heap total)
  spark.memory.fraction = 0.6          (% para Spark Memory)
  spark.memory.storageFraction = 0.5   (% de Spark Memory para Storage)
```

**Preguntas:**

1. Con la configuración anterior (8 GB heap), ¿cuánta memoria de ejecución
   tiene disponible una task para un shuffle?

2. Si una task de shuffle necesita 5 GB y solo hay 3.5 GB disponibles,
   ¿qué pasa exactamente?

3. ¿Cuándo "Storage Memory" cede espacio a "Execution Memory"?
   ¿Qué ocurre con los datos cacheados?

4. ¿El `spark.executor.memoryOverhead` (off-heap) es parte del heap de 8 GB?

5. Para un job con mucho shuffle y sin caché:
   ¿Cómo configurarías `spark.memory.storageFraction`?

**Pista:** Storage y Execution compiten por el mismo pool de Spark Memory.
Si Execution necesita más memoria y Storage tiene datos cacheados, Spark
puede desalojar (evict) datos cacheados para dársela a Execution.
Si no hay caché pero Execution sigue necesitando más, hace spill a disco.
Para jobs sin caché, reducir `spark.memory.storageFraction` (ej: 0.2 en lugar de 0.5)
da más memoria a Execution y reduce el spill.

---

### Ejercicio 5.6.2 — Detectar y medir el spill

```python
# El spill se detecta en Spark UI → Stage → Summary Metrics:
# "Spill (memory)": bytes desalojados de memoria antes de ir a disco
# "Spill (disk)": bytes escritos a disco (lo que realmente es el cuello de botella)

# También visible en los logs del executor:
# [info] ExternalSorter: spilling in-memory map of 256.0 MB to disk

# En el código: spark.executor.extraJavaOptions puede añadir GC logging
# para ver cuándo el GC trabaja duro (síntoma de presión de memoria)

# Forzar un spill para observarlo:
spark.conf.set("spark.memory.fraction", "0.3")  # reducir memoria disponible
spark.conf.set("spark.sql.shuffle.partitions", "10")  # particiones grandes

df.groupBy("user_id").agg(F.collect_list("evento"))  # collect_list acumula en memoria
# Con configuración restrictiva → spill garantizado
```

**Restricciones:**
1. Crear un job que genera spill
2. Medir el impacto del spill en tiempo de ejecución
3. Resolver el spill de tres formas: más memoria, más particiones, cambio de algoritmo
4. Comparar el speedup de cada solución

---

### Ejercicio 5.6.3 — OOM: cuándo el spill no es suficiente

```python
# El OOM ocurre cuando Spark no puede hacer spill de algunos datos:
# 1. Datos que deben estar en memoria completos (un solo objeto muy grande)
# 2. El heap del Driver (no del executor) se llena

# Causa más frecuente de Driver OOM:
resultado = df.collect()  # trae TODOS los datos al Driver
# Si df tiene 100 GB → el Driver necesita 100 GB de RAM

# La solución correcta:
df.write.parquet("s3://resultado/")  # escribir desde los executors
# O si necesitas iterar:
for batch in df.toLocalIterator():  # streaming desde executors al driver
    procesar(batch)

# Causa más frecuente de Executor OOM:
df.groupBy("session_id").agg(F.collect_list("evento"))
# Si una sesión tiene 10M eventos → collect_list crea una lista de 10M en memoria
# Sin posibilidad de spill (un solo objeto)
```

**Preguntas:**

1. ¿Por qué `collect()` sobre un DataFrame grande causa OOM en el Driver
   pero no en los executors?

2. ¿`collect_list()` puede hacer spill? ¿Por qué no?

3. ¿Qué alternativas a `collect_list` existen para el caso de sesiones
   con muchos eventos?

4. Un executor tiene 8 GB de heap pero el task intenta crear un objeto de 20 GB.
   ¿Spark puede distribuir ese objeto entre múltiples executors?

5. ¿Cuándo es correcto aumentar la memoria del Driver?

**Pista:** `collect_list` acumula todos los valores de un grupo en una lista Java.
Una lista de 10M strings de 50 bytes cada uno = ~500 MB solo para ese grupo.
Si hay 1,000 grupos así simultáneamente en el mismo executor, son 500 GB —
imposible de spill porque el GC de Java no puede serializar parcialmente una lista.
Las alternativas: (1) limitar con `collect_list` + slice para top-N eventos,
(2) usar `window functions` en lugar de collect + process,
(3) procesar con `foreachPartition` en Python para tener más control.

---

### Ejercicio 5.6.4 — Configurar memoria para un job específico

Para el siguiente job, calcular la configuración de memoria óptima:

```python
# El job:
resultado = (spark.read.parquet("s3://eventos/")  # 2 TB
    .groupBy("user_id", F.date_trunc("day", "timestamp"))
    .agg(
        F.count("*").alias("eventos"),
        F.sum("valor").alias("valor_total"),
        F.approx_count_distinct("session_id").alias("sesiones"),
    )
    .join(
        spark.read.parquet("s3://clientes/"),   # 5 GB
        on="user_id"
    )
)

# Cluster disponible: 20 máquinas × 16 cores × 64 GB RAM
```

Calcular:
1. Configuración de executors: cuántos por máquina, cuántos cores y RAM cada uno
2. `spark.sql.shuffle.partitions` para el groupBy
3. ¿El join con `clientes` (5 GB) puede ser broadcast?
4. Configuración de `spark.memory.fraction` y `spark.memory.storageFraction`
5. ¿Hay riesgo de spill? ¿Cómo lo calcularías?

**Pista:** Regla práctica para sizing de executors:
- No más de 5 cores por executor (el GC de Java escala mal con más de 5 threads)
- ~4 GB de RAM por core (ej: 5 cores × 4 GB = 20 GB por executor)
- Dejar 1 GB por executor para el OS
- Con 64 GB y 16 cores por máquina:
  → 3 executors por máquina (5 cores × 3 = 15 cores, 20 GB × 3 = 60 GB, +1 GB OS = 61 GB ✓)
  → 20 máquinas × 3 executors = 60 executors en total
  → 60 × 5 = 300 cores totales

> ⚙️ Versión: las recomendaciones de sizing de executors han evolucionado.
> En Spark 3.x con G1GC (el GC por defecto), el límite de 5 cores por executor
> es menos crítico que con el CMS GC de versiones anteriores. Sin embargo,
> el principio de balance entre paralelismo y overhead de GC sigue vigente.

---

### Ejercicio 5.6.5 — Diagnosticar: el job que falla solo en producción

**Tipo: Diagnosticar**

Un job funciona perfectamente en staging (datos de prueba, 10 GB)
pero falla con OOM en producción (datos reales, 2 TB):

```
ERROR: java.lang.OutOfMemoryError: GC overhead limit exceeded
  at org.apache.spark.sql.execution.joins.SortMergeJoinExec...

Stage 4 / Task 87 of 200
  GC Time: 85% of task time  ← !!
  Peak Memory: 7.8 GB of 8 GB available
  Spill (disk): 0 bytes  ← no hay spill
```

**Preguntas:**

1. ¿Por qué el GC time es 85%? ¿Qué significa eso en términos de rendimiento?

2. ¿Por qué no hay spill si la memoria está al límite?

3. "GC overhead limit exceeded" es un error específico de Java.
   ¿Qué condición exacta lo activa?

4. ¿El job de staging (10 GB) representa fielmente el de producción (2 TB)?
   ¿Qué condición hace que el staging no detecte este problema?

5. Propón dos soluciones, una a corto plazo (resolver hoy) y otra a largo plazo
   (prevenir en el futuro).

**Pista:** "GC overhead limit exceeded" se activa cuando el GC de Java gasta
más del 98% del tiempo recuperando menos del 2% del heap. Esto ocurre cuando
hay muchos objetos pequeños de larga vida que no se pueden liberar (memory leak
efectivo). En Spark, es frecuente con UDFs que crean muchos objetos Python
en el heap del executor, o con `collect_list` sobre muchos grupos.
El staging con 10 GB no detecta esto porque la proporción de memoria usada
es diferente — con 2 TB, el job tiene 200× más objetos en memoria simultáneamente.

---

## Sección 5.7 — El Pipeline de Optimización Completo

### Ejercicio 5.7.1 — El proceso de optimización step-by-step

```python
# Paso 1: Medir el baseline ANTES de optimizar
# (no optimizes lo que no has medido)

import time
from contextlib import contextmanager

@contextmanager
def medir_job(nombre: str):
    inicio = time.perf_counter()
    yield
    duracion = time.perf_counter() - inicio
    print(f"[{nombre}] {duracion:.1f}s")

# Ejecutar el pipeline con métricas:
with medir_job("pipeline_original"):
    resultado = pipeline_original(spark)

# Paso 2: Identificar el bottleneck con Spark UI
# (ver qué stage es el más lento)

# Paso 3: Aplicar UNA optimización a la vez
# (para saber qué funcionó)

with medir_job("con_broadcast"):
    resultado = pipeline_con_broadcast(spark)

with medir_job("con_salting"):
    resultado = pipeline_con_salting(spark)

# Paso 4: Verificar la corrección del resultado
assert_dataframes_iguales(resultado_original, resultado_optimizado)

# Paso 5: Medir en producción (los datos de prueba no siempre representan producción)
```

**Restricciones:**
1. Crear un pipeline con múltiples problemas (skew + particionamiento incorrecto
   + broadcast posible + caché no usada)
2. Resolver los problemas uno a uno, midiendo el impacto de cada cambio
3. Documentar cada optimización con: problema identificado, solución aplicada,
   speedup obtenido, tradeoff introducido

---

### Ejercicio 5.7.2 — Optimizar un pipeline real de e-commerce

El siguiente pipeline es lento. Tiene 4 problemas identificables.
Encontrarlos y resolverlos:

```python
# Pipeline de métricas de conversión por campaña de marketing:
def calcular_conversion_campana(spark: SparkSession) -> DataFrame:
    
    # Fuentes de datos:
    df_eventos = spark.read.parquet("s3://eventos/")          # 500 GB
    df_compras = spark.read.parquet("s3://compras/")          # 200 GB  
    df_campanas = spark.read.parquet("s3://campanas/")        # 50 MB
    df_productos = spark.read.parquet("s3://productos/")      # 800 MB
    
    # Paso 1: enriquecer eventos con campaña
    eventos_con_campana = df_eventos \
        .join(df_campanas, on="campana_id") \
        .filter(F.col("tipo_evento").isin(["click", "view", "add_to_cart"]))
    
    # Paso 2: calcular sesiones de usuario
    sesiones = eventos_con_campana \
        .groupBy("user_id", "campana_id") \
        .agg(
            F.collect_list("tipo_evento").alias("eventos"),   # ← problema potencial
            F.min("timestamp").alias("primera_interaccion"),
            F.max("timestamp").alias("ultima_interaccion"),
        )
    
    # Paso 3: join con compras
    sesiones_con_compras = sesiones \
        .join(df_compras, on="user_id") \
        .filter(
            F.col("timestamp_compra") > F.col("primera_interaccion")
        )
    
    # Paso 4: enriquecer con productos
    resultado = sesiones_con_compras \
        .join(df_productos, on="producto_id") \
        .groupBy("campana_id", "categoria_producto") \
        .agg(
            F.countDistinct("user_id").alias("usuarios_convertidos"),
            F.sum("monto_compra").alias("revenue"),
            F.count("*").alias("compras"),
        ) \
        .orderBy(F.col("revenue").desc())

    return resultado
```

Los 4 problemas:
1. Un join que debería ser broadcast pero no lo es
2. Un uso de `collect_list` que puede causar OOM en usuarios activos
3. Un join cuyo orden no está optimizado (filtra tarde)
4. Un `orderBy` global innecesariamente costoso

**Restricciones:**
1. Identificar los 4 problemas sin ejecutar el código (análisis estático)
2. Reescribir el pipeline corrigiendo los 4 problemas
3. Verificar con `explain()` que el plan físico refleja las optimizaciones
4. Estimar el speedup total esperado

---

### Ejercicio 5.7.3 — Benchmark: medir el impacto de cada optimización

```python
# Ejecutar el pipeline del ejercicio anterior en cuatro versiones:
configs = [
    ("original",         pipeline_original),
    ("con_broadcast",    pipeline_con_broadcast),
    ("sin_collect_list", pipeline_sin_collect_list),
    ("filtro_temprano",  pipeline_filtro_temprano),
    ("sin_orderby",      pipeline_sin_orderby),
    ("completo",         pipeline_todas_optimizaciones),
]

resultados = {}
for nombre, fn in configs:
    inicio = time.perf_counter()
    fn(spark).count()
    resultados[nombre] = time.perf_counter() - inicio
    print(f"{nombre}: {resultados[nombre]:.1f}s")

# Calcular el speedup acumulado de cada optimización:
baseline = resultados["original"]
for nombre, tiempo in resultados.items():
    print(f"{nombre}: {tiempo:.1f}s ({baseline/tiempo:.1f}× speedup)")
```

**Restricciones:**
1. Ejecutar el benchmark con un dataset de 10 GB
2. Identificar qué optimización tiene el mayor impacto
3. ¿El orden en que se aplican las optimizaciones importa?
4. ¿Qué optimización introduce el mayor riesgo (puede romper el resultado)?

---

### Ejercicio 5.7.4 — Crear un checklist de optimización

**Tipo: Construir**

Construir un checklist que un data engineer puede usar antes de lanzar
un job de Spark a producción:

```
CHECKLIST DE OPTIMIZACIÓN DE SPARK
====================================

ANTES DE EJECUTAR:
□ ¿Hay joins entre tablas grandes donde una podría ser broadcast?
  → Verificar tamaños, añadir hint broadcast() si < 500 MB en memoria
□ ¿Hay groupBy sobre columnas con distribución muy desigual?
  → Diagnosticar con diagnosticar_skew(), considerar salting o AQE
□ ¿El DataFrame base se usa más de una vez?
  → Añadir .cache() y materializar antes de las queries secundarias
□ ¿spark.sql.shuffle.partitions es apropiado para el volumen de datos?
  → Calcular con la fórmula, o dejar AQE ajustar automáticamente

AL LEER DATOS:
□ ???
□ ???

AL ESCRIBIR DATOS:
□ ???
□ ???

DESPUÉS DE EJECUTAR (verificar en Spark UI):
□ ???
□ ???
```

**Restricciones:**
1. Completar el checklist con al menos 5 items por sección
2. Para cada item, añadir: síntoma (cómo detectarlo), solución, riesgo
3. Ordenar los items por frecuencia de aparición en producción
4. Incluir el "golden path": la secuencia de pasos que resuelve el 80%
   de los problemas de rendimiento

---

### Ejercicio 5.7.5 — El pipeline de producción: integrar todo

**Tipo: Diseñar**

Diseñar un pipeline de Spark production-ready para el sistema de e-commerce
que hemos construido a lo largo del repositorio, con todos los problemas
de performance resueltos desde el diseño:

```
Requisitos:
  - Datos: 3 TB/día de eventos (clicks, vistas, compras)
  - Dimensiones: clientes (10 GB), productos (2 GB), campañas (50 MB)
  - SLA: completar en < 2 horas, una vez por hora
  - Salida: 5 tablas de métricas en Delta Lake

Métricas a calcular:
  1. Revenue por campaña (por hora, por día, por semana)
  2. Tasa de conversión por producto (funnel: vista → carrito → compra)
  3. Usuarios activos por segmento (por hora)
  4. Top 100 productos por revenue (por día)
  5. Alertas de anomalías: producto sin stock, caída de conversión
```

El diseño debe especificar:
1. Estrategia de lectura: particionamiento, column pruning
2. Estrategia de joins: broadcast, bucket, sort-merge según tamaño
3. Estrategia de agregación: combiner, groupBy vs window functions
4. Configuración de memoria y particiones
5. Estrategia de caché para dimensiones reutilizadas
6. Plan de degradación si el job no termina en 2 horas

---

## Resumen del capítulo

**El stack de optimización en orden de impacto:**

```
1. Formato y particionamiento (Cap.02)
   → 5-50× speedup solo por leer menos datos
   → Prerequisito: no se puede optimizar lo que no está bien almacenado

2. Broadcast join para tablas pequeñas
   → Elimina el shuffle más costoso
   → Impacto: el shuffle de 1 TB se convierte en broadcast de 500 MB
   → Cuándo: tabla < 500 MB en memoria, se usa en múltiples joins

3. Resolver data skew
   → El stage que tarda 100× más que los demás
   → AQE lo detecta automáticamente en Spark 3.2+
   → Para skew muy severo o no detectado: salting manual

4. Particionamiento correcto del shuffle
   → spark.sql.shuffle.partitions ≈ datos_shuffleados_MB / 128
   → AQE ajusta automáticamente con coalescePartitions
   → Impacto: reduce spill, reduce overhead de scheduling

5. Caché para datos reutilizados
   → Solo cuando el dataset se usa 2+ veces en el mismo job
   → Liberar siempre con unpersist() en un bloque finally
   → No cachear datasets más grandes que la memoria disponible
```

**La regla del 80/20 de la optimización de Spark:**

> El 80% de los problemas de rendimiento en producción son una de estas tres cosas:
> (1) un join que debería ser broadcast pero no lo es,
> (2) data skew en un groupBy o join,
> (3) formato o particionamiento incorrecto de los datos de entrada.
>
> Aprende a diagnosticar y resolver estos tres antes de explorar
> las optimizaciones más avanzadas.

**La secuencia correcta:**

```
Medir (Spark UI) → Identificar cuello de botella → Aplicar UNA optimización
→ Medir de nuevo → Verificar corrección → Repetir
```

Nunca optimizar sin medir primero.
Nunca aplicar varias optimizaciones a la vez sin medir cada una.
Nunca asumir que una optimización que funcionó para un dataset
funcionará para todos los datasets.
