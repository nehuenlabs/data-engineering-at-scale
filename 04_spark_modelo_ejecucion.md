# Guía de Ejercicios — Cap.04: Spark — Modelo de Ejecución y Diagnóstico

> Este capítulo construye sobre el modelo de Map/Reduce del Cap.03
> y los formatos del Cap.02 para explicar cómo Spark los implementa.
>
> El objetivo no es memorizar la API de Spark — eso lo hace la documentación.
> El objetivo es entender por qué Spark hace lo que hace: por qué hay stages,
> por qué el shuffle es costoso, por qué el número de particiones importa.
> Con ese entendimiento, los problemas de producción tienen causas obvias
> en lugar de parecer aleatorios.
>
> La Spark UI es el microscopio. Este capítulo enseña a usarlo.

---

## El mapa del territorio

Antes de los ejercicios, el modelo completo en una página:

```
TU CÓDIGO (Driver Python/Scala):
  spark.read.parquet(...).filter(...).groupBy(...).write(...)
  
  No procesa datos. Construye un plan lógico.
  
                    ↓ .explain() muestra esto
                    
PLAN LÓGICO → PLAN FÍSICO (Catalyst Optimizer):
  El optimizador reordena operaciones, aplica predicate pushdown,
  elige el tipo de join (broadcast vs sort-merge), y genera el plan físico.
  
                    ↓ DAGScheduler traduce esto
                    
STAGES (grupos de tareas sin shuffle):
  Stage 0: leer Parquet + filter (narrow transformations → 1 stage)
  Stage 1: groupBy (shuffle → nuevo stage)
  Stage 2: agregación final + write (narrow → 1 stage)
  
  Un stage termina donde empieza un shuffle.
  Un shuffle empieza un nuevo stage.
  
                    ↓ TaskScheduler asigna esto
                    
TASKS (unidad mínima de trabajo):
  1 task = 1 partición = 1 core = 1 thread en un executor.
  Stage 0: 100 tasks (100 particiones de Parquet)
  Stage 1: 200 tasks (spark.sql.shuffle.partitions = 200 por defecto)
  Stage 2: 200 tasks
  
  Total: 500 tasks distribuidas en N executors.
```

---

## La relación entre este capítulo y el repositorio de concurrencia

En el Cap.24 del repositorio de concurrencia se cubrió Spark en detalle.
Este capítulo es complementario, no repetitivo:

- **Cap.24 (concurrencia)**: Spark como caso de estudio de paralelismo.
  Enfoque en data skew, UDFs, y memoria.
- **Cap.04 (este)**: Spark como herramienta de data engineering.
  Enfoque en el modelo de ejecución, el DAG, y el diagnóstico sistemático.

Los ejercicios de diagnóstico (tipo "leer el Spark UI") son nuevos.
Los conceptos de particiones y shuffles se refuerzan desde un ángulo distinto.

---

## Tabla de contenidos

- [Sección 4.1 — El DAG: del código al plan de ejecución](#sección-41--el-dag-del-código-al-plan-de-ejecución)
- [Sección 4.2 — Stages y tasks: la unidad real de trabajo](#sección-42--stages-y-tasks-la-unidad-real-de-trabajo)
- [Sección 4.3 — El Catalyst Optimizer: quién optimiza antes que tú](#sección-43--el-catalyst-optimizer-quién-optimiza-antes-que-tú)
- [Sección 4.4 — Broadcast joins: eliminar el shuffle en joins](#sección-44--broadcast-joins-eliminar-el-shuffle-en-joins)
- [Sección 4.5 — Adaptive Query Execution: el optimizador en runtime](#sección-45--adaptive-query-execution-el-optimizador-en-runtime)
- [Sección 4.6 — La Spark UI: el diagnóstico sistemático](#sección-46--la-spark-ui-el-diagnóstico-sistemático)
- [Sección 4.7 — El checklist de diagnóstico](#sección-47--el-checklist-de-diagnóstico)

---

## Sección 4.1 — El DAG: del Código al Plan de Ejecución

### Ejercicio 4.1.1 — Construir el DAG manualmente

**Tipo: Leer/construir**

Antes de ejecutar cualquier código, Spark construye un DAG de operaciones.
Aprender a leerlo mentalmente — antes de ver el `explain()` — es la habilidad
más valiosa para escribir pipelines eficientes.

```python
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.getOrCreate()

df_ventas    = spark.read.parquet("s3://bucket/ventas/")       # A
df_clientes  = spark.read.parquet("s3://bucket/clientes/")     # B
df_productos = spark.read.parquet("s3://bucket/productos/")    # C (100 MB)

resultado = (
    df_ventas                                                   # (1)
    .filter(F.col("fecha") >= "2024-01-01")                    # (2)
    .join(df_clientes, on="cliente_id")                        # (3)
    .join(df_productos, on="producto_id")                      # (4)
    .filter(F.col("categoria") == "electronico")               # (5)
    .groupBy("region", F.month("fecha").alias("mes"))          # (6)
    .agg(
        F.sum("monto").alias("revenue"),
        F.countDistinct("cliente_id").alias("clientes_unicos")
    )                                                           # (7)
    .orderBy(F.col("revenue").desc())                          # (8)
)
```

**Sin ejecutar el código**, responder:

1. Dibujar el DAG de este pipeline:
   - ¿Cuántas fuentes de datos hay?
   - ¿Qué operaciones son narrow (sin shuffle)?
   - ¿Qué operaciones son wide (con shuffle)?

2. ¿Cuántos stages tendrá el plan? ¿Dónde empieza cada uno?

3. El join (3) entre `df_ventas` (grande) y `df_clientes` (¿tamaño desconocido?)
   puede generar un shuffle o no. ¿Qué información necesitas para saberlo?

4. El join (4) con `df_productos` (100 MB) probablemente no generará shuffle.
   ¿Por qué? ¿Qué haría Spark automáticamente?

5. ¿El filtro (5) ocurre antes o después del join (4) en el plan físico?
   ¿Importa el orden?

6. `countDistinct` es más costoso que `count`. ¿Por qué?
   ¿Genera un shuffle adicional?

**Después**: ejecutar `resultado.explain(True)` y comparar el plan real
con tu predicción. Anotar las diferencias y entender por qué existen.

**Pista:** El filtro (5) sobre `categoria` que es una columna de `df_productos`
no puede aplicarse antes del join (4) — `df_ventas` no tiene la columna `categoria`.
Pero el optimizer de Spark puede mover filtros sobre columnas propias de cada DataFrame
antes del join: si `df_ventas` tuviera una columna `tipo` y filtraras por `tipo`,
Spark movería ese filtro antes del join para reducir los datos que participan.
Esto se llama "predicate pushdown a través de joins".

---

### Ejercicio 4.1.2 — Leer el explain(): el plano de construcción del job

```python
df = spark.read.parquet("transacciones.parquet")

pipeline = (df
    .filter(F.col("monto") > 100)
    .withColumn("monto_iva", F.col("monto") * 1.19)
    .groupBy("region")
    .agg(F.sum("monto_iva").alias("total_iva"))
    .orderBy("total_iva")
)

# explain() sin argumentos: plan físico resumido
pipeline.explain()

# explain(True): plan lógico, plan optimizado, y plan físico
pipeline.explain(True)
```

El output de `explain(True)` para este pipeline:

```
== Parsed Logical Plan ==
Sort [total_iva ASC NULLS FIRST], true
+- Aggregate [region], [region, sum(monto_iva) AS total_iva]
   +- Project [region, (monto * 1.19) AS monto_iva]
      +- Filter (monto > 100)
         +- Relation [id,monto,region,...] parquet

== Analyzed Logical Plan ==
(igual, después de resolver nombres de columnas)

== Optimized Logical Plan ==
Sort [total_iva ASC NULLS FIRST], true
+- Aggregate [region], [region, sum((monto * 1.19)) AS total_iva]
   +- Project [region, monto]
      +- Filter (isnotnull(monto) AND (monto > 100))         ← isnotnull añadido
         +- Relation [monto,region] parquet                  ← solo columnas usadas

== Physical Plan ==
AdaptiveSparkPlan (isFinalPlan=false)
+- Sort [total_iva ASC], true, 0
   +- Exchange rangepartitioning(total_iva ASC, 200)         ← shuffle para sort
      +- HashAggregate [region] (functions: [sum(monto_iva)])   ← reduce final
         +- Exchange hashpartitioning(region, 200)            ← shuffle para groupBy
            +- HashAggregate [region] (functions: [partial_sum(monto_iva)])  ← combiner
               +- Project [region, (monto * 1.19)]
                  +- Filter (isnotnull(monto) AND (monto > 100))
                     +- FileScan parquet [monto, region]
                        PushedFilters: [IsNotNull(monto), GreaterThan(monto,100.0)]
                        PartitionFilters: []
                        ReadSchema: struct<monto:double,region:string>
```

**Preguntas:**

1. ¿Qué diferencia hay entre el "Parsed Logical Plan" y el "Optimized Logical Plan"?
   Identifica al menos tres optimizaciones que aplicó el Catalyst Optimizer.

2. ¿Por qué el plan físico tiene DOS `HashAggregate`?
   ¿Cuál es el "combiner" del Cap.03?

3. ¿Qué son los `PushedFilters`? ¿Cuándo un filtro NO puede ser "pushed"?

4. El `ReadSchema` muestra solo `monto` y `region`. El Parquet tiene 20 columnas.
   ¿Qué optimización es esta y cuánto I/O ahorra?

5. `AdaptiveSparkPlan (isFinalPlan=false)`: ¿qué cambia cuando `isFinalPlan=true`?
   ¿Cómo verías el plan final?

**Pista:** Las tres optimizaciones del Catalyst en este plan:
(1) Column pruning: el `ReadSchema` solo incluye las columnas necesarias,
(2) Predicate pushdown: los filtros se empujan al lector de Parquet (`PushedFilters`),
(3) isnotnull: el optimizer añade automáticamente un filtro de nulls para columnas
usadas en operaciones que no toleran null (como la comparación `> 100`).
Esto elimina filas null antes de que lleguen a la operación, ahorrando procesamiento.

---

### Ejercicio 4.1.3 — Lazy evaluation: cuándo Spark realmente trabaja

```python
import time

df = spark.read.parquet("s3://bucket/ventas_10gb.parquet")

# Ninguna de estas líneas ejecuta nada:
t0 = time.perf_counter()
df_filtrado = df.filter(F.col("monto") > 1000)              # (A) — lazy
df_enriquecido = df_filtrado.withColumn(                     # (B) — lazy
    "categoria", F.when(F.col("monto") > 5000, "premium").otherwise("standard")
)
df_agrupado = df_enriquecido.groupBy("region", "categoria") # (C) — lazy
df_resultado = df_agrupado.agg(F.sum("monto"))               # (D) — lazy
t1 = time.perf_counter()
print(f"Construir el DAG: {(t1-t0)*1000:.1f}ms")  # muy rápido

# Esta línea SÍ ejecuta todo el pipeline:
t2 = time.perf_counter()
df_resultado.show(10)                                        # (E) — ACCIÓN
t3 = time.perf_counter()
print(f"Ejecutar el job: {t3-t2:.1f}s")           # segundos a minutos

# Segunda acción sobre el mismo DAG — re-ejecuta todo:
t4 = time.perf_counter()
conteo = df_resultado.count()                                # (F) — ACCIÓN
t5 = time.perf_counter()
print(f"Segunda acción (sin caché): {t5-t4:.1f}s")  # similar a la primera

# Con caché: el DAG se ejecuta una vez y el resultado se guarda:
df_resultado.cache()
df_resultado.show(10)    # primera acción: ejecuta Y cachea
df_resultado.count()     # segunda acción: lee de caché
```

**Preguntas:**

1. ¿Por qué construir el DAG tarda milisegundos pero ejecutarlo tarda segundos?

2. ¿Cuántas veces se leen los 10 GB del Parquet en el código anterior
   (sin caché)? ¿Y con caché?

3. Las **acciones** disparan la ejecución. ¿Cuáles de las siguientes son acciones?
   `show()`, `collect()`, `count()`, `write()`, `toPandas()`, `explain()`,
   `printSchema()`, `cache()`, `filter()`, `groupBy()`

4. `cache()` es lazy también — no ejecuta nada cuando se llama.
   ¿Cuándo se materializa el caché?

5. Si tienes un pipeline con 3 acciones y el DataFrame no está cacheado,
   ¿cuántas veces se lee el archivo de origen?

**Pista:** `explain()` y `printSchema()` NO son acciones — no leen datos.
`explain()` solo muestra el plan. `cache()` tampoco es una acción — marca el
DataFrame para ser cacheado la próxima vez que se materialice.
La regla: si el resultado incluye datos reales (no solo metadatos),
es una acción. `count()` retorna un número (dato real) → acción.
`printSchema()` retorna el schema (metadato del plan) → no acción.

---

### Ejercicio 4.1.4 — El DAG de Spark vs el DAG de Map/Reduce clásico

**Tipo: Comparar**

```
Map/Reduce clásico (Hadoop):
  Job 1: [datos] → Map → Shuffle → Reduce → [HDFS]
  Job 2: [HDFS] → Map → Shuffle → Reduce → [HDFS]
  Job 3: [HDFS] → Map → Shuffle → Reduce → [HDFS]
  
  Para N etapas: N lecturas de disco + N escrituras de disco.
  Un pipeline de 5 etapas = 10 operaciones de disco.

Spark DAG:
  [datos] → Stage 0 → [memoria] → Stage 1 → [memoria] → Stage 2 → [resultado]
  
  Un shuffle escribe a disco local del executor (no HDFS compartido).
  Los datos entre stages pueden vivir en memoria.
  Un pipeline de 5 stages: 1 lectura de disco + 1 escritura de resultado
  + N shuffles (que pueden ser mayoritariamente en memoria con suficiente RAM).
```

**Restricciones:**

1. Implementar un pipeline de 5 etapas en Spark RDD:
```python
rdd = sc.textFile("corpus.txt")
# Etapa 1: tokenizar
# Etapa 2: filtrar stop words
# Etapa 3: calcular TF (term frequency por documento)
# Etapa 4: calcular IDF (inverse document frequency)
# Etapa 5: calcular TF-IDF
```

2. Usar `.cache()` entre etapas donde es beneficioso
3. Con `spark.eventLog.enabled=true`, ver el DAG completo en Spark History Server
4. Contar los shuffles reales del pipeline

**Pista:** TF-IDF requiere calcular IDF sobre todos los documentos,
lo que necesita un groupBy + sum (shuffle) para contar en cuántos documentos
aparece cada término. Luego join entre el TF por documento y el IDF global
(otro shuffle). El pipeline tiene exactamente 2 shuffles independientemente
de cuántas etapas "lógicas" definas.

---

### Ejercicio 4.1.5 — Leer: el DAG que tiene más shuffles de los necesarios

**Tipo: Diagnosticar**

Un data engineer reporta que su pipeline tarda 45 minutos para 50 GB de datos.
El explain() muestra:

```
Physical Plan:
Sort [revenue DESC]
+- Exchange rangepartitioning(revenue DESC, 200)          ← shuffle 4
   +- HashAggregate [region, mes] (final)
      +- Exchange hashpartitioning(region, mes, 200)      ← shuffle 3
         +- HashAggregate [region, mes] (partial)
            +- Project [region, mes, monto, cliente_id]
               +- SortMergeJoin [cliente_id]              ← shuffle 2
                  :- Sort [cliente_id]
                  :  +- Exchange hashpartitioning(cliente_id, 200)
                  :     +- Filter [fecha >= 2024-01-01]
                  :        +- FileScan parquet ventas [...]
                  +- Sort [cliente_id]
                     +- Exchange hashpartitioning(cliente_id, 200) ← shuffle 1
                        +- FileScan parquet clientes [...]

Bytes shuffleados:
  Shuffle 1: 45 GB (clientes completo)
  Shuffle 2: 50 GB (ventas filtrado)
  Shuffle 3: 800 MB (post-groupBy)
  Shuffle 4: 100 KB (post-sort, para ordenar resultado final)
```

El dataset de `clientes` tiene 500 MB.

**Preguntas:**

1. ¿El Shuffle 1 (45 GB para `clientes`) tiene sentido dado que el archivo
   de clientes tiene 500 MB? ¿Qué ocurrió?

2. ¿Por qué Spark eligió SortMergeJoin en lugar de BroadcastHashJoin
   para este join?

3. ¿Qué cambio en la configuración habría forzado el BroadcastHashJoin?

4. Si el BroadcastHashJoin elimina los Shuffles 1 y 2, ¿cuánto tiempo
   ahorrarías? (estimar basándote en los GB shuffleados)

5. ¿El Shuffle 4 (100 KB para el sort final) es necesario?
   ¿Cuándo podría eliminarse?

**Pista:** El threshold de autoBroadcast en Spark es 10 MB por defecto
(`spark.sql.autoBroadcastJoinThreshold = 10MB`). El archivo de clientes
tiene 500 MB — por encima del threshold. Si los datos de clientes necesarios
después del filtro (si hubiera un filtro) son < 10 MB, el broadcast ocurriría.
Para forzarlo manualmente: `df_clientes.join(broadcast(df_ventas), ...)` o
aumentar el threshold: `spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "600mb")`.

---

## Sección 4.2 — Stages y Tasks: la Unidad Real de Trabajo

### Ejercicio 4.2.1 — La relación entre particiones y tasks

```python
# El número de tasks de un stage = el número de particiones

# Stage de lectura: particiones determinadas por el archivo
df = spark.read.parquet("datos.parquet")
print(f"Particiones después de leer: {df.rdd.getNumPartitions()}")
# Típicamente: ceil(tamaño_archivo / 128MB)
# Para un Parquet de 5 GB: ~40 particiones → 40 tasks en Stage 0

# Stage de shuffle: particiones determinadas por spark.sql.shuffle.partitions
df_agrupado = df.groupBy("region").count()
# El Exchange crea spark.sql.shuffle.partitions particiones (default: 200)
# → 200 tasks en Stage 1

# Verificar con el plan:
df_agrupado.explain()
# Exchange hashpartitioning(region, 200) ← el "200" son las particiones del shuffle

# Cambiar las particiones del shuffle:
spark.conf.set("spark.sql.shuffle.partitions", "50")
df_agrupado_50 = df.groupBy("region").count()
# → 50 tasks en Stage 1
```

**Restricciones:**

1. Para un Parquet de 10 GB con Row Groups de 128 MB:
   ¿cuántas tasks tiene Stage 0 (lectura)?

2. ¿Qué pasa si el Parquet tiene un solo Row Group de 10 GB?
   (ver el Ejercicio 2.2.5 del Cap.02)

3. Ejecutar `groupBy` con `shuffle.partitions = 10, 200, 2000` sobre 10 GB.
   Medir el tiempo para cada configuración y explicar la curva.

4. ¿Cuándo 200 particiones de shuffle es demasiado?
   ¿Cuándo es muy poco?

5. Implementar una heurística que elija `shuffle.partitions` basándose en
   el tamaño estimado de los datos de entrada:
```python
def particiones_optimas(tamaño_gb: float, tamaño_particion_mb: int = 128) -> int:
    """Estimar el número óptimo de particiones de shuffle."""
    # TODO
    pass
```

**Pista:** La heurística práctica: el tamaño de cada partición de shuffle
debería ser 64-256 MB para un buen balance entre overhead de scheduling
y paralelismo. Con 10 GB de datos después del shuffle:
`10 GB / 128 MB ≈ 80 particiones`. Con AQE habilitado, Spark ajusta esto
automáticamente basándose en las estadísticas reales del shuffle.

---

### Ejercicio 4.2.2 — Task scheduling: dónde van las tasks

```
El TaskScheduler asigna tasks a executors considerando:

1. Locality (preferencia local):
   DATA_LOCAL:    el executor tiene los datos en su disco local
   NODE_LOCAL:    el executor está en el mismo nodo que los datos
   RACK_LOCAL:    el executor está en el mismo rack
   ANY:           cualquier executor (para shuffles, donde los datos vienen de la red)

2. Recursos disponibles:
   Un executor con N cores puede ejecutar N tasks simultáneamente.

3. Fairness:
   Spark intenta que todos los executors tengan trabajo.
   Si un executor termina antes, recibe las tasks pendientes de otros.

En S3 (no HDFS), la locality es siempre ANY — los datos no tienen
ubicación física asociada a nodos específicos del cluster.
Por eso S3 es ligeramente menos eficiente que HDFS para lectura
pero mucho más simple de administrar.
```

**Preguntas:**

1. Un cluster tiene 10 executors con 5 cores cada uno (50 cores totales).
   Un stage tiene 75 tasks. ¿Cuántas rondas de ejecución hay?
   ¿Qué pasa con las tasks más lentas de la primera ronda?

2. ¿Por qué la "speculative execution" de Spark es útil?
   ¿Cuándo puede ser contraproducente?

3. Si un executor tiene 4 cores y está ejecutando 4 tasks,
   ¿cuánto tiempo tarda una task de 10 segundos vs 100 segundos
   en afectar el tiempo total del stage?

4. ¿Qué es un "task failure" vs un "executor failure"?
   ¿Cómo reacciona Spark ante cada uno?

5. `spark.task.maxFailures = 4` (por defecto). ¿Qué significa?

**Pista:** Speculative execution (`.speculation=true`): si una task está
en el P75 de duración y las demás ya terminaron, Spark lanza una copia
en otro executor. Cuando cualquiera termina, la otra se cancela.
Contraproducente cuando: el problema es data skew (la task lenta tiene más datos,
no es "lenta" por una máquina defectuosa — la copia también será lenta),
o cuando la task tiene side effects (escribir a una base de datos, llamar a una API).

---

### Ejercicio 4.2.3 — Skipped stages: el caché en acción

```python
df = spark.read.parquet("datos.parquet")
df_filtrado = df.filter(F.col("monto") > 1000)
df_agrupado = df_filtrado.groupBy("region").agg(F.sum("monto"))

# Sin caché: cada acción re-ejecuta todo
df_agrupado.show()   # ejecuta Stage 0, 1, 2
df_agrupado.count()  # re-ejecuta Stage 0, 1, 2

# Con caché:
df_agrupado.cache()
df_agrupado.show()   # ejecuta Stage 0, 1, 2 y cachea el resultado
df_agrupado.count()  # Stage 0, 1, 2 aparecen como "Skipped" en Spark UI
```

En la Spark UI, los stages "Skipped" aparecen en verde con el tiempo `0s`.

**Restricciones:**

1. Verificar en Spark UI que los stages cacheados aparecen como "Skipped"
2. ¿Qué nivel de storage usa `.cache()`? (MEMORY_AND_DISK? MEMORY_ONLY?)
3. ¿Qué pasa si el resultado cacheado no cabe en la memoria del executor?
4. Implementar un pipeline con múltiples acciones y medir el ahorro del caché
5. ¿Cuándo es mejor no cachear, incluso si el DataFrame se usa múltiples veces?

**Pista:** `.cache()` es equivalente a `.persist(StorageLevel.MEMORY_AND_DISK)`.
Si el DataFrame no cabe en memoria, las particiones que no caben se escriben
a disco local del executor — más lento que memoria pero sin perder el caché.
`MEMORY_ONLY` descarta las particiones que no caben y las recalcula cuando se necesitan.
El caché puede no valer la pena cuando: el DataFrame es pequeño (el overhead de cachear
supera el beneficio de releer), o cuando la segunda acción accede a un subconjunto
diferente de datos (partition pruning puede ser más eficiente que cachear todo).

---

### Ejercicio 4.2.4 — Leer: el stage que tiene el doble de tasks esperado

**Tipo: Diagnosticar**

Un pipeline tiene esta configuración y este output:

```python
spark.conf.set("spark.sql.shuffle.partitions", "100")

df = spark.read.parquet("s3://bucket/ventas/")
resultado = (df
    .filter(F.col("fecha") >= "2024-01-01")
    .groupBy("region")
    .agg(F.sum("monto"))
    .join(df_regiones, on="region")  # df_regiones: 10 MB
    .orderBy("region")
)
resultado.write.parquet("s3://bucket/resultado/")
```

Spark UI muestra:
```
Stage 0: Read + Filter       → 500 tasks   (expected: ~80 para 10 GB)
Stage 1: GroupBy (partial)   → 500 tasks   (parte del combiner, narrow)
Stage 2: Shuffle + GroupBy   → 100 tasks   (spark.sql.shuffle.partitions)
Stage 3: Join + Sort         →  50 tasks   ← !! ¿por qué 50?
Stage 4: Write               →  50 tasks
```

**Preguntas:**

1. ¿Por qué Stage 0 tiene 500 tasks si el Parquet ocupa ~10 GB?
   (pista: cuántos archivos tiene el directorio)

2. ¿Por qué Stage 3 tiene 50 tasks en lugar de 100?

3. ¿El join con `df_regiones` (10 MB) generó un shuffle? ¿Cómo lo sabes?

4. ¿Por qué Stage 4 (Write) tiene el mismo número de tasks que Stage 3?

5. ¿Cómo cambiarías el número de tasks de Stage 4 para controlar
   el número de archivos de output?

**Pista:** Stage 0 con 500 tasks → el directorio tiene 500 archivos pequeños
(cada uno < 128 MB). El "small files problem" del Cap.02: muchos archivos pequeños
crean muchas particiones → muchas tasks → overhead de scheduling.
Stage 3 con 50 tasks: AQE detectó que el groupBy produjo datos pequeños
(pocos MB porque "region" tiene pocos valores distintos) y coalesció
las 100 particiones en 50. Esto es el "coalesce post-shuffle" de AQE.

---

### Ejercicio 4.2.5 — Calcular el grado de paralelismo efectivo

**Tipo: Calcular/diseñar**

```
Grado de paralelismo efectivo = min(tasks activas, cores disponibles)

Caso 1:
  Tasks: 10 (Stage 0)
  Cores: 100 (20 executors × 5 cores)
  Paralelismo efectivo: 10 (90 cores ociosos)
  → suboptimal: el stage tarda 1/10 del tiempo si hubiera 100 tasks

Caso 2:
  Tasks: 10,000
  Cores: 100
  Paralelismo efectivo: 100 (≈ óptimo por rondas)
  → ok: se ejecutan en 100 rondas de 100 tasks
  → overhead: 100 rondas de scheduling

Caso 3:
  Tasks: 101
  Cores: 100
  Paralelismo efectivo: 100 en la primera ronda, 1 en la segunda
  → suboptimal: el stage tarda 2 "rondas" pero la segunda tiene 1 sola task
  → en la segunda ronda, 99 cores están ociosos
```

**Restricciones:**

1. Calcular el "grado de paralelismo efectivo" y el tiempo estimado para:
   - 1,000 tasks, 50 cores, cada task tarda 10s
   - 1,000 tasks, 200 cores, cada task tarda 10s
   - 1,001 tasks, 1,000 cores, cada task tarda 10s
   - 999 tasks, 1,000 cores, cada task tarda 10s

2. ¿Por qué es mejor tener 1,000 tasks que 1,001 para 1,000 cores?

3. Para un job con 1 TB de datos y 50 cores, ¿cuál es el número ideal de tasks?

4. Implementar una función que recomiende el número de tasks dado
   el tamaño de datos y el número de cores:
```python
def tasks_optimas(
    tamaño_datos_gb: float,
    num_cores: int,
    tamaño_particion_mb: int = 128,
) -> int:
    # TODO
    # Considerar: overhead de scheduling, tamaño de partición,
    # múltiplo del número de cores para evitar el "último ronda vacía"
    pass
```

---

## Sección 4.3 — El Catalyst Optimizer: Quién Optimiza Antes que Tú

### Ejercicio 4.3.1 — Las cuatro fases del Catalyst

```
El Catalyst Optimizer transforma tu plan lógico en un plan físico eficiente
en cuatro fases:

Fase 1 — Analysis:
  Resolver nombres de columnas y tipos.
  "monto" → columna de tipo DOUBLE del DataFrame df_ventas
  Falla si una columna no existe → AnalysisException

Fase 2 — Logical Optimization:
  Aplicar reglas de reescritura al plan lógico:
  - Predicate pushdown: mover filtros lo más cerca posible de los datos
  - Constant folding: 2 + 3 → 5 (en tiempo de compilación)
  - Column pruning: eliminar columnas no usadas
  - Predicate simplification: (a > 5) AND (a > 3) → (a > 5)

Fase 3 — Physical Planning:
  Elegir implementaciones físicas para cada operación lógica:
  - Join: ¿BroadcastHashJoin, SortMergeJoin, o ShuffledHashJoin?
  - Aggregation: ¿HashAggregate o SortAggregate?
  - Sort: ¿TungstenSort o ExternalSort?

Fase 4 — Code Generation (Whole-Stage CodeGen):
  Generar bytecode Java optimizado para ejecutar el plan.
  En lugar de llamar funciones de Spark por columna,
  genera código que procesa toda la fila en un loop tight.
  Hasta 10× más rápido que la interpretación row-by-row.
```

**Preguntas:**

1. ¿Cuándo ocurre un `AnalysisException`? ¿En tiempo de definición del pipeline
   o en tiempo de ejecución? ¿Qué dice el error?

2. "Constant folding": ¿qué optimiza Catalyst en esta expresión?
   ```python
   F.col("monto") * 1.0 * 1.19 * 1.0
   ```

3. "Column pruning": si tu pipeline tiene `select("*")` al principio
   pero solo usa 3 de 50 columnas al final, ¿Catalyst elimina las 47
   columnas innecesarias? ¿En qué punto?

4. ¿Cuándo el Catalyst elige SortMergeJoin sobre BroadcastHashJoin?

5. "Whole-Stage CodeGen" genera bytecode JVM. ¿Esto beneficia a PySpark?
   ¿O solo a Scala/Java?

**Pista:** Whole-Stage CodeGen beneficia a PySpark para operaciones que
ocurren dentro de la JVM (como joins, aggregations con funciones nativas).
Las UDFs de Python interrumpen el CodeGen — el executor necesita cruzar
la frontera JVM/Python para cada fila, lo que elimina el beneficio del código
generado. Las Pandas UDFs parcialmente recuperan el beneficio al operar
sobre batches completos (columnas Arrow) en lugar de filas individuales.

---

### Ejercicio 4.3.2 — Predicate pushdown: ver el impacto real

```python
# Caso 1: filtro que Catalyst puede pushear al lector de Parquet
df = spark.read.parquet("transacciones.parquet")
df_filtrado = df.filter(F.col("monto") > 1000)
df_filtrado.explain()
# PushedFilters: [IsNotNull(monto), GreaterThan(monto,1000.0)]
# ReadSchema: struct<id:long,monto:double,region:string>
# → El filtro se aplica en el lector, antes de deserializar las otras columnas

# Caso 2: filtro que NO puede pushearse (función UDF)
@F.udf("boolean")
def es_fraude(monto, region):
    return monto > 10000 and region == "norte"

df_filtrado_udf = df.filter(es_fraude(F.col("monto"), F.col("region")))
df_filtrado_udf.explain()
# PushedFilters: []
# → Spark no puede pushear la UDF al lector → lee todo y filtra después

# Caso 3: filtro que se puede reescribir para que sea pusheable
df_filtrado_nativo = df.filter(
    (F.col("monto") > 10000) & (F.col("region") == "norte")
)
df_filtrado_nativo.explain()
# PushedFilters: [IsNotNull(monto), IsNotNull(region),
#                 GreaterThan(monto,10000.0), EqualTo(region,norte)]
# → Ambos filtros se pushean → lee solo filas que pasan ambos criterios
```

**Restricciones:**

1. Medir el I/O real (bytes leídos) para los tres casos con el mismo archivo de 10 GB
2. ¿Cuánto I/O ahorra el caso 3 vs el caso 2?
3. Encontrar 3 tipos de expresiones que bloquean el predicate pushdown
4. ¿El predicate pushdown funciona a través de joins?
   Si filtras después de un join, ¿puede Spark pushear el filtro antes?

---

### Ejercicio 4.3.3 — El optimizador que no puede ayudarte

**Tipo: Analizar**

El Catalyst Optimizer es poderoso pero tiene límites.
Identificar qué parte de cada pipeline el Catalyst NO puede optimizar
y por qué:

```python
# Pipeline 1: UDF en el filter
df.filter(mi_udf_python(F.col("descripcion")))

# Pipeline 2: orderBy temprano
df.orderBy("timestamp").filter(F.col("monto") > 1000)

# Pipeline 3: select("*") sin necesidad
df.select("*").filter(...).groupBy("region").count()

# Pipeline 4: múltiples count distintos en el mismo groupBy
df.groupBy("region").agg(
    F.countDistinct("cliente_id"),    # shuffle 1
    F.countDistinct("producto_id"),   # ¿shuffle adicional?
)

# Pipeline 5: join después de agregar cuando debería ser antes
df_grande.groupBy("region").agg(F.sum("monto")).join(df_pequeño, on="region")
# vs.
df_grande.join(broadcast(df_pequeño), on="region").groupBy("region").agg(F.sum("monto"))
```

**Preguntas:** Para cada pipeline, responder:
- ¿Qué optimización debería aplicarse?
- ¿El Catalyst la aplica automáticamente?
- Si no, ¿cómo la aplicas tú manualmente?

**Pista:** El Pipeline 2 es interesante: Catalyst puede mover el filtro
ANTES del orderBy si el filtro no depende del orden. Pruébalo con `explain()`.
El Pipeline 4 tiene dos `countDistinct` — cada uno requiere de-duplicación
que en principio necesita un shuffle. El optimizer de Spark 3.x puede
combinar algunas aggregations en un solo shuffle, pero no siempre.
Verificar con `explain()` si hay uno o dos shuffles.

---

### Ejercicio 4.3.4 — Hint: cuando tú sabes más que el optimizador

```python
# El optimizer elige joins basándose en estadísticas del archivo.
# Si las estadísticas son incorrectas o no existen, puede elegir mal.
# Los hints permiten sobreescribir la decisión:

# Forzar broadcast join:
from pyspark.sql.functions import broadcast
df_ventas.join(broadcast(df_clientes), on="cliente_id")

# Forzar sort-merge join (aunque el optimizer elegiría broadcast):
df_ventas.hint("MERGE").join(df_clientes, on="cliente_id")

# Forzar shuffle-hash join:
df_ventas.hint("SHUFFLE_HASH").join(df_clientes, on="cliente_id")

# Hint para repartitionamiento:
df.hint("REPARTITION", 100)  # equivale a .repartition(100)
df.hint("REPARTITION_BY_RANGE", "region")  # por columna
```

**Restricciones:**

1. Comparar el plan y el tiempo con y sin hints para un join de 10 GB × 500 MB
2. ¿Cuándo el hint de broadcast puede causar OOM?
3. ¿Cómo verificas que el hint fue respetado (el optimizer puede ignorar algunos)?
4. Encontrar un caso donde el optimizer elige broadcast pero sort-merge sería mejor

**Pista:** El optimizer puede ignorar hints si contradicen restricciones de seguridad
(por ejemplo, un broadcast hint en un DataFrame de 100 GB podría causar OOM
y Spark lo ignora si el tamaño supera un límite absoluto).
Para verificar que el hint fue respetado: buscar el tipo de join en `explain()`.
Si ves `BroadcastHashJoin`, el hint de broadcast funcionó.
Si ves `SortMergeJoin` después de un hint de broadcast, fue ignorado.

---

### Ejercicio 4.3.5 — Leer: el plan que el optimizador no pudo mejorar

**Tipo: Diagnosticar**

Un data engineer trae este plan que tarda 3 horas:

```
Physical Plan:
SortMergeJoin [user_id]
:- Sort [user_id ASC]
:  +- Exchange hashpartitioning(user_id, 200)        ← shuffle A: 800 GB
:     +- FileScan parquet eventos [user_id, evento, ts]
:        PartitionFilters: []                        ← !! sin partition filter
:        PushedFilters: []                           ← !! sin pushed filters
:        ReadSchema: struct<user_id:long,evento:string,ts:timestamp,
:                          payload:string,metadata:string,raw:string>
:                   (incluye columnas no usadas)    ← !! column pruning fallido
+- Sort [user_id ASC]
   +- Exchange hashpartitioning(user_id, 200)        ← shuffle B: 50 GB
      +- FileScan parquet perfiles [user_id, region, segmento]
```

El código que generó este plan:

```python
df_eventos = spark.read.parquet("s3://eventos/")      # 800 GB, particionado por fecha
df_perfiles = spark.read.parquet("s3://perfiles/")    # 50 GB

resultado = df_eventos.join(df_perfiles, on="user_id")

# El ingeniero solo necesita eventos de hoy:
hoy = "2024-01-22"
resultado_filtrado = resultado.filter(F.col("ts").cast("date") == hoy)
resultado_filtrado.select("user_id", "evento", "region").show()
```

**Preguntas:**

1. ¿Por qué `PartitionFilters: []` si los datos están particionados por fecha?

2. ¿Por qué `PushedFilters: []` si hay un filtro sobre `ts`?

3. ¿Por qué el `ReadSchema` incluye columnas no usadas?

4. ¿En qué orden debería haberse escrito el pipeline para que el
   Catalyst pudiera optimizarlo?

5. Reescribir el pipeline optimizado y verificar con `explain()`
   que el plan mejoró.

**Pista:** Los tres problemas tienen la misma causa: el filtro se aplica
DESPUÉS del join, no antes. El Catalyst puede mover filtros a través
de algunos operadores pero no de joins complejos con ciertas condiciones.
Al aplicar el filtro antes del join:
```python
df_eventos_hoy = df_eventos.filter(F.col("ts").cast("date") == "2024-01-22")
```
El Catalyst ve la condición directamente sobre `df_eventos` → puede aplicar
PartitionFilter (saltarse los directorios de otras fechas) y PushedFilter.
El `select` antes del join activa column pruning → ReadSchema solo incluye
las columnas necesarias.

---

## Sección 4.4 — Broadcast Joins: Eliminar el Shuffle en Joins

### Ejercicio 4.4.1 — Cómo funciona un broadcast join

```
Sort-Merge Join (para tablas grandes):
  Fase 1: shuffle de ambas tablas por la join key
  Fase 2: sort de cada partición por la join key
  Fase 3: merge de las dos tablas ordenadas
  
  Costo: 2 shuffles completos (ambas tablas viajan por la red)
  
Broadcast Hash Join (cuando una tabla es pequeña):
  Fase 1: el driver lee la tabla pequeña completa
  Fase 2: el driver envía (broadcast) la tabla a todos los executors
  Fase 3: cada executor hace hash join localmente con sus particiones de la tabla grande
  
  Costo: 0 shuffles (la tabla pequeña se replica, no se shufflea)
  
  Tradeoff: la tabla pequeña debe caber en memoria de cada executor
  Regla práctica: tabla pequeña < 10% de la memoria del executor
```

```python
# Spark UI muestra el tipo de join en el plan físico:
# BroadcastHashJoin → sin shuffle
# SortMergeJoin → con shuffle (2 Exchange nodes en el plan)
# ShuffledHashJoin → con shuffle (sin sort, para tablas medianas)

# Automatic broadcast (si la tabla < autoBroadcastJoinThreshold):
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "50mb")
resultado = df_ventas.join(df_productos, on="producto_id")
# Si df_productos < 50 MB → BroadcastHashJoin automático

# Manual broadcast (para forzar cuando el threshold no aplica):
from pyspark.sql.functions import broadcast
resultado = df_ventas.join(broadcast(df_productos), on="producto_id")

# Ver el plan para confirmar el tipo de join:
resultado.explain()
# BroadcastHashJoin [producto_id#5L], [producto_id#12L], Inner, BuildRight
#                                                                  ↑ la tabla pequeña
```

**Preguntas:**

1. ¿Por qué la tabla pequeña se llama `BuildRight` o `BuildLeft` en el plan?
   ¿Qué estructura de datos se "construye"?

2. Si tienes un join entre una tabla de 1 TB y una de 100 MB,
   ¿el broadcast join es siempre mejor que el sort-merge join?
   ¿Qué factores podrían hacerlo peor?

3. ¿Qué pasa si haces broadcast de una tabla de 10 GB en executors de 4 GB?

4. ¿El broadcast join puede usarse con condiciones de join complejas
   (no solo igualdad)? Por ejemplo: `df_a.join(df_b, df_a.id < df_b.id)`

5. ¿Cuántas veces se envía la tabla pequeña por la red en un broadcast join?
   ¿A quién se envía?

**Pista:** La tabla pequeña en un broadcast join se envía una vez desde el driver
a cada executor (no de executor a executor). Con 50 executors y una tabla de 100 MB:
se envían 50 × 100 MB = 5 GB de tráfico de red total.
Comparado con un sort-merge join donde ambas tablas se shufflean:
si la tabla grande es 1 TB y la pequeña es 100 MB, el sort-merge mueve ~1.1 TB.
El broadcast mueve solo 5 GB — mucho mejor a pesar de la replicación.

---

### Ejercicio 4.4.2 — Broadcast de variables: más que joins

El broadcast también aplica a variables usadas en UDFs:

```python
# Sin broadcast: cada task del executor recibe una copia de la variable
# Si la variable es grande y hay 1000 tasks → 1000 copias
precio_por_categoria = {
    "electronico": 1.15,
    "ropa": 1.0,
    "alimento": 0.8,
}

df_con_descuento = df.withColumn(
    "precio_ajustado",
    F.col("precio") * F.lit(precio_por_categoria).getItem(F.col("categoria"))
    # ← esto NO funciona directamente; necesitas una UDF o un join
)

# Con broadcast variable: distribuida eficientemente una vez por executor
precio_broadcast = spark.sparkContext.broadcast(precio_por_categoria)

@F.udf("double")
def ajustar_precio(precio, categoria):
    factor = precio_broadcast.value.get(categoria, 1.0)
    return precio * factor

df_con_descuento = df.withColumn(
    "precio_ajustado",
    ajustar_precio(F.col("precio"), F.col("categoria"))
)

# Limpiar cuando ya no se necesita:
precio_broadcast.unpersist()
```

**Restricciones:**

1. Comparar el tiempo con y sin broadcast variable para una UDF
   que usa un diccionario de 100 MB
2. ¿Cuántas veces se serializa el diccionario sin broadcast variable?
3. ¿Cuántas veces con broadcast variable?
4. Implementar el mismo resultado sin UDF (usando join con broadcast):
   ¿cuál es más eficiente?

**Pista:** Sin broadcast variable: el diccionario se serializa (pickle) para
cada task — si tienes 1000 tasks, se serializa 1000 veces.
Con broadcast: se serializa una vez por executor — si tienes 20 executors,
se serializa 20 veces. Para un diccionario de 100 MB: 1000 × 100 MB = 100 GB
de serialización vs 20 × 100 MB = 2 GB. La diferencia en tiempo es significativa.
Alternativa sin UDF: convertir el diccionario a un DataFrame de 2 columnas
(`categoria`, `factor`) y hacer un join con broadcast — sin UDF Python.

---

### Ejercicio 4.4.3 — Leer: el broadcast que hace OOM

**Tipo: Diagnosticar**

Un pipeline falla con OOM después de añadir un broadcast hint:

```
ERROR: java.lang.OutOfMemoryError: Java heap space
  at org.apache.spark.sql.execution.joins.BroadcastHashJoinExec...
  
Stage 2 failed after 3 attempts:
  Task 0 (attempt 3) failed:
  ExecutorLostFailure (executor 5 exited caused by one of the running tasks)
  Reason: Container killed by YARN for exceeding memory limits.
  12.5 GB of 12 GB physical memory used.
```

El código:
```python
df_ventas = spark.read.parquet("s3://ventas/")      # 2 TB
df_clientes = spark.read.parquet("s3://clientes/")  # 800 MB

resultado = df_ventas.join(broadcast(df_clientes), on="cliente_id")
```

Configuración del cluster:
```
spark.executor.memory = 10g
spark.executor.memoryOverhead = 2g
Total por executor: 12 GB
```

**Preguntas:**

1. ¿Por qué el broadcast de 800 MB causa OOM en executors de 12 GB?

2. ¿Qué overhead adicional tiene el broadcast table en memoria
   además del tamaño del dato original?

3. ¿Cuál es el límite seguro de broadcast para este cluster?

4. ¿Cuáles son las dos alternativas si el broadcast no es viable?

5. ¿Cómo habrías detectado este problema antes de que fallara en producción?

**Pista:** La tabla de 800 MB en disco puede ser mayor en memoria.
Con Parquet comprimido, el factor de expansión puede ser 3-5×.
800 MB en Parquet → 2.4-4 GB en memoria. Además, el broadcast crea
una copia del HashedRelation (la tabla indexada para el join) que puede
ser 2× el tamaño del dato. Total: 800 MB × 5 (expansión) × 2 (hash table)
= 8 GB — casi la mitad de la memoria del executor, dejando poco para el
procesamiento de las particiones de la tabla grande.

---

### Ejercicio 4.4.4 — El join correcto para cada caso

Para cada combinación, determinar el tipo de join óptimo y justificar:

```
Caso  Tabla A        Tabla B       Distribuidas por   Join óptimo
────────────────────────────────────────────────────────────────────
1     1 TB           100 KB        distinta key       ???
2     1 TB           500 MB        distinta key       ???
3     1 TB           500 MB        misma key (256 buckets) ???
4     1 TB           1 TB          distinta key       ???
5     1 TB           1 TB          misma key          ???
6     1 TB (stream)  10 MB (batch) N/A                ???
7     500 MB         500 MB        distinta key       ???
8     1 TB           100 MB        distinta key, Tabla B tiene muchos nulls ???
```

**Pista:** El Caso 3 (misma key, mismo número de buckets) permite un "bucket join"
sin shuffle — cada bucket de A se une directamente con el bucket correspondiente de B.
El Caso 8 (muchos nulls en la join key) es especialmente importante:
los nulls no hacen join con nada, pero si hay millones de nulls, se shufflean
a un solo reducer (los nulls se hashean al mismo bucket). Filtrar nulls antes
del join puede ahorrar mucho shuffle.

---

### Ejercicio 4.4.5 — Construir: un sistema de recomendación de tipo de join

```python
from pyspark.sql import DataFrame

def recomendar_join(
    df_left: DataFrame,
    df_right: DataFrame,
    join_key: str,
    memoria_executor_gb: float = 8.0,
    broadcast_threshold_mb: float = 200.0,
) -> dict:
    """
    Analiza los DataFrames y recomienda el tipo de join óptimo.
    
    Retorna:
        {
            "tipo_join": "broadcast" | "sort_merge" | "bucket",
            "justificacion": str,
            "advertencias": list[str],
            "codigo_sugerido": str,
        }
    """
    # Obtener estadísticas (solo del plan, sin ejecutar):
    # Nota: en Spark, el tamaño exacto requiere ejecutar el plan.
    # Se puede estimar del número de particiones y tamaño promedio.
    
    tamaño_left_mb = _estimar_tamaño(df_left)
    tamaño_right_mb = _estimar_tamaño(df_right)
    
    tabla_pequeña = min(tamaño_left_mb, tamaño_right_mb)
    
    # Lógica de recomendación:
    # TODO: implementar
    pass

def _estimar_tamaño(df: DataFrame) -> float:
    """Estimación del tamaño en MB basada en metadatos del plan."""
    # En Spark, el SizeInBytes está en el plan lógico:
    plan = df._jdf.queryExecution().analyzed()
    tamaño = plan.stats().sizeInBytes()
    return tamaño / (1024 * 1024)  # bytes → MB
```

**Restricciones:**
1. Implementar `recomendar_join()` con lógica para los tres tipos de join
2. Añadir detección de data skew potencial en la join key
3. Verificar la recomendación con los casos del ejercicio anterior

---

## Sección 4.5 — Adaptive Query Execution: el Optimizador en Runtime

### Ejercicio 4.5.1 — Qué puede cambiar AQE después de cada stage

```python
# AQE requiere Spark 3.0+
spark.conf.set("spark.sql.adaptive.enabled", "true")

# AQE puede cambiar tres cosas en runtime:

# 1. Coalescing de particiones de shuffle (reducir particiones vacías/pequeñas):
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128mb")
# Después de un shuffle con 200 particiones, si los datos son 500 MB total:
# AQE detecta que cada partición tiene ~2.5 MB (muy pequeño)
# → coalesce a ceil(500 MB / 128 MB) = 4 particiones
# → stage siguiente tiene 4 tasks en lugar de 200

# 2. Cambio de join strategy (de sort-merge a broadcast si la tabla es pequeña):
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")
# Después de ejecutar un stage, AQE conoce el tamaño real de sus outputs.
# Si el output de la tabla "grande" resulta ser 5 MB → puede hacerse broadcast.

# 3. Handling de skew en joins:
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
# Si una partición tiene >5× la mediana de datos → se divide en sub-particiones
```

**Preguntas:**

1. Antes de AQE, el optimizador elegía el número de particiones y el tipo de join
   basándose en estadísticas estáticas. ¿Qué estadísticas usaba?

2. ¿Por qué las estadísticas estáticas son frecuentemente imprecisas?

3. ¿Cuándo AQE puede cambiar un SortMergeJoin a BroadcastHashJoin
   durante la ejecución?

4. ¿El coalescing de AQE puede AUMENTAR el número de particiones?
   ¿O solo puede reducirlas?

5. ¿Cómo verificas en el plan final (`isFinalPlan=true`) qué cambió AQE?

**Pista:** Las estadísticas estáticas vienen del metastore (Hive, Glue) o de
ANALYZE TABLE que calcula estadísticas sobre los datos. Si los datos cambiaron
desde el último ANALYZE TABLE, las estadísticas son obsoletas. AQE no usa
estadísticas estáticas — usa los tamaños reales medidos después de cada shuffle.
La limitación de AQE: solo puede cambiar decisiones que vienen después de un
shuffle. La primera lectura del Parquet no puede beneficiarse de AQE.

---

### Ejercicio 4.5.2 — AQE y skew: el caso práctico

```python
# Dataset con data skew severo:
# 99% de los datos tienen region = "norte"
# 1% tiene region = "sur", "este", "oeste"

n = 10_000_000
df_skew = spark.range(n).select(
    F.col("id"),
    F.when(F.rand() < 0.99, "norte").otherwise(
        F.when(F.rand() < 0.33, "sur")
         .when(F.rand() < 0.66, "este")
         .otherwise("oeste")
    ).alias("region"),
    (F.rand() * 1000).alias("monto")
)

# Sin AQE: la partición de "norte" tiene 99× más datos → la task más lenta
spark.conf.set("spark.sql.adaptive.enabled", "false")
df_skew.groupBy("region").agg(F.sum("monto")).collect()
# Stage 1: 200 tasks, 199 terminan en <1s, 1 tarda 3 minutos

# Con AQE skew handling:
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
df_skew.groupBy("region").agg(F.sum("monto")).collect()
# AQE detecta la partición skewed y la divide en sub-particiones
# El plan final muestra "CustomShuffleReaderExec" con las sub-particiones
```

**Restricciones:**
1. Ejecutar ambas versiones y medir el tiempo
2. Verificar en Spark UI la distribución de duración de tasks con y sin AQE
3. Ver en el plan final cuántas sub-particiones creó AQE para "norte"
4. ¿AQE skew handling funciona para aggregations o solo para joins?

---

### Ejercicio 4.5.3 — Los límites de AQE

**Tipo: Analizar**

AQE no resuelve todos los problemas. Identificar qué NO puede mejorar:

```python
# Caso 1: skew en la LECTURA (antes del primer shuffle)
# AQE no puede ayudar aquí porque opera después de cada shuffle.
df = spark.read.parquet("s3://datos/")
# Si un archivo es 100× más grande que los demás → skew en Stage 0
# AQE no ve esto porque no hay shuffle antes

# Caso 2: un plan mal escrito que el optimizador no puede reordenar
df.groupBy("region").agg(F.collect_list("descripcion"))
# collect_list no puede tener combiner → groupByKey completo
# AQE no puede cambiar esto: la semántica de collect_list requiere
# que todos los valores estén en el mismo executor

# Caso 3: UDFs Python que bloquean la optimización
df.filter(mi_udf(F.col("descripcion")))
# AQE no puede pushear UDFs al lector de Parquet

# Caso 4: joins con condiciones no-equi
df_a.join(df_b, df_a.fecha.between(df_b.inicio, df_b.fin))
# No puede ser BroadcastHashJoin (requiere igualdad)
# AQE no puede cambiar la estrategia
```

**Preguntas:**

1. Para el Caso 1 (skew en lectura), ¿qué alternativa tienes?

2. Para el Caso 4 (non-equi join), ¿qué tipo de join hace Spark?
   ¿Es eficiente?

3. ¿Existe alguna forma de resolver el Caso 2 sin cambiar la semántica?

4. Propón un caso donde AQE empeora el rendimiento.
   (pista: el coalescing puede ser contraproducente)

**Pista:** AQE puede empeorar el rendimiento cuando coalesce muchas
particiones pequeñas en pocas grandes justo antes de un join:
si coalesce de 200 a 4 particiones, el join siguiente tiene solo 4 tasks
en paralelo en lugar de 200 → subutilización del cluster para ese stage.
La configuración `spark.sql.adaptive.advisoryPartitionSizeInBytes` controla
cuánto coalesce hace AQE — a veces hay que desactivarlo selectivamente.

---

### Ejercicio 4.5.4 — Configurar AQE para tu workload

```python
# Configuraciones de AQE y cuándo ajustarlas:

spark.conf.set("spark.sql.adaptive.enabled", "true")

# Coalescing: reduce particiones pequeñas post-shuffle
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128mb")
# Si tienes shuffles con muy pocos datos, AQE coalesce hasta 1 partición.
# Si tienes muchos joins posteriores, puede que quieras un mínimo más alto.

# Skew join: divide particiones skewed
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
# Una partición es "skewed" si es >5× la mediana Y >256 MB
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256mb")

# Join switch: cambiar sort-merge a broadcast si la tabla resulta pequeña
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")
```

**Restricciones:**

Para cada tipo de workload, determinar la configuración óptima de AQE:

1. ETL batch diario: 100 tablas de 1-500 GB, muchos joins, datos balanceados
2. Análisis de clickstream: datos muy skewed (páginas populares vs raras)
3. Agregaciones simples: groupBy + sum sobre 1 TB, pocas claves distintas
4. Pipeline de ML features: muchos joins pequeños + aggregations complejas

---

### Ejercicio 4.5.5 — Leer: antes y después de habilitar AQE

**Tipo: Diagnosticar**

Antes de habilitar AQE, un job tarda 2 horas:
```
Stage 3: HashAggregate (final)
  Tasks: 200
  Duration distribution:
    P50: 45s
    P95: 52s
    Max: 2h 3min  ← !!
  Shuffle read per task:
    P50: 250 MB
    P95: 280 MB
    Max: 95 GB    ← !!
```

Después de habilitar AQE con `skewJoin.enabled=true`:
```
Stage 3: HashAggregate (final)
  Tasks: 235  ← aumentó de 200
  Duration distribution:
    P50: 43s
    P95: 55s
    Max: 4min
  Shuffle read per task:
    P50: 240 MB
    P95: 270 MB
    Max: 750 MB
```

**Preguntas:**

1. ¿Por qué el número de tasks aumentó de 200 a 235?

2. El Max de duración bajó de 2h 3min a 4min. ¿Cuántas sub-particiones
   creó AQE para la partición skewed?

3. El Max de shuffle read bajó de 95 GB a 750 MB.
   ¿Cuántas veces se dividió la partición skewed?

4. ¿El P50 (mediana) mejoró significativamente? ¿Por qué?

5. ¿Hay alguna configuración adicional que podría mejorar el P95?

---

## Sección 4.6 — La Spark UI: el Diagnóstico Sistemático

### Ejercicio 4.6.1 — Anatomía de la Spark UI

**Tipo: Leer/explorar**

La Spark UI tiene estas pestañas y lo que muestra cada una:

```
Jobs:
  Lista de todos los jobs (una acción = un job).
  Para cada job: duración, stages, estado.
  La primera pestaña para diagnosticar: "¿qué job es lento?"

Stages:
  Lista de todos los stages de todos los jobs.
  Para cada stage: tasks, duración, input/output, shuffle read/write.
  La segunda pestaña: "¿qué stage es el cuello de botella?"

Tasks (dentro de un stage):
  Lista de todas las tasks de un stage.
  Para cada task: duración, executor, GC time, shuffle read/write.
  La tercera pestaña: "¿hay skew? ¿hay un executor lento?"

SQL:
  DAG visual de cada query SQL/DataFrame.
  Muestra nodos del plan con tiempos de ejecución.
  Esencial para entender dónde está el tiempo.

Storage:
  DataFrames cacheados: cuánto espacio ocupan, qué % está en memoria vs disco.
  Para diagnosticar: "¿el caché está funcionando?"

Executors:
  Estado de cada executor: memoria usada, GC time, tasks activas.
  Para diagnosticar: "¿hay un executor problemático?"

Environment:
  Configuración de Spark.
  Para verificar: "¿los parámetros que configuré realmente se aplicaron?"
```

**Restricciones:**

1. Ejecutar un pipeline complejo y navegar todas las pestañas
2. Encontrar el stage más lento y el motivo
3. Verificar que AQE está habilitado (pestaña Environment)
4. Identificar si hay un executor con más GC time que los demás

---

### Ejercicio 4.6.2 — Leer la distribución de tasks: detectar skew

**Tipo: Leer**

La distribución de tasks en la pestaña Stage es el diagnóstico de skew más rápido.

```
Stage 5 — HashAggregate (final)
Tasks: 200 completed

Summary Metrics:
                    Min      25th    Median   75th     Max
Duration            0.1s     1.2s    1.4s     1.6s     47min   ← skew!!
GC Time             0s       0.1s    0.1s     0.2s     12min
Input               0 B      0 B     0 B      0 B      0 B
Shuffle Read        1.2 MB   280 MB  310 MB   340 MB   72 GB   ← skew!!
Shuffle Write       0 B      0 B     0 B      0 B      0 B
Peak Execution Mem  50 MB    180 MB  200 MB   220 MB   45 GB   ← !!
```

**Preguntas:**

1. ¿Cuántas tasks tienen el problema de skew? (una sola, varias, o muchas)

2. El ratio Max/Median para Shuffle Read es 72 GB / 310 MB ≈ 230×.
   ¿Cuántas veces más datos tiene la task más lenta que la mediana?

3. El "Peak Execution Memory" de 45 GB para la task más lenta supera
   la memoria del executor (que es 32 GB típicamente).
   ¿Qué pasa en ese caso?

4. ¿El problema está en el stage actual o en el stage anterior que generó el shuffle?

5. ¿Cómo encontrarías la clave responsable del skew sin leer el código?

**Pista:** Para encontrar la clave responsable del skew sin leer el código:
en la pestaña Tasks, hacer click en la task más lenta para ver su detalle.
El log de la task frecuentemente muestra la clave que está procesando.
Alternativamente, añadir instrumentación al pipeline:
`df.groupBy("clave").count().orderBy(F.desc("count")).show(5)`
para ver las claves más frecuentes antes de ejecutar el join o groupBy problemático.

---

### Ejercicio 4.6.3 — Leer el SQL DAG: correlacionar código y métricas

**Tipo: Leer**

La pestaña SQL de Spark UI muestra el DAG visual con tiempos reales.
Para el siguiente output de la UI:

```
SQL Query #12 — Duration: 38min

DAG:
  [FileScan parquet]              → 2min  (500 GB leídos, 490 GB output)
       ↓
  [Filter]                        → 0s   (narrow, incluido en FileScan)
       ↓
  [Exchange hashpartitioning]     → 12min (shuffle: 450 GB escritos)
       ↓
  [Sort]                          → 3min
       ↓
  [SortMergeJoin]                 → 15min ← !! el más lento
       ↓
  [Exchange hashpartitioning]     → 4min  (shuffle: 180 GB)
       ↓
  [HashAggregate]                 → 2min
       ↓
  [Exchange rangepartitioning]    → 1min
       ↓
  [Sort]                          → 0.5min
       ↓
  [WriteFiles]                    → 0.5min
```

El join es entre `df_ventas` (500 GB) y `df_clientes` (200 MB).

**Preguntas:**

1. ¿Por qué el SortMergeJoin tarda 15 minutos siendo el paso más lento?

2. El primer Exchange (shuffle) mueve 450 GB. ¿Debería ser así para
   un join entre 500 GB y 200 MB?

3. ¿El Exchange no usa BroadcastHashJoin para la tabla de 200 MB?
   ¿Qué threshold tendría que configurarse?

4. Si agregas `broadcast(df_clientes)`, ¿cuántos nodos desaparecen del DAG?

5. ¿El tiempo de 38 minutos para el job podría reducirse a menos de 10
   solo con un cambio en el tipo de join? Calcular.

**Pista:** El threshold de autoBroadcast por defecto es 10 MB.
El archivo de clientes es 200 MB — por encima del threshold.
Si el archivo en Parquet es 200 MB pero los datos en memoria son 400 MB
(factor de expansión ×2), y el executor tiene 8 GB de heap para ejecución,
un broadcast de 400 MB es el 5% de la memoria — razonable.
Configurar: `spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "300mb")`
y verificar con `explain()` que el plan cambia a `BroadcastHashJoin`.

---

### Ejercicio 4.6.4 — Leer el Executors tab: diagnosticar problemas de memoria

**Tipo: Leer/diagnosticar**

```
Executors tab (10 executors, cada uno con 5 cores y 20 GB heap):

ID  Address     State   RDD Blocks  Storage   Disk      Cores  Active  Failed  Completed  GC%    Shuffle Read   Shuffle Write
 0  worker-1    Active  12          8.1/14GB  0 B       5      5       0       1,247      8%     45 GB          32 GB
 1  worker-2    Active  12          7.9/14GB  0 B       5      5       0       1,251      7%     44 GB          31 GB
 2  worker-3    Active  12          8.3/14GB  0 B       5      3       0       1,198      9%     43 GB          30 GB
 3  worker-4    Active  12          12.1/14GB 2.3 GB    5      5       2       1,105     38%     39 GB          28 GB  ← !!
 4  worker-5    Active  12          13.7/14GB 5.8 GB    5      2       8        847      52%     36 GB          25 GB  ← !!
 5  worker-6    Active  12          8.0/14GB  0 B       5      5       0       1,243      8%     44 GB          32 GB
 6  worker-7    Active  12          7.8/14GB  0 B       5      5       0       1,255      7%     45 GB          31 GB
 7  worker-8    Active  12          8.2/14GB  0 B       5      5       0       1,231      8%     44 GB          31 GB
 8  worker-9    Active  12          8.1/14GB  0 B       5      5       0       1,238      8%     43 GB          32 GB
 9  worker-10   Active  12          8.0/14GB  0 B       5      5       0       1,244      8%     44 GB          31 GB
```

**Preguntas:**

1. ¿Qué está pasando con worker-4 y worker-5?

2. ¿Por qué el GC% de worker-4 (38%) y worker-5 (52%) es tan alto?

3. La columna "Disk" muestra 2.3 GB y 5.8 GB para esos workers.
   ¿Qué significa?

4. Worker-5 tiene 8 Failed tasks. ¿Cuál es la causa más probable?

5. ¿Por qué worker-5 tiene menos Completed tasks (847) que los demás (~1250)?

6. ¿Qué acción tomarías de inmediato?

**Pista:** GC% alto (>20%) indica que el executor está luchando por memoria —
el garbage collector de la JVM trabaja más para liberar espacio.
Con 13.7/14 GB de storage memory usada y 5.8 GB en disco (spill),
el executor-4 casi ha llenado su memoria de storage y está spillando a disco.
Las failed tasks probablemente fallaron con OOM antes de poder hacer spill.
La causa raíz: estos executors recibieron las particiones skewed del shuffle —
tienen más datos que los demás (observa que su Shuffle Read es menor —
puede que sean los executors que ESCRIBEN los datos skewed, no los que los leen).

---

### Ejercicio 4.6.5 — Construir: el dashboard de diagnóstico automático

**Tipo: Construir**

La Spark UI es excelente para diagnóstico manual pero no para monitoreo continuo.
Usando la API REST de Spark, construir un script que diagnostica automáticamente:

```python
import requests
from dataclasses import dataclass

SPARK_UI_URL = "http://localhost:4040"

@dataclass
class DiagnosticoStage:
    stage_id: int
    duracion_s: float
    skew_ratio: float          # max_task_duration / median_task_duration
    shuffle_write_gb: float
    spill_disk_gb: float
    gc_porcentaje: float
    problemas: list[str]

def diagnosticar_job(job_id: int) -> list[DiagnosticoStage]:
    """
    Consulta la Spark UI API y diagnostica un job.
    Identifica: skew, spill, GC pressure, shuffles excesivos.
    """
    # Obtener stages del job:
    stages = requests.get(
        f"{SPARK_UI_URL}/api/v1/applications/app-{job_id}/stages"
    ).json()

    diagnosticos = []
    for stage in stages:
        stage_id = stage["stageId"]

        # Obtener métricas de tasks:
        tasks = requests.get(
            f"{SPARK_UI_URL}/api/v1/applications/app-{job_id}/stages/{stage_id}/taskList"
        ).json()

        # TODO: calcular métricas y detectar problemas:
        # - Si max_duration / median_duration > 5 → DATA SKEW
        # - Si spill_disk > 0 → MEMORY PRESSURE
        # - Si gc_time / total_time > 0.1 → GC PRESSURE
        # - Si shuffle_write > threshold → EXCESSIVE SHUFFLE

        diagnostico = DiagnosticoStage(
            stage_id=stage_id,
            # TODO: completar campos
            problemas=[]
        )
        diagnosticos.append(diagnostico)

    return diagnosticos
```

**Restricciones:**
1. Implementar `diagnosticar_job()` con detección de los 4 problemas
2. Añadir recomendaciones específicas para cada problema detectado
3. Generar un reporte en texto con el resumen del diagnóstico
4. Extender para que corra como monitoreo continuo durante la ejecución

---

## Sección 4.7 — El Checklist de Diagnóstico

### Ejercicio 4.7.1 — El protocolo de diagnóstico de 5 minutos

**Tipo: Construir/memorizar**

Cuando un job de Spark es lento, hay un protocolo de diagnóstico que
en 5 minutos identifica la causa en el 80% de los casos:

```
PASO 1 (30s): Spark UI → Jobs tab
  → ¿Cuál es el job más lento?
  → ¿Cuántos stages tiene?
  → ¿Hay stages "Failed"?

PASO 2 (30s): Click en el job → ver stages
  → ¿Cuál es el stage más lento?
  → ¿Tiene muchos tasks? ¿Pocos?

PASO 3 (1min): Click en el stage lento → Summary Metrics
  DIAGNÓSTICO DE SKEW:
  → Max Duration >> P75 Duration → DATA SKEW
    (ver Shuffle Read: Max >> P75 → el skew viene del shuffle anterior)

  DIAGNÓSTICO DE MEMORIA:
  → GC Time > 10% del Duration → MEMORY PRESSURE
  → Stage tiene "Spill (disk)" > 0 → SPILL TO DISK

  DIAGNÓSTICO DE SHUFFLE:
  → Stage tiene Shuffle Write grande → SHUFFLE COSTOSO
    (ir al plan para ver si es evitable)

PASO 4 (1min): Spark UI → SQL tab → ver el DAG
  → ¿Qué nodo tiene más tiempo?
  → ¿Hay Exchange (shuffle) donde podría haber BroadcastHashJoin?
  → ¿Hay Exchange donde ya hay AQE activo (CustomShuffleReader)?

PASO 5 (2min): explain() del pipeline → confirmar el diagnóstico
  → ¿El plan tiene los PushedFilters esperados?
  → ¿El tipo de join es el correcto?
  → ¿Hay columnas innecesarias en ReadSchema?
```

**Restricciones:**

Ejecutar el checklist en los siguientes jobs problemáticos (de ejercicios anteriores):
1. El job con skew severo del Ejercicio 4.6.2
2. El join sin broadcast del Ejercicio 4.6.3
3. El executor con OOM del Ejercicio 4.6.4

Para cada uno: ¿el checklist de 5 minutos llega al diagnóstico correcto?
¿En cuánto tiempo?

---

### Ejercicio 4.7.2 — Leer: el incidente de producción del lunes

**Tipo: Diagnosticar**

Un pipeline de producción que normalmente tarda 45 minutos tarda 6 horas
el lunes por la mañana. El equipo de datos recibe una alerta a las 9:03 AM.

Datos disponibles:
```
Job duration: 6h 12min (normal: 45min)
Input data: 2.3 TB (normal: ~500 GB)
```

Spark UI (accedida a las 9:10 AM):
```
Stage 0: Read + Filter    → 8min    (normal: ~2min)   input: 2.3 TB
Stage 1: Shuffle (join)   → 4h 20min ← !!
Stage 2: GroupBy          → 1h 32min ← !!
Stage 3: Write            → 12min
```

Stage 1 details:
```
Tasks: 200
Duration: P50=1min, P75=2min, Max=4h 18min
Shuffle Read: P50=1.2 GB, Max=892 GB   ← !!
```

**Preguntas:**

1. ¿El aumento de 500 GB a 2.3 TB de input es la causa principal del problema?
   ¿O hay algo adicional?

2. El Shuffle Read máximo de 892 GB para una task. Si los datos de input
   son 2.3 TB y hay 200 particiones de shuffle, ¿cuánto debería ser el
   máximo "normal"? ¿Qué indica la diferencia?

3. ¿Qué evento del fin de semana podría explicar un data skew que no
   existía antes?

4. ¿Cuáles son las dos acciones inmediatas para resolver el incidente?

5. ¿Cómo prevenir este tipo de incidente en el futuro?

**Pista:** Si los datos son 2.3 TB y hay 200 particiones, el promedio
sería 2.3 TB / 200 = 11.5 GB por partición. El máximo normal podría ser
2-3× la mediana (algo de desbalance es esperado) → ~15-20 GB.
Un máximo de 892 GB indica que el 39% del dataset total está en una sola
partición → skew extremo. Una causa frecuente de skew que aparece el lunes:
un evento del fin de semana (rebajas, un viral en redes sociales) generó
millones de transacciones para un solo vendedor/categoría/región, convirtiendo
una key que normalmente tiene el 1% de los datos en una que tiene el 39%.

---

### Ejercicio 4.7.3 — Los antipatrones más frecuentes

**Tipo: Identificar/corregir**

Cada uno de los siguientes es un antipatrón frecuente. Identificarlo,
explicar por qué es problemático, y proponer la corrección:

```python
# Antipatrón 1:
df = spark.read.parquet("datos/")
n = df.count()          # acción 1: ejecuta el job
datos = df.collect()    # acción 2: re-ejecuta el job + trae todo al driver
for fila in datos:
    procesar(fila)

# Antipatrón 2:
resultado = df.groupBy("region").count()
resultado.write.parquet("resultado/")
resultado.show()        # re-ejecuta desde el principio (no hay caché)

# Antipatrón 3:
from functools import reduce
dfs = [spark.read.parquet(f"datos_{i}/") for i in range(100)]
df_final = reduce(lambda a, b: a.union(b), dfs)
# Spark construye un plan con 100 niveles de profundidad

# Antipatrón 4:
df.rdd.map(lambda row: procesar_en_python(row)).toDF()
# Usa RDD Python cuando podrían usarse funciones nativas o Pandas UDF

# Antipatrón 5:
df.orderBy("timestamp").write.parquet("resultado/")
# Ordena globalmente (shuffle grande) antes de escribir a Parquet
# Los readers de Parquet no necesitan orden global
```

---

### Ejercicio 4.7.4 — Construir: el test de regresión de performance

Un problema de producción frecuente: los pipelines se vuelven más lentos
con el tiempo porque los datos crecen o porque alguien hizo un cambio
"inocente" en el código.

```python
from pyspark.sql import SparkSession
from dataclasses import dataclass
import time

@dataclass
class MetricasJob:
    duracion_s: float
    shuffle_read_gb: float
    shuffle_write_gb: float
    num_stages: int
    spill_gb: float
    bytes_leidos_gb: float

def medir_job(spark: SparkSession, pipeline_fn, *args) -> MetricasJob:
    """
    Ejecuta un pipeline y retorna sus métricas de performance.
    Útil para detectar regresiones de rendimiento.
    """
    # Limpiar el caché para medición limpia:
    spark.catalog.clearCache()

    inicio = time.perf_counter()
    pipeline_fn(*args)
    duracion = time.perf_counter() - inicio

    # Obtener métricas del último job via Spark UI API:
    # TODO: obtener shuffle_read, shuffle_write, stages, spill

    return MetricasJob(duracion_s=duracion, ...)

def detectar_regresion(
    metricas_base: MetricasJob,
    metricas_actual: MetricasJob,
    umbral_duracion: float = 1.5,  # 50% más lento
    umbral_shuffle: float = 2.0,   # 100% más shuffle
) -> list[str]:
    """
    Compara métricas y reporta regresiones.
    """
    # TODO: implementar comparación y detección
    pass
```

**Restricciones:**
1. Implementar `medir_job()` usando la API REST de Spark UI
2. Implementar `detectar_regresion()` con umbrales configurables
3. Añadir este test al CI/CD del pipeline con datos de muestra
4. ¿Qué tamaño de datos usar en el test para que sea representativo
   pero rápido de ejecutar?

---

### Ejercicio 4.7.5 — El sistema completo: del código al diagnóstico

**Tipo: Integrar**

Este es el ejercicio integrador del capítulo. Dado el siguiente pipeline
del sistema de e-commerce (introducido en el README):

```python
def pipeline_metricas_diarias(fecha: str):
    """
    Pipeline batch diario: calcular métricas de e-commerce por región y categoría.
    Input: ~50 GB de eventos del día.
    Expected duration: < 15 minutos.
    """
    df_clicks = spark.read.parquet(f"s3://eventos/clicks/fecha={fecha}/")
    df_compras = spark.read.parquet(f"s3://eventos/compras/fecha={fecha}/")
    df_catalogo = spark.read.parquet("s3://catalogo/productos/")   # 500 MB
    df_regiones = spark.read.csv("s3://config/regiones.csv")       # 10 KB

    resultado = (
        df_clicks
        .join(df_compras, on=["session_id", "user_id"], how="left")
        .join(df_catalogo, on="producto_id")
        .join(df_regiones, on="region_code")
        .groupBy("region", "categoria", F.hour("ts").alias("hora"))
        .agg(
            F.count("click_id").alias("clicks"),
            F.count("compra_id").alias("compras"),
            F.sum("monto").alias("revenue"),
            F.countDistinct("user_id").alias("usuarios_unicos"),
        )
        .withColumn("conversion_rate",
            F.col("compras") / F.col("clicks"))
    )

    resultado.write \
        .partitionBy("region", "hora") \
        .mode("overwrite") \
        .parquet(f"s3://metricas/diarias/fecha={fecha}/")
```

**Restricciones:**

1. **Analizar**: sin ejecutar, predecir:
   - ¿Cuántos stages tendrá?
   - ¿Cuáles joins generan shuffle?
   - ¿Cuáles se pueden hacer con broadcast?
   - ¿Hay operaciones potencialmente lentas?

2. **Optimizar**: reescribir el pipeline optimizado:
   - Aplicar broadcast donde corresponde
   - Reordenar filtros y joins
   - Eliminar shuffles innecesarios

3. **Instrumentar**: añadir `explain()` estratégico y logging de métricas

4. **Diagnosticar**: dado este output de Spark UI (hipotético):
   ```
   Stage 2: SortMergeJoin (clicks × compras)
     Tasks: 200, Duration: P50=2min, Max=45min
     Shuffle Read: P50=250MB, Max=90GB
   ```
   ¿Cuál es el problema? ¿Cómo lo resuelves?

5. **Prevenir**: ¿qué monitoreo añadirías para detectar regresiones antes
   de que afecten a los usuarios?

---

## Resumen del capítulo

**El modelo completo de Spark en cinco oraciones:**

```
1. Tu código construye un DAG de transformaciones — ninguna se ejecuta
   hasta la primera acción.

2. El Catalyst Optimizer transforma el DAG lógico en un plan físico
   eficiente: predicate pushdown, column pruning, elección del tipo de join.

3. El DAGScheduler divide el plan en stages (separados por shuffles)
   y el TaskScheduler asigna tasks (una por partición) a executors.

4. AQE ajusta el plan en runtime: coalesce particiones vacías,
   divide particiones skewed, cambia el tipo de join si los datos lo permiten.

5. La Spark UI muestra exactamente lo que pasó: qué stage fue el cuello
   de botella, cuántos datos se shufflearon, si hubo skew o spill.
```

**La cadena de diagnóstico:**

```
Job lento
  ↓ Spark UI → Jobs → ¿qué stage?
Stage lento
  ↓ Spark UI → Stage → ¿skew en tasks? ¿spill? ¿GC alto?
  ↓ Spark UI → SQL → ¿qué nodo del DAG?
Causa identificada
  ↓ explain() → confirmar
  ↓ optimizar → broadcast, repartition, salting, caché
Job optimizado
  ↓ medir nuevamente → verificar mejora
```

**Lo que conecta este capítulo con el Cap.05:**

> Este capítulo cubrió el diagnóstico: cómo encontrar el problema.
> El Cap.05 cubre la solución: cómo resolverlo.
> La secuencia correcta es siempre diagnosticar primero —
> optimizar sin entender el problema es adivinar.
