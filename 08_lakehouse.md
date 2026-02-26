# Guía de Ejercicios — Cap.08: El Lakehouse — Delta Lake, Iceberg y Hudi

> Antes del lakehouse, el stack de datos tenía dos capas separadas:
> el data lake (almacenamiento barato, sin transacciones, difícil de actualizar)
> y el data warehouse (rápido para queries, caro, requiere ETL rígido).
>
> El lakehouse es la apuesta de que puedes tener las propiedades de un warehouse
> (ACID, esquema, actualizaciones eficientes) sobre el almacenamiento barato de un lake.
>
> Delta Lake, Apache Iceberg, y Apache Hudi son tres implementaciones
> de esa apuesta — con filosofías distintas y tradeoffs distintos.

---

## El problema que resuelven

```
Data Lake tradicional (Parquet en S3):
  ✓ Barato: $23/TB/mes en S3
  ✓ Escalable: exabytes sin problema
  ✗ Sin transacciones: dos writes simultáneos pueden corromperse
  ✗ Sin schema enforcement: cualquiera puede escribir cualquier cosa
  ✗ Actualizar una fila: tienes que reescribir el archivo completo
  ✗ Time travel: imposible sin copias manuales
  ✗ Vacuum: los archivos viejos se acumulan indefinidamente

Data Warehouse (Snowflake, Redshift):
  ✓ ACID completo
  ✓ Schema enforcement
  ✓ Actualizaciones eficientes (row-level)
  ✓ Time travel integrado
  ✗ Caro: $2,000-5,000/TB/mes de almacenamiento
  ✗ Lock-in: formato propietario
  ✗ Difícil de integrar con ML (exportar datos es lento)

Lakehouse (Delta Lake / Iceberg / Hudi):
  ✓ Barato: almacenamiento en S3/GCS/ADLS ($23/TB/mes)
  ✓ ACID completo
  ✓ Schema enforcement y evolución
  ✓ Actualizaciones eficientes (file-level)
  ✓ Time travel integrado
  ✓ Abierto: Parquet + metadata en JSON/Avro
  ✓ Multi-engine: Spark, Flink, Trino, DuckDB pueden leer la misma tabla
```

---

## La diferencia central: cómo gestionan el metadata

```
Delta Lake:
  Transacciones como archivo de log JSON en _delta_log/
  _delta_log/00000000000000000000.json  ← commit inicial
  _delta_log/00000000000000000001.json  ← add files
  _delta_log/00000000000000000002.json  ← remove files (delete)
  _delta_log/00000000000000000010.json  ← checkpoint (snapshot)
  Modelo: append-only log de commits

Apache Iceberg:
  Árbol de metadata: snapshot → manifest list → manifest files → data files
  v2/metadata/snap-1234567890.avro   ← snapshot (lista de manifests)
  v2/metadata/manifest-abc.avro      ← manifest (lista de data files)
  v2/data/*.parquet                  ← data files
  Modelo: árbol inmutable de snapshots

Apache Hudi:
  Timeline de commits en .hoodie/
  .hoodie/20240115142301.commit       ← commit metadata
  .hoodie/20240115142301.deltacommit  ← delta de cambios
  Datos en Copy-on-Write o Merge-on-Read
  Modelo: timeline de operaciones con dos storage types
```

---

## Tabla de contenidos

- [Sección 8.1 — Delta Lake: el log de transacciones](#sección-81--delta-lake-el-log-de-transacciones)
- [Sección 8.2 — ACID en el lakehouse: cómo funciona realmente](#sección-82--acid-en-el-lakehouse-cómo-funciona-realmente)
- [Sección 8.3 — Time travel y auditoría](#sección-83--time-travel-y-auditoría)
- [Sección 8.4 — Schema evolution y enforcement](#sección-84--schema-evolution-y-enforcement)
- [Sección 8.5 — Operaciones DML: UPDATE, DELETE, MERGE](#sección-85--operaciones-dml-update-delete-y-merge)
- [Sección 8.6 — Apache Iceberg: el modelo de árbol](#sección-86--apache-iceberg-el-modelo-de-árbol)
- [Sección 8.7 — Comparativa y decisión: Delta Lake vs Iceberg vs Hudi](#sección-87--comparativa-y-decisión-delta-lake-vs-iceberg-vs-hudi)

---

## Sección 8.1 — Delta Lake: el Log de Transacciones

### Ejercicio 8.1.1 — Leer: la estructura del _delta_log

**Tipo: Leer/analizar**

Inspeccionar la estructura real de una tabla Delta Lake:

```python
from delta import DeltaTable
from pyspark.sql import SparkSession
import os
import json

spark = SparkSession.builder \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Crear una tabla Delta Lake:
df = spark.createDataFrame([
    (1, "norte", 100.0),
    (2, "sur",   200.0),
    (3, "norte", 150.0),
], ["id", "region", "monto"])

df.write.format("delta").save("/tmp/mi_tabla_delta/")

# Inspeccionar el _delta_log:
log_dir = "/tmp/mi_tabla_delta/_delta_log/"
for archivo in sorted(os.listdir(log_dir)):
    ruta = os.path.join(log_dir, archivo)
    print(f"\n=== {archivo} ===")
    with open(ruta) as f:
        for linea in f:
            print(json.dumps(json.loads(linea), indent=2))
```

**Preguntas:**

1. ¿Qué información contiene el primer archivo JSON del `_delta_log`?
   ¿Qué acción (`add`, `remove`, `metaData`, `commitInfo`) esperas ver?

2. Si escribes 5 veces a la tabla, ¿cuántos archivos hay en el `_delta_log`?

3. ¿Un archivo `add` en el log contiene los datos o solo la referencia al archivo?

4. ¿Qué son los "checkpoints" del `_delta_log` y por qué son necesarios?

5. Si borras un archivo del `_delta_log` manualmente, ¿qué pasa cuando
   intentas leer la tabla?

**Pista:** El primer commit de Delta Lake contiene al menos tres tipos de acciones:
`metaData` (schema de la tabla, configuración), `protocol` (versión del protocolo
que necesita el lector), y `add` (uno por cada archivo Parquet creado).
Los checkpoints son necesarios porque leer el log de transacciones completo
requiere leer todos los archivos JSON desde el inicio — con 10,000 commits,
eso es 10,000 archivos. El checkpoint consolida todos los commits anteriores
en un solo snapshot Parquet, reduciendo el tiempo de lectura del log.

---

### Ejercicio 8.1.2 — Entender el protocolo de escritura

```python
from delta import DeltaTable
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0") \
    .getOrCreate()

# Escritura concurrente: ¿qué pasa si dos writers escriben simultáneamente?
# Delta Lake usa Optimistic Concurrency Control (OCC):
# 1. Writer A lee la versión actual del log (v5)
# 2. Writer B lee la versión actual del log (v5)
# 3. Writer A escribe su commit (v6) → éxito
# 4. Writer B intenta escribir su commit (v6) → conflicto detectado
#    Delta verifica: ¿las operaciones de A y B se solapan?
#    Si no se solapan (particiones distintas): merge automático → ambos tienen éxito
#    Si se solapan: Writer B falla con ConcurrentModificationException

# Simular escritura concurrente (en práctica, desde dos procesos):
tabla_delta = DeltaTable.forPath(spark, "/tmp/tabla_concurrente/")

# Operación 1: append a partición norte
df_norte = spark.createDataFrame([(10, "norte", 500.0)], ["id", "region", "monto"])
df_norte.write.format("delta").mode("append") \
    .option("txnAppId", "writer_norte") \
    .option("txnVersion", 1) \
    .save("/tmp/tabla_concurrente/")

# Operación 2: append a partición sur (no conflicto con operación 1)
df_sur = spark.createDataFrame([(11, "sur", 600.0)], ["id", "region", "monto"])
df_sur.write.format("delta").mode("append") \
    .option("txnAppId", "writer_sur") \
    .option("txnVersion", 1) \
    .save("/tmp/tabla_concurrente/")
```

**Preguntas:**

1. ¿Qué es Optimistic Concurrency Control (OCC) y cómo lo implementa Delta Lake?

2. ¿Cuándo dos escrituras concurrentes se pueden "mergear" automáticamente?
   ¿Cuándo no?

3. Si Writer B falla con `ConcurrentModificationException`, ¿debe reintentar
   desde cero o puede recalcular solo el conflicto?

4. ¿Delta Lake usa locking (bloquear la tabla durante la escritura)?
   ¿Por qué no?

5. ¿El OCC funciona con S3 que no garantiza consistencia inmediata?
   ¿Cómo Delta Lake gestiona esto?

**Pista:** OCC no bloquea recursos — permite múltiples writers simultáneos
y resuelve conflictos al commitear. Delta Lake usa una transacción atómica
del filesystem (renombrado atómico en HDFS, put-if-absent en S3)
para el commit del log. Si dos writers intentan crear el mismo archivo
`00000000000006.json`, solo uno tiene éxito — el otro recibe un error
y debe reintentar. La "resolución automática" ocurre cuando las particiones
modificadas no se solapan — Delta Lake detecta que A modificó partición norte
y B modificó partición sur, y ambas pueden coexistir sin conflicto.

---

### Ejercicio 8.1.3 — Leer el historial de una tabla

```python
from delta import DeltaTable

spark = SparkSession.builder \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0") \
    .getOrCreate()

# Crear una tabla con historial de operaciones:
spark.createDataFrame([(1, 100.0), (2, 200.0)], ["id", "monto"]) \
    .write.format("delta").save("/tmp/tabla_historial/")

spark.createDataFrame([(3, 300.0), (4, 400.0)], ["id", "monto"]) \
    .write.format("delta").mode("append").save("/tmp/tabla_historial/")

DeltaTable.forPath(spark, "/tmp/tabla_historial/") \
    .delete("id = 1")

spark.createDataFrame([(5, 500.0)], ["id", "monto"]) \
    .write.format("delta").mode("append").save("/tmp/tabla_historial/")

# Ver el historial completo:
tabla = DeltaTable.forPath(spark, "/tmp/tabla_historial/")
historial = tabla.history()
historial.show(truncate=False)

# El historial muestra:
# version  timestamp   userId  operationName  operationParameters
# 3        2024-01-15  ...     WRITE           {mode: Append, ...}
# 2        2024-01-15  ...     DELETE          {predicate: [id = 1]}
# 1        2024-01-15  ...     WRITE           {mode: Append, ...}
# 0        2024-01-15  ...     WRITE           {mode: ErrorIfExists, ...}
```

**Restricciones:**
1. Crear el historial de operaciones descrito
2. ¿El historial muestra cuántos archivos se leyeron y escribieron en cada operación?
3. ¿El historial persiste indefinidamente? ¿Cuándo se trunca?
4. Implementar una función que audita quién hizo qué cambio y cuándo

---

### Ejercicio 8.1.4 — Vacuum: limpiar archivos huérfanos

```python
from delta import DeltaTable

tabla = DeltaTable.forPath(spark, "/tmp/mi_tabla/")

# Los archivos que Delta Lake marca como "remove" en el log
# siguen existiendo en disco — son necesarios para time travel.
# VACUUM los elimina físicamente:

# Ver cuántos archivos se eliminarían (dry run):
tabla.vacuum(retentionHours=0, dryRun=True)

# Eliminar archivos más viejos que 7 días (168 horas, el default):
tabla.vacuum(retentionHours=168)

# PELIGROSO: eliminar archivos más viejos que 0 horas
# (desactiva time travel completamente, pero libera espacio inmediato)
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
tabla.vacuum(retentionHours=0)
```

```
Estado de la tabla antes del vacuum:

/tmp/mi_tabla/
  _delta_log/
    00000.json  ← commit: add file_A.parquet
    00001.json  ← commit: add file_B.parquet, remove file_A.parquet
    00002.json  ← commit: add file_C.parquet
  file_A.parquet  ← "remove" en el log, pero existe físicamente
  file_B.parquet  ← activo
  file_C.parquet  ← activo

Después del vacuum (retentionHours=168):
  file_A.parquet se elimina si tiene más de 7 días
  file_B.parquet y file_C.parquet se conservan
```

**Preguntas:**

1. ¿Por qué el vacuum tiene un retention period mínimo de 7 días por defecto?

2. ¿Qué pasa si un lector está leyendo `file_A.parquet` en el momento
   en que el vacuum lo elimina?

3. ¿El vacuum elimina archivos del `_delta_log/` también?

4. ¿Cuánto espacio ocupa típicamente el `_delta_log/` vs los datos?
   ¿Crece indefinidamente?

5. ¿Qué es `OPTIMIZE` y cómo se diferencia de `VACUUM`?

**Pista:** El retention period de 7 días protege contra el caso donde
un lector tiene una transacción long-running que lee `file_A` (que fue
"eliminado" en el log pero sigue en disco). Si el vacuum elimina el archivo
mientras el lector aún lo está usando, el lector recibirá un error de
"file not found". Los 7 días es una heurística conservadora para asegurar
que ninguna transacción legítima dure más de eso. `OPTIMIZE` reorganiza
los archivos pequeños en archivos más grandes (compactación) — no elimina
datos, solo los reescribe. VACUUM elimina archivos huérfanos.

---

### Ejercicio 8.1.5 — Diagnosticar: la tabla Delta que creció sin control

**Tipo: Diagnosticar**

Una tabla Delta Lake en producción ocupa 50 TB en S3 pero los datos
reales son solo 8 TB. El costo mensual es $1,150 en lugar de $184.

```python
# Investigación:
tabla = DeltaTable.forPath(spark, "s3://mi-tabla/ventas/")

historial = tabla.history()
historial.show()
# version  operationName   rowsAdded  rowsRemoved  numFiles
# 7,432    WRITE           1,200,000  0            120
# 7,431    DELETE          0          45,000       12 (archivos removidos del log)
# 7,430    WRITE           1,100,000  0            110
# ...

# Ver los archivos físicos:
import subprocess
resultado = subprocess.run(
    ["aws", "s3", "ls", "--recursive", "s3://mi-tabla/ventas/"],
    capture_output=True, text=True
)
# Hay 7,432 versiones × ~120 archivos = ~891,840 archivos en S3
# La mayoría son "remove" en el log pero siguen en disco
```

**Preguntas:**

1. ¿Por qué la tabla tiene 50 TB si los datos son 8 TB?

2. ¿Cuántos archivos hay en total? ¿Por qué tantos?

3. ¿Qué operación se debería haber ejecutado regularmente?
   ¿Con qué frecuencia?

4. ¿Cuánto tarda el vacuum sobre 50 TB con ~900,000 archivos en S3?
   ¿Cómo estimarlo?

5. ¿Cómo prevenirías este problema en el futuro?

**Pista:** El problema: nunca se ejecutó `VACUUM`. Con 7,432 escrituras
y un DELETE diario, hay ~7,432 versiones en el log y la mayoría de los
archivos son "huérfanos" (marcados como `remove` en el log pero no eliminados).
El vacuum debe ejecutarse regularmente — típicamente después de cada batch
o al menos semanalmente. En producción, es común configurar un job de
mantenimiento diario que ejecuta `OPTIMIZE` (para compactar small files)
y `VACUUM` (para eliminar archivos huérfanos).
Tiempo de vacuum: S3 limita el rate de operaciones DELETE — para 900,000 archivos
a ~3,500 deletes/segundo, serían ~4 minutos mínimo, probablemente 15-30 minutos
con throttling.

---

## Sección 8.2 — ACID en el Lakehouse: Cómo Funciona Realmente

### Ejercicio 8.2.1 — Atomicidad: todo o nada

```python
from pyspark.sql import SparkSession
from delta import DeltaTable
import time

spark = SparkSession.builder \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0") \
    .getOrCreate()

# Sin Delta Lake: si el job falla a mitad, quedan datos parciales
def escribir_sin_transaccion(ruta: str, n_particiones: int):
    for i in range(n_particiones):
        df = spark.createDataFrame(
            [(i * 1000 + j, f"data_{i}_{j}") for j in range(1000)],
            ["id", "valor"]
        )
        df.write.mode("append").parquet(ruta + f"/part_{i}/")
        if i == 3:
            raise Exception("Fallo simulado a mitad del job")
    # Si falla en partición 3 → quedan 3 particiones escritas (datos parciales)

# Con Delta Lake: todo el write es atómico
def escribir_con_delta(ruta: str, n_particiones: int):
    dfs = []
    for i in range(n_particiones):
        df = spark.createDataFrame(
            [(i * 1000 + j, f"data_{i}_{j}") for j in range(1000)],
            ["id", "valor"]
        )
        dfs.append(df)

    # Union y write en una sola operación atómica:
    from functools import reduce
    df_total = reduce(lambda a, b: a.union(b), dfs)

    try:
        # Si el job falla durante la escritura, ningún dato es visible:
        df_total.write.format("delta").mode("overwrite").save(ruta)
        raise Exception("Fallo simulado durante el write")
    except Exception:
        pass

    # Un lector que lee la tabla durante el write solo ve el estado anterior
    lector_df = spark.read.format("delta").load(ruta)
    # No ve datos parciales — atomicidad garantizada
```

**Preguntas:**

1. ¿Cómo Delta Lake garantiza la atomicidad si los archivos Parquet
   se escriben individualmente antes del commit?

2. Si el job falla después de escribir todos los archivos Parquet pero
   ANTES de escribir el commit JSON al `_delta_log`, ¿qué pasa?

3. ¿Un lector que lee durante un write ve datos del write en progreso?

4. ¿La atomicidad de Delta Lake funciona si el `_delta_log` está en S3?
   ¿S3 garantiza operaciones atómicas?

**Pista:** Delta Lake escribe primero todos los archivos Parquet en ubicaciones
temporales (sin registrarlos en el log). Solo después escribe el archivo
JSON de commit al `_delta_log/`. Si el job falla antes del commit, los archivos
Parquet existen en S3 pero ningún lector los conoce — son "fantasmas" que
el vacuum eliminará eventualmente. La "atomicidad" del commit en S3 se logra
con `put-if-absent`: el writer intenta crear el archivo `00006.json` — si ya
existe (otro writer ganó la carrera), falla y reintenta con `00007.json`.

---

### Ejercicio 8.2.2 — Aislamiento: lectores no afectados por escritores

```python
# Demostrar snapshot isolation de Delta Lake:
# Los lectores ven siempre un snapshot consistente de la tabla,
# independientemente de las escrituras concurrentes.

import threading
import time
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0") \
    .getOrCreate()

# Crear tabla inicial:
spark.createDataFrame([(1, 100.0), (2, 200.0), (3, 300.0)], ["id", "monto"]) \
    .write.format("delta").save("/tmp/tabla_isolation/")

lecturas_durante_write = []

def leer_continuamente():
    """Lee la tabla repetidamente durante la escritura."""
    for _ in range(10):
        df = spark.read.format("delta").load("/tmp/tabla_isolation/")
        conteo = df.count()
        suma = df.agg({"monto": "sum"}).collect()[0][0]
        lecturas_durante_write.append({"conteo": conteo, "suma": suma})
        time.sleep(0.1)

def escribir_datos_nuevos():
    """Escribe datos nuevos mientras se lee."""
    time.sleep(0.3)  # dar tiempo al lector de arrancar
    spark.createDataFrame([(4, 400.0), (5, 500.0)], ["id", "monto"]) \
        .write.format("delta").mode("append").save("/tmp/tabla_isolation/")

# Ejecutar lectura y escritura concurrentemente:
t_lector = threading.Thread(target=leer_continuamente)
t_escritor = threading.Thread(target=escribir_datos_nuevos)

t_lector.start()
t_escritor.start()
t_lector.join()
t_escritor.join()

print("Lecturas durante el write:")
for lectura in lecturas_durante_write:
    print(f"  conteo={lectura['conteo']}, suma={lectura['suma']}")
# Las lecturas antes del write ven 3 filas, suma=600
# Las lecturas después del write ven 5 filas, suma=1500
# NUNCA ven 4 filas o suma=1000 (estado inconsistente)
```

**Preguntas:**

1. ¿Qué nivel de aislamiento ofrece Delta Lake por defecto?
   (Serializable, Snapshot Isolation, Read Committed, Read Uncommitted)

2. ¿Es posible que un lector vea 4 filas (2 originales + 2 nuevas incompletos)?
   ¿Por qué no?

3. ¿El aislamiento de Delta Lake tiene algún costo de rendimiento?

4. ¿Qué es "dirty read" y Delta Lake lo previene?

---

### Ejercicio 8.2.3 — Consistencia: schema enforcement

```python
from delta import DeltaTable
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0") \
    .getOrCreate()

# Crear tabla con schema definido:
spark.createDataFrame(
    [(1, "norte", 100.0)],
    ["id", "region", "monto"]
).write.format("delta").save("/tmp/tabla_schema/")

# Intento 1: escribir con schema diferente (columna extra)
try:
    spark.createDataFrame(
        [(2, "sur", 200.0, "extra_columna")],
        ["id", "region", "monto", "nueva_columna"]
    ).write.format("delta").mode("append").save("/tmp/tabla_schema/")
except Exception as e:
    print(f"Error esperado: {e}")
    # AnalysisException: A schema mismatch detected when writing to...

# Intento 2: escribir con tipo incorrecto
try:
    spark.createDataFrame(
        [(3, "este", "no_es_numero")],
        ["id", "region", "monto"]  # monto debería ser float
    ).write.format("delta").mode("append").save("/tmp/tabla_schema/")
except Exception as e:
    print(f"Error de tipo: {e}")

# Permitir evolución del schema:
spark.createDataFrame(
    [(4, "oeste", 300.0, "nuevo_valor")],
    ["id", "region", "monto", "columna_nueva"]
).write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .save("/tmp/tabla_schema/")
```

**Preguntas:**

1. ¿Qué operaciones de schema están permitidas con `mergeSchema=true`
   y cuáles no?

2. Si alguien escribe accidentalmente con `mergeSchema=true` y añade
   una columna con nombre incorrecto, ¿cómo lo corriges?

3. ¿Schema enforcement protege contra datos inválidos dentro de una columna
   (ej: `monto = -1` cuando el negocio dice que debe ser positivo)?

4. ¿Cómo añadirías constraints de negocio (CHECK constraints) a Delta Lake?

**Pista:** Delta Lake 2.0+ soporta constraints con `ALTER TABLE ADD CONSTRAINT`:
```sql
ALTER TABLE ventas ADD CONSTRAINT monto_positivo CHECK (monto > 0);
```
Si intentas insertar una fila con `monto = -1`, Delta Lake rechaza el commit.
Los constraints se almacenan en el `_delta_log` como metadatos y se verifican
en cada write. Para constraints más complejos que SQL no puede expresar,
la alternativa es validar en el pipeline antes de escribir.

---

### Ejercicio 8.2.4 — Durabilidad: qué pasa si S3 pierde un archivo

```python
# Delta Lake usa S3 como almacenamiento — S3 garantiza:
# - 99.999999999% (11 nines) de durabilidad
# - Replicación automática en múltiples AZs
# - Consistencia eventual en lectura tras escritura (corregido en S3 strong consistency desde 2020)

# ¿Qué pasa si S3 pierde un archivo de datos (probabilidad ~0 pero teórico)?
# 1. El _delta_log tiene el registro del archivo (add action)
# 2. Los lectores intentan leer el archivo → error 404
# 3. Delta Lake NO puede recuperar el dato automáticamente
# 4. La solución: backups del _delta_log y de los archivos de datos

# Estrategia de backup con Delta Lake:
# Opción A: S3 Versioning (mantiene versiones de cada objeto)
# Opción B: S3 Replication (copia a otro bucket/región)
# Opción C: Export periódico a otro sistema
```

**Preguntas:**

1. ¿La "durabilidad" de Delta Lake depende 100% de la durabilidad de S3?

2. ¿Si el `_delta_log` se corrompe pero los archivos Parquet están intactos,
   puedes recuperar los datos? ¿Cómo?

3. ¿Qué es "Delta Log checkpointing" y cómo ayuda a la recuperación?

4. ¿Qué diferencia hay entre la durabilidad de HDFS (replicación interna)
   y la de S3 (replicación gestionada por AWS)?

---

### Ejercicio 8.2.5 — Leer: ACID en un lakehouse vs en PostgreSQL

**Tipo: Comparar**

Delta Lake ofrece ACID pero con garantías diferentes a PostgreSQL:

```
PostgreSQL:
  Transacciones a nivel de fila: puedes actualizar una sola fila
  Aislamiento: Serializable (el más fuerte)
  Granularidad del lock: fila, página, tabla
  Latencia de commit: milisegundos
  Uso: OLTP (miles de transacciones pequeñas por segundo)

Delta Lake:
  Transacciones a nivel de commit: un write es atómico
  Aislamiento: Snapshot Isolation (no Serializable por defecto)
  Granularidad: archivo Parquet completo
  Latencia de commit: segundos (el commit al log + S3)
  Uso: OLAP (pocos commits grandes por hora)
```

**Preguntas:**

1. ¿Por qué Delta Lake no ofrece transacciones a nivel de fila como PostgreSQL?

2. ¿Puedes usar Delta Lake para un sistema de pagos donde cada transacción
   actualiza el saldo de una cuenta? ¿Por qué sería una mala idea?

3. ¿Qué workload de base de datos es claramente mejor en PostgreSQL?
   ¿Cuál es claramente mejor en Delta Lake?

4. ¿"Snapshot Isolation" de Delta Lake puede causar anomalías que
   Serializable previene? ¿Cuáles?

**Pista:** La anomalía clásica de Snapshot Isolation que Serializable previene
es el "write skew": dos transacciones leen el mismo dato, calculan algo basado
en él, y ambas escriben sin ver la escritura de la otra.
Ejemplo en Delta Lake: dos jobs calculan "el monto total de la tabla" y ambos
ven $1000. Job A escribe "nuevo total = 1000 + mi_incremento". Job B escribe
"nuevo total = 1000 + mi_incremento". El resultado final ignora uno de los
incrementos. Para analytics batch donde esto no ocurre (cada job escribe
particiones distintas), Snapshot Isolation es suficiente.

---

## Sección 8.3 — Time Travel y Auditoría

### Ejercicio 8.3.1 — Leer versiones anteriores

```python
from delta import DeltaTable
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0") \
    .getOrCreate()

# Crear tabla con múltiples versiones:
spark.createDataFrame([(1, 100.0), (2, 200.0)], ["id", "monto"]) \
    .write.format("delta").save("/tmp/tabla_tt/")   # versión 0

spark.createDataFrame([(3, 300.0)], ["id", "monto"]) \
    .write.format("delta").mode("append").save("/tmp/tabla_tt/")  # versión 1

DeltaTable.forPath(spark, "/tmp/tabla_tt/").delete("id = 1")  # versión 2

# Time travel por versión:
df_v0 = spark.read.format("delta") \
    .option("versionAsOf", 0) \
    .load("/tmp/tabla_tt/")
# Contiene: [(1, 100.0), (2, 200.0)]

df_v1 = spark.read.format("delta") \
    .option("versionAsOf", 1) \
    .load("/tmp/tabla_tt/")
# Contiene: [(1, 100.0), (2, 200.0), (3, 300.0)]

df_actual = spark.read.format("delta").load("/tmp/tabla_tt/")
# Contiene: [(2, 200.0), (3, 300.0)]  — id=1 fue eliminado en v2

# Time travel por timestamp:
df_ayer = spark.read.format("delta") \
    .option("timestampAsOf", "2024-01-14") \
    .load("/tmp/tabla_tt/")
```

**Restricciones:**
1. Verificar que cada versión contiene los datos correctos
2. ¿El time travel por timestamp usa la hora UTC o local?
3. ¿Puedes hacer time travel a un momento entre dos commits?
4. Implementar una query de "cambios entre versiones" (qué se añadió y qué se eliminó)

---

### Ejercicio 8.3.2 — CDF (Change Data Feed): capturar cambios

```python
# Change Data Feed: expone los cambios (inserts, updates, deletes)
# como filas en una tabla especial — útil para CDC (Change Data Capture)

# Habilitar CDF en la tabla:
spark.sql("""
    ALTER TABLE delta.`/tmp/tabla_cdf/`
    SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

# Hacer operaciones:
spark.createDataFrame([(1, 100.0), (2, 200.0)], ["id", "monto"]) \
    .write.format("delta").save("/tmp/tabla_cdf/")  # v0: inserts

DeltaTable.forPath(spark, "/tmp/tabla_cdf/").update(
    condition="id = 1",
    set={"monto": "150.0"}
)  # v1: update

DeltaTable.forPath(spark, "/tmp/tabla_cdf/").delete("id = 2")  # v2: delete

# Leer los cambios entre versiones:
cambios = spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 0) \
    .option("endingVersion", 2) \
    .load("/tmp/tabla_cdf/")

cambios.show()
# id  monto  _change_type  _commit_version  _commit_timestamp
# 1   100.0  insert        0                2024-01-15 14:00
# 2   200.0  insert        0                2024-01-15 14:00
# 1   100.0  update_preimage  1             2024-01-15 14:01
# 1   150.0  update_postimage 1             2024-01-15 14:01
# 2   200.0  delete        2                2024-01-15 14:02
```

**Preguntas:**

1. ¿Qué es `update_preimage` y `update_postimage`?

2. ¿CDF tiene overhead en el rendimiento de escritura?

3. ¿Para qué casos de uso es útil CDF?

4. ¿CDF puede usarse para sincronizar Delta Lake con una base de datos operacional?

5. ¿CDF de Delta Lake es equivalente a un Kafka topic de cambios?

**Pista:** CDF es útil para:
(1) Sincronizar Delta Lake con sistemas downstream sin releer la tabla completa
(2) Auditoría: saber exactamente qué cambió, cuándo, y cuál era el valor anterior
(3) Pipelines de ML: entrenar incrementalmente solo con datos nuevos/cambiados
(4) Data mesh: propagar cambios a otras tablas o sistemas
La diferencia con Kafka: CDF es pull (tú decides cuándo leer los cambios)
mientras Kafka es push (los consumidores se suscriben y reciben en tiempo real).

---

### Ejercicio 8.3.3 — Reproducibilidad: volver a calcular un resultado de hace 6 meses

**Tipo: Implementar**

Un auditor pide reproducir exactamente el reporte de revenue de julio 2024:

```python
from delta import DeltaTable
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0") \
    .getOrCreate()

def calcular_revenue_julio_2024(tabla_ventas: str) -> float:
    """
    Calcula el revenue de julio 2024 usando los datos exactos de esa fecha.
    La tabla puede haber sido actualizada desde entonces (correcciones, etc.)
    """
    # Sin time travel: usaría los datos actuales (posiblemente modificados)
    df_actual = spark.read.format("delta").load(tabla_ventas)
    revenue_actual = df_actual.filter(
        (F.col("fecha") >= "2024-07-01") & (F.col("fecha") < "2024-08-01")
    ).agg(F.sum("monto")).collect()[0][0]

    # Con time travel: datos exactos de julio 2024
    df_julio = spark.read.format("delta") \
        .option("timestampAsOf", "2024-07-31 23:59:59") \
        .load(tabla_ventas)
    revenue_julio = df_julio.filter(
        (F.col("fecha") >= "2024-07-01") & (F.col("fecha") < "2024-08-01")
    ).agg(F.sum("monto")).collect()[0][0]

    print(f"Revenue actual (con correcciones):  ${revenue_actual:,.2f}")
    print(f"Revenue julio (datos originales):   ${revenue_julio:,.2f}")
    print(f"Diferencia (correcciones):          ${revenue_actual - revenue_julio:,.2f}")

    return revenue_julio
```

**Restricciones:**
1. ¿El time travel funciona si el VACUUM eliminó los archivos de julio 2024?
2. ¿Cómo defines una política de retención que garantiza la reproducibilidad?
3. ¿Cuánto espacio adicional ocupa mantener 12 meses de historial?
4. Implementar una función de "auditoría de cambios" entre dos fechas

---

### Ejercicio 8.3.4 — Restaurar una tabla a una versión anterior

```python
from delta import DeltaTable

tabla = DeltaTable.forPath(spark, "/tmp/tabla_restaurar/")

# Restaurar a una versión anterior (operación destructiva para las versiones más recientes):
tabla.restoreToVersion(5)
# La tabla vuelve al estado de la versión 5
# Las versiones 6, 7, 8... todavía existen en el log (para auditoría)
# pero la "versión actual" es ahora el estado de la v5 restaurado

# Restaurar a un timestamp:
tabla.restoreToTimestamp("2024-01-01")

# ¿La restauración crea una nueva versión o sobrescribe?
tabla.history(5).show()
# version  operationName
# 9        RESTORE  ← nueva versión 9 que representa el estado de v5
# 8        ...
# 7        ...
# 6        ...
# 5        ...  ← el estado al que volvimos
```

**Preguntas:**

1. `restoreToVersion(5)` crea una nueva versión 9 (si estábamos en v8)
   o sobrescribe? ¿Por qué?

2. ¿Puedes hacer un "restore parcial" (restaurar solo algunas filas)?

3. ¿Cuál es la diferencia entre `RESTORE` y leer con time travel + reescribir?

4. ¿`RESTORE` respeta el schema actual si cambió desde la versión restaurada?

---

### Ejercicio 8.3.5 — Leer: time travel en producción — cuándo es indispensable

**Tipo: Analizar**

Para cada escenario, evaluar si el time travel de Delta Lake resuelve el problema:

```
Escenario 1:
  Un pipeline de ML entrenó un modelo hace 3 meses y quiere reproducir
  exactamente las features que usó para debugging.
  
Escenario 2:
  Un bug en el pipeline de ingesta escribió datos incorrectos durante 2 horas.
  Necesitas revertir esas 2 horas de datos sin afectar los datos anteriores.
  
Escenario 3:
  La tabla tiene 500 GB. Hace 7 días se ejecutó VACUUM con retentionHours=0.
  Un auditor pide los datos de hace 30 días.
  
Escenario 4:
  Dos pipelines calculan el mismo KPI con lógica diferente.
  Quieres comparar sus resultados históricos semana a semana.
  
Escenario 5:
  Un sistema de recomendación usa features de una tabla Delta Lake
  actualizada cada hora. Quieres saber qué features usó para
  recomendar un producto específico hace 2 días.
```

Para cada escenario: ¿el time travel resuelve el problema?
Si no, ¿qué solución alternativa existe?

---

## Sección 8.4 — Schema Evolution y Enforcement

### Ejercicio 8.4.1 — Los modos de schema evolution de Delta Lake

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0") \
    .getOrCreate()

# Tabla original:
spark.createDataFrame(
    [(1, "norte", 100.0)],
    ["id", "region", "monto"]
).write.format("delta").save("/tmp/tabla_schema_evo/")

# Modo 1: mergeSchema — añadir columnas nuevas
spark.createDataFrame(
    [(2, "sur", 200.0, "USD")],
    ["id", "region", "monto", "moneda"]
).write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .save("/tmp/tabla_schema_evo/")
# Resultado: columna "moneda" añadida, filas anteriores tienen moneda=null

# Modo 2: overwriteSchema — cambiar el schema completamente
spark.createDataFrame(
    [(3, "este", 300.0, "EUR", "premium")],
    ["id", "region", "monto", "moneda", "segmento"]
).write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("/tmp/tabla_schema_evo/")
# CUIDADO: esto borra los datos anteriores y cambia el schema

# Verificar el schema actual:
print(spark.read.format("delta").load("/tmp/tabla_schema_evo/").schema)
```

**Restricciones:**
1. Documentar el schema en cada versión usando time travel
2. ¿Qué versiones son accesibles con time travel después del overwriteSchema?
3. Implementar la estrategia de "schema versionado" para producción
4. ¿Cuándo usar mergeSchema vs overwriteSchema?

---

### Ejercicio 8.4.2 — Column mapping: renombrar columnas sin reescribir datos

Delta Lake 2.0+ soporta renombrar y eliminar columnas sin reescribir los archivos:

```python
# Habilitar column mapping:
spark.sql("""
    ALTER TABLE delta.`/tmp/tabla_mapping/`
    SET TBLPROPERTIES (
        'delta.columnMapping.mode' = 'name',
        'delta.minReaderVersion' = '2',
        'delta.minWriterVersion' = '5'
    )
""")

# Renombrar una columna (sin reescribir los datos):
spark.sql("""
    ALTER TABLE delta.`/tmp/tabla_mapping/`
    RENAME COLUMN monto TO precio_total
""")

# Eliminar una columna (sin reescribir los datos):
spark.sql("""
    ALTER TABLE delta.`/tmp/tabla_mapping/`
    DROP COLUMN descripcion_interna
""")
```

```
Sin column mapping:
  Renombrar "monto" a "precio_total":
  - Reescribir TODOS los archivos Parquet (cambia el nombre de la columna en cada uno)
  - Para 1 TB de datos: proceso de horas, costo significativo

Con column mapping:
  - Solo actualizar el metadata en el _delta_log
  - Los archivos Parquet siguen teniendo "monto" internamente
  - El lector traduce "precio_total" → "monto" usando el mapping
  - Para 1 TB de datos: proceso de segundos, costo mínimo
```

**Preguntas:**

1. ¿Hay algún costo de rendimiento en lectura al usar column mapping?

2. ¿Column mapping es compatible con todos los lectores de Delta Lake?

3. ¿Puedes usar column mapping con `versionAsOf` para leer una versión
   anterior que tenía el nombre de columna original?

4. ¿Qué limitaciones tiene column mapping? ¿Puedo renombrar a un nombre
   que ya usé antes?

---

### Ejercicio 8.4.3 — Schema evolution en un pipeline de producción

**Tipo: Diseñar**

Un sistema de e-commerce tiene una tabla Delta Lake de eventos que recibe
100,000 eventos/hora de múltiples microservicios. El equipo quiere
añadir nuevas columnas sin interrumpir el servicio:

```
Schema actual (v1):
  user_id: int
  evento: string
  timestamp: timestamp
  monto: double

Schema nuevo (v2) — cambios propuestos:
  user_id: int
  evento: string
  timestamp: timestamp
  monto: double
  moneda: string         ← nueva, requerida
  region: string         ← nueva, opcional (algunos microservicios no la tienen)
  metadata: struct<...>  ← nueva, para datos adicionales del microservicio
```

**Preguntas:**

1. ¿Puedes añadir `moneda` como columna requerida sin romper los microservicios
   que ya están en producción?

2. ¿Cuál es el orden seguro para desplegar el cambio de schema?

3. ¿Cómo manejas los eventos históricos que no tienen `moneda`?

4. ¿Si un microservicio envía `moneda = null`, es un error o aceptable?

5. Proponer la estrategia completa de migración (con etapas y rollback plan).

**Pista:** El orden seguro para añadir columnas a una tabla activa:
1. Añadir la columna con `mergeSchema=true` como nullable (no rompe los writers actuales)
2. Actualizar los microservicios uno a uno para enviar la nueva columna
3. Solo después de que todos los writers envíen la columna, marcarlo como NOT NULL
   (si es necesario)
4. Rellenar los valores históricos (backfill) con un valor por defecto si se necesitan
El rollback: simplemente no hacer el paso 3 — los writers nuevos envían la columna,
los viejos envían null, y el lector maneja ambos casos.

---

### Ejercicio 8.4.4 — Detectar y alertar sobre cambios de schema inesperados

```python
from delta import DeltaTable
from pyspark.sql.types import StructType
import json

def verificar_schema_compatible(
    tabla: str,
    schema_esperado: StructType,
    alerta_fn=None,
) -> bool:
    """
    Verifica que el schema actual de la tabla Delta Lake es compatible
    con el schema esperado. Alerta si hay cambios incompatibles.
    """
    tabla_delta = DeltaTable.forPath(spark, tabla)
    schema_actual = spark.read.format("delta").load(tabla).schema

    # Columnas esperadas que faltan:
    campos_faltantes = set(schema_esperado.fieldNames()) - set(schema_actual.fieldNames())

    # Columnas extra no esperadas:
    campos_extra = set(schema_actual.fieldNames()) - set(schema_esperado.fieldNames())

    # Columnas con tipo diferente:
    tipos_diferentes = []
    for campo in schema_esperado:
        if campo.name in schema_actual.fieldNames():
            campo_actual = schema_actual[campo.name]
            if campo_actual.dataType != campo.dataType:
                tipos_diferentes.append({
                    "columna": campo.name,
                    "esperado": str(campo.dataType),
                    "actual": str(campo_actual.dataType),
                })

    problemas = {
        "campos_faltantes": list(campos_faltantes),
        "campos_extra": list(campos_extra),
        "tipos_diferentes": tipos_diferentes,
    }

    hay_problemas = any(len(v) > 0 for v in problemas.values())
    if hay_problemas and alerta_fn:
        alerta_fn(f"Schema incompatible en {tabla}: {json.dumps(problemas)}")

    return not hay_problemas
```

**Restricciones:**
1. Implementar la función completa
2. Integrarla en el pipeline de ingesta para verificar el schema de cada batch
3. ¿Cómo distingues "cambio incompatible" de "cambio compatible" (nueva columna nullable)?
4. ¿Dónde en el pipeline debería ejecutarse esta verificación?

---

### Ejercicio 8.4.5 — Leer: schema evolution en Iceberg vs Delta Lake

**Tipo: Comparar**

Delta Lake y Apache Iceberg tienen enfoques distintos para schema evolution:

```
Delta Lake schema evolution:
  - mergeSchema: añadir columnas compatibles
  - overwriteSchema: cambiar el schema completamente (destructivo)
  - Column mapping: renombrar/eliminar sin reescribir
  - Constraints: CHECK constraints en columnas

Apache Iceberg schema evolution:
  - ADD COLUMN: añadir columna en cualquier posición
  - DROP COLUMN: eliminar columna (los datos siguen, solo se ocultan)
  - RENAME COLUMN: renombrar sin reescribir
  - ALTER COLUMN: cambiar tipo (con reglas de compatibilidad)
  - Partición evolution: cambiar la partición sin reescribir
  - Hidden partitioning: las particiones no son visibles en el schema del usuario
```

**Preguntas:**

1. ¿Iceberg puede renombrar columnas sin reescribir datos?
   ¿Tiene algo como column mapping de Delta Lake?

2. ¿Qué es "partition evolution" de Iceberg y por qué no existe en Delta Lake?

3. ¿Cuál de los dos tiene mejor soporte para cambios de tipo de columna
   (ej: `int` → `long`)?

4. ¿El "hidden partitioning" de Iceberg simplifica los queries para el usuario?
   ¿Tiene desventajas?

> 📖 Profundizar: el paper *Apache Iceberg: An Architectural Look Under the Covers*
> (Russell, VLDB 2022) explica el diseño del árbol de metadata de Iceberg
> y las ventajas de su approach vs el log de Delta Lake. Especialmente relevante
> la Sección 3 sobre schema evolution y la Sección 4 sobre partition evolution.

---

## Sección 8.5 — Operaciones DML: UPDATE, DELETE y MERGE

### Ejercicio 8.5.1 — La mecánica de UPDATE en Delta Lake

```python
from delta import DeltaTable

tabla = DeltaTable.forPath(spark, "/tmp/tabla_update/")

# UPDATE: modificar filas existentes
tabla.update(
    condition="region = 'norte'",
    set={"monto": "monto * 1.1"}  # 10% de aumento para región norte
)

# ¿Qué hace Delta Lake internamente?
# 1. Leer todos los archivos Parquet que contienen filas de región='norte'
# 2. Escribir NUEVOS archivos Parquet con las filas modificadas
# 3. Añadir las filas NUEVAS al log (add actions)
# 4. Marcar las filas VIEJAS como removed (remove actions)
# 5. Commit atómico del log

# Con Z-ordering (para UPDATE eficientes):
tabla.optimize().executeZOrderBy("region")
# Si los datos están Z-ordenados por region, el UPDATE solo necesita
# leer los archivos que contienen región='norte' → menos I/O
```

**Preguntas:**

1. ¿Delta Lake puede actualizar una sola fila sin reescribir el archivo entero?

2. Si un archivo Parquet de 1 GB tiene 100,000 filas y quieres actualizar
   solo 1 fila, ¿cuántos datos se reescriben?

3. ¿Qué es Z-ordering y cómo reduce el costo de los UPDATEs?

4. ¿Cuánto más costoso es un UPDATE en Delta Lake vs en PostgreSQL?

5. ¿Cuándo es preferible "actualizar" datos en Delta Lake vs simplemente
   añadir una nueva fila con los valores actualizados (SCD Type 2)?

**Pista:** Delta Lake escribe Copy-on-Write (CoW) por defecto — para actualizar
1 fila en un archivo de 1 GB, reescribe el archivo completo de 1 GB con la fila
modificada. Esto es costoso para actualizaciones frecuentes pero eficiente para
lectura (no hay merge necesario). La alternativa para workloads con muchas
actualizaciones pequeñas es Hudi con Merge-on-Read (MoR) — escribe solo el delta
y hace el merge al leer. Más rápido para escribir, más costoso para leer.

---

### Ejercicio 8.5.2 — MERGE: upsert eficiente

```python
from delta import DeltaTable
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0") \
    .getOrCreate()

# MERGE: la operación más versátil del lakehouse
# Permite: INSERT nuevas filas, UPDATE filas existentes, DELETE filas

tabla_delta = DeltaTable.forPath(spark, "/tmp/tabla_clientes/")

# Datos de actualización:
actualizaciones = spark.createDataFrame([
    (1, "Alice", "premium", 1500.0),    # existente → actualizar
    (2, "Bob",   "standard", 200.0),    # existente → actualizar
    (5, "Eve",   "new", 0.0),           # nuevo → insertar
], ["id", "nombre", "segmento", "gasto"])

# MERGE INTO:
tabla_delta.alias("t").merge(
    actualizaciones.alias("s"),
    "t.id = s.id"
).whenMatchedUpdate(set={
    "nombre": "s.nombre",
    "segmento": "s.segmento",
    "gasto": "s.gasto",
}).whenNotMatchedInsert(values={
    "id": "s.id",
    "nombre": "s.nombre",
    "segmento": "s.segmento",
    "gasto": "s.gasto",
}).execute()

# MERGE con DELETE:
tabla_delta.alias("t").merge(
    actualizaciones.alias("s"),
    "t.id = s.id"
).whenMatchedUpdate(
    condition="s.segmento != 'deleted'",
    set={"segmento": "s.segmento"}
).whenMatchedDelete(
    condition="s.segmento = 'deleted'"
).whenNotMatchedInsert(
    values={"id": "s.id", "nombre": "s.nombre", "segmento": "s.segmento"}
).execute()
```

**Restricciones:**
1. Implementar un pipeline de SCD Type 2 usando MERGE (mantener historial de cambios)
2. Medir el rendimiento del MERGE para 1M de actualizaciones sobre 100M de filas
3. ¿El MERGE genera un shuffle? ¿Cuánto shuffle?
4. ¿Cuándo es más eficiente un MERGE que un batch de DELETEs + INSERTs?

---

### Ejercicio 8.5.3 — Implementar CDC con MERGE

CDC (Change Data Capture) es el patrón de sincronizar cambios de una fuente
(base de datos operacional) a un destino (lakehouse):

```python
def aplicar_cambios_cdc(
    spark: SparkSession,
    tabla_delta: str,
    cambios_kafka: "DataFrame",  # columnas: id, operacion (I/U/D), timestamp, datos...
) -> dict:
    """
    Aplica un batch de cambios CDC a una tabla Delta Lake.
    
    cambios_kafka contiene:
      - operacion: 'I' (insert), 'U' (update), 'D' (delete)
      - id: clave primaria
      - resto de columnas: datos actualizados
    
    Para cada operación, el MERGE hace lo correcto:
      - 'I': INSERT si no existe, UPDATE si ya existe (idempotente)
      - 'U': UPDATE
      - 'D': DELETE
    """
    tabla = DeltaTable.forPath(spark, tabla_delta)

    # Deduplicar: si el mismo id tiene múltiples cambios, quedarse con el último
    cambios_dedup = cambios_kafka \
        .withColumn("rank",
            F.row_number().over(
                Window.partitionBy("id")
                      .orderBy(F.col("timestamp").desc())
            )
        ).filter(F.col("rank") == 1).drop("rank")

    # MERGE diferenciado por tipo de operación:
    tabla.alias("t").merge(
        cambios_dedup.alias("s"),
        "t.id = s.id"
    ).whenMatchedUpdate(
        condition="s.operacion IN ('I', 'U')",
        set={col: f"s.{col}" for col in tabla.toDF().columns if col != "id"}
    ).whenMatchedDelete(
        condition="s.operacion = 'D'"
    ).whenNotMatchedInsert(
        condition="s.operacion IN ('I', 'U')",
        values={col: f"s.{col}" for col in tabla.toDF().columns}
    ).execute()

    return {
        "cambios_aplicados": cambios_dedup.count(),
        "operacion": "cdc_merge",
    }
```

**Restricciones:**
1. Implementar la función completa
2. ¿Qué pasa si llegan eventos CDC fuera de orden? ¿El pipeline es correcto?
3. Medir el rendimiento para 100K cambios/batch sobre una tabla de 1 TB
4. ¿Cómo garantizas exactly-once processing con este patrón?

---

### Ejercicio 8.5.4 — OPTIMIZE y Z-ordering: compactar para queries eficientes

```python
from delta import DeltaTable

# OPTIMIZE: compactar archivos pequeños en archivos grandes
tabla = DeltaTable.forPath(spark, "/tmp/tabla_fragmentada/")

# Ver el estado actual:
tabla.detail().show()
# numFiles: 10,000 (!) — muchos archivos pequeños

# Compactar:
tabla.optimize().executeCompaction()
# numFiles: 100 — archivos de ~128 MB

# Z-ordering: organizar los datos para acceso eficiente por columnas frecuentes
tabla.optimize().executeZOrderBy("region", "fecha")
# Ahora las filas de "norte" + "2024-01" están en los mismos archivos
# → queries con WHERE region='norte' AND fecha='2024-01' leen mucho menos

# Verificar el resultado:
tabla.detail().show()
# numFiles: 100 (compactado)
# clusteringColumns: [region, fecha] (z-ordenado)
```

**Preguntas:**

1. ¿Por qué OPTIMIZE no cambia los datos, solo los reorganiza?

2. ¿Z-ordering es equivalente a particionamiento? ¿En qué se diferencia?

3. ¿OPTIMIZE tiene que ejecutarse periódicamente? ¿Cuándo se "deshace"?

4. Si tienes `region` con 4 valores y `fecha` con 365 valores,
   ¿cuántas combinaciones hay y cómo afecta al Z-ordering?

5. ¿Cuánto tiempo tarda OPTIMIZE sobre 1 TB de datos fragmentados?

**Pista:** Z-ordering vs particionamiento: el particionamiento crea directorios
separados por valor (`region=norte/`, `region=sur/`) — queries que filtran
por región solo leen el directorio correspondiente. Z-ordering organiza los datos
DENTRO de los archivos para que los registros similares (mismo region+fecha) estén
físicamente cerca. Puedes combinar ambos: particionar por año/mes y Z-ordenar
por region+user_id. El resultado: queries que filtran por año/mes+region+user_id
son muy eficientes.

---

### Ejercicio 8.5.5 — Diagnosticar: el pipeline con demasiados archivos pequeños

**Tipo: Diagnosticar**

Un pipeline de streaming escribe 1 archivo Parquet por micro-batch en Delta Lake.
El micro-batch procesa 10,000 filas cada 5 minutos.
Después de 30 días, el reporte diario de analytics tarda 3 horas:

```python
tabla = DeltaTable.forPath(spark, "s3://lakehouse/eventos/")
detalle = tabla.detail().collect()[0]

print(f"Número de archivos: {detalle['numFiles']}")
# 30 días × 24 horas × 12 batches/hora = 8,640 archivos

print(f"Tamaño promedio por archivo: {detalle['sizeInBytes'] / detalle['numFiles'] / 1024:.0f} KB")
# ~100 KB por archivo (!!)

print(f"Tamaño total: {detalle['sizeInBytes'] / 1024**3:.1f} GB")
# ~860 MB de datos reales — pero 8,640 archivos de 100 KB cada uno
```

**Preguntas:**

1. ¿Por qué el reporte de analytics tarda 3 horas si solo hay 860 MB de datos?

2. ¿Cuántos archivos Parquet se esperan para 860 MB de datos bien empaquetados?

3. ¿Cómo arreglas el problema sin detener el pipeline de streaming?

4. ¿Cómo prevenirías el problema en el diseño inicial?

5. ¿OPTIMIZE puede ejecutarse mientras el pipeline de streaming escribe activamente?

**Pista:** 8,640 archivos de 100 KB cada uno: cuando Spark lee esto,
crea 8,640 tasks de lectura (una por archivo). El overhead de scheduling
de 8,640 tasks (50ms cada una) = 7 minutos solo en scheduling.
Más el overhead de S3 LIST para descubrir los archivos y el overhead de
abrir 8,640 archivos Parquet (cada uno tiene su propio footer que se lee
para obtener las estadísticas). La solución: OPTIMIZE periódico (cada hora o día).
La prevención: usar `trigger(once=True)` con acumulación de más datos por batch,
o configurar el streaming job para hacer micro-batches menos frecuentes pero más grandes.

---

## Sección 8.6 — Apache Iceberg: el Modelo de Árbol

### Ejercicio 8.6.1 — La estructura de metadata de Iceberg

```
Apache Iceberg organiza el metadata en un árbol:

Table metadata file (JSON):
  {
    "format-version": 2,
    "table-uuid": "abc-123",
    "location": "s3://bucket/tabla/",
    "schemas": [schema_v1, schema_v2],
    "current-schema-id": 1,
    "partition-specs": [spec_v1, spec_v2],
    "current-spec-id": 1,
    "snapshots": [...],
    "current-snapshot-id": 9876543210
  }

Snapshot:
  {
    "snapshot-id": 9876543210,
    "timestamp-ms": 1705000000000,
    "manifest-list": "s3://bucket/tabla/metadata/snap-9876543210.avro"
  }

Manifest List (Avro):
  [{
    "manifest-path": "s3://bucket/tabla/metadata/manifest-abc.avro",
    "added-data-files-count": 5,
    "existing-data-files-count": 100,
    "deleted-data-files-count": 0,
  }]

Manifest File (Avro):
  [{
    "status": "ADDED",
    "data-file": {
      "file-path": "s3://bucket/tabla/data/0001.parquet",
      "record-count": 100000,
      "column-sizes": {...},
      "value-counts": {...},
      "lower-bounds": {"monto": 10.0},
      "upper-bounds": {"monto": 9999.0},
    }
  }]
```

**Preguntas:**

1. ¿Por qué Iceberg usa un árbol de metadata en lugar del log lineal de Delta Lake?

2. ¿Cuántas lecturas de S3 necesita Iceberg para responder a la pregunta
   "¿qué archivos contienen datos del mes de enero 2024?"

3. ¿Dónde están las estadísticas por columna en Iceberg?
   (min, max, null count)

4. ¿Qué ventaja tiene Iceberg sobre Delta Lake para tablas con miles
   de particiones?

5. ¿El modelo de árbol de Iceberg hace el time travel más rápido o más
   lento que el log de Delta Lake?

**Pista:** La ventaja del árbol de Iceberg para tablas grandes: Delta Lake
necesita leer todos los archivos JSON del log para construir el estado actual
(mitigado por los checkpoints). Iceberg apunta directamente al snapshot actual —
una lectura de la metadata file te da el snapshot, una lectura del manifest list
te da los manifests, y puedes hacer predicate pushdown sobre los manifest files
para saltarte los que no contienen datos relevantes. Para tablas con 100,000
particiones y 1M de archivos, Iceberg puede planificar la query en segundos;
Delta Lake necesita procesar el log completo (o el último checkpoint).

---

### Ejercicio 8.6.2 — Partition evolution en Iceberg

Una de las capacidades más valoradas de Iceberg:

```python
# pip install pyiceberg

from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (NestedField, LongType, StringType, 
                              DoubleType, TimestampType)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform, MonthTransform, IdentityTransform

# Crear tabla con partición por día:
catalog = load_catalog("local", **{"type": "sql",
                                    "uri": "sqlite:///catalog.db"})

schema = Schema(
    NestedField(1, "id", LongType()),
    NestedField(2, "timestamp", TimestampType()),
    NestedField(3, "region", StringType()),
    NestedField(4, "monto", DoubleType()),
)

# Partición inicial: por día
spec_v1 = PartitionSpec(
    PartitionField(source_id=2, field_id=1000,
                   transform=DayTransform(), name="day")
)

tabla = catalog.create_table(
    identifier="ventas",
    schema=schema,
    partition_spec=spec_v1,
)

# ... escribir datos con partición por día ...

# Evolucionar la partición a mes (sin reescribir datos existentes!):
with tabla.update_spec() as update:
    update.remove_field("day")
    update.add_identity("month")  # ahora particiona por mes

# Los datos VIEJOS siguen con partición por día
# Los datos NUEVOS se escribirán con partición por mes
# Las queries funcionan correctamente en ambos
```

**Preguntas:**

1. ¿Por qué la partition evolution de Iceberg es especialmente valiosa?
   ¿Qué problema resuelve que Delta Lake no puede resolver fácilmente?

2. Si la tabla tiene datos con partición por día Y datos con partición por mes,
   ¿cómo planifica Iceberg una query que filtra por un rango de fechas?

3. ¿Cuál es el "hidden partitioning" de Iceberg y por qué simplifica
   los queries del usuario?

4. ¿Delta Lake puede cambiar la partición de una tabla existente?

**Pista:** El "hidden partitioning" de Iceberg: cuando usas
`PartitionField(source_id=timestamp, transform=DayTransform())`, Iceberg
genera automáticamente el valor de partición a partir del timestamp —
el usuario no necesita añadir una columna `date` calculada explícitamente.
En Delta Lake, tienes que añadir una columna `date = date(timestamp)` y
usar `.partitionBy("date")`. En Iceberg, el usuario simplemente escribe el
timestamp y Iceberg gestiona la partición internamente. Las queries también
son más limpias: `WHERE timestamp BETWEEN '2024-01-01' AND '2024-01-31'`
automáticamente hace predicate pushdown sobre las particiones de día.

---

### Ejercicio 8.6.3 — Multi-engine: Iceberg con Spark, Trino y Polars

```python
# La misma tabla Iceberg accesible desde múltiples engines:

# Desde PySpark:
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .config("spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0") \
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.mi_catalog",
            "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.mi_catalog.type", "hadoop") \
    .config("spark.sql.catalog.mi_catalog.warehouse", "s3://mi-bucket/iceberg/") \
    .getOrCreate()

spark.table("mi_catalog.default.ventas").show()

# Desde Python con PyIceberg (sin Spark):
from pyiceberg.catalog import load_catalog
catalog = load_catalog("mi_catalog", **{
    "type": "glue",  # AWS Glue como catalog
    "warehouse": "s3://mi-bucket/iceberg/",
})
tabla = catalog.load_table("default.ventas")

# Leer como Arrow y procesar con Polars:
import polars as pl
arrow_scan = tabla.scan(row_filter="region = 'norte'").to_arrow()
df_polars = pl.from_arrow(arrow_scan)
```

**Preguntas:**

1. ¿Qué es el "catalog" en Iceberg y por qué es necesario?

2. ¿Cuántos catalogs soporta Iceberg? ¿Cuáles son los más comunes?

3. ¿Delta Lake también soporta múltiples engines (Trino, DuckDB, Polars)?

4. ¿Qué es el "Open Table Format" y cómo se relaciona con Iceberg?

5. ¿Cuándo el soporte multi-engine de Iceberg es una ventaja real
   vs ser sobre-ingeniería?

> 🔗 Ecosistema: el catálogo Iceberg más común en producción es:
> AWS Glue (para AWS), Hive Metastore (para Hadoop/on-prem),
> y Nessie (open-source, git-like versioning para tablas).
> Unity Catalog de Databricks soporta tanto Delta Lake como Iceberg.

---

### Ejercicio 8.6.4 — Row-level deletes en Iceberg v2

Iceberg v2 añade soporte para deletes a nivel de fila sin reescribir archivos:

```
Iceberg v1 (Copy-on-Write como Delta Lake):
  DELETE de 1 fila → reescribir el archivo completo
  Eficiente para lectura, costoso para DELETE frecuentes

Iceberg v2 (Merge-on-Read con delete files):
  DELETE de 1 fila → escribir un "delete file" con la posición de la fila
  El archivo original NO se modifica
  Al leer: merge del archivo de datos + delete files
  Eficiente para DELETE frecuentes, más costoso para lectura

Tipo de delete files:
  - Position deletes: "eliminar fila en posición 1,234 del archivo X"
  - Equality deletes: "eliminar todas las filas donde id = 12345"
```

**Preguntas:**

1. ¿Cuándo es preferible Merge-on-Read (Iceberg v2) sobre Copy-on-Write (Delta Lake)?

2. ¿El Merge-on-Read tiene impacto en el rendimiento de las queries de lectura?

3. ¿Hay un comando equivalente al `OPTIMIZE` de Delta Lake en Iceberg para
   compactar los delete files?

4. ¿Los delete files de Iceberg son compatibles con todos los engines
   (Spark, Trino, Flink)?

---

### Ejercicio 8.6.5 — Leer: cuándo elegir Iceberg sobre Delta Lake

**Tipo: Comparar**

Para cada criterio, determinar si Iceberg o Delta Lake es superior,
o si son equivalentes:

```
Criterio                          Iceberg    Delta Lake
──────────────────────────────────────────────────────────
Partition evolution              ✓ mejor    ✗ manual
Multi-engine por defecto         ✓ mejor    ✓ (mejorando)
Schema evolution                 ✓          ✓ (similar)
Madurez del ecosistema           ✓ (+ años) ✓ (+ Databricks)
Integración con Spark            ✓          ✓✓ (Delta es de Databricks)
Operaciones DML (UPDATE/MERGE)   ✓          ✓✓ (mejor implementado)
Time travel                      ✓          ✓ (similar)
Gestión de metadata a escala     ✓ mejor    ✓ (checkpoints)
Comet (aceleración Spark)        ✗ (solo δ) ✓
```

Completar la tabla y añadir 3 criterios propios.

---

## Sección 8.7 — Comparativa y Decisión: Delta Lake vs Iceberg vs Hudi

### Ejercicio 8.7.1 — Apache Hudi: el especialista en CDC

```python
# Apache Hudi: diseñado para CDC y workloads con muchos updates/deletes

# Dos storage types en Hudi:
# Copy-on-Write (CoW): como Delta Lake, actualiza copiando archivos
# Merge-on-Read (MoR): escribe deltas, merge al leer
#   - escritura más rápida
#   - lectura más costosa (merge de base + deltas)

# En PySpark:
df_nuevos = spark.createDataFrame(
    [(1, "norte", 100.0), (2, "sur", 200.0)],
    ["id", "region", "monto"]
)

df_nuevos.write.format("hudi") \
    .option("hoodie.table.name", "mi_tabla_hudi") \
    .option("hoodie.datasource.write.recordkey.field", "id") \
    .option("hoodie.datasource.write.precombine.field", "timestamp") \
    .option("hoodie.datasource.write.operation", "upsert") \
    .option("hoodie.datasource.write.table.type", "MERGE_ON_READ") \
    .mode("append") \
    .save("/tmp/tabla_hudi/")
```

**Preguntas:**

1. ¿Cuál es el caso de uso principal de Hudi que lo diferencia de Delta e Iceberg?

2. ¿Qué son los "base files" y "delta files" en Hudi MoR?

3. ¿Hudi tiene algo equivalente al time travel de Delta Lake?

4. ¿Por qué Hudi es popular en Uber, LinkedIn, y otras empresas con
   workloads de streaming + updates frecuentes?

5. ¿Hudi soporta múltiples engines como Iceberg?

> ⚙️ Versión: Hudi 0.14+ tiene cambios significativos en la API de Python.
> Las opciones de configuración (`hoodie.*`) han cambiado en versiones recientes.
> Para proyectos nuevos en 2024, verificar la documentación oficial de Apache Hudi.

---

### Ejercicio 8.7.2 — La matriz de decisión final

**Tipo: Construir**

```
                     Delta Lake    Iceberg    Hudi
─────────────────────────────────────────────────────────────
Mejor para:
  Batch analytics     ✓✓           ✓          ✓
  CDC/streaming       ✓            ✓          ✓✓
  Multi-engine        ✓            ✓✓         ✓
  Databricks          ✓✓           ✓          ✓
  AWS Glue            ✓            ✓✓         ✓
  Updates frecuentes  ✓            ✓          ✓✓

Ecosistema:
  Maduro              ✓✓           ✓✓         ✓
  Open source         ✓            ✓✓         ✓✓
  Databricks-backed   ✓✓           ✓(neutral) ✓

Características:
  Partition evolution ✗            ✓✓         ✓
  Row-level deletes   ✓(CoW)       ✓(v2 MoR)  ✓✓(MoR)
  Schema evolution    ✓✓           ✓✓         ✓
  Time travel         ✓✓           ✓✓         ✓
  VACUUM/Compaction   ✓✓           ✓✓         ✓✓
```

**Restricciones:**
1. Completar las celdas marcadas con `?`
2. ¿Hay un "ganador claro" en alguna categoría?
3. ¿La elección es siempre técnica o también política (vendor lock-in)?

---

### Ejercicio 8.7.3 — Diseñar el lakehouse del sistema de e-commerce

**Tipo: Diseñar**

Para el sistema de e-commerce del repositorio, diseñar el lakehouse completo:

```
Fuentes:
  - Eventos de click/vista/compra: 50M eventos/día, streaming desde Kafka
  - Catálogo de productos: 1M productos, actualización por batch desde PostgreSQL
  - Clientes: 10M usuarios, actualizaciones frecuentes (GDPR: derecho al olvido)
  - Inventario: 50K SKUs, actualizaciones en tiempo real (10K updates/hora)

Capas del lakehouse:
  Bronze: datos crudos tal como llegan (sin transformación)
  Silver: datos limpios y normalizados (join, dedup, enrich)
  Gold:   métricas de negocio (revenue, conversión, stock)

SLA:
  - Datos disponibles para BI en < 1 hora desde su ingesta
  - Historial de 2 años para auditoría
  - Derecho al olvido (GDPR): eliminar datos de un usuario en < 24 horas
```

Para cada tabla y capa, especificar:
1. ¿Delta Lake, Iceberg, o Hudi? ¿Por qué?
2. Estrategia de particionamiento
3. Operaciones DML necesarias (INSERT, UPDATE, MERGE, DELETE)
4. Política de retención y vacuum
5. Estrategia de schema evolution

---

### Ejercicio 8.7.4 — El problema del GDPR en el lakehouse

**Tipo: Implementar**

GDPR requiere que puedas eliminar todos los datos de un usuario en 24 horas.
En un lakehouse con 2 años de historial y time travel, esto es un desafío:

```python
def eliminar_usuario_gdpr(
    spark: SparkSession,
    user_id: int,
    tablas: list[str],
) -> dict:
    """
    Elimina todos los datos de un usuario de todas las tablas del lakehouse.
    Desafío: el time travel mantiene versiones antiguas con esos datos.
    """
    resultados = {}

    for tabla in tablas:
        delta_tabla = DeltaTable.forPath(spark, tabla)

        # Paso 1: eliminar del estado actual
        filas_antes = spark.read.format("delta").load(tabla).count()
        delta_tabla.delete(f"user_id = {user_id}")
        filas_despues = spark.read.format("delta").load(tabla).count()

        resultados[tabla] = {
            "filas_eliminadas": filas_antes - filas_despues,
        }

        # Paso 2: eliminar el historial (time travel)
        # PROBLEMA: vacuum elimina archivos más viejos que el retention period
        # Si el retention period es 7 días y el usuario tiene datos de hace 2 años,
        # esos datos siguen accesibles via time travel hasta que vacuum los elimine

        # ¿Hay una forma de eliminar inmediatamente sin esperar el vacuum?
        # ...

    return resultados
```

**Preguntas:**

1. ¿Delta Lake puede eliminar datos de versiones históricas (time travel)
   de forma inmediata?

2. ¿VACUUM resuelve el problema del GDPR? ¿Con qué `retentionHours`?

3. ¿Hay tensión entre el time travel (retener historial) y el GDPR (eliminar datos)?
   ¿Cómo la resuelves en el diseño?

4. ¿Delta Lake tiene alguna operación específica para "borrado permanente"?
   ¿Y Apache Iceberg?

5. ¿Qué técnica criptográfica permite el "right to be forgotten" sin eliminar
   datos físicamente?

**Pista:** El patrón criptográfico para GDPR con time travel: en lugar de
almacenar datos personales directamente, almacenar `encrypt(datos_personales, clave_usuario)`.
Para "olvidar" al usuario, eliminar su clave de cifrado — los datos cifrados
en el historial siguen existiendo pero son indescifrables sin la clave.
Este patrón se llama "crypto-shredding". Delta Lake Deletion Vectors (DV, v3.0+)
permiten marcar filas como eliminadas sin reescribir los archivos — similar
a Iceberg v2 equality deletes, pero la fila puede recuperarse con time travel
si el DV se elimina. Para eliminación verdaderamente permanente, VACUUM sigue
siendo necesario.

---

### Ejercicio 8.7.5 — El repositorio a mitad de camino: reflexión

**Tipo: Reflexión/integrar**

Llegamos al final de la Parte 2 (batch processing).
Los capítulos 04-08 cubrieron: Spark (modelo + optimización), Polars, DataFusion,
y el Lakehouse.

Volviendo a la pregunta del Ejercicio 1.5.5 del Cap.01:

> *"El 80% de los problemas de performance son skew, shuffles, y formatos.
> ¿Estás de acuerdo?"*

Ahora que conoces Spark, Polars, DataFusion, y el Lakehouse:

1. ¿Cambiaría tu respuesta? ¿Añadirías o quitarías algo de la lista?

2. ¿Qué problema de los cinco capítulos te resultó más sorprendente?
   (algo que no esperabas que fuera así)

3. ¿Qué herramienta del batch stack (Spark, Polars, DataFusion, Delta Lake)
   usarías en tu trabajo actual? ¿Por qué?

4. La "cadena de causalidad" del Cap.03:
   ```
   Cap.01: framework controla el paralelismo
   Cap.02: formato determina el I/O
   Cap.03: Map/Reduce + shuffle es el modelo
   Cap.04: Spark implementa ese modelo
   Cap.05: optimizar = minimizar shuffles, evitar skew
   Cap.06: Polars evita el problema distribuido cuando los datos caben
   Cap.07: DataFusion es el motor embebible
   Cap.08: el Lakehouse añade ACID y gestión sobre el almacenamiento
   ```
   ¿Esta cadena es coherente? ¿Qué eslabón falta?

5. La Parte 3 cubre streaming (Kafka, Beam, Spark Streaming, Flink).
   Basándote en lo aprendido en batch, ¿qué esperas que sea diferente
   en streaming? ¿Qué esperas que sea igual?

---

## Resumen del capítulo

**Las cuatro garantías del lakehouse y su costo:**

```
1. Atomicidad
   Qué da: un write es todo-o-nada
   Costo: overhead de escritura en dos fases (archivos + commit al log)
   Sin esto: datos parciales después de un fallo

2. Consistencia (schema enforcement)
   Qué da: nadie puede escribir datos incorrectos
   Costo: overhead de verificación en cada write + evolución más rígida
   Sin esto: corrupción silenciosa de datos

3. Aislamiento (snapshot isolation)
   Qué da: lectores no ven escrituras en progreso
   Costo: overhead de gestionar versiones concurrentes
   Sin esto: dirty reads (leer datos a medio escribir)

4. Durabilidad
   Qué da: los datos sobreviven fallos del sistema
   Costo: dependencia de la durabilidad del almacenamiento subyacente (S3)
   Sin esto: perder datos en fallos de hardware
```

**La decisión práctica en una oración por formato:**

```
Delta Lake: si estás en Databricks o en el ecosistema Spark, y quieres
            la integración más profunda y el tooling más maduro.

Iceberg: si necesitas multi-engine sin vendor lock-in, o si necesitas
         partition evolution, o si usas AWS con Glue catalog.

Hudi: si tienes workloads con CDC/streaming y muchos updates frecuentes
      (Uber, LinkedIn style) y no te importa la mayor complejidad.
```

**Lo que conecta este capítulo con la Parte 3 (streaming):**

> El lakehouse que construimos en la Parte 2 es el destino de los pipelines
> de streaming de la Parte 3. Kafka (Cap.09) produce eventos. Flink (Cap.12)
> los procesa. Delta Lake / Iceberg es donde aterrizan.
> La Parte 3 cierra el ciclo: de la fuente de datos al lakehouse,
> pasando por el procesamiento en tiempo real.
