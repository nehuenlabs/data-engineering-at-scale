# Guía de Ejercicios — Cap.13: Python — PySpark, Arrow, y la GIL en Data Engineering

> Python domina el data engineering moderno — no porque sea el lenguaje más rápido,
> sino porque es el lenguaje donde se conectan piezas que sí son rápidas.
>
> PySpark no ejecuta código Python en los executors — ejecuta código JVM
> y llama a Python cuando es absolutamente necesario. Polars está escrito en Rust.
> Arrow elimina la copia de datos entre Python y C/C++/Rust.
>
> Entender Python en data engineering es entender cómo un lenguaje lento
> orquesta componentes rápidos — y cuándo el boundary entre Python
> y el componente rápido se convierte en el cuello de botella.

---

## El modelo mental: Python como pegamento

```
Lo que realmente ocurre cuando ejecutas un job de Spark en Python:

  tu código Python
        ↓
  PySpark (Python) construye el plan de ejecución
        ↓
  Py4J (bridge Python→JVM) envía el plan al JVM
        ↓
  Spark (JVM/Scala) ejecuta el plan en el cluster
        ↓
  Si hay UDFs Python: JVM → serializar datos → Python subprocess → deserializar → JVM
        ↑ ← este boundary es costoso

  La mayoría del procesamiento ocurre en JVM.
  Python solo construye el plan y ejecuta las partes que tú escribiste en Python.
```

```
Lo que ocurre con Polars:

  tu código Python
        ↓
  Polars Python API construye el plan (Python objects que apuntan a Rust)
        ↓
  Polars (Rust) ejecuta el plan — sin Python en el loop interno
        ↓
  El resultado regresa como Arrow buffer
        ↓  (zero-copy si el tipo es compatible)
  tu código Python recibe el resultado

  Python nunca entra en el loop de procesamiento.
  Solo inicia y recibe el resultado.
```

---

## Tabla de contenidos

- [Sección 13.1 — La GIL: por qué Python no paralela y por qué no importa](#sección-131--la-gil-por-qué-python-no-paralela-y-por-qué-no-importa)
- [Sección 13.2 — PySpark: la arquitectura del bridge JVM-Python](#sección-132--pyspark-la-arquitectura-del-bridge-jvm-python)
- [Sección 13.3 — UDFs: el boundary que más duele](#sección-133--udfs-el-boundary-que-más-duele)
- [Sección 13.4 — Arrow: el formato que une el ecosistema Python](#sección-134--arrow-el-formato-que-une-el-ecosistema-python)
- [Sección 13.5 — El ecosistema PyData: NumPy, Pandas, y sus sucesores](#sección-135--el-ecosistema-pydata-numpy-pandas-y-sus-sucesores)
- [Sección 13.6 — Python en producción: packaging, entornos, y dependencias](#sección-136--python-en-producción-packaging-entornos-y-dependencias)
- [Sección 13.7 — Integración: Python como pegamento del stack](#sección-137--integración-python-como-pegamento-del-stack)

---

## Sección 13.1 — La GIL: Por Qué Python No Paralela y Por Qué No Importa

### Ejercicio 13.1.1 — Leer: qué es la GIL y qué protege

**Tipo: Leer**

```
GIL = Global Interpreter Lock

¿Qué es?
  Un mutex que el intérprete de CPython adquiere antes de ejecutar
  cualquier bytecode Python. Solo un thread puede ejecutar bytecode
  Python en un proceso dado en cualquier momento.

¿Por qué existe?
  CPython gestiona la memoria con reference counting.
  Si dos threads modifican el mismo objeto simultáneamente,
  el conteo de referencias puede corromperse → memory corruption → crash.
  El GIL serializa todos los accesos para garantizar la integridad.

¿Qué significa en la práctica?
  # Este código NO es más rápido con 4 threads que con 1:
  import threading

  def calcular(n):
      resultado = 0
      for i in range(n):
          resultado += i * i
      return resultado

  # 4 threads — pero el GIL solo deja ejecutar a uno a la vez:
  threads = [threading.Thread(target=calcular, args=(10_000_000,)) for _ in range(4)]
  # Tiempo: ~igual que un solo thread (y a veces más lento por overhead de context switching)

¿Cuándo el GIL NO bloquea?
  El GIL se libera durante operaciones I/O (red, disco) y
  cuando código C/C++/Rust externo está ejecutando.
  NumPy, Pandas, Polars, PyArrow — todos liberan el GIL mientras procesan.
```

**Preguntas:**

1. ¿Por qué NumPy puede usar múltiples cores aunque Python tenga GIL?

2. ¿`multiprocessing` evita el GIL? ¿A qué costo?

3. Si tienes un job de Polars que procesa 1 GB de datos, ¿el GIL limita
   el uso de múltiples cores?

4. ¿Python 3.13 (con GIL opcional) cambia el panorama para data engineering?

5. ¿En qué parte de un job de PySpark el GIL sí es un problema?

**Pista:** NumPy libera el GIL explícitamente en sus loops internos de C —
las operaciones vectorizadas de NumPy corren en C sin el GIL, permitiendo
que múltiples threads ejecuten código NumPy simultáneamente. El GIL
solo bloquea la ejecución de bytecode Python puro. En PySpark, el GIL
importa en las UDFs Python: si tienes 8 cores y una UDF Python que procesa
cada fila, las 8 particiones intentan ejecutar código Python pero el GIL
las serializa → se usan los 8 cores en paralelo pero cada uno espera el GIL.
La solución: Pandas UDFs (que procesan en batches de Pandas/NumPy, liberando el GIL).

---

### Ejercicio 13.1.2 — Medir: el impacto real del GIL en data engineering

```python
import threading
import multiprocessing
import time
import numpy as np

# Caso 1: Código Python puro — el GIL importa
def suma_python(n: int) -> int:
    """Código Python puro — el GIL serializa."""
    return sum(i * i for i in range(n))

# Caso 2: Código NumPy — el GIL no importa (C interno)
def suma_numpy(n: int) -> float:
    """NumPy — libera el GIL durante el cálculo."""
    arr = np.arange(n, dtype=np.float64)
    return np.sum(arr * arr)

def benchmark(fn, n: int, num_workers: int, modo: str):
    inicio = time.perf_counter()

    if modo == "sequential":
        for _ in range(num_workers):
            fn(n)
    elif modo == "threads":
        threads = [threading.Thread(target=fn, args=(n,)) for _ in range(num_workers)]
        [t.start() for t in threads]
        [t.join() for t in threads]
    elif modo == "processes":
        with multiprocessing.Pool(num_workers) as pool:
            pool.map(fn, [n] * num_workers)

    return time.perf_counter() - inicio

N = 5_000_000
WORKERS = 4

print("Función Python pura:")
print(f"  Secuencial:    {benchmark(suma_python, N, WORKERS, 'sequential'):.2f}s")
print(f"  4 threads:     {benchmark(suma_python, N, WORKERS, 'threads'):.2f}s")
print(f"  4 procesos:    {benchmark(suma_python, N, WORKERS, 'processes'):.2f}s")

print("\nNumPy:")
print(f"  Secuencial:    {benchmark(suma_numpy, N, WORKERS, 'sequential'):.2f}s")
print(f"  4 threads:     {benchmark(suma_numpy, N, WORKERS, 'threads'):.2f}s")
print(f"  4 procesos:    {benchmark(suma_numpy, N, WORKERS, 'processes'):.2f}s")
```

**Restricciones:**
1. Ejecutar y completar la tabla de tiempos
2. ¿El speedup de threads para NumPy es lineal con el número de workers?
3. ¿El overhead de `multiprocessing` (fork + serialización) supera
   el beneficio de evitar el GIL para operaciones cortas?
4. ¿Cuándo vale la pena usar `multiprocessing` en lugar de confiar
   en Polars/NumPy para paralelismo?

---

### Ejercicio 13.1.3 — asyncio en data engineering: cuándo es útil

```python
import asyncio
import aiohttp
import time

# asyncio: concurrencia cooperativa, no paralelismo
# Útil para I/O concurrente — muchas operaciones de red simultáneas
# NO útil para CPU-bound (el GIL sigue siendo el límite)

async def obtener_datos(session: aiohttp.ClientSession, url: str) -> dict:
    async with session.get(url) as respuesta:
        return await respuesta.json()

async def enriquecer_batch(ids: list[str]) -> list[dict]:
    """
    Consultar una API para 1000 IDs concurrentemente.
    Con asyncio: todas las requests en vuelo simultáneamente.
    Sin asyncio: secuencialmente (1000 × latencia_red = minutos).
    """
    async with aiohttp.ClientSession() as session:
        tareas = [
            obtener_datos(session, f"https://api.ejemplo.com/item/{id}")
            for id in ids
        ]
        return await asyncio.gather(*tareas)

# Integración con Polars (que es síncrono):
import polars as pl

def enriquecer_dataframe(df: pl.DataFrame) -> pl.DataFrame:
    """
    Enriquecer un DataFrame de Polars con datos de una API,
    usando asyncio para las requests en paralelo.
    """
    ids = df["id"].to_list()

    # Correr el loop de asyncio:
    datos_enriquecidos = asyncio.run(enriquecer_batch(ids))

    df_extra = pl.DataFrame(datos_enriquecidos)
    return df.join(df_extra, on="id")
```

**Preguntas:**

1. ¿`asyncio.gather` ejecuta las requests en paralelo o concurrentemente?
   ¿Cuál es la diferencia?

2. ¿Puedes usar asyncio dentro de un UDF de PySpark?
   ¿Por qué sería problemático?

3. ¿asyncio tiene ventaja sobre `ThreadPoolExecutor` para I/O concurrente?
   ¿Cuándo uno supera al otro?

4. ¿Cuándo en un pipeline de data engineering necesitas asyncio?

---

### Ejercicio 13.1.4 — Free-threaded Python (3.13+): el futuro sin GIL

```python
# Python 3.13 introduce el modo "free-threaded" (sin GIL) como opción experimental.
# Para activarlo: compilar Python con --disable-gil o usar una distribución pre-compilada.

# Verificar si el intérprete es free-threaded:
import sys
print(sys.flags.ignore_environment)  # no directamente útil
# En Python 3.13+: sys._is_gil_enabled() retorna False si está deshabilitado

# Con free-threaded Python, threads Python pueden ejecutarse en paralelo real:
import threading
import time

def trabajo_cpu():
    resultado = 0
    for i in range(10_000_000):
        resultado += i
    return resultado

# Con GIL: los 4 threads se serializan → ~mismo tiempo que 1 thread
# Sin GIL: los 4 threads corren en paralelo real → ~4× más rápido

inicio = time.perf_counter()
threads = [threading.Thread(target=trabajo_cpu) for _ in range(4)]
[t.start() for t in threads]
[t.join() for t in threads]
print(f"Tiempo: {time.perf_counter()-inicio:.2f}s")
```

**Preguntas:**

1. ¿El free-threaded Python (3.13) cambia el panorama para data engineering?
   ¿O los frameworks ya evitan el GIL de otras formas?

2. ¿Las librerías de data engineering (NumPy, Polars, PyArrow) funcionan
   sin cambios en free-threaded Python? ¿Hay problemas de thread safety?

3. ¿Cuándo beneficia el free-threaded Python a un data engineer típico?

4. ¿`multiprocessing` sigue siendo relevante en Python 3.13+?

---

### Ejercicio 13.1.5 — Leer: diagnóstico de un pipeline lento por el GIL

**Tipo: Diagnosticar**

Un pipeline de PySpark tarda 45 minutos para 500 GB. El profiling muestra:

```
Stage 12 (groupBy + agregación):
  Executor 1 — Task 45:
    CPU time:        8.2s
    GC time:         0.1s
    Python eval:     38.4s  ← !!
    JVM overhead:    0.3s
    Total:           47.0s

  Executor 1 — Task 46:
    CPU time:        9.1s
    Python eval:     39.2s
    Total:           48.3s

El código de la stage:
  df.groupBy("region") \
    .agg(python_udf_compleja(F.col("datos_json")).alias("resultado"))
```

**Preguntas:**

1. ¿Por qué "Python eval" representa el 82% del tiempo de la task?

2. ¿El GIL está limitando el paralelismo en esta stage?
   ¿O es el overhead de cruzar el boundary JVM→Python?

3. ¿Cuántas llamadas a la UDF Python hace la task si el groupBy
   produce 10,000 grupos con 50 filas promedio cada uno?

4. ¿Cómo rediseñarías el pipeline para eliminar la UDF Python?

5. Si la lógica de la UDF no puede expresarse con funciones nativas de Spark,
   ¿cuál es la mejor alternativa?

**Pista:** Cada llamada a una UDF Python requiere: serializar los datos de JVM
a Python (pickle o Arrow), ejecutar la función en Python, deserializar el resultado
de vuelta a JVM. Para 500,000 filas en la task, son 500,000 llamadas de
ida y vuelta. El overhead de serialización domina — no es el cálculo en sí
lo que tarda, sino el cruce del boundary. La mejor alternativa cuando la lógica
es compleja: convertir a Pandas UDF (procesa en batches de Arrow, mucho menos
cruces del boundary) o reescribir la lógica en SQL/expresiones nativas de Spark.

---

## Sección 13.2 — PySpark: la Arquitectura del Bridge JVM-Python

### Ejercicio 13.2.1 — Leer: cómo funciona Py4J

**Tipo: Leer**

```
PySpark usa Py4J para comunicarse entre Python y la JVM:

  Python Process                    JVM Process (Spark)
  ─────────────────                 ─────────────────────
  SparkSession (Python) ─Py4J─────→ SparkSession (Scala)
  DataFrame (Python)    ─Py4J─────→ Dataset (Scala)
  Column (Python)       ─Py4J─────→ Column (Scala)

  Cuando escribes:
    df.filter(F.col("monto") > 100)
  
  PySpark:
  1. Crea un objeto Column en Python que representa la expresión
  2. Cuando llamas .collect(), serializa el plan completo via Py4J
  3. La JVM ejecuta el plan (Spark no sabe que estás en Python)
  4. Los resultados se serializan de JVM → Python (via Arrow o pickle)

  El código del DataFrame API no "va" a los executors.
  El plan lógico sí va — y se ejecuta en JVM puro.
```

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

# Este código Python NUNCA ejecuta en los executors:
df = spark.read.parquet("datos.parquet")
resultado = df \
    .filter(F.col("monto") > 100) \
    .groupBy("region") \
    .agg(F.sum("monto").alias("total"))

# Hasta aquí: solo se construyó el plan (Python objects + Py4J calls)
# El plan se serializa via Py4J y se envía a la JVM

# Aquí: la JVM ejecuta el plan, devuelve resultados a Python
datos = resultado.collect()
# resultado → Python list de Row objects
```

**Preguntas:**

1. ¿El DataFrame de PySpark contiene los datos en Python?
   ¿Cuándo los datos "salen" de la JVM a Python?

2. ¿`df.show()` transfiere todos los datos a Python?
   ¿Y `df.collect()`?

3. ¿La llamada a `F.col("monto") > 100` ejecuta algún código de filtrado
   en Python?

4. ¿Cuántas llamadas via Py4J hace este pipeline? ¿Es un número grande?

5. ¿Hay overhead de Py4J en el critical path de procesamiento?
   ¿O solo en la construcción del plan?

---

### Ejercicio 13.2.2 — El overhead de collect() y toPandas()

```python
from pyspark.sql import SparkSession, functions as F
import time

spark = SparkSession.builder \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .getOrCreate()

df = spark.range(0, 1_000_000).withColumn(
    "valor", (F.col("id") * 3.14).cast("double")
)

# Sin Arrow (serialización via pickle, fila por fila):
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
inicio = time.perf_counter()
df_pandas_sin_arrow = df.toPandas()
print(f"toPandas() sin Arrow: {time.perf_counter()-inicio:.2f}s")

# Con Arrow (serialización en batch, columnar):
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
inicio = time.perf_counter()
df_pandas_con_arrow = df.toPandas()
print(f"toPandas() con Arrow: {time.perf_counter()-inicio:.2f}s")

# collect() — retorna Python objects (no usa Arrow):
inicio = time.perf_counter()
rows = df.collect()
print(f"collect() (Python Rows): {time.perf_counter()-inicio:.2f}s")

# Verificar que los resultados son iguales:
import pandas as pd
assert df_pandas_sin_arrow.equals(df_pandas_con_arrow)
```

**Restricciones:**
1. Ejecutar y comparar los tres métodos
2. ¿Con Arrow habilitado, `toPandas()` hace zero-copy?
3. ¿Cuándo `collect()` es preferible a `toPandas()`?
4. ¿Qué pasa si el DataFrame tiene 1 billón de filas y llamas `collect()`?

---

### Ejercicio 13.2.3 — Serialización: qué formatos usa PySpark para enviar datos a Python

```python
# PySpark puede usar tres formatos para transferir datos entre JVM y Python:

# 1. Pickle (legacy, fila por fila):
#    - Compatible con cualquier tipo de Python
#    - Lento para datasets grandes
#    - Cada fila se serializa como un Python Row object

# 2. Arrow (batch columnar, recomendado para Pandas UDFs):
#    - Muy rápido para datos numéricos y strings
#    - Requiere tipos compatibles con Arrow
#    - Zero-copy posible en algunos casos

# 3. CloudPickle (para UDFs Python):
#    - Serializa la función Python junto con sus closures
#    - Se envía a cada executor al inicio del job
#    - El executor deserializa y llama la función

import cloudpickle
import pickle

# Ejemplo de lo que PySpark hace con tus UDFs:
def mi_funcion(x):
    return x * 2

# Serializar la función para enviarla a los executors:
bytes_funcion = cloudpickle.dumps(mi_funcion)
print(f"Tamaño serializado: {len(bytes_funcion)} bytes")

# Deserializar (lo que hace el executor al recibirla):
funcion_reconstruida = pickle.loads(bytes_funcion)
print(f"Resultado: {funcion_reconstruida(5)}")  # 10

# Caso problemático: UDF con un closure que captura un objeto grande
df_grande = [i for i in range(1_000_000)]

def udf_con_closure(x):
    return x in df_grande  # cierra sobre df_grande (1M elementos)

bytes_con_closure = cloudpickle.dumps(udf_con_closure)
print(f"Tamaño con closure: {len(bytes_con_closure):,} bytes")
# El df_grande (8MB) se serializa y se envía a CADA executor
```

**Preguntas:**

1. ¿Por qué un UDF que captura un objeto grande en su closure
   puede causar problemas de rendimiento en PySpark?

2. ¿Cómo evitas capturar objetos grandes en closures de UDFs?

3. ¿`cloudpickle` serializa correctamente funciones que usan
   imports dentro de la función? ¿Y lambdas?

4. ¿Cuándo PySpark usa Arrow y cuándo pickle para transferir datos?

---

### Ejercicio 13.2.4 — El modo local vs cluster: diferencias de comportamiento

```python
from pyspark.sql import SparkSession
import os

# Modo local (desarrollo en laptop):
spark_local = SparkSession.builder \
    .appName("local-dev") \
    .master("local[*]")  # usar todos los cores del laptop
    .getOrCreate()

# Modo cluster (producción en EMR/Databricks):
# La SparkSession se configura via spark-submit:
# spark-submit --master yarn --deploy-mode cluster mi_job.py

# Diferencia crítica: en modo local, driver y executor son el mismo proceso.
# En modo cluster, están en máquinas distintas.

# Esto significa que en modo LOCAL esto funciona:
datos_locales = [1, 2, 3, 4, 5]
df = spark_local.sparkContext.parallelize(datos_locales)
# datos_locales está en el mismo proceso — sin serialización

# En modo CLUSTER, datos_locales debe serializarse y enviarse al cluster:
# (el driver es la máquina que lanza el job, los executors están en otro lado)
```

**Preguntas:**

1. ¿Un job que funciona en modo local puede fallar en modo cluster?
   Da un ejemplo concreto.

2. ¿Las rutas de archivos (`/tmp/datos.parquet`) funcionan igual
   en local y en cluster?

3. ¿Qué configuraciones típicas del SparkSession son diferentes
   entre local y cluster?

4. ¿Cómo escribes código de PySpark que funciona sin cambios
   en ambos entornos?

---

### Ejercicio 13.2.5 — Leer: depurar un job de PySpark que falla solo en cluster

**Tipo: Diagnosticar**

Un job funciona perfectamente en modo `local[*]` pero falla en EMR con:

```
org.apache.spark.SparkException: Task failed while writing rows.
Caused by: net.razorvine.pickle.PickleException:
  expected zero arguments for construction of ClassDict (for builtins.function)

Stack trace:
  ... [Spark internals] ...
  at org.apache.spark.api.python.PythonWorkerFactory ...
```

El código problemático:
```python
import pandas as pd
from sklearn.preprocessing import StandardScaler

scaler = StandardScaler()
scaler.fit(X_entrenamiento)  # entrenado en el driver

@F.udf(returnType=DoubleType())
def normalizar(valor):
    return float(scaler.transform([[valor]])[0][0])  # usa scaler del driver

df.withColumn("normalizado", normalizar("valor")).show()
```

**Preguntas:**

1. ¿Por qué el job falla en cluster pero no en local?

2. ¿Qué hace `cloudpickle` con el objeto `scaler` (scikit-learn)?
   ¿Por qué falla la deserialización?

3. ¿Cómo rediseñas el pipeline para que funcione en cluster?

4. ¿El problema cambiaría si usas un Pandas UDF en lugar de un UDF estándar?

5. ¿Qué estrategia usar para aplicar un modelo de ML sobre un DataFrame
   de Spark de forma correcta y eficiente?

**Pista:** En modo local, `scaler` está en el mismo proceso que los "executors"
— no hay serialización. En cluster, el UDF captura `scaler` en su closure,
cloudpickle intenta serializarlo, y la deserialización falla porque el objeto
sklearn usa referencias internas que cloudpickle no puede reconstruir.
La solución más robusta: guardar el modelo a disco (S3/HDFS con `joblib.dump`),
cargar en el método `open()` de un Pandas UDF, o usar `spark.broadcast()`
para distribuir el modelo serializado manualmente.

---

## Sección 13.3 — UDFs: el Boundary que Más Duele

### Ejercicio 13.3.1 — Los tres tipos de UDF en PySpark

```python
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType
import pandas as pd
import numpy as np

# TIPO 1: UDF Python estándar (el más lento)
# Recibe: un valor Python por cada fila
# Costo: serializar/deserializar cada fila, llamada Python por fila

@F.udf(returnType=DoubleType())
def descuento_v1(monto: float, region: str) -> float:
    if region == "norte":
        return monto * 0.9
    return monto * 0.95

# TIPO 2: Pandas UDF (vectorized, mucho más rápido)
# Recibe: pd.Series completas (batch de datos)
# Costo: serializar/deserializar en batch via Arrow, sin llamada por fila

@F.pandas_udf(returnType=DoubleType())
def descuento_v2(monto: pd.Series, region: pd.Series) -> pd.Series:
    resultado = monto.copy()
    resultado[region == "norte"] *= 0.9
    resultado[region != "norte"] *= 0.95
    return resultado

# TIPO 3: Pandas UDF de agregación (UDAF con Pandas)
@F.pandas_udf(returnType=DoubleType())
def percentil_99(monto: pd.Series) -> float:
    return np.percentile(monto, 99)

# Comparar rendimiento:
df = spark.range(1_000_000) \
    .withColumn("monto", (F.col("id") % 1000).cast("double")) \
    .withColumn("region", F.when(F.col("id") % 2 == 0, "norte").otherwise("sur"))

import time

inicio = time.time()
df.withColumn("descuento", descuento_v1("monto", "region")).count()
print(f"UDF estándar:  {time.time()-inicio:.2f}s")

inicio = time.time()
df.withColumn("descuento", descuento_v2("monto", "region")).count()
print(f"Pandas UDF:    {time.time()-inicio:.2f}s")

inicio = time.time()
df.withColumn("descuento",
    F.when(F.col("region") == "norte", F.col("monto") * 0.9)
     .otherwise(F.col("monto") * 0.95)
).count()
print(f"SQL nativo:    {time.time()-inicio:.2f}s")
```

**Restricciones:**
1. Ejecutar y comparar los tres enfoques para 1M, 10M y 100M filas
2. ¿El speedup del Pandas UDF sobre el UDF estándar es constante?
3. ¿Para qué lógica el SQL nativo no es suficiente y se necesita un UDF?
4. ¿Las UDFs de Python aparecen en el plan de `explain()`?

---

### Ejercicio 13.3.2 — Pandas UDFs: tipos y semánticas

```python
import pandas as pd
import numpy as np
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Pandas UDF: Scalar (función de columna → columna)
@F.pandas_udf(DoubleType())
def normalizar_zscore(serie: pd.Series) -> pd.Series:
    """Z-score normalization."""
    return (serie - serie.mean()) / serie.std()

# Pandas UDF: Scalar Iterator (más eficiente para inicialización costosa)
from typing import Iterator
@F.pandas_udf(DoubleType())
def aplicar_modelo(iter: Iterator[pd.Series]) -> Iterator[pd.Series]:
    """
    Cargar el modelo una vez, aplicar a todos los batches.
    Más eficiente que cargar el modelo en cada llamada.
    """
    import joblib
    modelo = joblib.load("modelo.pkl")  # una sola carga

    for serie in iter:
        features = serie.values.reshape(-1, 1)
        predicciones = modelo.predict(features)
        yield pd.Series(predicciones)

# Pandas UDF: Grouped Map (apply sobre grupos)
def calcular_stats_grupo(df: pd.DataFrame) -> pd.DataFrame:
    """
    Para cada grupo, calcular estadísticas.
    El DataFrame recibido es el grupo completo.
    """
    return pd.DataFrame({
        "region": [df["region"].iloc[0]],
        "mean": [df["monto"].mean()],
        "std": [df["monto"].std()],
        "p95": [df["monto"].quantile(0.95)],
    })

resultado = df.groupBy("region").applyInPandas(
    calcular_stats_grupo,
    schema="region string, mean double, std double, p95 double"
)

# Pandas UDF: Grouped Aggregate
@F.pandas_udf(DoubleType())
def coef_variacion(serie: pd.Series) -> float:
    return serie.std() / serie.mean() if serie.mean() != 0 else 0.0
```

**Preguntas:**

1. ¿El "Scalar Iterator" UDF es más eficiente porque evita recargar el modelo
   o porque usa Arrow en batch? ¿O ambas cosas?

2. ¿`applyInPandas` tiene límite en el tamaño del grupo que puede procesar?
   ¿Qué pasa si un grupo tiene 1M filas?

3. ¿Los Pandas UDFs garantizan que el mismo batch no se procese dos veces?

4. ¿Cuándo `applyInPandas` es mejor que `groupBy().agg()` con funciones nativas?

---

### Ejercicio 13.3.3 — Arrow UDFs en Spark 3.5+: la evolución

```python
# Spark 3.5 introduce Arrow UDFs, una mejora sobre los Pandas UDFs:
# - Reciben y retornan PyArrow arrays en lugar de Pandas Series
# - Menor overhead de conversión Arrow→Pandas→Arrow
# - Mejor integración con el ecosistema Arrow

from pyspark.sql.functions import udf
import pyarrow as pa
import pyarrow.compute as pc

# Arrow UDF: recibe pa.ChunkedArray, retorna pa.Array
@udf(returnType=DoubleType(), useArrow=True)
def normalizar_arrow(serie: pa.ChunkedArray) -> pa.Array:
    arr = serie.combine_chunks().to_pylist()
    mean = pc.mean(serie).as_py()
    std_val = pc.stddev(serie).as_py()
    if std_val == 0:
        return pa.array([0.0] * len(arr))
    return pc.divide(
        pc.subtract(serie, pa.scalar(mean)),
        pa.scalar(std_val)
    )
```

**Preguntas:**

1. ¿Arrow UDFs son siempre más rápidos que Pandas UDFs?
   ¿En qué casos Pandas UDFs pueden ser equivalentes o mejores?

2. ¿Los Arrow UDFs pueden usarse con `groupBy().apply()`?

3. ¿Qué tipos de datos de Spark no tienen equivalente en Arrow?

---

### Ejercicio 13.3.4 — El anti-patrón: collect() dentro de un UDF

```python
# Anti-patrón muy frecuente en código PySpark de equipos con experiencia en Pandas:

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType

spark = SparkSession.builder.getOrCreate()
df = spark.range(1_000_000).withColumn("valor", F.col("id").cast("double"))

# ANTI-PATRÓN: collect() dentro del UDF (trae todos los datos al driver
# para calcular algo que podría calcularse distribuido)
datos_referencia = df.groupBy().agg(F.avg("valor")).collect()[0][0]

@F.udf(returnType=DoubleType())
def normalizar_mal(x: float) -> float:
    # PROBLEMA: datos_referencia es un closure que se serializa con el UDF
    # Pero el cálculo de datos_referencia ya ocurrió en el driver — eso está bien.
    # El problema real es otro: ¿qué pasa si datos_referencia fuera un DataFrame?
    return x / datos_referencia

# MEJOR: usar expresiones SQL sin UDF
media = df.groupBy().agg(F.avg("valor").alias("media")).collect()[0]["media"]
df_normalizado = df.withColumn("normalizado", F.col("valor") / F.lit(media))

# AÚN MEJOR: sin collect() en el driver
df_con_media = df.crossJoin(df.groupBy().agg(F.avg("valor").alias("media")))
df_normalizado_2 = df_con_media.withColumn(
    "normalizado", F.col("valor") / F.col("media")
)
```

**Preguntas:**

1. ¿El primer enfoque (`collect()` en el driver antes del UDF) es un problema
   de rendimiento o solo de estilo? ¿Cuándo sí lo es?

2. ¿`df.crossJoin(df.groupBy().agg(...))` escala para 1 billón de filas?

3. ¿Cómo calcularías la normalización por grupo (z-score dentro de cada región)
   sin UDFs?

4. ¿Qué pasa si el UDF hace `spark.read.parquet(...)` internamente?
   ¿Por qué es un problema grave?

---

### Ejercicio 13.3.5 — Implementar: el pipeline de ML sin UDFs

**Tipo: Implementar**

Un pipeline de ML necesita:
1. Preprocesar features (normalizar, encodear categóricas)
2. Aplicar un modelo sklearn entrenado
3. Guardar predicciones en Delta Lake

Implementar **sin UDFs Python estándar**, usando Pandas UDFs donde sea necesario:

```python
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *
import pandas as pd
import numpy as np

def pipeline_ml_sin_udfs(
    ruta_datos: str,
    ruta_modelo: str,
    ruta_salida: str,
) -> None:
    """
    Pipeline de ML que maximiza el uso de código JVM nativo
    y minimiza los cruces Python-JVM.
    """
    spark = SparkSession.builder.getOrCreate()

    df = spark.read.parquet(ruta_datos)

    # Paso 1: preprocesamiento con funciones nativas de Spark
    # (sin UDFs para operaciones que Spark puede hacer nativo)
    df_prep = df \
        .fillna({"monto": 0.0, "region": "desconocido"}) \
        .withColumn("log_monto", F.log1p(F.col("monto"))) \
        .withColumn("es_norte", (F.col("region") == "norte").cast("int")) \
        .withColumn("mes", F.month("fecha"))

    # Paso 2: aplicar el modelo sklearn con Pandas UDF (Scalar Iterator)
    # para cargar el modelo UNA VEZ por executor
    feature_cols = ["log_monto", "es_norte", "mes"]

    # TODO: implementar el Pandas UDF que aplica el modelo
    # Usar el patrón Iterator para cargar el modelo una sola vez

    # Paso 3: guardar resultados
    df_predicciones.write \
        .format("delta") \
        .mode("overwrite") \
        .save(ruta_salida)
```

**Restricciones:**
1. Implementar el Pandas UDF para aplicar el modelo
2. ¿Cómo distribuyes el modelo a los executors sin capturarlo en el closure?
3. Medir el throughput (filas/segundo) del pipeline completo

---

## Sección 13.4 — Arrow: el Formato que Une el Ecosistema Python

### Ejercicio 13.4.1 — Arrow como formato universal en Python

```python
import pyarrow as pa
import pandas as pd
import polars as pl
import numpy as np

# Un array Arrow puede ser visto por múltiples librerías sin copia:
arr_arrow = pa.array([1.0, 2.0, 3.0, 4.0, 5.0], type=pa.float64())

# Arrow → NumPy (zero-copy para tipos numéricos):
arr_numpy = arr_arrow.to_numpy()
print(np.shares_memory(arr_arrow.buffers()[1], arr_numpy))  # True = zero-copy

# Arrow → Pandas Series (zero-copy para float64):
serie_pandas = arr_arrow.to_pandas()

# Arrow → Polars Series (zero-copy):
serie_polars = pl.Series(arr_arrow)

# Tabla Arrow (múltiples columnas):
tabla_arrow = pa.table({
    "id": pa.array([1, 2, 3, 4, 5]),
    "valor": pa.array([10.0, 20.0, 30.0, 40.0, 50.0]),
    "categoria": pa.array(["a", "b", "a", "b", "a"]),
})

# Convertir entre formatos:
df_pandas = tabla_arrow.to_pandas()
df_polars = pl.from_arrow(tabla_arrow)
# Ambas conversiones son zero-copy para columnas numéricas

# Verificar zero-copy:
print(f"Pandas buffer == Arrow buffer: "
      f"{np.shares_memory(tabla_arrow.column('valor').buffers()[1], df_pandas['valor'].values)}")
```

**Preguntas:**

1. ¿Qué tipos de datos Arrow son zero-copy a NumPy? ¿Cuáles requieren copia?

2. ¿Las columnas de strings son zero-copy entre Arrow y Pandas?
   ¿Y entre Arrow y Polars?

3. ¿Cuándo NO quieres zero-copy (ej: cuando quieres modificar el resultado
   sin afectar al array original)?

4. ¿El formato Arrow en memoria es el mismo que el formato Parquet en disco?
   ¿Cómo se relacionan?

---

### Ejercicio 13.4.2 — IPC: compartir datos entre procesos con Arrow

```python
import pyarrow as pa
import pyarrow.ipc as ipc
import io

# Arrow IPC permite compartir datos entre procesos sin copia:
tabla = pa.table({
    "id": pa.array(range(1_000_000)),
    "valor": pa.array([float(i) for i in range(1_000_000)]),
})

# Serializar a bytes (Arrow IPC format):
buffer = io.BytesIO()
writer = ipc.new_stream(buffer, tabla.schema)
writer.write(tabla)
writer.close()
bytes_serializados = buffer.getvalue()
print(f"Tamaño serializado: {len(bytes_serializados):,} bytes")

# Deserializar:
reader = ipc.open_stream(io.BytesIO(bytes_serializados))
tabla_reconstruida = reader.read_all()

# Arrow Plasma Store: memoria compartida entre procesos (deprecated en Arrow 12+)
# La alternativa moderna: usar mmap o frameworks como Ray

# Comparar con pickle:
import pickle
import time

inicio = time.perf_counter()
bytes_pickle = pickle.dumps(tabla.to_pandas())
print(f"Pickle size: {len(bytes_pickle):,} bytes, time: {time.perf_counter()-inicio:.3f}s")

inicio = time.perf_counter()
_ = pickle.loads(bytes_pickle)
print(f"Pickle deserialize: {time.perf_counter()-inicio:.3f}s")
```

**Restricciones:**
1. Comparar velocidad de serialización/deserialización Arrow IPC vs pickle
2. ¿Arrow IPC es siempre más eficiente que pickle? ¿Cuándo no?
3. ¿Cómo usarías Arrow IPC para comunicar dos procesos Python
   (productor y consumidor) sin pasar por Redis o Kafka?

---

### Ejercicio 13.4.3 — Arrow Flight: streaming de datos entre servicios

```python
import pyarrow as pa
import pyarrow.flight as flight
import threading

# Arrow Flight es un protocolo RPC para transferir datos Arrow eficientemente.
# Ver Cap.02 §2.3.3 para la implementación completa.
# Aquí: integrarlo con PySpark y Polars.

class ServidorDatos(flight.FlightServerBase):
    """Servidor que expone datos procesados con Polars via Arrow Flight."""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        import polars as pl
        # Pre-procesar datos con Polars (rápido, local):
        self._datos = pl.read_parquet("datos.parquet") \
            .filter(pl.col("activo") == True) \
            .group_by("region") \
            .agg(pl.sum("monto").alias("revenue"))
    
    def do_get(self, context, ticket):
        tabla = self._datos.to_arrow()
        return flight.RecordBatchStream(tabla)

# El cliente (PySpark) obtiene los datos via Flight:
def enriquecer_con_flight(spark, url_servidor: str):
    cliente = flight.connect(url_servidor)
    tabla_arrow = cliente.do_get(flight.Ticket(b"datos")).read_all()
    # Convertir Arrow → Spark DataFrame:
    df_pandas = tabla_arrow.to_pandas()
    return spark.createDataFrame(df_pandas)
```

**Preguntas:**

1. ¿Arrow Flight es más eficiente que una API REST + JSON para transferir
   datos analíticos? ¿Por qué?

2. ¿Arrow Flight puede transmitir datos más grandes que la RAM del cliente?

3. ¿Cuándo tiene sentido usar Arrow Flight en un pipeline de data engineering
   en lugar de escribir a S3 y leer desde S3?

---

### Ejercicio 13.4.4 — El costo real de la conversión Spark→Pandas

```python
# Medir el costo de diferentes formas de llevar datos de Spark a Python:
from pyspark.sql import SparkSession, functions as F
import time

spark = SparkSession.builder \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .getOrCreate()

# Dataset de prueba:
df = spark.range(0, 5_000_000) \
    .withColumn("a", (F.col("id") % 1000).cast("double")) \
    .withColumn("b", (F.col("id") % 500).cast("double")) \
    .withColumn("cat", F.when(F.col("id") % 3 == 0, "x")
                        .when(F.col("id") % 3 == 1, "y")
                        .otherwise("z"))

def medir(nombre, fn):
    inicio = time.perf_counter()
    resultado = fn()
    print(f"{nombre}: {time.perf_counter()-inicio:.3f}s")
    return resultado

# Método 1: toPandas() con Arrow
df.cache().count()  # materializar primero
medir("toPandas() con Arrow", lambda: df.toPandas())

# Método 2: toPandas() sin Arrow
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
medir("toPandas() sin Arrow", lambda: df.toPandas())
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

# Método 3: collect() (Python Row objects)
medir("collect()", lambda: df.collect())

# Método 4: guardar a Parquet y leer con Polars
medir("write Parquet + Polars read", lambda: (
    df.write.mode("overwrite").parquet("/tmp/benchmark.parquet"),
    __import__("polars").read_parquet("/tmp/benchmark.parquet")
))
```

**Restricciones:**
1. ¿Cuál es el método más rápido para 5M filas?
2. ¿El resultado cambia para 50M filas?
3. ¿Cuándo "escribir a Parquet y leer" tiene sentido vs `toPandas()`?

---

### Ejercicio 13.4.5 — Leer: elegir el formato de transferencia correcto

**Tipo: Analizar**

Para cada escenario, elegir el formato de transferencia más apropiado:

```
Escenario 1:
  Un job de Spark produce un resultado de 100K filas × 20 columnas.
  Una aplicación Flask necesita servir ese resultado a usuarios via API REST.
  La aplicación Flask usa Pandas internamente para filtrar y ordenar.

Escenario 2:
  Un servicio de Python necesita enriquecer un stream de Flink con datos
  de un modelo de ML. Throughput: 50K eventos/segundo.
  El modelo tarda 1ms por evento.

Escenario 3:
  Un analista necesita explorar un dataset de 50 GB en un notebook Jupyter.
  El dato está en S3 como Parquet.

Escenario 4:
  Dos microservicios en el mismo datacenter necesitan intercambiar
  DataFrames de 1 GB frecuentemente (cada 30 segundos).
```

---

## Sección 13.5 — El Ecosistema PyData: NumPy, Pandas, y sus Sucesores

### Ejercicio 13.5.1 — El árbol genealógico: NumPy → Pandas → Arrow → Polars

```python
# Entender por qué el ecosistema evolucionó de esta forma:

import numpy as np
import pandas as pd
import pyarrow as pa
import polars as pl
import time

n = 10_000_000

# NumPy (1995-ish): arrays numéricos homogéneos, vectorizados
arr = np.random.uniform(0, 1000, n)
inicio = time.perf_counter()
resultado = arr[arr > 500].sum()
print(f"NumPy filtro+suma: {time.perf_counter()-inicio:.3f}s")

# Pandas (2008): tablas con múltiples tipos, operaciones de DataFrame
# Construido sobre NumPy, añade strings, fechas, NA values, groupby
df_pandas = pd.DataFrame({
    "valor": np.random.uniform(0, 1000, n),
    "categoria": np.random.choice(["a", "b", "c"], n),
})
inicio = time.perf_counter()
resultado = df_pandas[df_pandas["valor"] > 500].groupby("categoria")["valor"].sum()
print(f"Pandas filtro+groupby: {time.perf_counter()-inicio:.3f}s")

# Polars (2020): reimplementación de Pandas en Rust con Arrow
# Aprovecha SIMD, evita el GIL, lazy evaluation
df_polars = pl.DataFrame({
    "valor": np.random.uniform(0, 1000, n).tolist(),
    "categoria": np.random.choice(["a", "b", "c"], n).tolist(),
})
inicio = time.perf_counter()
resultado = df_polars.filter(pl.col("valor") > 500).group_by("categoria").agg(pl.sum("valor"))
print(f"Polars filtro+groupby: {time.perf_counter()-inicio:.3f}s")
```

**Preguntas:**

1. ¿Por qué Pandas construyó sobre NumPy en lugar de reimplementar
   la gestión de memoria desde cero?

2. ¿Pandas 2.0 con Arrow backend es tan rápido como Polars?
   ¿En qué operaciones sí y en cuáles no?

3. ¿Cuándo NumPy puro es más rápido que Pandas o Polars?

4. ¿Qué le falta a NumPy para usarse directamente como motor de data engineering
   (en lugar de Pandas/Polars)?

---

### Ejercicio 13.5.2 — Pandas 2.0 con Arrow backend

```python
import pandas as pd
import numpy as np

# Pandas 2.0 (2023) añade soporte para Arrow como backend de almacenamiento:
df_pandas_arrow = pd.DataFrame({
    "id": range(1_000_000),
    "monto": np.random.uniform(10, 1000, 1_000_000),
    "region": np.random.choice(["norte", "sur", "este"], 1_000_000),
}).convert_dtypes(dtype_backend="pyarrow")

# La diferencia: las columnas están almacenadas como Arrow arrays
# en lugar de NumPy arrays internamente
print(df_pandas_arrow.dtypes)
# id       int64[pyarrow]
# monto    double[pyarrow]
# region   string[pyarrow]

# Ventajas del Arrow backend en Pandas:
# - Mejor manejo de NA (Arrow tiene NA nativo, NumPy usa NaN/sentinel values)
# - Mejor rendimiento para strings (Arrow StringArray vs object array)
# - Conversión a Arrow sin copia (ya es Arrow internamente)
# - Tipos adicionales: list, struct, dictionary

# Verificar mejora de rendimiento:
import time

df_numpy = pd.DataFrame({
    "texto": [f"item_{i}" for i in range(1_000_000)],
    "valor": np.random.uniform(0, 100, 1_000_000),
})
df_arrow = df_numpy.convert_dtypes(dtype_backend="pyarrow")

inicio = time.perf_counter()
_ = df_numpy["texto"].str.upper()
print(f"Pandas numpy backend str.upper(): {time.perf_counter()-inicio:.3f}s")

inicio = time.perf_counter()
_ = df_arrow["texto"].str.upper()
print(f"Pandas arrow backend str.upper(): {time.perf_counter()-inicio:.3f}s")
```

**Preguntas:**

1. ¿El Arrow backend de Pandas 2.0 hace que Pandas sea tan rápido como Polars?

2. ¿Qué código de Pandas existente se rompe al cambiar al Arrow backend?

3. ¿Cuándo migrar el código de Pandas existente a Polars vs simplemente
   usar el Arrow backend?

---

### Ejercicio 13.5.3 — Cuándo Pandas es la respuesta correcta

```python
# Pandas sigue siendo la respuesta correcta para muchos casos:

# 1. Datasets pequeños (< 1 GB) con análisis exploratorio
#    - Pandas tiene una API madura con miles de métodos
#    - La documentación y los ejemplos online son vastísimos
#    - Jupyter + Pandas + Matplotlib es el flujo de trabajo estándar

# 2. Integración con librerías de ML que esperan DataFrames Pandas
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import GradientBoostingClassifier

df = pd.read_parquet("training_data.parquet")
X_train, X_test, y_train, y_test = train_test_split(
    df[["feature_1", "feature_2", "feature_3"]],
    df["label"],
    test_size=0.2,
)
modelo = GradientBoostingClassifier().fit(X_train, y_train)

# 3. Operaciones que Polars no soporta bien (aún en 2024):
#    - .pivot_table() complejo
#    - Algunas operaciones de resample con tiempo irregular
#    - Integración con ciertos formatos (xls específico, etc.)

# 4. Código existente que funciona — no reescribir si no hay problema de rendimiento
```

**Preguntas:**

1. ¿Cuál es el criterio más importante para decidir entre Pandas y Polars?

2. ¿Migrar de Pandas a Polars siempre mejora el rendimiento?
   ¿Hay casos donde Pandas es más rápido?

3. Para un data scientist que trabaja principalmente con datasets < 10 GB
   en notebooks, ¿vale la pena aprender Polars?

---

### Ejercicio 13.5.4 — NumPy avanzado: broadcasting y strides

```python
import numpy as np
import time

# Broadcasting: operar sobre arrays de formas distintas sin copia
a = np.ones((1_000, 1))    # (1000, 1)
b = np.ones((1, 1_000))    # (1, 1000)
c = a + b                   # (1000, 1000) — sin copiar a ni b

# Strides: acceder a los datos de formas distintas sin copia
arr = np.arange(12).reshape(3, 4)
# arr.strides = (32, 8) — 32 bytes por fila, 8 bytes por columna (int64)

# Trasponer sin copia (solo cambia los strides):
arr_T = arr.T
print(f"Comparte memoria: {np.shares_memory(arr, arr_T)}")  # True

# Ventana deslizante con strides (sin bucles Python):
def ventana_deslizante(arr, ventana):
    n = len(arr) - ventana + 1
    shape = (n, ventana)
    strides = (arr.strides[0], arr.strides[0])
    return np.lib.stride_tricks.as_strided(arr, shape=shape, strides=strides)

datos = np.random.randn(1_000_000)
ventanas = ventana_deslizante(datos, 7)
medias = ventanas.mean(axis=1)
# 1M - 7 + 1 = 999,994 medias móviles, sin bucle Python
```

**Preguntas:**

1. ¿`np.lib.stride_tricks.as_strided` es zero-copy?
   ¿Qué precaución debes tener con la memoria?

2. ¿El broadcasting de NumPy es equivalente a `.over()` en Polars
   para la misma operación?

3. ¿Cuándo elegirías NumPy con strides sobre Polars para media móvil?

---

### Ejercicio 13.5.5 — Leer: migrar un pipeline de Pandas a escala

**Tipo: Diseñar**

Un equipo tiene 50,000 líneas de código de análisis en Pandas que funcionan
perfectamente para 500 MB de datos. El negocio crece y ahora necesitan
procesar 500 GB con el mismo pipeline.

Evaluar tres estrategias de migración:

```
Estrategia 1: Reemplazar Pandas por Polars
  - Reescribir la lógica usando la API de Polars
  - Ventaja: código limpio, rendimiento máximo en una máquina
  - Riesgo: costo de reescritura, diferencias semánticas entre Pandas y Polars

Estrategia 2: Mantener Pandas, procesar en chunks
  - Iterar el archivo en chunks de 1 GB con pd.read_parquet(chunksize=...)
  - Ventaja: sin reescritura, aprovechar código existente
  - Riesgo: semántica de algunos agregados cambia (ej: mean de means ≠ mean global)

Estrategia 3: Migrar a PySpark con Pandas UDFs
  - Mover la lógica compleja a Pandas UDFs dentro de PySpark
  - Ventaja: distribución automática, familiar para el equipo
  - Riesgo: overhead de Spark para dataset que cabe en una máquina grande
```

**Restricciones:**
1. ¿Qué estrategia elegirías y por qué?
2. ¿Qué partes del pipeline son más difíciles de migrar en cada estrategia?
3. ¿Hay una "cuarta estrategia" que no se consideró?

---

## Sección 13.6 — Python en Producción: Packaging, Entornos, y Dependencias

### Ejercicio 13.6.1 — Distribuir dependencias Python a un cluster de Spark

```python
# Problema: tu job usa pandas, scikit-learn, y polars.
# Los executors del cluster no tienen estas librerías instaladas.

# Solución 1: PySpark con conda/virtualenv
spark = SparkSession.builder \
    .config("spark.archives", "my_env.tar.gz#environment") \
    .config("spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON",
            "environment/bin/python") \
    .config("spark.yarn.appMasterEnv.PYSPARK_PYTHON",
            "environment/bin/python") \
    .getOrCreate()
# Empaqueta tu entorno conda como tar.gz y lo distribuye a los executors

# Solución 2: pex (Python EXecutable)
# Un .pex es un archivo Python auto-contenido con todas las dependencias
# pex polars scikit-learn pandas -o mi_job.pex

# Solución 3: Docker image con todas las dependencias
# El executor corre en un container con las librerías pre-instaladas
# Recomendado para Kubernetes

# Solución 4: DBFS en Databricks / S3 + bootstrapping en EMR
spark.sparkContext.addPyFile("s3://deps/mi_modulo.py")
# Solo funciona para archivos Python, no para librerías C compiladas
```

**Preguntas:**

1. ¿Por qué las librerías con extensiones C (NumPy, Polars) no pueden
   distribuirse con `addPyFile`?

2. ¿Qué diferencia hay entre distribuir un entorno conda vs un Docker image
   para un job de Spark?

3. ¿Cómo manejas las actualizaciones de dependencias en un cluster compartido
   donde múltiples jobs corren simultáneamente?

4. ¿Databricks Connect o Spark Connect cambia esta ecuación?

---

### Ejercicio 13.6.2 — Poetry, pyproject.toml, y reproducibilidad

```toml
# pyproject.toml — el estándar moderno para proyectos Python de datos

[tool.poetry]
name = "pipeline-ecommerce"
version = "1.0.0"
description = "Pipeline de analytics del sistema de e-commerce"

[tool.poetry.dependencies]
python = "^3.11"
pyspark = "3.5.0"
polars = "0.20.6"
pyarrow = "14.0.2"
delta-spark = "3.0.0"
scikit-learn = "1.3.2"
pandas = "2.1.4"

[tool.poetry.group.dev.dependencies]
pytest = "^7.4"
pytest-spark = "^0.6"
black = "^23.0"
mypy = "^1.5"

# Para distribuir al cluster:
# poetry export -f requirements.txt --output requirements.txt --without-hashes
```

**Preguntas:**

1. ¿Por qué fijar las versiones exactas (ej: `pyspark = "3.5.0"`) es importante
   en data engineering pero puede ser problemático en librerías?

2. ¿`pyarrow = "14.0.2"` y `pyspark = "3.5.0"` son compatibles?
   ¿Cómo verificas la compatibilidad entre dependencias?

3. ¿Poetry lock file garantiza reproducibilidad entre máquinas con
   diferentes sistemas operativos?

---

### Ejercicio 13.6.3 — Type hints en data engineering: cuándo ayudan

```python
from typing import Optional
import polars as pl
import pyarrow as pa
from pyspark.sql import DataFrame as SparkDataFrame

# Type hints para pipelines de datos:

def calcular_revenue(
    df: pl.DataFrame,
    columna_monto: str = "monto",
    grupo: str = "region",
) -> pl.DataFrame:
    """
    Calcular revenue total por grupo.
    
    Returns:
        DataFrame con columnas: [grupo, "revenue_total"]
    """
    return df.group_by(grupo).agg(
        pl.sum(columna_monto).alias("revenue_total")
    )

# Type hints para DataFrames de Spark (menos util — el schema es dinámico):
def leer_ventas(spark, ruta: str) -> SparkDataFrame:
    return spark.read.parquet(ruta)

# Para schemas más precisos en Polars, se puede usar un tipo personalizado:
from typing import TypedDict

class EsquemaVentas(TypedDict):
    user_id: str
    monto: float
    region: str
    timestamp: str

# O usar dataclasses para representar el schema esperado:
from dataclasses import dataclass

@dataclass
class ConfigPipeline:
    ruta_entrada: str
    ruta_salida: str
    watermark_minutos: int = 10
    parallelism: int = 8
```

**Preguntas:**

1. ¿Los type hints en funciones de Polars o PySpark son verificados
   en tiempo de ejecución o solo por herramientas estáticas (mypy)?

2. ¿Polars tiene soporte para annotaciones de schema a nivel de tipo?
   (ej: `pl.DataFrame[Ventas]` que garantice el schema en compilación)

3. ¿Cuándo los type hints añaden más confusión que claridad en data engineering?

---

### Ejercicio 13.6.4 — Logging y observabilidad en código Python de datos

```python
import logging
import time
from functools import wraps
from typing import Callable, Any

# Configurar logging estructurado para pipelines:
import structlog

log = structlog.get_logger()

def log_pipeline_step(paso: str):
    """Decorator para loggear el tiempo y resultado de cada paso del pipeline."""
    def decorator(fn: Callable) -> Callable:
        @wraps(fn)
        def wrapper(*args, **kwargs) -> Any:
            inicio = time.perf_counter()
            log.info("paso_iniciado", paso=paso)
            try:
                resultado = fn(*args, **kwargs)
                duracion = time.perf_counter() - inicio

                # Inferir métricas del resultado:
                filas = None
                if hasattr(resultado, "__len__"):
                    filas = len(resultado)
                elif hasattr(resultado, "count"):
                    filas = resultado.count()  # para Spark DataFrames (caro!)

                log.info("paso_completado",
                         paso=paso,
                         duracion_segundos=round(duracion, 3),
                         filas_resultado=filas)
                return resultado

            except Exception as e:
                duracion = time.perf_counter() - inicio
                log.error("paso_fallado",
                          paso=paso,
                          duracion_segundos=round(duracion, 3),
                          error=str(e))
                raise

        return wrapper
    return decorator

# Uso:
@log_pipeline_step("leer_ventas")
def leer_ventas(spark, ruta: str):
    return spark.read.parquet(ruta)

@log_pipeline_step("calcular_metricas")
def calcular_metricas(df):
    return df.groupBy("region").agg({"monto": "sum"})
```

**Restricciones:**
1. Implementar el decorator completo con métricas de memoria
2. ¿Llamar `resultado.count()` en el decorator es una buena idea?
   ¿Cuándo sí y cuándo no?
3. ¿Cómo enviarías estos logs a un sistema centralizado (DataDog, ELK)?

---

### Ejercicio 13.6.5 — Leer: el entorno que no era reproducible

**Tipo: Diagnosticar**

Un job que funciona en la laptop del data engineer falla en producción
con este error:

```
ImportError: libfontconfig.so.1: cannot open shared object file: No such file or directory
  File "/usr/local/lib/python3.11/site-packages/matplotlib/backends/backend_cairo.py"
  File "pipeline/visualizacion.py", line 3, in <module>
    import matplotlib.pyplot as plt
```

La solución "obvia" del equipo: `apt-get install libfontconfig1` en el cluster.
Pero el infra team no quiere modificar las imágenes base del cluster por un solo job.

**Preguntas:**

1. ¿Por qué `matplotlib` no es una dependencia apropiada para un job
   de producción de data engineering?

2. ¿Cómo el job de producción llegó a depender de `matplotlib`?

3. ¿Cuál es la solución correcta a largo plazo?

4. ¿Qué lección de arquitectura hay aquí sobre separar las responsabilidades?

---

## Sección 13.7 — Integración: Python como Pegamento del Stack

### Ejercicio 13.7.1 — Python como orquestador: llamar a Spark desde Python

```python
# Patrón común: un script Python orquesta un pipeline que usa múltiples herramientas

import subprocess
import boto3
import polars as pl
from pyspark.sql import SparkSession

def pipeline_completo(fecha: str) -> None:
    """
    Pipeline que usa Polars para datos pequeños, Spark para datos grandes,
    y boto3 para interactuar con AWS.
    """
    
    # 1. Polars: preprocesar dimensiones (< 1 GB, rápido)
    df_clientes = pl.read_parquet("s3://dims/clientes/") \
        .filter(pl.col("activo") == True) \
        .with_columns([
            pl.col("nombre").str.to_lowercase().alias("nombre_norm"),
        ])
    
    # 2. Guardar dimensiones procesadas para que Spark las lea:
    df_clientes.write_parquet(f"/tmp/clientes_procesados_{fecha}.parquet")
    
    # 3. Spark: procesar el dataset grande (100+ GB)
    spark = SparkSession.builder.getOrCreate()
    spark.read.parquet(f"s3://eventos/{fecha}/") \
        .join(spark.read.parquet(f"/tmp/clientes_procesados_{fecha}.parquet"),
              on="cliente_id") \
        .groupBy("segmento") \
        .agg({"monto": "sum"}) \
        .write.parquet(f"s3://resultados/{fecha}/")
    
    # 4. Notificar via AWS SNS:
    sns = boto3.client("sns")
    sns.publish(
        TopicArn="arn:aws:sns:us-east-1:123:pipeline-completado",
        Message=f"Pipeline {fecha} completado",
    )
```

**Preguntas:**

1. ¿Esta arquitectura tiene problemas de reproducibilidad?
   ¿Qué pasa si el job falla a mitad?

2. ¿Es correcto mezclar Polars y PySpark en el mismo proceso?
   ¿Hay conflictos de recursos?

3. ¿Cómo reemplazarías este script Python con Airflow o Dagster?
   ¿Qué ganas y qué pierdes?

---

### Ejercicio 13.7.2 — Python + Rust: cuándo añadir extensiones nativas

```python
# Cuando Python es demasiado lento y las librerías existentes no son suficientes,
# se pueden escribir extensiones en Rust usando PyO3 o maturin.

# Ejemplo: un parser de formato propietario que no existe en ninguna librería:
# En Python (lento):
def parse_formato_propietario_python(datos: bytes) -> list[dict]:
    resultados = []
    offset = 0
    while offset < len(datos):
        magic = datos[offset:offset+4]
        if magic != b"PROP":
            break
        longitud = int.from_bytes(datos[offset+4:offset+8], "little")
        payload = datos[offset+8:offset+8+longitud]
        resultados.append({"magic": magic.decode(), "payload": payload.hex()})
        offset += 8 + longitud
    return resultados

# En Rust con PyO3 (10-100× más rápido para operaciones byte-level):
# (código Rust — para entender el patrón, no para ejecutar aquí)
# #[pyfunction]
# fn parse_formato_propietario(datos: &[u8]) -> PyResult<Vec<HashMap<String, String>>> {
#     // Implementación Rust con zero-copy cuando es posible
# }

# Cuándo vale la pena escribir una extensión Rust:
# 1. La operación es CPU-bound y Python puro es 100× más lento
# 2. No existe una librería Python/C que haga lo que necesitas
# 3. El volumen de datos es suficientemente grande para que importe
# 4. El equipo tiene (o puede adquirir) conocimiento de Rust
```

**Preguntas:**

1. ¿maturin y PyO3 son los únicos caminos para extensiones Rust en Python?
   ¿Qué otras opciones existen?

2. ¿Una extensión Rust en Python evita el GIL automáticamente?

3. ¿Cuándo CFFI (C Foreign Function Interface) es preferible a PyO3/Rust?

---

### Ejercicio 13.7.3 — El notebook de producción: anti-patrón y alternativas

```python
# ANTI-PATRÓN: notebooks de Jupyter en producción
# Muchos equipos ceden a la tentación de poner los notebooks de exploración
# en producción "porque ya funciona".

# Problemas:
# 1. Sin control de flujo claro (celda vs celda)
# 2. Estado implícito entre celdas (variable definida en celda 3, usada en celda 7)
# 3. Difícil de testear (no hay funciones claras con inputs/outputs)
# 4. Difícil de versionar (los .ipynb contienen outputs que hacen el diff ilegible)
# 5. No hay manejo de errores

# ALTERNATIVA: convertir el notebook a módulos Python con lógica clara:

# notebook_exploración.ipynb → pipeline/steps/calcular_revenue.py

def calcular_revenue_diario(
    df_ventas: pl.DataFrame,
    fecha: str,
) -> pl.DataFrame:
    """
    Calcula el revenue diario por región y segmento.
    
    Args:
        df_ventas: DataFrame con columnas [user_id, monto, region, timestamp]
        fecha: fecha del cálculo en formato YYYY-MM-DD
    
    Returns:
        DataFrame con columnas [region, segmento, revenue_total]
    """
    return df_ventas \
        .filter(pl.col("timestamp").dt.date() == pl.lit(fecha).str.to_date()) \
        .group_by(["region", "segmento"]) \
        .agg(pl.sum("monto").alias("revenue_total"))

# Con esta estructura, puedes:
# - Testear con unittest
# - Versionar en git con diffs legibles
# - Llamar desde Airflow con inputs/outputs claros
```

**Preguntas:**

1. ¿Databricks Notebooks o Jupyter para producción son siempre un anti-patrón?
   ¿Hay casos donde está justificado?

2. ¿Papermill (ejecutar notebooks parametrizados) es una solución aceptable?
   ¿Cuáles son sus limitaciones?

3. ¿Cómo testeas una función que toma un DataFrame de Spark como input?

---

### Ejercicio 13.7.4 — Python en el sistema de e-commerce: el pipeline completo

```python
# Integrar todo lo aprendido en un pipeline Python production-ready:

import polars as pl
from pyspark.sql import SparkSession, functions as F
import pyarrow as pa
import structlog
from dataclasses import dataclass
from typing import Optional
import time

log = structlog.get_logger()

@dataclass
class ConfigPipeline:
    fecha: str
    ruta_eventos: str
    ruta_clientes: str
    ruta_salida: str
    spark_partitions: int = 200
    polars_streaming: bool = False

def ejecutar_pipeline(config: ConfigPipeline) -> dict:
    """
    Pipeline de analytics diario del e-commerce.
    
    Usa Polars para procesar dimensiones (< 5 GB).
    Usa PySpark para unir con eventos (100+ GB).
    Usa Arrow para transferencia eficiente entre herramientas.
    """
    inicio = time.perf_counter()
    metricas = {}

    # PASO 1: Polars — preprocesar dimensiones (rápido, local)
    log.info("procesando_dimensiones")
    df_clientes = (
        pl.scan_parquet(config.ruta_clientes)
        .filter(pl.col("activo") == True)
        .with_columns([
            pl.col("nombre").str.to_lowercase(),
            pl.when(pl.col("gasto_historico") > 10_000)
              .then(pl.lit("premium"))
              .otherwise(pl.lit("standard"))
              .alias("segmento"),
        ])
        .collect(streaming=config.polars_streaming)
    )
    metricas["clientes_activos"] = len(df_clientes)

    # PASO 2: Transferir dimensiones a Spark via Arrow
    spark = SparkSession.builder \
        .config("spark.sql.shuffle.partitions", config.spark_partitions) \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()

    df_clientes_spark = spark.createDataFrame(df_clientes.to_pandas())
    df_clientes_spark.cache().count()  # materializar en memoria

    # PASO 3: PySpark — procesar eventos (100+ GB)
    log.info("procesando_eventos")
    df_resultado = (
        spark.read.parquet(f"{config.ruta_eventos}/fecha={config.fecha}/")
        .join(df_clientes_spark, on="user_id", how="left")
        .groupBy("region", "segmento")
        .agg(
            F.sum("monto").alias("revenue"),
            F.count("*").alias("transacciones"),
            F.countDistinct("user_id").alias("usuarios_unicos"),
        )
    )

    # PASO 4: Guardar resultados
    df_resultado.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("region") \
        .save(f"{config.ruta_salida}/fecha={config.fecha}/")

    metricas["duracion_total"] = time.perf_counter() - inicio
    return metricas
```

**Restricciones:**
1. Implementar el pipeline completo con manejo de errores y logging
2. Añadir validación del schema de los datos de entrada
3. Implementar la lógica de reintento si un paso falla

---

### Ejercicio 13.7.5 — El ecosystem fit: Python en el stack de 2024

**Tipo: Analizar**

Reflexionar sobre el rol de Python en data engineering en 2024:

```
Python domina porque:
  1. La API es simple y expresiva — exploración rápida
  2. El ecosistema de librerías es incomparable (PyData, ML, etc.)
  3. Las librerías de datos rápidas (Polars, Arrow) son accesibles en Python
  4. La mayoría de los data engineers y data scientists lo conocen

Python tiene limitaciones porque:
  1. El GIL limita el paralelismo de código Python puro
  2. Los UDFs Python en Spark son lentos (boundary JVM-Python)
  3. El tipo dinámico hace que los errores de schema sean runtime errors
  4. La gestión de dependencias y entornos es compleja

¿Cómo evolucionará?
  - Python 3.13+ con GIL opcional
  - Polars y Arrow eliminan la necesidad de código Python en loops críticos
  - Type hints + mypy se vuelven más comunes en pipelines de producción
  - La frontera Python/Rust se vuelve más fácil de cruzar (PyO3, maturin)
```

**Preguntas:**

1. ¿Python seguirá dominando data engineering en 5 años?
   ¿Qué lenguaje podría desplazarlo y bajo qué condiciones?

2. ¿La tendencia hacia Rust (Polars, DataFusion, Arrow2) en las librerías
   de base cambia el rol de Python?

3. ¿Para un nuevo data engineer hoy, ¿cuánto tiempo invertir en Python
   vs en Rust o Scala?

---

## Resumen del capítulo

**Python en data engineering: las tres capas**

```
Capa 1: Python como orquestador
  Construir planes de ejecución, conectar herramientas, manejar el flujo.
  El código Python no ejecuta en los datos — describe cómo procesarlos.
  → SparkSession, DataFrames de PySpark, LazyFrames de Polars

Capa 2: Python como lógica de negocio
  Las partes que necesitan lógica que no cabe en SQL/expresiones nativas.
  El código Python SÍ ejecuta en los datos — en batches (Pandas UDFs).
  → Pandas UDFs, ProcessFunction de Flink, UDFs vectorizadas

Capa 3: Python como interfaz a componentes rápidos
  NumPy, Polars, PyArrow — el código Python invoca Rust/C que hace el trabajo.
  Zero overhead en el loop interno.
  → Expresiones nativas de Polars, PyArrow compute, NumPy vectorized ops
```

**El principio de los UDFs Python en PySpark:**

```
Costo de una UDF Python =
  (número de filas) × (overhead de serialización por fila) +
  (número de batches) × (overhead de cruce JVM-Python por batch)

Para minimizar el costo:
  1. Eliminar la UDF: usar expresiones SQL/DataFrame nativas cuando sea posible
  2. Usar Pandas UDF: reduce el número de cruces JVM-Python (N filas → N/batch_size cruces)
  3. Usar Arrow UDF: reduce el overhead de serialización (Arrow vs pickle)
  4. Usar SQL nativo: cero cruces JVM-Python
```

**La conexión con el Cap.14 (Scala):**

> Python abstrae la JVM via Py4J — conveniente pero con overhead.
> Scala es la JVM — el mismo lenguaje en que está escrito Spark.
> Para los casos donde el overhead de Python importa (UDFs complejas, jobs muy largos),
> Scala elimina el boundary y da acceso directo a las APIs internas de Spark.
> El Cap.14 explora cuándo esa diferencia justifica aprender Scala.
