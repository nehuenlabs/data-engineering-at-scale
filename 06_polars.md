# Guía de Ejercicios — Cap.06: Polars — Paralelismo sin JVM

> Polars hace una apuesta distinta a Spark: en lugar de distribuir el
> procesamiento entre múltiples máquinas, maximiza el uso de una sola.
>
> En la práctica, la mayoría de los datasets de una empresa mediana
> caben en una máquina bien equipada. Para esos casos, Polars es
> frecuentemente 10-50× más rápido que Spark — sin cluster, sin JVM,
> sin overhead de shuffle.
>
> Pero el argumento no es "Polars reemplaza a Spark". Es entender cuándo
> cada herramienta es la respuesta correcta, y por qué.

---

## Por qué Polars es rápido: las tres razones

```
1. Rust + sin GIL
   Polars está escrito en Rust. No hay JVM que arrancar, no hay GC que pause,
   no hay GIL que serialice threads. Los cores trabajan en paralelo real.

2. Apache Arrow en memoria
   Polars usa Arrow como su representación interna. Todas las operaciones
   trabajan sobre buffers contiguos de memoria — SIMD natural, cache-friendly.
   Sin conversiones entre formatos al combinar con otras herramientas Arrow.

3. Lazy evaluation con optimización del plan
   Como Spark, Polars no ejecuta hasta que le pides el resultado (.collect()).
   El optimizador puede reordenar operaciones, eliminar columnas no usadas,
   y aplicar predicate pushdown — antes de tocar los datos.
```

```python
# La diferencia se siente en la primera línea:
import polars as pl

# Spark: arranca JVM, SparkSession, ejecutors...  (~5-10 segundos de overhead)
# Polars: sin overhead inicial — la primera operación tarda lo que tarda

df = pl.read_parquet("transacciones.parquet")  # listo en ~ms
resultado = df.filter(pl.col("monto") > 1000).group_by("region").agg(
    pl.sum("monto").alias("total")
)
# No ha ejecutado nada aún — es un plan lazy

resultado.collect()  # ahora ejecuta, en paralelo, sobre todos los cores
```

---

## Polars vs Pandas: el cambio de paradigma

Antes de los ejercicios, la comparación que más confunde a quienes vienen de Pandas:

```
Pandas:                              Polars:
  df["nueva"] = df["a"] * 2           df.with_columns(
                                         (pl.col("a") * 2).alias("nueva")
  df[df["a"] > 1]                      )
                                       df.filter(pl.col("a") > 1)

  df.groupby("x")["y"].sum()           df.group_by("x").agg(pl.sum("y"))

  df.apply(fn, axis=1)  ← LENTO        df.select(pl.struct(cols).map_elements(fn))
                                        o mejor: expresiones nativas sin apply
```

La diferencia conceptual: en Pandas, operas sobre Series (columnas) y puedes
referenciar el DataFrame desde afuera. En Polars, las operaciones son expresiones
(`pl.col(...)`) que se evalúan dentro del contexto del DataFrame — sin estado externo.

---

## Tabla de contenidos

- [Sección 6.1 — El modelo de ejecución de Polars](#sección-61--el-modelo-de-ejecución-de-polars)
- [Sección 6.2 — Expresiones: el lenguaje de Polars](#sección-62--expresiones-el-lenguaje-de-polars)
- [Sección-6.3 — Lazy API: planificar antes de ejecutar](#sección-63--lazy-api-planificar-antes-de-ejecutar)
- [Sección 6.4 — Joins y GroupBy: sin shuffle explícito](#sección-64--joins-y-groupby-sin-shuffle-explícito)
- [Sección 6.5 — Polars streaming: datos más grandes que la RAM](#sección-65--polars-streaming-datos-más-grandes-que-la-ram)
- [Sección 6.6 — Integración con el ecosistema Arrow](#sección-66--integración-con-el-ecosistema-arrow)
- [Sección 6.7 — Cuándo Polars y cuándo Spark](#sección-67--cuándo-polars-y-cuándo-spark)

---

## Sección 6.1 — El Modelo de Ejecución de Polars

### Ejercicio 6.1.1 — Leer: paralelismo sin coordinación de red

**Tipo: Leer/comparar**

Polars y Spark ejecutan el mismo GroupBy de forma muy diferente:

```
Spark GroupBy (distribuido):
  1. Cada executor procesa sus particiones → calcula parciales
  2. SHUFFLE: mover datos por la red para que cada clave quede en un nodo
  3. Cada executor recibe sus claves y calcula el resultado final
  Costo dominante: el shuffle (I/O de red)

Polars GroupBy (una máquina, múltiples cores):
  1. Fase de particionamiento: asigna filas a buckets en memoria
     (hash(clave) % num_buckets) — sin red, solo RAM
  2. Cada thread procesa su bucket en paralelo
  3. Merge final de los buckets en el thread principal
  Costo dominante: acceso a memoria (L3 cache, RAM)
```

```python
import polars as pl
import time

# Dataset: 50M filas, 4 columnas
df = pl.DataFrame({
    "user_id": pl.arange(0, 50_000_000, eager=True),
    "monto": pl.Series("monto", [1.0] * 50_000_000),
    "region": pl.Series("region",
        ["norte", "sur", "este", "oeste"] * 12_500_000),
    "mes": pl.Series("mes", list(range(1, 13)) * 4_166_666 + [1, 2]),
})

# GroupBy en Polars (paralelo, en memoria):
inicio = time.perf_counter()
resultado = df.group_by("region").agg(pl.sum("monto"))
print(f"Polars (lazy, no ejecutado): {time.perf_counter()-inicio:.4f}s")

inicio = time.perf_counter()
resultado = df.group_by("region").agg(pl.sum("monto"))
# collect() no es necesario aquí porque group_by es eager por defecto en Polars
print(f"Polars (eager group_by): {time.perf_counter()-inicio:.3f}s")
```

**Preguntas:**

1. ¿Por qué el GroupBy de Polars no necesita shuffle de red?
   ¿Qué mecanismo usa para coordinar entre threads?

2. ¿Cuántos threads usa Polars por defecto? ¿Cómo lo controlas?

3. Para un GroupBy con 4 claves únicas (como `region`), ¿cuántos buckets
   crea Polars internamente? ¿Tiene sentido usar todos los cores para 4 claves?

4. ¿El GroupBy de Polars escala linealmente con el número de cores?
   ¿Qué lo limita?

5. Si la máquina tiene 8 cores y el dataset tiene 50M filas,
   ¿cuántas filas procesa cada core en la fase de particionamiento?

**Pista:** Polars usa el número de cores físicos del sistema por defecto
(`os.cpu_count()`). Para un GroupBy con 4 claves, puede haber más buckets
que claves (para reducir contención en memoria), pero el merge final
colapsa a exactamente 4 grupos. El límite de escalado: para operaciones
muy rápidas (pocos datos), el overhead de crear y sincronizar threads supera
el beneficio — Polars tiene heurísticas para usar un solo thread en datasets pequeños.

---

### Ejercicio 6.1.2 — Medir: Polars vs Pandas vs Spark

```python
import polars as pl
import pandas as pd
import time

# Dataset de referencia: 10M filas
n = 10_000_000
data = {
    "id": list(range(n)),
    "monto": [float(i % 10000) for i in range(n)],
    "region": ["norte", "sur", "este", "oeste"][i % 4]
               if True else None for i in range(n),
    "mes": [i % 12 + 1 for i in range(n)],
}

df_pandas = pd.DataFrame(data)
df_polars = pl.DataFrame(data)

def medir(nombre, fn):
    inicio = time.perf_counter()
    resultado = fn()
    duracion = time.perf_counter() - inicio
    print(f"{nombre}: {duracion:.3f}s")
    return resultado

# Query 1: filtro + suma de columna
medir("Pandas  - filtro+suma",
    lambda: df_pandas[df_pandas["region"] == "norte"]["monto"].sum())
medir("Polars  - filtro+suma",
    lambda: df_polars.filter(pl.col("region") == "norte")["monto"].sum())

# Query 2: groupby + múltiples agregaciones
medir("Pandas  - groupby",
    lambda: df_pandas.groupby("region").agg({"monto": ["sum", "mean", "count"]}))
medir("Polars  - groupby",
    lambda: df_polars.group_by("region").agg([
        pl.sum("monto"), pl.mean("monto"), pl.count()
    ]))

# Query 3: join (requiere dos DataFrames)
df_dim_pandas = pd.DataFrame({"region": ["norte","sur","este","oeste"],
                               "factor": [1.1, 1.2, 1.3, 1.4]})
df_dim_polars = pl.DataFrame({"region": ["norte","sur","este","oeste"],
                               "factor": [1.1, 1.2, 1.3, 1.4]})
medir("Pandas  - join",
    lambda: df_pandas.merge(df_dim_pandas, on="region"))
medir("Polars  - join",
    lambda: df_polars.join(df_dim_polars, on="region"))

# Query 4: string operations
medir("Pandas  - str ops",
    lambda: df_pandas["region"].str.upper())
medir("Polars  - str ops",
    lambda: df_polars["region"].str.to_uppercase())
```

**Restricciones:**
1. Ejecutar y completar la tabla de tiempos
2. Calcular el speedup de Polars sobre Pandas para cada query
3. ¿Para cuál query la diferencia es menor? ¿Por qué?
4. ¿Qué pasa con el uso de memoria en cada caso?
5. Añadir Spark (local mode) a la comparación y completar la tabla

**Pista:** La diferencia de Polars sobre Pandas es más grande para operaciones
de groupby y join (estructuras de datos complejas donde el layout columnar
de Arrow brilla) y menor para operaciones simples de filtro o suma sobre
una sola columna (donde Pandas + NumPy también es eficiente).
Para strings, Polars es especialmente más rápido porque usa su propio
motor de strings construido sobre Arrow, mientras Pandas usa Python objects.

---

### Ejercicio 6.1.3 — El modelo de memoria: por qué Polars no tiene GC pauses

```python
# Polars está escrito en Rust — sin Garbage Collector.
# La memoria se libera determinísticamente cuando los objetos salen de scope.

import polars as pl
import psutil
import os

def uso_memoria_mb():
    proceso = psutil.Process(os.getpid())
    return proceso.memory_info().rss / 1_000_000

print(f"Antes: {uso_memoria_mb():.0f} MB")

# Crear un DataFrame grande:
df_grande = pl.DataFrame({
    "datos": list(range(5_000_000)),
    "mas_datos": [float(i) for i in range(5_000_000)],
})

print(f"Con DataFrame (5M filas): {uso_memoria_mb():.0f} MB")

# Cuando df_grande sale de scope (o se reasigna), la memoria se libera
# inmediatamente en Rust — sin esperar al GC de Python
del df_grande

print(f"Después de del: {uso_memoria_mb():.0f} MB")
# En Python, 'del' reduce el refcount. Si llega a 0, Rust libera la memoria.
# (Python sí tiene GC, pero el objeto de Rust se libera con el refcount)

# Comparar con Pandas (NumPy arrays también se liberan con refcount,
# pero los objetos Python internos de Pandas pueden ser más lentos de liberar):
import pandas as pd
import gc

df_pandas_grande = pd.DataFrame({
    "datos": range(5_000_000),
    "mas_datos": [float(i) for i in range(5_000_000)],
})
print(f"Con Pandas DataFrame: {uso_memoria_mb():.0f} MB")
del df_pandas_grande
gc.collect()  # forzar GC de Python
print(f"Después de del+gc: {uso_memoria_mb():.0f} MB")
```

**Preguntas:**

1. ¿Por qué los GC pauses de la JVM (en Spark) son un problema para
   los tiempos de respuesta (latencia P99)?

2. ¿Polars tiene GC pauses? ¿Por qué no?

3. ¿Cuándo puede Python (el wrapper de Polars) introducir pausas de GC
   aunque Rust no las tenga?

4. Para un job de Spark con GC time = 30%, ¿cuánto se aceleraría si se
   eliminasen las pauses? ¿Eso es realista?

5. ¿El modelo de memoria de Rust (ownership) garantiza ausencia de memory leaks?
   ¿Qué tipos de memory leaks son posibles de todas formas?

**Pista:** Polars no tiene GC pauses del lado de Rust porque el ownership
system de Rust garantiza que la memoria se libera cuando el propietario
sale de scope — sin necesidad de un GC. Sin embargo, el wrapper de Python
sí tiene el GC de Python para los objetos Python (el `pl.DataFrame` en Python
es un objeto Python con una referencia al objeto Rust subyacente). Las pausas
de GC de Python son mucho más cortas que las de JVM y menos frecuentes,
porque Python no tiene que trazar referencias en un heap grande de objetos.

---

### Ejercicio 6.1.4 — Leer: cuándo Polars usa un solo thread

**Tipo: Diagnosticar**

Un data engineer reporta que Polars "no usa todos los cores":

```python
import polars as pl
import psutil

# Observar el uso de CPU mientras ejecuta:
print(f"Cores disponibles: {psutil.cpu_count()}")  # 16 cores

# Este query usa todos los cores:
df.group_by("region").agg(pl.sum("monto"))

# Este query usa un solo core:
df.select(pl.col("region").unique())

# Este query es complicado:
df.sort("timestamp")
```

**Preguntas:**

1. ¿Por qué `unique()` puede usar solo un thread?

2. ¿Por qué `sort()` tiene comportamiento variable en paralelismo?

3. Polars tiene una configuración `POLARS_MAX_THREADS`. Si la estableces en 1,
   ¿todos los queries se vuelven single-threaded?

4. ¿Hay casos donde single-thread en Polars es más rápido que multi-thread?

5. ¿Cómo verificas en código cuántos threads usa Polars para una operación dada?

**Pista:** Polars paralleliza internamente según la operación:
- `group_by`: altamente paralelizable (hash partitioning paralelo)
- `sort`: paralelo en la fase de particionamiento, merge-sort final puede
  tener una fase secuencial para garantizar orden global
- `unique()`: puede ser paralelo (hash-based) o secuencial dependiendo
  de si el resultado debe estar ordenado
- Operaciones de IO (read_parquet): paralelo entre archivos, potencialmente
  paralelo dentro de un archivo si tiene múltiples row groups

---

### Ejercicio 6.1.5 — Implementar: medir el speedup real de paralelismo en Polars

```python
import polars as pl
import os
import time

def benchmark_paralelismo(n_filas: int = 10_000_000):
    """
    Mide el speedup de Polars según el número de threads.
    """
    df = pl.DataFrame({
        "x": pl.arange(0, n_filas, eager=True),
        "y": pl.Series([float(i % 1000) for i in range(n_filas)]),
        "cat": pl.Series(["a", "b", "c", "d"] * (n_filas // 4)),
    })

    resultados = {}
    for n_threads in [1, 2, 4, 8, 16]:
        os.environ["POLARS_MAX_THREADS"] = str(n_threads)
        # Nota: cambiar POLARS_MAX_THREADS en runtime no siempre funciona.
        # Para un benchmark real, relanzar el proceso con la variable establecida.

        inicio = time.perf_counter()
        _ = df.group_by("cat").agg([
            pl.sum("y"),
            pl.mean("y"),
            pl.std("y"),
            pl.min("x"),
            pl.max("x"),
        ])
        resultados[n_threads] = time.perf_counter() - inicio

    # Calcular speedup:
    baseline = resultados[1]
    for threads, tiempo in resultados.items():
        speedup = baseline / tiempo
        eficiencia = speedup / threads * 100
        print(f"{threads:2d} threads: {tiempo:.3f}s  speedup={speedup:.1f}×  eficiencia={eficiencia:.0f}%")

    return resultados
```

**Restricciones:**
1. Ejecutar el benchmark (requiere relanzar el proceso por cada configuración de threads)
2. Calcular la eficiencia de paralelismo (speedup / threads × 100%)
3. ¿La eficiencia es constante o decrece con más threads? ¿Por qué?
4. ¿Qué operación escala mejor? ¿Cuál peor?

---

## Sección 6.2 — Expresiones: el Lenguaje de Polars

Las expresiones (`pl.col(...)`, `pl.lit(...)`, `pl.when(...)`) son el
corazón de Polars. Son objetos lazy — describen una computación
sin ejecutarla — y se componen para construir planes complejos.

### Ejercicio 6.2.1 — Expresiones básicas: col, lit, when, alias

```python
import polars as pl

df = pl.DataFrame({
    "id": [1, 2, 3, 4, 5],
    "monto": [100.0, 250.0, 50.0, 1500.0, 75.0],
    "region": ["norte", "sur", "norte", "este", "sur"],
    "activo": [True, True, False, True, False],
})

# col: referenciar una columna
expr_monto = pl.col("monto")

# lit: un valor literal
expr_iva = pl.lit(1.19)

# Operaciones aritméticas sobre expresiones:
expr_con_iva = pl.col("monto") * pl.lit(1.19)

# when/then/otherwise: condicional vectorizado
expr_categoria = (
    pl.when(pl.col("monto") > 1000)
    .then(pl.lit("premium"))
    .when(pl.col("monto") > 200)
    .then(pl.lit("standard"))
    .otherwise(pl.lit("basico"))
).alias("categoria")

# Usar expresiones en select/with_columns:
df_transformado = df.select([
    pl.col("id"),
    expr_con_iva.alias("monto_con_iva"),
    expr_categoria,
    pl.col("region").str.to_uppercase().alias("region_upper"),
    (~pl.col("activo")).alias("inactivo"),  # NOT booleano
])

print(df_transformado)
```

**Restricciones:**
1. Añadir una expresión que calcula `descuento_aplicado`:
   si `activo=True` → `monto * 0.9`, si `activo=False` → `monto`
2. Añadir una expresión que extrae el primer carácter de `region`
3. ¿Cuántas veces se lee la columna `monto` en el plan si usas
   `pl.col("monto")` tres veces en el mismo `select()`?
4. ¿Qué error produce `pl.col("columna_que_no_existe")` y cuándo se detecta?

**Pista:** Polars optimiza el plan y no lee la columna `monto` tres veces —
lee el buffer de Arrow una vez y aplica las tres expresiones sobre él.
Esta es una de las ventajas del modelo de expresiones lazy: el optimizador
puede eliminar lecturas duplicadas. El error por columna inexistente se
lanza en tiempo de ejecución (al llamar `.collect()` en lazy o al ejecutar
la operación en eager) — no en tiempo de compilación.

---

### Ejercicio 6.2.2 — Expresiones avanzadas: over, cumsum, rolling

```python
import polars as pl

df = pl.DataFrame({
    "fecha": pl.date_range(
        start=pl.date(2024, 1, 1),
        end=pl.date(2024, 1, 31),
        interval="1d",
        eager=True,
    ),
    "region": ["norte", "sur"] * 15 + ["norte"],
    "monto": [float(i * 10) for i in range(1, 32)],
})

# over(): equivale a OVER PARTITION BY en SQL (window function)
# Calcula el monto máximo POR región (sin groupby que reduce filas)
df_con_max = df.with_columns(
    pl.col("monto").max().over("region").alias("max_por_region"),
    pl.col("monto").rank("dense").over("region").alias("rank_en_region"),
    pl.col("monto").sum().over("region").alias("suma_region"),
)

# cumsum: suma acumulada
df_con_cumsum = df.with_columns(
    pl.col("monto").cum_sum().alias("monto_acumulado"),
    pl.col("monto").cum_sum().over("region").alias("monto_acum_por_region"),
)

# rolling: media móvil
df_con_rolling = df.with_columns(
    pl.col("monto").rolling_mean(window_size=7).alias("media_movil_7d"),
    pl.col("monto")
      .rolling_mean(window_size=7)
      .over("region")
      .alias("media_movil_7d_por_region"),
)
```

**Preguntas:**

1. ¿Cuál es la diferencia entre `group_by("region").agg(pl.sum("monto"))`
   y `with_columns(pl.col("monto").sum().over("region"))`?
   ¿Cuántas filas tiene el resultado de cada uno?

2. `.over()` es una window function. ¿Hace un "shuffle" interno en Polars?
   ¿Cómo gestiona los grupos?

3. ¿`rolling_mean` puede tener valores nulos al principio? ¿Por qué?
   ¿Cómo manejarlos?

4. Implementar "ratio de monto sobre el total de su región" en una sola
   expresión usando `over()`.

5. ¿Las window functions de Polars son equivalentes a las de Spark SQL?
   ¿Hay diferencias de semántica?

**Pista:** `group_by().agg()` reduce el DataFrame: si hay 4 regiones,
el resultado tiene 4 filas. `.over()` no reduce: el resultado mantiene
todas las filas originales, pero añade una columna calculada por grupo.
Es el equivalente de SQL `SUM(monto) OVER (PARTITION BY region)` — el DataFrame
tiene las mismas filas pero con el total de la región repetido en cada fila.

---

### Ejercicio 6.2.3 — Expresiones sobre listas y structs

```python
import polars as pl

# Polars soporta columnas de tipo List y Struct (datos anidados):
df = pl.DataFrame({
    "user_id": [1, 2, 3],
    "eventos": [
        ["click", "view", "purchase"],
        ["view", "click"],
        ["purchase", "purchase", "view", "click"],
    ],
    "metadata": [
        {"canal": "web", "dispositivo": "laptop"},
        {"canal": "mobile", "dispositivo": "iphone"},
        {"canal": "web", "dispositivo": "desktop"},
    ],
})

# Operaciones sobre listas:
df_con_lista = df.with_columns([
    pl.col("eventos").list.len().alias("num_eventos"),
    pl.col("eventos").list.contains("purchase").alias("tiene_compra"),
    pl.col("eventos").list.first().alias("primer_evento"),
    pl.col("eventos").list.eval(
        pl.element().filter(pl.element() == "purchase").len()
    ).list.first().alias("num_compras"),
])

# Unnesting: explotar la lista en filas separadas
df_exploded = df.explode("eventos")

# Operaciones sobre structs:
df_con_struct = df.with_columns([
    pl.col("metadata").struct.field("canal").alias("canal"),
    pl.col("metadata").struct.field("dispositivo").alias("dispositivo"),
])
```

**Restricciones:**
1. Calcular la secuencia de eventos más frecuente (par de eventos consecutivos)
2. Filtrar usuarios que tienen al menos 2 "purchase" en su lista de eventos
3. Implementar "conversión de click a purchase": usuarios con "click" Y "purchase"
   como porcentaje del total con "click"
4. Comparar la velocidad de `.explode()` + `group_by` vs `.list.eval()` para
   el mismo cálculo

**Pista:** Para calcular pares consecutivos:
```python
df.with_columns(
    pl.col("eventos").list.eval(
        pl.concat_list([
            pl.element().slice(0, pl.element().len() - 1),
            pl.element().slice(1)
        ]).list.first()  # no exactamente — hay que usar zip o similar
    )
)
```
La solución más limpia: `.explode()` + `.shift()` dentro de un `group_by`.
El tradeoff: `.list.eval()` mantiene el dato anidado; `.explode()` lo aplana
pero permite usar todas las expresiones de columna estándar.

---

### Ejercicio 6.2.4 — El reemplazo de apply/map en Polars

```python
import polars as pl

# En Pandas, apply() es el "martillo de todo":
# df["nueva"] = df.apply(lambda row: fn(row["a"], row["b"]), axis=1)
# → lento porque itera fila a fila con Python

# En Polars, la alternativa casi siempre es una expresión nativa:

df = pl.DataFrame({
    "precio": [100.0, 250.0, 50.0, 1500.0],
    "cantidad": [2, 1, 5, 1],
    "descuento": [0.1, 0.0, 0.2, 0.05],
})

# MAL: map_elements (equivalente a apply, lento — fila por fila en Python)
resultado_lento = df.with_columns(
    pl.struct(["precio", "cantidad", "descuento"]).map_elements(
        lambda row: row["precio"] * row["cantidad"] * (1 - row["descuento"]),
        return_dtype=pl.Float64,
    ).alias("total_con_descuento")
)

# BIEN: expresión nativa (vectorizado, SIMD)
resultado_rapido = df.with_columns(
    (pl.col("precio") * pl.col("cantidad") * (1 - pl.col("descuento")))
    .alias("total_con_descuento")
)

# Casos donde map_elements es inevitable:
# - Lógica compleja que no se puede expresar con las funciones nativas
# - Llamadas a APIs externas (HTTP, base de datos)
# - Algoritmos con estado entre filas (no vectorizables)
```

**Restricciones:**
1. Medir la diferencia de velocidad entre `map_elements` y la expresión nativa
   para 1M filas
2. Identificar 5 operaciones que parecen requerir `map_elements` pero tienen
   equivalente nativo
3. Implementar una función que aplica un modelo de ML (sklearn) sobre
   columnas de Polars de forma eficiente

**Pista:** Para el modelo de sklearn, la solución eficiente no es `map_elements`
por cada fila — es convertir las columnas relevantes a numpy con `.to_numpy()`,
aplicar el modelo vectorizado (`modelo.predict(X)`), y asignar el resultado
de vuelta como columna. Esto es O(n) en numpy/sklearn sin iterar en Python.
El truco: `pl.Series(modelo.predict(df.select(features).to_numpy()))`.

---

### Ejercicio 6.2.5 — Leer: debuggear expresiones complejas

**Tipo: Diagnosticar**

Una expresión produce un resultado inesperado:

```python
import polars as pl

df = pl.DataFrame({
    "monto": [100.0, None, 300.0, None, 500.0],
    "region": ["norte", "norte", "sur", "sur", "norte"],
})

# El ingeniero quiere: suma de monto por región, ignorando nulls
resultado = df.group_by("region").agg(
    pl.col("monto").fill_null(0).sum().alias("total")
)

# Resultado esperado: norte=600, sur=300
# Resultado obtenido: norte=600, sur=300  ← correcto aquí, pero...

# Versión 2 con lógica diferente:
resultado_2 = df.group_by("region").agg(
    pl.col("monto").sum().fill_null(0).alias("total")
)
# ¿Es igual? ¿Cuándo no sería igual?

# Versión 3:
resultado_3 = df.with_columns(
    pl.col("monto").fill_null(0)
).group_by("region").agg(
    pl.col("monto").sum().alias("total")
)
# ¿Es igual a la versión 1?
```

**Preguntas:**

1. ¿`fill_null(0).sum()` y `sum().fill_null(0)` producen siempre el mismo resultado?
   Da un contraejemplo donde difieran.

2. ¿Cómo maneja Polars los nulls en `sum()` por defecto?

3. ¿La Versión 3 es equivalente a la Versión 1? ¿Qué diferencia hay?

4. Si quieres la media ignorando nulls vs incluyendo nulls como 0,
   ¿cómo afecta el resultado?

5. ¿Existe una forma de ver el plan de ejecución de una expresión de Polars
   (equivalente al `explain()` de Spark)?

**Pista:** `sum()` en Polars ignora nulls por defecto — `[1, null, 3].sum() = 4`.
Entonces `fill_null(0).sum()` = `[1, 0, 3].sum() = 4` — mismo resultado.
La diferencia aparece con `mean()`: `[1, null, 3].mean() = 2` (ignora null)
vs `[1, 0, 3].mean() = 1.33` (incluye el 0). El plan lazy se puede ver
con `df.lazy().group_by(...).agg(...).explain()`.

---

## Sección 6.3 — Lazy API: Planificar Antes de Ejecutar

### Ejercicio 6.3.1 — Eager vs Lazy: cuándo usar cada una

```python
import polars as pl

# API Eager (default en Polars para operaciones de DataFrame):
# Ejecuta inmediatamente. Útil para exploración interactiva.
df = pl.read_parquet("datos.parquet")
resultado = df.filter(pl.col("monto") > 100)  # ejecuta ahora
print(resultado)  # ya tiene el resultado

# API Lazy: construye un plan, ejecuta al final
lf = pl.scan_parquet("datos.parquet")  # scan, no read — no carga en memoria
plan = lf.filter(pl.col("monto") > 100).select(["id", "monto"])
# Nada ha ejecutado aún

plan.explain()  # ver el plan optimizado
resultado = plan.collect()  # ejecutar y materializar

# La diferencia crítica: scan_parquet con lazy API hace predicate pushdown
# automáticamente — solo lee las columnas y filas necesarias del Parquet
```

```
Sin lazy (eager):
  read_parquet → carga TODO en memoria → filter → select
  I/O: leer 10 GB completos
  Memoria pico: 10 GB

Con lazy (scan + collect):
  scan_parquet → (plan) → filter pushdown a Parquet → select de columnas
  I/O: leer solo las columnas y row groups que pasan el filtro → ~500 MB
  Memoria pico: 500 MB + resultado
```

**Preguntas:**

1. ¿`pl.read_parquet()` carga todo el archivo en memoria? ¿Y `pl.scan_parquet()`?

2. ¿Por qué el predicate pushdown funciona con `scan_parquet` pero no con `read_parquet`?

3. ¿Cuándo la API eager es preferible a la lazy?

4. ¿Qué pasa si llamas a `.collect()` dos veces sobre el mismo LazyFrame?

5. ¿Puedes mezclar DataFrames eager con LazyFrames en el mismo pipeline?

**Pista:** `pl.scan_parquet()` no lee nada — registra la fuente de datos.
El plan de ejecución puede entonces "bajar" el filtro hasta la capa de
lectura del Parquet y usar las estadísticas de row groups para saltar
los que no contienen datos relevantes. `pl.read_parquet()` ya leyó todo
antes de que el filtro tenga oportunidad de reducir el I/O.
Llamar `.collect()` dos veces re-ejecuta el plan — si quieres reutilizar
el resultado, almacenarlo en un DataFrame: `df = lf.collect()`.

---

### Ejercicio 6.3.2 — El optimizador de Polars en acción

```python
import polars as pl

# Plan sin optimizar vs optimizado:
lf = pl.scan_parquet("transacciones.parquet")

plan_sin_opt = (lf
    .select(["id", "monto", "region", "timestamp",
             "user_id", "producto_id"])  # select de 6 columnas
    .filter(pl.col("region") == "norte")  # filtro
    .select(["id", "monto"])             # luego seleccionamos solo 2
)

# El optimizador de Polars:
# 1. Projection pushdown: elimina columnas innecesarias antes del filtro
#    (solo necesita "region", "id", "monto" para el plan completo)
# 2. Predicate pushdown: baja el filtro de region al nivel del scan

plan_sin_opt.explain(optimized=False)  # plan sin optimizar
plan_sin_opt.explain(optimized=True)   # plan optimizado

# ¿Son iguales los dos plans?
```

**Restricciones:**
1. Comparar los dos planes y documentar qué optimizaciones aplicó Polars
2. Crear un pipeline donde la optimización tiene un impacto medible en tiempo
3. ¿Hay casos donde el plan optimizado es peor que el sin optimizar?

---

### Ejercicio 6.3.3 — Streaming lazy: datos más grandes que el plan

```python
import polars as pl

# Polars Streaming: ejecutar el plan en chunks para datasets que no caben en RAM
# IMPORTANTE: no es streaming de eventos (como Kafka) — es procesamiento
# de archivos grandes en batches para reducir uso de memoria pico

# Activar el modo streaming:
resultado = (
    pl.scan_parquet("datos_grandes/*.parquet")  # múltiples archivos
    .filter(pl.col("monto") > 100)
    .group_by("region")
    .agg(pl.sum("monto"))
    .collect(streaming=True)  # ← activar streaming mode
)

# Sin streaming=True: Polars carga todo en memoria antes de agrupar
# Con streaming=True: procesa en chunks, usa mucho menos memoria pico

# Verificar que el plan soporta streaming:
lf = pl.scan_parquet("datos_grandes/*.parquet") \
    .filter(pl.col("monto") > 100) \
    .group_by("region") \
    .agg(pl.sum("monto"))

lf.explain(streaming=True)
# Si el plan muestra "[STREAMING]", la operación es compatible
# Si alguna operación no es compatible, Polars vuelve al modo no-streaming
```

**Preguntas:**

1. ¿Todas las operaciones de Polars soportan el modo streaming?
   ¿Cuáles no?

2. ¿El resultado de `collect(streaming=True)` es idéntico a `collect()`?
   ¿O puede variar por el orden?

3. ¿Cuándo el modo streaming de Polars es preferible a Spark?
   ¿Cuándo Spark sigue siendo mejor?

4. ¿El modo streaming de Polars puede usar múltiples cores?

**Pista:** Las operaciones que no soportan streaming (aún en 2024):
`sort()` global (requiere todos los datos para ordenar), `join()` de tipo
right/full outer con ambas tablas muy grandes, y algunas operaciones de ventana
complejas. Polars detecta automáticamente qué partes del plan pueden ejecutarse
en streaming y cuáles no, y aplica streaming solo donde es posible.
El resultado es idéntico siempre que no haya dependencia de orden.

---

### Ejercicio 6.3.4 — Leer: el pipeline que no se optimizó correctamente

**Tipo: Diagnosticar**

Un pipeline de Polars tarda 45 segundos para 2 GB de datos.
El ingeniero asegura que "usó la API lazy":

```python
import polars as pl

df = pl.read_parquet("datos.parquet")  # ← carga 2 GB en memoria

resultado = (df
    .lazy()                                    # ← convierte a lazy después de cargar
    .filter(pl.col("region") == "norte")
    .select(["id", "monto", "region"])
    .collect()
)
```

**Preguntas:**

1. ¿Qué error cometió el ingeniero en el uso de la API lazy?

2. ¿El predicate pushdown se aplica en este pipeline? ¿Por qué?

3. ¿Cuántos GB de datos lee el pipeline del disco?

4. Reescribir el pipeline correctamente y estimar el speedup.

5. ¿Hay algún caso donde `df.lazy()` (convertir un DataFrame eager a lazy)
   sea útil?

**Pista:** El error: `pl.read_parquet()` carga TODO el archivo en memoria
antes de llamar a `.lazy()`. La conversión eager→lazy no hace "un-load"
de la memoria — el DataFrame ya está materializado. Para que el predicate
pushdown y el projection pushdown funcionen, hay que empezar con
`pl.scan_parquet()` que no carga nada. La regla: si el objetivo es eficiencia,
siempre comenzar con `scan_*` y terminar con `.collect()`.

---

### Ejercicio 6.3.5 — Implementar: un pipeline lazy completo para el sistema de e-commerce

```python
import polars as pl
from pathlib import Path

def calcular_metricas_diarias(
    ruta_eventos: str,
    ruta_clientes: str,
    ruta_productos: str,
    fecha: str,  # "2024-01-15"
) -> dict[str, pl.DataFrame]:
    """
    Calcula métricas diarias usando Polars con lazy API y streaming.
    
    Diseñado para datasets de hasta ~100 GB en una sola máquina.
    """
    # Fuentes lazy (nada se carga aún):
    eventos = pl.scan_parquet(f"{ruta_eventos}/fecha={fecha}/*.parquet")
    clientes = pl.scan_parquet(f"{ruta_clientes}/*.parquet")
    productos = pl.scan_parquet(f"{ruta_productos}/*.parquet")

    # Filtrar solo los eventos relevantes (predicate pushdown):
    eventos_filtrados = eventos.filter(
        pl.col("tipo").is_in(["click", "compra", "vista"])
    )

    # TODO: implementar los siguientes cálculos como LazyFrames
    # (no llamar a .collect() hasta el final):
    
    # 1. Revenue por región (solo compras)
    revenue_por_region = ...

    # 2. Tasa de conversión por producto (click → compra)
    conversion = ...

    # 3. Top 10 productos por revenue
    top_productos = ...

    # Ejecutar todo en paralelo (Polars puede optimizar múltiples LazyFrames):
    resultados = pl.collect_all([revenue_por_region, conversion, top_productos])

    return {
        "revenue_por_region": resultados[0],
        "conversion": resultados[1],
        "top_productos": resultados[2],
    }
```

**Restricciones:**
1. Implementar los tres cálculos como LazyFrames
2. Usar `pl.collect_all()` para ejecutarlos en paralelo
3. Verificar con `explain()` que el predicate pushdown está activo
4. Medir el tiempo vs la versión que hace `read_parquet` al inicio

---

## Sección 6.4 — Joins y GroupBy: sin Shuffle Explícito

### Ejercicio 6.4.1 — Tipos de join en Polars

```python
import polars as pl

df_izq = pl.DataFrame({
    "id": [1, 2, 3, 4],
    "valor": [10, 20, 30, 40],
})

df_der = pl.DataFrame({
    "id": [2, 3, 5],
    "extra": ["a", "b", "c"],
})

# Inner join (solo los que están en ambos):
inner = df_izq.join(df_der, on="id", how="inner")
# id: [2, 3]

# Left join (todos los de izquierda):
left = df_izq.join(df_der, on="id", how="left")
# id: [1, 2, 3, 4] — extra es null para 1 y 4

# Full outer join:
outer = df_izq.join(df_der, on="id", how="full")
# id: [1, 2, 3, 4, 5]

# Semi join (filtrar izquierda por existencia en derecha):
semi = df_izq.join(df_der, on="id", how="semi")
# id: [2, 3] — sin columnas de df_der

# Anti join (filtrar izquierda por ausencia en derecha):
anti = df_izq.join(df_der, on="id", how="anti")
# id: [1, 4]

# Cross join (producto cartesiano):
cross = df_izq.join(df_der, how="cross")
# 4 × 3 = 12 filas
```

**Preguntas:**

1. ¿Qué algoritmo usa Polars para el join internamente?
   ¿Hash join, sort-merge join, o depende del tamaño?

2. ¿Por qué el semi join y el anti join son útiles en data engineering?
   Da un ejemplo real para cada uno.

3. ¿El join de Polars puede spill a disco si los datos son muy grandes
   para la memoria?

4. ¿Polars hace broadcast automáticamente para la tabla pequeña en un join?

5. ¿Hay diferencia de rendimiento entre `join(how="left")` y
   `join(how="right")` si los DataFrames son del mismo tamaño?

**Pista:** Polars usa hash join para la mayoría de los casos: construye
una hash table del DataFrame más pequeño (la "build side") y hace probe
sobre el más grande (la "probe side"). Cuando ambos DataFrames son grandes,
el hash table puede ser costoso en memoria — Polars puede hacer spill
a disco en modo streaming pero no en modo eager por defecto.
El broadcast automático: Polars analiza el tamaño relativo de los DataFrames
y construye la hash table sobre el más pequeño, similar al broadcast join de Spark.

---

### Ejercicio 6.4.2 — GroupBy avanzado: aggregations múltiples eficientes

```python
import polars as pl

df = pl.DataFrame({
    "user_id": [1, 1, 2, 2, 3],
    "producto_id": [10, 20, 10, 30, 20],
    "monto": [100.0, 200.0, 150.0, 50.0, 300.0],
    "tipo": ["compra", "compra", "devolucion", "compra", "compra"],
    "fecha": pl.date_range(
        start=pl.date(2024, 1, 1), periods=5, interval="1d", eager=True
    ),
})

# Múltiples agregaciones en un solo group_by (una sola pasada sobre los datos):
resultado = df.group_by("user_id").agg([
    pl.count().alias("num_transacciones"),
    pl.sum("monto").alias("revenue_total"),
    pl.mean("monto").alias("ticket_promedio"),
    pl.std("monto").alias("desviacion_monto"),
    pl.col("producto_id").n_unique().alias("productos_distintos"),
    pl.col("tipo").filter(pl.col("tipo") == "compra").count().alias("compras"),
    pl.col("tipo").filter(pl.col("tipo") == "devolucion").count().alias("devoluciones"),
    pl.col("fecha").min().alias("primera_compra"),
    pl.col("fecha").max().alias("ultima_compra"),
    pl.col("monto").top_k(3).alias("top3_montos"),  # lista de top 3
])

# group_by dinámico: agrupar por múltiples columnas
resultado_multi = df.group_by(["user_id", "tipo"]).agg(
    pl.sum("monto").alias("total")
)
```

**Restricciones:**
1. Ejecutar y verificar el resultado
2. ¿Cuántas pasadas sobre los datos hace Polars para calcular todas las agregaciones?
3. Añadir una agregación "personalizada": la ratio de devoluciones sobre compras
4. Comparar el tiempo de estas 9 agregaciones en Polars vs Pandas vs Spark SQL

**Pista:** Polars calcula todas las agregaciones en **una sola pasada** sobre
los datos — es una de sus fortalezas principales. En Pandas, algunas combinaciones
de agregaciones requieren múltiples pasadas (por ejemplo, si combinas `.agg()` con
operaciones de filtro por grupo). En Spark SQL, el optimizer también colapsa
múltiples agregaciones en una sola pasada (HashAggregate con múltiples funciones).

---

### Ejercicio 6.4.3 — Join con condiciones complejas (non-equi join)

```python
import polars as pl

# Polars 0.20+ soporta joins con condiciones no-equi:
df_transacciones = pl.DataFrame({
    "id": [1, 2, 3, 4, 5],
    "monto": [100.0, 500.0, 150.0, 2000.0, 50.0],
    "fecha": pl.date_range(
        start=pl.date(2024, 1, 1), periods=5, interval="1d", eager=True
    ),
})

df_rangos_descuento = pl.DataFrame({
    "monto_min": [0.0, 100.0, 500.0, 1000.0],
    "monto_max": [100.0, 500.0, 1000.0, float("inf")],
    "descuento": [0.0, 0.05, 0.10, 0.20],
})

# Join por rango (non-equi):
resultado = df_transacciones.join_where(
    df_rangos_descuento,
    pl.col("monto") >= pl.col("monto_min"),
    pl.col("monto") < pl.col("monto_max"),
)
```

**Preguntas:**

1. ¿Qué algoritmo usa Polars para el join non-equi?
   ¿Por qué es más caro que el equi-join?

2. ¿Este tipo de join es posible en Pandas directamente?
   ¿Cómo lo resolverías en Pandas?

3. ¿Cuándo es preferible un join non-equi sobre un `when/then` en una columna?

4. ¿El join non-equi soporta el modo streaming de Polars?

---

### Ejercicio 6.4.4 — Optimización de joins: order matters

```python
import polars as pl

# Dataset: 10M transacciones, 100K usuarios, 10K productos
df_trans = pl.scan_parquet("transacciones.parquet")  # 10M filas
df_users = pl.scan_parquet("usuarios.parquet")        # 100K filas
df_prods = pl.scan_parquet("productos.parquet")       # 10K filas

# Versión A: join en orden subóptimo
plan_a = (df_trans
    .join(df_users, on="user_id")      # 10M × 100K → resultado: 10M
    .join(df_prods, on="producto_id")  # 10M × 10K → resultado: 10M
    .filter(pl.col("activo") == True)  # filtro al final
    .filter(pl.col("categoria") == "electronico")
)

# Versión B: filtros primero, luego joins
plan_b = (df_trans
    .filter(pl.col("monto") > 100)    # elimina ~70% de transacciones
    .join(
        df_prods.filter(pl.col("categoria") == "electronico"),
        on="producto_id"
    )
    .join(
        df_users.filter(pl.col("activo") == True),
        on="user_id"
    )
)
```

**Restricciones:**
1. Ejecutar ambos planes y medir el tiempo
2. Ver los planes con `.explain()` — ¿Polars optimiza la Versión A automáticamente?
3. ¿El optimizador de Polars reordena los joins automáticamente?
4. Si el optimizador no lo hace, ¿cuánto speedup da ordenar manualmente?

---

### Ejercicio 6.4.5 — Leer: diagnosticar un GroupBy lento en Polars

**Tipo: Diagnosticar**

Un pipeline de Polars tarda 8 minutos para 500M filas. El profiling muestra:

```
group_by("session_id"):          7m 45s  ← 97% del tiempo
  hash table construction:       3m 20s
  data partitioning:             2m 15s
  aggregation:                   2m 10s

filter + select:                 15s
write_parquet:                   0m 0s (nada escrito todavía)
```

El schema:
```
session_id: String  ← UUIDs únicos por sesión (500M valores distintos!)
evento: String
timestamp: Datetime
monto: Float64
```

**Preguntas:**

1. ¿Por qué el GroupBy tarda tanto si es Polars y no Spark?

2. `session_id` tiene 500M valores distintos. ¿Qué implica eso para
   la hash table de Polars?

3. ¿Cuánta memoria ocupa la hash table para 500M UUIDs?
   (asumiendo UUID de 36 chars + overhead de hash table)

4. ¿Es este un buen caso de uso para Polars? ¿O debería usarse Spark?

5. ¿Cómo rediseñarías el pipeline para evitar el GroupBy con
   alta cardinalidad?

**Pista:** 500M claves distintas en un GroupBy: la hash table necesita
espacio para 500M entradas. Cada entrada en una hash table de Polars
ocupa aproximadamente 32-64 bytes (clave + valor + overhead). Con 500M entradas:
500M × 48 bytes ≈ 24 GB de RAM solo para la hash table. Si la máquina tiene
32 GB, la hash table ocupa el 75% de la RAM disponible, causando presión de memoria
y potencial swap. Este es el límite de Polars: GroupBy con cardinalidad extrema
sobre datasets muy grandes supera lo que una máquina puede manejar eficientemente.

---

## Sección 6.5 — Polars Streaming: Datos más Grandes que la RAM

### Ejercicio 6.5.1 — Cuándo usar el modo streaming

```python
import polars as pl

# El modo streaming de Polars NO es lo mismo que Kafka/Flink streaming.
# Es un modo de ejecución que procesa los datos en chunks para reducir
# el uso de memoria pico.

# Caso 1: archivo de 200 GB en una máquina con 32 GB RAM
# Sin streaming: OOM (intenta cargar 200 GB en RAM)
# Con streaming: procesa en chunks de ~1 GB, usa ~2 GB de RAM pico

resultado = (
    pl.scan_parquet("archivo_enorme.parquet")
    .filter(pl.col("año") == 2024)
    .group_by("region")
    .agg(pl.sum("monto"))
    .collect(streaming=True)
)

# Caso 2: múltiples archivos que juntos superan la RAM
resultado = (
    pl.scan_parquet("datos/*.parquet")  # puede ser 500 archivos × 500 MB
    .filter(pl.col("activo") == True)
    .select(["id", "region", "monto"])
    .collect(streaming=True)
)
```

**Preguntas:**

1. ¿El modo streaming de Polars puede procesar datos de una fuente
   que crece continuamente (como Kafka)? ¿Por qué no?

2. ¿Qué operaciones de Polars NO soportan el modo streaming?
   ¿Por qué `sort()` global no es compatible?

3. Si el chunk size del streaming es 1 GB y tienes 200 GB de datos,
   ¿cuántos chunks procesa Polars?

4. ¿El modo streaming es siempre más lento que el modo normal?
   ¿Cuándo puede ser más rápido?

5. ¿Cómo determina Polars el tamaño del chunk en streaming?
   ¿Es configurable?

**Pista:** El modo streaming es más rápido que el normal cuando:
(1) el dataset no cabe en RAM — sin streaming, el proceso fallaría o
usaría swap (que es mucho más lento); (2) el pipeline elimina muchos datos
temprano (filtros selectivos) — el chunk procesado es pequeño aunque el
archivo de origen sea grande. El tamaño del chunk es configurable pero
Polars tiene heurísticas basadas en la RAM disponible del sistema.

---

### Ejercicio 6.5.2 — Comparar: Polars streaming vs Spark para datasets medianos

```python
# Benchmark para datos de 50 GB (caben en Spark pero son grandes para Polars eager)
# Hardware: máquina con 32 GB RAM, 16 cores

# Opción A: Polars eager (falla o usa swap)
# resultado = pl.read_parquet("50gb/*.parquet")  # OOM probable

# Opción B: Polars streaming (funciona pero sin broadcast de dimensiones)
resultado_polars = (
    pl.scan_parquet("50gb/*.parquet")
    .filter(pl.col("region") == "norte")
    .group_by("mes")
    .agg(pl.sum("monto"))
    .collect(streaming=True)
)

# Opción C: Spark local (distribuye en los 16 cores con spill a disco)
from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder \
    .config("spark.driver.memory", "16g") \
    .config("spark.executor.memory", "12g") \
    .getOrCreate()

resultado_spark = (spark.read.parquet("50gb/*.parquet")
    .filter(F.col("region") == "norte")
    .groupBy("mes")
    .agg(F.sum("monto"))
    .collect()
)
```

**Restricciones:**
1. Diseñar el benchmark para 50 GB de datos con el pipeline descrito
2. Medir tiempo, uso de RAM pico, y I/O para cada opción
3. ¿Cuál es más rápido? ¿Depende del hardware?
4. ¿Hay una cuarta opción (DuckDB) que vale comparar?

> 🔗 Ecosistema: DuckDB es otro motor analítico de una sola máquina que
> compite directamente con Polars para datasets que no caben en RAM.
> DuckDB usa columnar on-disk storage y puede procesar terabytes en una
> sola máquina. No se cubre en este repo, pero en 2024 es relevante compararlo.

---

### Ejercicio 6.5.3 — Implementar: pipeline streaming con escritura incremental

```python
import polars as pl
from pathlib import Path

def procesar_archivos_grandes(
    ruta_entrada: str,
    ruta_salida: str,
    chunk_size: int = 1_000_000,  # filas por chunk
) -> dict:
    """
    Procesa archivos más grandes que la RAM usando streaming de Polars.
    Escribe el resultado incrementalmente para evitar acumular en memoria.
    """
    stats = {"archivos_procesados": 0, "filas_procesadas": 0, "errores": []}

    # Usar sink en lugar de collect para escritura incremental:
    (pl.scan_parquet(f"{ruta_entrada}/*.parquet")
        .filter(pl.col("activo") == True)
        .with_columns(
            (pl.col("monto") * 1.19).alias("monto_con_iva"),
        )
        .group_by("region", "mes")
        .agg([
            pl.sum("monto_con_iva").alias("revenue"),
            pl.count().alias("transacciones"),
        ])
        .sink_parquet(  # escribe incrementalmente sin acumular en RAM
            f"{ruta_salida}/resultado.parquet",
            maintain_order=False,  # más eficiente sin orden garantizado
        )
    )

    return stats
```

**Restricciones:**
1. Implementar usando `sink_parquet` (que ejecuta en streaming automáticamente)
2. ¿Cuál es la diferencia entre `.collect(streaming=True)` y `.sink_parquet()`?
3. ¿`sink_parquet` puede escribir particionado por columna?
4. Implementar manejo de errores: si un archivo está corrupto, registrar el error
   y continuar con los demás

---

### Ejercicio 6.5.4 — El límite del streaming: cuándo necesitas Spark

**Tipo: Analizar**

Para cada workload, determinar si Polars streaming es suficiente
o si se necesita Spark:

```
Workload 1:
  Dataset: 500 GB de transacciones en Parquet
  Operación: filtro + GroupBy por región (4 valores únicos) + suma
  Hardware: máquina con 64 GB RAM, 32 cores

Workload 2:
  Dataset: 2 TB de eventos, particionado por día
  Operación: join entre eventos (2 TB) y clientes (50 GB)
  Hardware: máquina con 256 GB RAM, 64 cores

Workload 3:
  Dataset: 100 GB en S3, actualizándose continuamente (1 GB/hora)
  Operación: calcular KPIs cada 15 minutos con los últimos 24 horas
  Hardware: cualquier configuración de cloud

Workload 4:
  Dataset: 10 TB distribuidos en 500 archivos en S3
  Operación: sort global por timestamp (ordenar TODO el dataset)
  Hardware: cualquier configuración de cloud
```

Para cada uno: ¿Polars streaming, Polars normal, Spark, u otro?

**Pista:** Workload 2: el join entre 2 TB y 50 GB es el caso difícil.
Polars streaming puede manejar la lectura del dataset de 2 TB en chunks,
pero el join requiere tener la tabla de 50 GB ("build side") en memoria
completa para construir la hash table. Con 256 GB de RAM, 50 GB caben
cómodamente — Polars streaming puede funcionar aquí.
Workload 4: el sort global de 10 TB requiere comparar todos los elementos
entre sí. Polars streaming puede hacerlo en chunks, pero el merge-sort final
necesita acceso a todo el dataset. Para 10 TB esto es impracticable en
una sola máquina — Spark distribuye el sort entre múltiples nodos.

---

### Ejercicio 6.5.5 — Leer: la historia de un dataset de 80 GB en una máquina de 32 GB

**Tipo: Diagnosticar**

Un equipo tiene un dataset de 80 GB y una máquina con 32 GB de RAM.
El pipeline actual usa Pandas y falla con OOM. El equipo está evaluando tres opciones:

```
Opción 1: Comprar más RAM (256 GB)
  Costo: ~$500/mes en cloud
  Tiempo de implementación: inmediato

Opción 2: Migrar a Polars con streaming
  Costo: $0 adicional (misma máquina)
  Tiempo de implementación: 1-2 días de desarrollo

Opción 3: Migrar a Spark (cluster de 5 nodos × 16 GB)
  Costo: ~$200/mes en cloud
  Tiempo de implementación: 1-2 semanas (setup + reescritura)
```

El pipeline: filtrar, hacer GroupBy con 100 claves únicas, agregar 3 métricas.
Se ejecuta una vez al día, no hay SLA estricto de latencia.

**Preguntas:**

1. ¿Cuál de las tres opciones recomendarías y por qué?

2. ¿El dataset crecerá con el tiempo? ¿Cómo cambia la recomendación
   si el año que viene son 500 GB?

3. ¿Qué información adicional necesitarías para hacer una recomendación
   más precisa?

4. Si el equipo ya tiene experiencia con Spark (tiene un cluster existente),
   ¿cambia la recomendación?

5. ¿Existe una "cuarta opción" que el equipo no consideró?

---

## Sección 6.6 — Integración con el Ecosistema Arrow

### Ejercicio 6.6.1 — De Polars a Pandas y viceversa

```python
import polars as pl
import pandas as pd
import numpy as np

df_polars = pl.DataFrame({
    "id": [1, 2, 3, 4, 5],
    "monto": [100.0, None, 300.0, 400.0, None],
    "region": ["norte", "sur", "norte", "este", "sur"],
    "activo": [True, False, True, True, False],
})

# Polars → Pandas (zero-copy para tipos compatibles):
df_pandas = df_polars.to_pandas()
# monto: Float64 en Polars → float64 en Pandas (zero-copy si no hay nulls)
# Con nulls: necesita pd.NA o NaN → puede requerir copia

# Pandas → Polars:
df_polars_2 = pl.from_pandas(df_pandas)

# Verificar zero-copy:
arr_polars = df_polars["monto"].to_numpy()
arr_pandas = df_pandas["monto"].values
print(f"Comparte buffer: {np.shares_memory(arr_polars, arr_pandas)}")
# True si zero-copy, False si se copió

# Vía Arrow (más explícito):
tabla_arrow = df_polars.to_arrow()
df_pandas_via_arrow = tabla_arrow.to_pandas()
```

**Preguntas:**

1. ¿La columna `monto` con nulls requiere copia al convertir a Pandas?
   ¿Por qué (qué tipo de dato usa Pandas para floats con nulls)?

2. ¿La columna `activo` (bool) requiere copia? ¿Qué tipo usa Pandas?

3. ¿Cuándo preferirías `df.to_pandas()` vs `df.to_arrow().to_pandas()`?

4. Si tienes un DataFrame de Pandas con 1M filas y lo conviertes a Polars,
   ¿cuánta memoria adicional se usa? ¿Depende del tipo de las columnas?

---

### Ejercicio 6.6.2 — Integración con Spark via Arrow

```python
# Transferir datos entre Polars y Spark usando Arrow como formato intermedio:
import polars as pl
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .getOrCreate()

# Polars → Spark:
df_polars = pl.read_parquet("datos_locales.parquet")
tabla_arrow = df_polars.to_arrow()
df_spark = spark.createDataFrame(tabla_arrow.to_pandas())
# Con Arrow habilitado, la conversión es mucho más eficiente

# Spark → Polars:
df_spark_resultado = spark.read.parquet("s3://resultado/")
df_polars_resultado = pl.from_pandas(
    df_spark_resultado.toPandas()
)
# toPandas() con Arrow → Arrow buffer en memoria → Polars sin copia adicional

# Mejor alternativa: escribir a Parquet y leer
df_spark_resultado.write.parquet("/tmp/resultado_local.parquet")
df_polars_resultado = pl.read_parquet("/tmp/resultado_local.parquet")
```

**Preguntas:**

1. ¿Cuántas copias de los datos hay en la cadena
   `Polars → Arrow → Pandas → Spark`?

2. ¿Por qué escribir a Parquet y leer es frecuentemente más eficiente
   que la conversión en memoria para datasets grandes?

3. ¿Existe una forma de transferir datos entre Polars y Spark sin
   pasar por Pandas?

4. ¿Cuándo tendría sentido usar Polars para preprocesar datos y luego
   enviarlos a Spark para procesamiento distribuido?

---

### Ejercicio 6.6.3 — Polars con DeltaLake

```python
import polars as pl

# Polars puede leer tablas Delta Lake directamente (via delta-rs):
df = pl.read_delta("s3://mi-lakehouse/ventas/")

# Leer una versión específica (time travel):
df_historico = pl.read_delta(
    "s3://mi-lakehouse/ventas/",
    version=10,  # versión 10 de la tabla
)

# Escribir a Delta Lake:
df.write_delta(
    "s3://mi-lakehouse/metricas/",
    mode="append",
)

# Upsert (merge) con Delta Lake desde Polars:
# (aún en desarrollo en 2024 — verificar la versión actual de delta-rs)
from deltalake import DeltaTable, write_deltalake

write_deltalake(
    "s3://mi-lakehouse/ventas/",
    df.to_arrow(),
    mode="merge",
    predicate="s.id = t.id",
)
```

> ⚙️ Versión: la integración de Polars con Delta Lake via `delta-rs` está
> activamente en desarrollo. Las APIs de escritura y merge cambian entre
> versiones menores de `polars` y `deltalake`. Verificar la documentación
> de `delta-rs` para la versión que uses en producción.

**Preguntas:**

1. ¿Qué es `delta-rs` y cómo se relaciona con Delta Lake de Databricks?

2. ¿Polars puede leer tablas Delta Lake particionadas eficientemente?
   ¿Hace predicate pushdown sobre las particiones?

3. ¿Cuáles son las limitaciones de Polars + Delta Lake vs Spark + Delta Lake?

---

### Ejercicio 6.6.4 — Compartir datos entre microservicios con Arrow Flight

```python
# Ver Cap.02 §2.3.3 para la implementación de Arrow Flight.
# Aquí: integrar Polars con Arrow Flight.

import polars as pl
import pyarrow.flight as flight

class ServidorPolars(flight.FlightServerBase):
    """Servidor que expone DataFrames de Polars via Arrow Flight."""
    
    def __init__(self, datasets: dict[str, pl.DataFrame], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.datasets = datasets
    
    def do_get(self, context, ticket):
        nombre = ticket.ticket.decode()
        df = self.datasets[nombre]
        # Polars → Arrow sin copia:
        tabla = df.to_arrow()
        return flight.RecordBatchStream(tabla)

# El cliente recibe datos como Arrow y los convierte a Polars:
def obtener_dataset(nombre: str, host: str, port: int) -> pl.DataFrame:
    cliente = flight.connect(f"grpc://{host}:{port}")
    tabla_arrow = cliente.do_get(
        flight.Ticket(nombre.encode())
    ).read_all()
    return pl.from_arrow(tabla_arrow)  # zero-copy desde Arrow a Polars
```

**Restricciones:**
1. Implementar el servidor y cliente completos
2. Medir la velocidad de transferencia vs HTTP+JSON para 1M filas
3. ¿Cuándo este patrón es más útil que simplemente escribir a S3?

---

### Ejercicio 6.6.5 — Polars en Python con extensiones nativas

```python
# Polars permite escribir extensiones en Rust que se llaman desde Python.
# Para casos donde ninguna función nativa de Polars es suficiente.

# Ejemplo: una transformación personalizada de alto rendimiento
# (normalmente no necesitas esto — las expresiones nativas cubren el 99%)

# Usando la API de plugins de Polars (Polars 0.20+):
# 1. Escribir la función en Rust (polars_plugin crate)
# 2. Compilar como librería dinámica
# 3. Registrar en Polars
# 4. Llamar desde Python como si fuera una función nativa

# Para el 99% de los casos: map_elements con una función Python
# es suficiente aunque sea más lento

# Caso válido para extensión Rust:
# Algoritmo de criptografía custom sobre columnas de strings
# Parsing de formato binario propietario
# Algoritmo iterativo que no puede vectorizarse

# Para este ejercicio: explorar el ecosistema de extensiones de Polars
# y documentar 3 extensiones útiles disponibles en 2024
```

**Restricciones:**
1. Investigar el ecosistema de plugins de Polars
2. Identificar 3 casos donde una extensión Rust mejora significativamente
   el rendimiento sobre `map_elements` Python
3. ¿Cuándo vale el esfuerzo de escribir una extensión en Rust?

> 🔗 Ecosistema: el repositorio `pola-rs/polars-plugins` contiene ejemplos
> de cómo escribir extensiones. También `polars-ds` y `polars-ols` son
> extensiones de terceros para data science y regresión, respectivamente.

---

## Sección 6.7 — Cuándo Polars y Cuándo Spark

### Ejercicio 6.7.1 — El árbol de decisión actualizado

**Tipo: Construir**

Completar el árbol de decisión para elegir entre Polars, Spark y otros:

```
¿Los datos caben en una sola máquina (< RAM disponible)?
  Sí → ¿Necesitas operaciones de ML/estadísticas avanzadas?
       Sí → Polars + scikit-learn/scipy (Polars para preprocesar, sklearn para ML)
       No → Polars eager (rápido, simple)
       
  ¿Los datos caben en una sola máquina con streaming (< 10× RAM)?
  Sí → ???
  No → ???

¿Necesitas datos en tiempo real (latencia < 1 minuto)?
  Sí → ???

¿El equipo ya tiene un cluster de Spark funcionando?
  Sí → ???

¿Los datos están en Delta Lake / Iceberg con historial y ACID?
  Sí → ???
```

**Restricciones:**
1. Completar el árbol con todas las ramas
2. Añadir DuckDB como opción en las ramas relevantes
3. ¿Hay casos donde la respuesta correcta es "los dos en secuencia"?

---

### Ejercicio 6.7.2 — Casos donde Polars supera a Spark

```python
import polars as pl
from pyspark.sql import SparkSession, functions as F
import time

spark = SparkSession.builder.getOrCreate()

# Caso 1: Dataset pequeño (<10 GB), job frecuente
# Polars: sin overhead de SparkSession, sin serialización
for i in range(100):  # 100 ejecuciones al día
    inicio = time.perf_counter()
    df = pl.read_parquet(f"datos_{i}.parquet")  # 500 MB
    resultado = df.group_by("region").agg(pl.sum("monto"))
    tiempo_polars = time.perf_counter() - inicio

    inicio = time.perf_counter()
    df_s = spark.read.parquet(f"datos_{i}.parquet")
    resultado_s = df_s.groupBy("region").agg(F.sum("monto")).collect()
    tiempo_spark = time.perf_counter() - inicio
    
    print(f"Polars: {tiempo_polars:.2f}s | Spark: {tiempo_spark:.2f}s")
```

**Restricciones:**
1. Medir ambas opciones para 10, 100, y 1000 MB de datos
2. Calcular el breakeven: ¿a qué tamaño de datos Spark empieza a ser más rápido?
3. Calcular el costo total para 100 ejecuciones al día de cada opción
4. ¿El overhead de Spark baja si reutilizas la SparkSession? ¿Cuánto?

---

### Ejercicio 6.7.3 — Casos donde Spark es mejor que Polars

Para cada caso, explicar por qué Spark supera a Polars y no al revés:

```
Caso 1: 10 TB de datos de click stream distribuidos en 1,000 archivos en S3
        Job: calcular revenue por campaña por día

Caso 2: Join entre tabla de 1 TB y tabla de 800 GB (ambas grandes)
        Hardware disponible: máquina de 64 GB RAM

Caso 3: Pipeline que necesita tolerancia a fallos: si un nodo falla
        a mitad del job, debe retomar desde donde quedó sin reempezar

Caso 4: Pipeline ejecutado en EMR o Databricks con autoescalado:
        algunos días son 1 TB, otros 50 TB

Caso 5: 20 data scientists ejecutan queries ad-hoc simultáneamente
        sobre el mismo dataset de 5 TB (multi-tenancy)
```

---

### Ejercicio 6.7.4 — Polars en producción: patrones reales

**Tipo: Diseñar**

Un equipo de analytics tiene este stack:
- Pipeline batch diario: procesa 50 GB/día de eventos de app móvil
- Dimensiones: usuarios (5 GB), productos (2 GB), campañas (200 MB)
- Job actual: Spark en EMR, tarda 45 minutos
- Equipo: 3 data engineers con experiencia en Pandas/Python, sin experiencia en Spark
- Presupuesto: quieren reducir el costo de la infraestructura

Diseñar la migración a Polars:
1. ¿Es viable? ¿Qué parte del pipeline puede migrar y cuál no?
2. ¿Qué máquina recomiendas? (RAM, cores)
3. ¿Polars eager o lazy? ¿Modo streaming?
4. ¿Cómo manejas las dimensiones (joins frecuentes)?
5. Estimación del nuevo tiempo de ejecución y costo

---

### Ejercicio 6.7.5 — El pipeline híbrido: Polars + Spark

**Tipo: Implementar**

Algunos pipelines se benefician de usar ambas herramientas:

```python
import polars as pl
from pyspark.sql import SparkSession, functions as F

# Escenario: pipeline con tres etapas
# Etapa 1: preprocesar dimensiones pequeñas (Polars, rápido)
# Etapa 2: join distribuido con datos grandes (Spark)
# Etapa 3: calcular métricas finales sobre resultado pequeño (Polars)

spark = SparkSession.builder \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .getOrCreate()

# Etapa 1: Polars preprocesa las dimensiones
df_clientes_polars = (pl.scan_parquet("clientes.parquet")
    .filter(pl.col("activo") == True)
    .with_columns([
        pl.col("nombre").str.strip_chars().str.to_lowercase(),
        pl.when(pl.col("gasto_historico") > 10000)
          .then(pl.lit("premium"))
          .otherwise(pl.lit("standard"))
          .alias("segmento"),
    ])
    .collect()
)

# Convertir a Spark para el join distribuido:
df_clientes_spark = spark.createDataFrame(df_clientes_polars.to_pandas())
df_clientes_spark.cache().count()

# Etapa 2: Spark hace el join distribuido
resultado_spark = (spark.read.parquet("s3://eventos-3tb/")
    .join(df_clientes_spark, on="cliente_id")
    .groupBy("segmento", "mes")
    .agg(F.sum("monto").alias("revenue"))
)

# Etapa 3: Polars procesa el resultado pequeño
resultado_polars = pl.from_pandas(resultado_spark.toPandas())
reporte_final = (resultado_polars
    .with_columns(
        (pl.col("revenue") / pl.col("revenue").sum()).alias("share")
    )
    .sort("revenue", descending=True)
)
```

**Restricciones:**
1. Implementar el pipeline híbrido completo
2. Medir el tiempo de cada etapa
3. ¿Hay una forma más eficiente de transferir los resultados de Spark
   a Polars que via Pandas?
4. ¿Cuándo este patrón híbrido es mejor que "solo Spark" o "solo Polars"?

---

## Resumen del capítulo

**Las cinco razones por las que Polars es rápido (y cuándo no lo son):**

```
1. Rust sin GIL → paralelismo real en todos los cores
   Límite: el paralelismo está limitado por la máquina, no el cluster

2. Apache Arrow en memoria → SIMD, cache-friendly, zero-copy con el ecosistema
   Límite: datos más grandes que la RAM necesitan el modo streaming

3. Lazy evaluation → predicate pushdown, projection pushdown, eliminar duplicados
   Límite: hay que usar scan_* (no read_*) para activar la lazy API

4. Expresiones nativas → sin bucles Python, sin GIL por fila
   Límite: lógica compleja que no cabe en expresiones requiere map_elements (lento)

5. Sin overhead de JVM y cluster → arranque inmediato, sin serialización entre nodos
   Límite: exactamente ese mismo overhead es lo que permite escalar a múltiples máquinas
```

**El criterio de decisión en una línea:**

> Si los datos caben en una máquina bien equipada y el job no necesita
> tolerancia a fallos ni multi-tenancy, Polars es casi siempre más rápido y más simple que Spark.
> Si los datos no caben o el cluster ya existe, Spark es la respuesta.

**Lo que conecta este capítulo con el Cap.07 (DataFusion):**

> Polars usa Arrow como formato de memoria y está escrito en Rust.
> DataFusion también usa Arrow y también está escrito en Rust.
> Pero DataFusion es un motor de queries SQL, no un DataFrame API.
> El Cap.07 explora cuándo la interfaz SQL y la naturaleza embebible de DataFusion
> son ventajas sobre el API de DataFrame de Polars.
