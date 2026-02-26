# Guía de Ejercicios — Cap.03: El Modelo Map/Reduce

> Este capítulo no es sobre Hadoop.
>
> Es sobre el paradigma que subyace a Spark, Beam, Flink, y Kafka Streams —
> y que apareció décadas antes que cualquiera de ellos.
> Entender Map/Reduce como abstracción (no como tecnología) es entender
> por qué todos esos frameworks tienen la forma que tienen.
>
> Al final del capítulo, el código de Spark que escribes en el resto
> del repositorio dejará de parecer una API que hay que memorizar
> y empezará a parecer una consecuencia inevitable de un modelo matemático.

---

## La idea en una oración

Map/Reduce es la observación de que la mayoría de las computaciones
sobre grandes colecciones de datos puede expresarse como:

```
1. MAP:    transformar cada elemento independientemente
2. REDUCE: combinar los resultados de elementos relacionados
```

Y que esa separación tiene una propiedad extraordinaria:
el MAP puede ejecutarse en paralelo sin coordinación,
y el REDUCE puede ejecutarse en paralelo por grupos.

```
Sin Map/Reduce (procesamiento secuencial):
  [e1, e2, e3, ..., e1M] → procesar uno a uno → resultado
  Tiempo: proporcional a N

Con Map/Reduce (paralelo):
  [e1, e2, e3] → map → [r1, r2, r3] → reduce → resultado parcial 1
  [e4, e5, e6] → map → [r4, r5, r6] → reduce → resultado parcial 2
  [resultado parcial 1, resultado parcial 2] → reduce final → resultado
  Tiempo: proporcional a N/workers + log(workers) para el reduce final
```

El costo: el REDUCE requiere que todos los elementos del mismo grupo
estén juntos. Eso requiere comunicación entre workers. En sistemas
distribuidos, esa comunicación se llama **shuffle** — el concepto
más importante del Cap.04.

---

## El origen: el paper de Google (2004)

El modelo Map/Reduce fue descrito por Jeffrey Dean y Sanjay Ghemawat
en el paper *MapReduce: Simplified Data Processing on Large Clusters* (2004).
El paper describe cómo Google procesaba cientos de terabytes de datos
para construir el índice web.

La observación central del paper:

> *"Most of our computations involve applying a map operation to each logical
> record in our input in order to compute a set of intermediate key/value pairs,
> and then applying a reduce operation to all values that shared the same key."*

> 📖 Profundizar: el paper original (Dean & Ghemawat, OSDI 2004) tiene 13 páginas
> y es uno de los papers más leídos en sistemas distribuidos. La Sección 3
> (Implementation) explica el shuffle en detalle. Disponible en research.google.com.

---

## Tabla de contenidos

- [Sección 3.1 — Map: transformación independiente](#sección-31--map-transformación-independiente)
- [Sección 3.2 — Reduce: combinación por clave](#sección-32--reduce-combinación-por-clave)
- [Sección 3.3 — El shuffle: el costo de la coordinación](#sección-33--el-shuffle-el-costo-de-la-coordinación)
- [Sección 3.4 — Combiners: optimizar antes del shuffle](#sección-34--combiners-optimizar-antes-del-shuffle)
- [Sección 3.5 — Más allá de Map/Reduce: el modelo generalizado](#sección-35--más-allá-de-mapreduce-el-modelo-generalizado)
- [Sección 3.6 — Map/Reduce en los frameworks modernos](#sección-36--mapreduce-en-los-frameworks-modernos)
- [Sección 3.7 — Cuándo Map/Reduce no es suficiente](#sección-37--cuándo-mapreduce-no-es-suficiente)

---

## Sección 3.1 — Map: Transformación Independiente

### Ejercicio 3.1.1 — La propiedad fundamental del map

**Tipo: Leer/analizar**

El **map** tiene una propiedad que lo hace trivialmente paralelizable:
cada elemento se transforma **sin depender de ningún otro elemento**.

```python
# Map: aplicar una función a cada elemento independientemente
def map_fn(elemento):
    return elemento * 2

datos = [1, 2, 3, 4, 5, 6, 7, 8]

# Secuencial:
resultado_seq = [map_fn(x) for x in datos]
# → [2, 4, 6, 8, 10, 12, 14, 16]

# Paralelo — mismo resultado, cualquier orden de ejecución:
# Worker 1: map_fn(1), map_fn(2) → [2, 4]
# Worker 2: map_fn(3), map_fn(4) → [6, 8]
# Worker 3: map_fn(5), map_fn(6) → [10, 12]
# Worker 4: map_fn(7), map_fn(8) → [14, 16]
# Resultado combinado: [2, 4, 6, 8, 10, 12, 14, 16]
```

La propiedad clave: `map_fn(3)` no necesita saber el resultado de `map_fn(1)`.
Cada aplicación es completamente independiente.

**Preguntas:**

1. ¿Cuál de las siguientes funciones puede usarse como `map_fn`
   en un map paralelo? ¿Cuál no puede y por qué?

```python
# Opción A:
def normalizar(precio):
    return precio / max_precio_global  # max_precio_global es una variable externa

# Opción B:
def calcular_descuento(transaccion):
    return transaccion["monto"] * 0.1

# Opción C:
contador_global = 0
def contar_y_transformar(elemento):
    global contador_global
    contador_global += 1  # modifica estado global
    return elemento * contador_global

# Opción D:
historial = []
def con_historial(elemento):
    historial.append(elemento)
    return sum(historial) / len(historial)  # promedio acumulado

# Opción E:
cache = {}
def con_cache(elemento):
    if elemento not in cache:
        cache[elemento] = calcular_costoso(elemento)
    return cache[elemento]
```

2. La Opción A accede a `max_precio_global`. ¿Cuándo esto es seguro
   en un map paralelo y cuándo no lo es?

3. La Opción E usa una caché compartida. ¿Qué problema tiene en un
   entorno distribuido donde cada worker tiene su propio proceso?

4. ¿Qué significa que una función de map sea **pura**?
   ¿Cuál de las opciones anteriores es pura?

5. En Spark, `df.withColumn("nuevo", F.col("monto") * 0.1)` es un map.
   ¿Por qué Spark puede ejecutarlo en paralelo sin coordinación?

**Pista:** La Opción E es segura en un solo proceso (la caché se comparte
entre threads del mismo proceso) pero no en un sistema distribuido donde
cada worker tiene su propia memoria. En Spark, la caché de la Opción E
existiría de forma independiente en cada executor — lo que significa que
cada executor recalcularía `calcular_costoso` para los elementos que procesa,
sin beneficiarse de los cálculos de otros executors.
La solución distribuida: broadcast variable (Cap.04 §4.4).

---

### Ejercicio 3.1.2 — Implementar map paralelo desde cero

```python
from multiprocessing import Pool
from typing import Callable, TypeVar, Iterator
import time

T = TypeVar("T")
R = TypeVar("R")

def map_paralelo(
    datos: list[T],
    fn: Callable[[T], R],
    num_workers: int = 4,
    tamaño_chunk: int = None,
) -> list[R]:
    """
    Aplica fn a cada elemento de datos en paralelo.
    
    Args:
        datos: la colección de entrada
        fn: función pura a aplicar a cada elemento
        num_workers: número de procesos paralelos
        tamaño_chunk: cuántos elementos por worker en cada batch
                      (None = dividir uniformemente)
    """
    if tamaño_chunk is None:
        tamaño_chunk = max(1, len(datos) // num_workers)

    with Pool(processes=num_workers) as pool:
        # imap_unordered: más eficiente pero no preserva el orden
        # imap: preserva el orden, ligeramente menos eficiente
        resultado = list(pool.imap(fn, datos, chunksize=tamaño_chunk))

    return resultado

# Comparar tiempos:
def calcular_costoso(n: int) -> float:
    """Simula una operación costosa (en producción: llamada a una API, cálculo ML)."""
    time.sleep(0.001)  # 1ms por elemento
    return n ** 2

datos = list(range(1000))

inicio = time.perf_counter()
resultado_seq = [calcular_costoso(x) for x in datos]
tiempo_seq = time.perf_counter() - inicio

inicio = time.perf_counter()
resultado_par = map_paralelo(datos, calcular_costoso, num_workers=8)
tiempo_par = time.perf_counter() - inicio

print(f"Secuencial: {tiempo_seq:.2f}s")
print(f"Paralelo (8 workers): {tiempo_par:.2f}s")
print(f"Speedup: {tiempo_seq/tiempo_par:.1f}×")
```

**Restricciones:**
1. Ejecutar y medir el speedup real vs el teórico (8 workers → 8× speedup ideal)
2. ¿Por qué el speedup real es menor que 8×? Identificar las fuentes de overhead
3. Medir cómo cambia el speedup con `tamaño_chunk = 1, 10, 100, 1000`
4. Implementar una versión con `concurrent.futures.ThreadPoolExecutor`
   y comparar con `multiprocessing.Pool`. ¿Cuándo threads > procesos para map?

**Pista:** Las fuentes de overhead en map paralelo:
(1) serialización de datos y resultados entre procesos (pickle),
(2) overhead de scheduling (crear y comunicarse con procesos),
(3) si `calcular_costoso` libera el GIL, threads pueden ser suficientes;
si no (código Python puro), necesitas procesos.
El `tamaño_chunk` grande reduce el overhead de serialización por elemento
pero puede crear desbalance si los elementos tardan distinto tiempo.

---

### Ejercicio 3.1.3 — FlatMap: cuando un elemento produce muchos

El map produce exactamente un resultado por elemento.
El **flatMap** produce cero, uno, o muchos resultados por elemento:

```python
# Map: 1 entrada → 1 salida
["hola mundo", "foo bar"] → map(split) → [["hola", "mundo"], ["foo", "bar"]]

# FlatMap: 1 entrada → N salidas (aplana el resultado)
["hola mundo", "foo bar"] → flatMap(split) → ["hola", "mundo", "foo", "bar"]
```

```python
from itertools import chain

def flat_map(datos: list, fn: Callable) -> list:
    """Aplica fn y aplana el resultado."""
    return list(chain.from_iterable(fn(x) for x in datos))

# Ejemplos de uso:
oraciones = [
    "el gato come pescado",
    "el perro come carne",
    "el gato bebe leche",
]

# Extraer palabras:
palabras = flat_map(oraciones, str.split)
# → ["el", "gato", "come", "pescado", "el", "perro", ...]

# Generar pares (palabra, 1) para contar:
pares = flat_map(oraciones, lambda oracion: [(w, 1) for w in oracion.split()])
# → [("el", 1), ("gato", 1), ("come", 1), ("pescado", 1), ...]
```

**Preguntas:**

1. En Spark, `df.select(F.explode(F.col("items")))` es un flatMap.
   ¿Cuándo necesitas explode en lugar de withColumn?

2. ¿Un flatMap puede reducir el número de elementos? Da un ejemplo.

3. ¿Un flatMap puede producir 0 elementos para algunos inputs?
   ¿Cuándo es esto útil?

4. ¿La propiedad de independencia del map se conserva en el flatMap?

5. En el ejemplo de generación de pares `(palabra, 1)`:
   ¿ves el comienzo del algoritmo de wordcount?
   ¿Qué operación vendría después para completarlo?

**Pista:** El flatMap que produce 0 elementos es equivalente a un filter —
para las entradas que quieres eliminar, retorna una lista vacía.
Muchos frameworks lo usan para combinar filter + transform en una sola
operación eficiente: "para cada elemento, si cumple la condición retorna
el elemento transformado, si no retorna vacío".
En Spark: `df.select(F.explode_outer(col))` vs `F.explode(col)` —
`explode` elimina filas con arrays vacíos/null, `explode_outer` las conserva
como null.

---

### Ejercicio 3.1.4 — Map en el contexto de Spark: transformaciones narrow

En Spark, las transformaciones de tipo map son "narrow transformations":
cada partición de output depende de exactamente una partición de input.

```python
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet("transacciones.parquet")

# Todas estas son narrow transformations (map-like):
df_transformado = (df
    .withColumn("monto_con_iva", F.col("monto") * 1.19)          # map
    .withColumn("region_upper", F.upper(F.col("region")))         # map
    .filter(F.col("monto") > 100)                                  # filter
    .select("id", "monto_con_iva", "region_upper")                 # projection
    .withColumn("categoria",
        F.when(F.col("monto_con_iva") > 1000, "premium")
         .otherwise("standard"))                                   # map condicional
)

# Estas transformaciones NO crean un shuffle:
# Cada partición de df_transformado se calcula independientemente
# de las otras particiones → paralelismo perfecto
df_transformado.explain()
```

**Preguntas:**

1. ¿Por qué las narrow transformations no crean shuffles?

2. ¿Cuántas veces se lee el archivo de Parquet si ejecutas:
   ```python
   df_a = df.filter(F.col("monto") > 100)
   df_b = df.filter(F.col("region") == "norte")
   df_a.count()
   df_b.count()
   ```

3. Spark agrupa múltiples narrow transformations en un solo "stage".
   ¿Cuántos stages tiene el plan del pipeline anterior?

4. ¿Qué es el "pipeline fusion" y cómo lo aprovechan Spark y Polars?

5. Si una función de map tarda 10ms por fila y tienes 100M filas con 100 cores,
   ¿cuánto tarda la operación? ¿Qué supuesto estás haciendo?

**Pista:** Pipeline fusion: en lugar de materializar el resultado de cada
transformación en memoria, Spark encadena múltiples transformaciones en
una sola pasada sobre los datos. En el pipeline anterior, la secuencia
`withColumn → filter → select → withColumn` se ejecuta como una sola
función por fila, sin guardar resultados intermedios. En Polars esto se llama
"lazy evaluation" — el plan se optimiza antes de ejecutarse.

---

### Ejercicio 3.1.5 — Leer: el map que es más lento con más workers

**Tipo: Diagnosticar**

Un equipo experimenta con paralelismo para acelerar un map costoso:

```
Experimento: aplicar un modelo de ML a 1 millón de imágenes.
Cada imagen tarda ~50ms en procesarse.

Resultados:
  1 worker:    50,000s (13.9 horas) — baseline
  2 workers:  25,100s (expected: 25,000s) ✓
  4 workers:  12,580s (expected: 12,500s) ✓
  8 workers:  6,420s  (expected: 6,250s)  ✓
  16 workers: 4,100s  (expected: 3,125s)  ✗ ← solo 1.56× speedup vs 2×
  32 workers: 4,350s  (expected: 1,563s)  ✗ ← ¡más lento que 16!
  64 workers: 5,200s  (expected:   781s)  ✗ ← aún más lento
```

**Preguntas:**

1. ¿Por qué el speedup es casi perfecto hasta 8 workers pero colapsa a partir de 16?

2. ¿Por qué 32 workers es más lento que 16?

3. ¿Qué recursos físicos están siendo el cuello de botella a partir de 16 workers?

4. ¿El modelo de ML es relevante para diagnosticar este problema?

5. Propón cómo encontrar el número óptimo de workers para este workload
   de forma experimental.

**Pista:** A partir de cierto punto, añadir más workers no aumenta el throughput
porque hay un recurso compartido que se satura. Las causas más frecuentes:
(1) ancho de banda de I/O — si las imágenes están en disco y 16+ workers
compiten por leer, el I/O se satura antes que la CPU,
(2) memoria — cada worker necesita cargar el modelo de ML (~GB), 16 workers
pueden saturar la RAM disponible causando swap,
(3) la GPU — si el modelo usa GPU, solo hay N GPUs disponibles.
La forma de distinguir: medir el uso de CPU, memoria, I/O, y GPU durante
el experimento con cada configuración de workers.

---

## Sección 3.2 — Reduce: Combinación por Clave

### Ejercicio 3.2.1 — La estructura del reduce

El **reduce** combina múltiples valores en uno, respetando una clave de agrupación:

```python
from collections import defaultdict
from typing import Callable

def map_reduce_simple(
    datos: list,
    map_fn: Callable,           # elemento → (clave, valor)
    reduce_fn: Callable,        # (valor_acumulado, valor_nuevo) → valor_acumulado
    valor_inicial,              # el valor de partida del acumulador
) -> dict:
    """
    Implementación simple de Map/Reduce en un solo proceso.
    """
    # Fase MAP: transformar cada elemento en (clave, valor)
    pares_clave_valor = [map_fn(elemento) for elemento in datos]
    
    # Fase SHUFFLE (implícita): agrupar por clave
    agrupados = defaultdict(list)
    for clave, valor in pares_clave_valor:
        agrupados[clave].append(valor)
    
    # Fase REDUCE: combinar los valores de cada clave
    resultado = {}
    for clave, valores in agrupados.items():
        acumulado = valor_inicial
        for valor in valores:
            acumulado = reduce_fn(acumulado, valor)
        resultado[clave] = acumulado
    
    return resultado

# Wordcount clásico:
oraciones = [
    "el gato come pescado y el perro come carne",
    "el gato bebe leche y el perro bebe agua",
    "el gato y el perro son amigos",
]

conteo = map_reduce_simple(
    datos=oraciones,
    map_fn=lambda oracion: [(palabra, 1) for palabra in oracion.split()],
    reduce_fn=lambda acumulado, nuevo: acumulado + nuevo,
    valor_inicial=0,
)
# Pero map_fn retorna una lista → necesitamos flatMap, no map
```

**Restricciones:**
1. Arreglar `map_reduce_simple` para que use flatMap en lugar de map
2. Implementar wordcount usando la función corregida
3. Implementar "suma de montos por región" para una lista de transacciones
4. Implementar "monto máximo por región"
5. ¿Qué tiene en común la estructura de los tres ejercicios?

**Pista:** La corrección para flatMap:
```python
pares_clave_valor = []
for elemento in datos:
    pares_clave_valor.extend(map_fn(elemento))
```
O más elegante: `list(chain.from_iterable(map_fn(e) for e in datos))`.
Lo que tienen en común los tres ejercicios: todos son `(clave, valor)` →
agrupar por clave → aplicar una función de reducción a los valores.
La diferencia está solo en la función de reducción: `+` para suma, `max` para máximo.

---

### Ejercicio 3.2.2 — La propiedad de asociatividad en el reduce

Para que el reduce pueda ejecutarse en paralelo, la función de reducción
debe ser **asociativa** y, idealmente, **conmutativa**:

```
Asociativa:  reduce(reduce(a, b), c) == reduce(a, reduce(b, c))
Conmutativa: reduce(a, b) == reduce(b, a)

Si es asociativa: puedo reducir en árbol (paralelo)
Si también es conmutativa: puedo reducir en cualquier orden
```

```
Reduce en árbol (paralelo):
  Nivel 0: [1, 2, 3, 4, 5, 6, 7, 8]
  Nivel 1: [sum(1,2), sum(3,4), sum(5,6), sum(7,8)] = [3, 7, 11, 15]
  Nivel 2: [sum(3,7), sum(11,15)]                   = [10, 26]
  Nivel 3: sum(10, 26)                               = 36
  
  Profundidad: log2(8) = 3 niveles en paralelo
  vs
  Reduce secuencial: 7 pasos
```

**Preguntas:**

1. Para cada función, determinar si es asociativa, conmutativa, o ambas:
   - `suma: (a, b) → a + b`
   - `max: (a, b) → max(a, b)`
   - `concatenar: (a, b) → a + b` (para strings)
   - `promedio: (a, b) → (a + b) / 2`
   - `append: (lista, elem) → lista + [elem]`
   - `merge_dict: (d1, d2) → {**d1, **d2}`

2. ¿Por qué el **promedio** no puede reducirse directamente en paralelo?
   ¿Cómo lo resolverías?

3. Implementar reduce en árbol paralelo:
```python
def reduce_arbol(datos: list, fn: Callable, num_workers: int = 4) -> any:
    """
    Reduce en árbol usando múltiples workers.
    Requiere que fn sea asociativa.
    """
    # TODO: implementar
    # Hint: dividir datos en chunks, reducir cada chunk en paralelo,
    # luego reducir los resultados parciales
    pass
```

4. ¿Para qué tamaño de datos el reduce en árbol es más rápido que el secuencial?

**Pista:** El promedio no es asociativo: `avg(avg(1,2), 3) = avg(1.5, 3) = 2.25`,
pero `avg(1, avg(2,3)) = avg(1, 2.5) = 1.75`. La solución: reducir por suma
y count por separado (ambos sí son asociativos), luego dividir al final:
`sum([1,2,3]) / count([1,2,3]) = 6/3 = 2`. Esto es exactamente lo que hace
Spark cuando calculas `F.mean()` — internamente reduce `(sum, count)` tuplas.

---

### Ejercicio 3.2.3 — GroupByKey vs ReduceByKey: el tradeoff crucial

Este ejercicio introduce el concepto más importante antes del shuffle.

```python
from pyspark.sql import SparkSession
from pyspark import RDD

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

# Dataset: (región, monto) para 100M transacciones
transacciones_rdd = sc.parallelize([
    ("norte", 100.0), ("sur", 200.0), ("norte", 150.0),
    ("este", 50.0), ("norte", 300.0), ("sur", 75.0),
    # ... 100M elementos más
])

# Opción A: groupByKey → suma manual
# PROBLEMA: mueve TODOS los valores al reducer antes de reducir
suma_a = (transacciones_rdd
    .groupByKey()           # todos los valores de "norte" van al mismo nodo
    .mapValues(sum)         # luego suma
)

# Opción B: reduceByKey → combina localmente ANTES del shuffle
# MEJOR: combina los valores en cada worker antes de moverlos
suma_b = (transacciones_rdd
    .reduceByKey(lambda a, b: a + b)  # suma parcial en cada worker primero
)

# Ambas producen el mismo resultado:
# [("norte", 550.0), ("sur", 275.0), ("este", 50.0)]
```

```
groupByKey en un cluster de 3 workers:

Worker 1: ("norte", 100), ("norte", 150) → shuffle "norte" → Worker A
Worker 2: ("norte", 300), ("sur", 200)   → shuffle "norte" → Worker A,
                                                              "sur"   → Worker B
Worker 3: ("sur", 75), ("este", 50)      → shuffle "sur"   → Worker B,
                                                              "este"  → Worker C

Worker A recibe: [100, 150, 300] → sum → 550
Worker B recibe: [200, 75]       → sum → 275
Worker C recibe: [50]            → sum → 50

Total de datos shuffleados: 6 valores × 8 bytes = 48 bytes

reduceByKey en el mismo cluster:

Worker 1: ("norte", 100+150=250) → shuffle → Worker A
Worker 2: ("norte", 300), ("sur", 200) → reduce local →
          ("norte", 300), ("sur", 200) → shuffle → Workers A, B
Worker 3: ("sur", 75+0=75), ("este", 50) → shuffle → Workers B, C

Worker A recibe: [250, 300] → sum → 550
Worker B recibe: [200, 75]  → sum → 275
Worker C recibe: [50]       → sum → 50

Total de datos shuffleados: 4 valores × 8 bytes = 32 bytes
(con más datos, la diferencia es mucho mayor)
```

**Restricciones:**
1. Medir el tiempo de `groupByKey` vs `reduceByKey` para 10M pares
2. Medir los bytes de shuffle en Spark UI para cada operación
3. ¿La diferencia de rendimiento aumenta o disminuye con más datos?
4. ¿Cuándo `groupByKey` es inevitable? (es decir, cuando `reduceByKey` no funciona)

**Pista:** `groupByKey` es inevitable cuando necesitas acceder a todos los valores
del grupo simultáneamente y no puedes expresar la operación como un reduce asociativo.
Por ejemplo: calcular la mediana (necesitas ordenar todos los valores),
encontrar el top-N de cada grupo, o calcular percentiles exactos.
Para estas operaciones, el shuffle de todos los valores es necesario —
no hay "combinación previa" posible.

---

### Ejercicio 3.2.4 — Implementar reduce distribuido con multiprocessing

```python
from multiprocessing import Pool
from collections import defaultdict
from typing import Callable, TypeVar
import itertools

K = TypeVar("K")
V = TypeVar("V")

def map_reduce_distribuido(
    datos: list,
    map_fn: Callable,       # elemento → list[(clave, valor)]
    reduce_fn: Callable,    # (acumulado, valor) → acumulado
    valor_inicial,
    num_workers: int = 4,
) -> dict:
    """
    Map/Reduce distribuido con múltiples procesos.
    Incluye un combiner local (pre-reduce) antes del shuffle.
    """
    tamaño_chunk = max(1, len(datos) // num_workers)
    chunks = [datos[i:i+tamaño_chunk] for i in range(0, len(datos), tamaño_chunk)]

    def map_y_combinar_local(chunk: list) -> dict:
        """
        MAP + COMBINER: aplicar map y combinar localmente antes del shuffle.
        Equivale al 'partial reduce' de Hadoop / combiner de Spark.
        """
        pares = list(itertools.chain.from_iterable(map_fn(e) for e in chunk))
        resultado_local = defaultdict(lambda: valor_inicial)
        for clave, valor in pares:
            resultado_local[clave] = reduce_fn(resultado_local[clave], valor)
        return dict(resultado_local)

    # Fase MAP + COMBINER (paralela):
    with Pool(processes=num_workers) as pool:
        resultados_parciales = pool.map(map_y_combinar_local, chunks)

    # Fase SHUFFLE + REDUCE FINAL (secuencial en este ejemplo simplificado):
    resultado_final = defaultdict(lambda: valor_inicial)
    for parcial in resultados_parciales:
        for clave, valor in parcial.items():
            resultado_final[clave] = reduce_fn(resultado_final[clave], valor)

    return dict(resultado_final)

# Wordcount:
textos = [f"el gato y el perro en el barrio {i}" for i in range(100_000)]

resultado = map_reduce_distribuido(
    datos=textos,
    map_fn=lambda texto: [(palabra, 1) for palabra in texto.split()],
    reduce_fn=lambda a, b: a + b,
    valor_inicial=0,
    num_workers=8,
)
```

**Restricciones:**
1. Ejecutar y verificar la corrección del resultado
2. Comparar el tiempo con y sin el combiner local
3. Medir cuántos datos se "shufflean" (pasan de los workers al reduce final)
   con y sin combiner para 100,000 textos
4. ¿La función `reduce_fn` que usas aquí debe ser asociativa?
   ¿Y conmutativa? ¿Por qué?

---

### Ejercicio 3.2.5 — Leer: diagnosticar un reduce incorrecto

**Tipo: Diagnosticar**

Un pipeline calcula el precio promedio de productos por categoría.
El resultado parece incorrecto — el promedio de "electrónico" es 450.0
pero el equipo de negocio dice que debería ser alrededor de 320.0.

```python
# El código:
promedios = (transacciones_rdd
    .map(lambda t: (t["categoria"], t["precio"]))
    .groupByKey()
    .mapValues(lambda precios: sum(precios) / len(list(precios)))
)

# Resultado:
# [("electrónico", 450.0), ("ropa", 85.0), ("hogar", 120.0)]
```

Al investigar, se encuentra que:
- Los datos están particionados en 5 archivos
- El archivo 3 tiene el 60% de los productos electrónicos más caros
- Los archivos 1, 2, 4, 5 tienen el 40% más barato

**Preguntas:**

1. ¿El código tiene un bug? ¿El resultado de 450.0 es correcto
   dado el código, o hay un error en la implementación?

2. Si el resultado es correcto dado el código, ¿por qué no coincide
   con la expectativa del equipo de negocio?

3. ¿Podría haber un problema si `list(precios)` se consume más de una vez?

4. ¿El particionamiento de los datos (60% en un archivo) afecta
   el resultado del promedio? ¿Debería afectarlo?

5. Propón la corrección si el resultado es incorrecto, o una explicación
   para el equipo de negocio si es correcto.

**Pista:** `groupByKey().mapValues(lambda precios: sum(precios) / len(list(precios)))`
es correcto matemáticamente — agrupa todos los precios de cada categoría y
calcula el promedio. Si el resultado es 450.0 y el equipo espera 320.0,
la discrepancia puede ser que el equipo está excluyendo ciertos productos
(por ejemplo, productos con descuento o productos sin stock) que el pipeline
incluye. El particionamiento no afecta el resultado del promedio porque
`groupByKey` mueve todos los valores al mismo reducer, independientemente
de cómo estén particionados en el origen.

---

## Sección 3.3 — El Shuffle: el Costo de la Coordinación

### Ejercicio 3.3.1 — Por qué el shuffle es inevitable

**Tipo: Leer/analizar**

El shuffle es la operación más costosa en Map/Reduce — y es inevitablemente
necesaria para cualquier operación que requiere ver múltiples elementos relacionados.

```
Sin shuffle: cada elemento se procesa independientemente
  → solo operaciones de map son posibles
  → no puedes calcular sumas, promedios, joins, ordenamientos

Con shuffle: elementos relacionados se mueven al mismo nodo
  → puedes calcular cualquier aggregation, join, sort
  → costo: serializar datos, moverlos por la red, deserializar

El shuffle tiene tres fases:

Fase 1 — MAP OUTPUT / SPILL:
  Cada mapper escribe sus (clave, valor) pares ordenados por clave
  en archivos locales. Si los datos no caben en memoria → spill to disk.
  
Fase 2 — FETCH / COPY:
  Cada reducer contacta a todos los mappers para leer sus datos.
  Todos los valores de la clave "norte" vienen de todos los mappers.
  → Tráfico de red proporcional a los datos shuffleados
  
Fase 3 — MERGE / SORT:
  El reducer ordena los datos recibidos por clave y los combina.
  Luego aplica la función de reduce.
```

**Preguntas:**

1. Para calcular `COUNT(*)` (contar todas las filas), ¿es necesario un shuffle?

2. Para calcular `COUNT(*) GROUP BY region`, ¿es necesario un shuffle?
   ¿Qué datos se mueven exactamente?

3. Para un `JOIN` entre dos tablas de 1 TB cada una,
   ¿cuántos bytes se shufflean en el peor caso?

4. ¿Qué operaciones de Spark NO requieren shuffle (son narrow transformations)?
   ¿Qué operaciones SÍ lo requieren (wide transformations)?

5. Si el shuffle de 100 GB tarda 10 minutos en una red de 10 Gbps,
   ¿cuánto tarda en una red de 1 Gbps? ¿Y si el shuffle es de 10 GB?

**Pista:** Para la pregunta 1: `COUNT(*)` sin GROUP BY puede ejecutarse
como un map (cada nodo cuenta sus filas) seguido de un reduce trivial
(sumar los conteos). El shuffle solo mueve los conteos parciales —
un número por nodo, no los datos completos. Si tienes 100 nodos,
el shuffle mueve 100 enteros, no 1 TB de datos.

---

### Ejercicio 3.3.2 — Implementar shuffle simplificado

```python
from collections import defaultdict
from typing import Callable
import hashlib

def shuffle(
    pares: list[tuple],
    num_reducers: int,
    key_fn: Callable = None,
) -> list[list[tuple]]:
    """
    Distribuye pares (clave, valor) entre N reducers.
    El reducer que recibe cada par se determina por hash(clave) % num_reducers.
    
    Retorna una lista de N listas, una por reducer.
    """
    buckets = [[] for _ in range(num_reducers)]

    for clave, valor in pares:
        # Determinar qué reducer recibe este par:
        hash_clave = int(hashlib.md5(str(clave).encode()).hexdigest(), 16)
        reducer_idx = hash_clave % num_reducers
        buckets[reducer_idx].append((clave, valor))

    return buckets

# Simular Map/Reduce distribuido con shuffle explícito:
def map_reduce_con_shuffle(
    datos: list,
    map_fn: Callable,
    reduce_fn: Callable,
    valor_inicial,
    num_reducers: int = 4,
) -> dict:
    # FASE MAP:
    todos_los_pares = []
    for elemento in datos:
        todos_los_pares.extend(map_fn(elemento))

    # FASE SHUFFLE:
    buckets = shuffle(todos_los_pares, num_reducers)
    print(f"Datos por reducer: {[len(b) for b in buckets]}")

    # FASE REDUCE:
    resultado = {}
    for bucket in buckets:
        agrupados = defaultdict(list)
        for clave, valor in bucket:
            agrupados[clave].append(valor)
        for clave, valores in agrupados.items():
            acumulado = valor_inicial
            for v in valores:
                acumulado = reduce_fn(acumulado, v)
            resultado[clave] = acumulado

    return resultado
```

**Restricciones:**
1. Implementar y verificar con wordcount
2. Observar la distribución de datos por reducer — ¿es uniforme?
3. ¿Qué pasa si todos los datos tienen la misma clave?
   (simula data skew extremo)
4. Implementar una función de particionamiento alternativa al hash:
   range partitioning (asignar rangos de claves a reducers)
5. ¿Cuándo range partitioning es mejor que hash partitioning?

**Pista:** Si todos los datos tienen la misma clave, el hash partitioning
envía todos los datos al mismo reducer — uno trabaja 100%, el resto 0%.
Esto es exactamente el "data skew" del Cap.04. El range partitioning
divide el espacio de claves en rangos: A-F → reducer 0, G-M → reducer 1, etc.
Para datos uniformemente distribuidos, esto da distribución balanceada.
Pero si hay hot spots (muchos datos en un rango específico), range partitioning
también puede crear skew — no es una solución universal.

---

### Ejercicio 3.3.3 — Medir el costo real del shuffle

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

# Dataset de 1M transacciones:
n = 1_000_000
df = spark.range(n).select(
    F.col("id"),
    (F.rand() * 10000).alias("monto"),
    F.when(F.rand() < 0.25, "norte")
     .when(F.rand() < 0.50, "sur")
     .when(F.rand() < 0.75, "este")
     .otherwise("oeste").alias("region"),
)
df.cache().count()  # materializar en caché

import time

# Job sin shuffle:
inicio = time.perf_counter()
df.withColumn("monto_con_iva", F.col("monto") * 1.19).count()
sin_shuffle = time.perf_counter() - inicio

# Job con shuffle (groupBy):
inicio = time.perf_counter()
df.groupBy("region").agg(F.sum("monto")).collect()
con_shuffle = time.perf_counter() - inicio

# Job con shuffle pesado (groupBy con alta cardinalidad):
inicio = time.perf_counter()
df.groupBy("id").agg(F.sum("monto")).collect()  # 1M claves distintas
shuffle_pesado = time.perf_counter() - inicio

print(f"Sin shuffle: {sin_shuffle:.2f}s")
print(f"Shuffle (4 claves): {con_shuffle:.2f}s")
print(f"Shuffle (1M claves): {shuffle_pesado:.2f}s")
```

**Restricciones:**
1. Ejecutar y registrar los tiempos
2. En Spark UI, ver los bytes shuffleados para cada job
3. ¿Por qué el shuffle con 1M claves es más lento que con 4 claves,
   aunque los datos son los mismos?
4. Medir el impacto del número de particiones del shuffle
   (`spark.sql.shuffle.partitions = 200` vs `2000` vs `20`)

**Pista:** El shuffle con 1M claves es más lento por dos razones:
(1) el reducer necesita mantener en memoria entradas para 1M claves distintas
vs 4 claves, aumentando el uso de memoria y potencialmente el spill a disco,
(2) el merge/sort de los datos shuffleados es más costoso con más claves distintas.
El número de particiones del shuffle determina cuántos archivos se escriben
en la fase de map output y cuántos reducers hay — con más datos, más particiones
reducen el tamaño por partición y el spill; con menos datos, menos particiones
reducen el overhead de scheduling.

---

### Ejercicio 3.3.4 — Leer: el shuffle que no se puede evitar vs el que sí

**Tipo: Analizar**

Para cada operación, determinar si el shuffle es evitable o inevitable,
y proponer la alternativa si es evitable:

```python
# 1. Contar filas por región (4 regiones):
df.groupBy("region").count()

# 2. Ordenar todo el dataset por timestamp:
df.orderBy("timestamp")

# 3. Join entre df_ventas (1 TB) y df_productos (100 MB):
df_ventas.join(df_productos, on="producto_id")

# 4. Calcular el percentil 95 de montos:
df.approxQuantile("monto", [0.95], 0.01)

# 5. Eliminar filas duplicadas:
df.distinct()

# 6. Join entre df_ventas (1 TB) y df_clientes (1 TB)
#    donde ambas ya están particionadas por user_id:
df_ventas.join(df_clientes, on="user_id")

# 7. Calcular el promedio de monto:
df.agg(F.avg("monto"))

# 8. Calcular el monto máximo POR usuario (10M usuarios distintos):
df.groupBy("user_id").agg(F.max("monto"))
```

**Pista:** La operación 3 puede evitar el shuffle si `df_productos` es pequeño
(broadcast join — Spark envía `df_productos` completo a cada executor).
La operación 6 puede evitar el shuffle si ambas tablas están pre-particionadas
por la misma key con el mismo número de particiones (bucket join).
La operación 7 puede reducirse en dos fases sin shuffle completo: cada executor
calcula su sum y count local, luego un reduce final combina.

---

### Ejercicio 3.3.5 — Diseñar: un pipeline sin shuffles innecesarios

**Tipo: Diseñar**

El siguiente pipeline tiene shuffles innecesarios. Rediseñarlo para minimizarlos:

```python
df_ventas = spark.read.parquet("s3://ventas/")           # 500 GB
df_clientes = spark.read.parquet("s3://clientes/")        # 200 GB
df_productos = spark.read.parquet("s3://productos/")      # 500 MB

# Pipeline actual:
resultado = (df_ventas
    .filter(F.col("activo") == True)                      # filtro 1
    .join(df_clientes, on="cliente_id")                   # join 1 → shuffle
    .filter(F.col("cliente_premium") == True)             # filtro 2
    .join(df_productos, on="producto_id")                 # join 2 → shuffle
    .filter(F.col("categoria") == "electronico")          # filtro 3
    .groupBy("region", "mes")                             # groupby → shuffle
    .agg(F.sum("monto").alias("revenue"),
         F.count("*").alias("transacciones"))
    .orderBy(F.col("revenue").desc())                     # orderby → shuffle
)
```

1. Identificar todos los shuffles del pipeline
2. ¿Cuáles son evitables y cómo?
3. ¿En qué orden deberían aplicarse los filtros?
4. Reescribir el pipeline optimizado con anotaciones
5. Estimar la reducción en datos shuffleados

---

## Sección 3.4 — Combiners: Optimizar Antes del Shuffle

### Ejercicio 3.4.1 — El combiner como pre-reduce local

```
Sin combiner:
  Mapper 1 emite: [("norte", 100), ("norte", 150), ("norte", 200)]
  → shuffle: 3 pares viajan a través de la red

Con combiner:
  Mapper 1 combina localmente: ("norte", 450)
  → shuffle: 1 par viaja a través de la red
  
  Reducción de 3× en tráfico de red → job 3× más rápido en fase shuffle
  
  Requisito: la función del combiner debe ser la MISMA que la del reducer
  (o al menos producir valores que el reducer puede combinar correctamente)
```

```python
# En Hadoop MapReduce clásico, el combiner es explícito:
# job.setCombinerClass(SumReducer.class);

# En Spark, se usa implícitamente con reduceByKey (vs groupByKey):
rdd.reduceByKey(lambda a, b: a + b)
# Spark aplica el combiner automáticamente: combina localmente antes del shuffle

# En Spark SQL, el optimizador lo hace automáticamente con HashAggregate:
# La primera fase (partial) es el combiner
df.groupBy("region").agg(F.sum("monto"))
# Plan físico: HashAggregate(partial) → Exchange → HashAggregate(final)
```

**Preguntas:**

1. ¿Para qué funciones de agregación el combiner es siempre correcto?
   ¿Para cuáles no puede usarse?

2. ¿Puede usarse un combiner para calcular la mediana? ¿Por qué?

3. ¿Puede usarse un combiner para calcular el promedio?
   Si sí, ¿cómo debe modificarse la función?

4. En Spark UI, ¿cómo distingues el "HashAggregate partial" del "HashAggregate final"?

5. Si un combiner reduce los datos de 100 GB a 1 GB antes del shuffle,
   ¿cuánto impacto tiene en el tiempo total del job?

**Pista:** El promedio con combiner requiere un truco: en lugar de emitir
el promedio parcial, emitir la tupla `(suma, count)`. El combiner combina
tuplas: `(suma1, count1) + (suma2, count2) = (suma1+suma2, count1+count2)`.
El reducer final calcula `suma_total / count_total`. Esto es exactamente
lo que hace Spark internamente cuando calculas `F.avg()`.

---

### Ejercicio 3.4.2 — Implementar un combiner generalizado

```python
from dataclasses import dataclass
from typing import Any, Callable

@dataclass
class Combiner:
    """
    Abstracción de un combiner para Map/Reduce.
    Requiere tres funciones:
      - create_accumulator: crear el acumulador inicial
      - add_input: añadir un valor al acumulador
      - merge_accumulators: combinar dos acumuladores
    """
    create_accumulator: Callable[[], Any]
    add_input: Callable[[Any, Any], Any]
    merge_accumulators: Callable[[Any, Any], Any]
    extract_output: Callable[[Any], Any] = None

    def __post_init__(self):
        if self.extract_output is None:
            self.extract_output = lambda acc: acc

# Combiner para suma:
combiner_suma = Combiner(
    create_accumulator=lambda: 0,
    add_input=lambda acc, val: acc + val,
    merge_accumulators=lambda a, b: a + b,
)

# Combiner para promedio (requiere (sum, count)):
combiner_promedio = Combiner(
    create_accumulator=lambda: (0.0, 0),
    add_input=lambda acc, val: (acc[0] + val, acc[1] + 1),
    merge_accumulators=lambda a, b: (a[0] + b[0], a[1] + b[1]),
    extract_output=lambda acc: acc[0] / acc[1] if acc[1] > 0 else None,
)

# Combiner para top-N (más complejo):
def crear_combiner_top_n(n: int) -> Combiner:
    import heapq
    return Combiner(
        create_accumulator=lambda: [],
        add_input=lambda acc, val: heapq.nlargest(n, acc + [val]),
        merge_accumulators=lambda a, b: heapq.nlargest(n, a + b),
    )
```

**Restricciones:**
1. Implementar y verificar `combiner_suma` y `combiner_promedio`
2. Implementar `combiner_top_n` y verificar para `n=10`
3. Implementar `combiner_conteo_distintos` (HyperLogLog simplificado)
4. Integrar el Combiner con `map_reduce_distribuido` del ejercicio anterior

**Pista:** El combiner de top-N tiene un truco: `add_input` y `merge_accumulators`
deben mantener solo los top-N elementos en el acumulador para evitar que crezca
sin límite. El reducer final también aplica top-N al resultado combinado de todos
los mappers. En Spark, esto es el `UDAF` (User-Defined Aggregate Function)
o la función `approx_top_k`.

---

### Ejercicio 3.4.3 — Leer: cuándo el combiner hace daño

**Tipo: Analizar**

El combiner no siempre es beneficioso. Analizar estos casos:

```python
# Caso 1: el combiner con datos ya reducidos
# Si cada clave aparece una sola vez en cada mapper,
# el combiner no reduce nada pero añade overhead de procesamiento
rdd_sin_repeticiones = sc.parallelize([
    ("usuario_1", datos_1), ("usuario_2", datos_2), ...
    # cada usuario_id aparece exactamente una vez
])
rdd_sin_repeticiones.reduceByKey(fn_costosa)
# El combiner local crea acumuladores para 1M usuarios → overhead de memoria

# Caso 2: el combiner con función costosa
rdd.reduceByKey(lambda a, b: ejecutar_modelo_ml(a, b))
# El combiner ejecuta el modelo ML en la fase de map → 2x el trabajo

# Caso 3: el combiner con datos no uniformes
# 99% de los datos van a la clave "hot_key"
# El combiner ayuda poco porque el reducer sigue recibiendo muchos datos
```

**Preguntas:**

1. ¿Cómo detectas en Spark UI si el combiner está ayudando?
   (pista: comparar "Shuffle Write" antes y después de un groupBy)

2. ¿Spark aplica el combiner automáticamente siempre, o solo en algunos casos?

3. Para el Caso 2, ¿existe una forma de desactivar el combiner
   sin cambiar `reduceByKey` por `groupByKey`?

4. ¿En qué situación preferirías `groupByKey` sobre `reduceByKey`
   incluso siendo consciente del mayor shuffle?

---

### Ejercicio 3.4.4 — El combiner en Spark SQL: el plan de ejecución

```python
from pyspark.sql import functions as F

df = spark.read.parquet("transacciones.parquet")

# Aggregation simple:
df.groupBy("region").agg(F.sum("monto")).explain()

# Output del explain (simplificado):
# == Physical Plan ==
# AdaptiveSparkPlan
# +- HashAggregate(keys=[region], functions=[sum(monto)])  ← REDUCE FINAL
#    +- Exchange hashpartitioning(region, 200)              ← SHUFFLE
#       +- HashAggregate(keys=[region], functions=[partial_sum(monto)])  ← COMBINER
#          +- FileScan parquet [region, monto]
```

**Preguntas:**

1. ¿Por qué hay dos `HashAggregate` en el plan?

2. El `partial_sum` en el primer HashAggregate es el combiner.
   ¿En qué fase del Map/Reduce clásico corresponde?

3. Si cambias `F.sum("monto")` por `F.collect_list("monto")`,
   ¿aparece el `partial_` en el plan? ¿Por qué?

4. ¿Qué datos viajan en el `Exchange hashpartitioning(region, 200)`?
   ¿Son los datos originales o los resultados del primer HashAggregate?

5. `AdaptiveSparkPlan` puede cambiar el número de particiones del Exchange.
   ¿Cuándo lo hace y cómo lo ves en el plan final?

---

### Ejercicio 3.4.5 — Construir: el pipeline wordcount end-to-end con combiner

Implementar wordcount completo con todas las optimizaciones:

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

# Dataset: 10GB de texto
df = spark.read.text("s3://bucket/corpus/*.txt")

# Versión 1: naive (sin optimizaciones)
wc_naive = (df
    .select(F.explode(F.split(F.col("value"), r"\s+")).alias("palabra"))
    .filter(F.col("palabra") != "")
    .groupBy("palabra")
    .count()
    .orderBy(F.col("count").desc())
)

# Versión 2: optimizada
# TODO: implementar con:
# - Filtrado de stop words antes del groupBy
# - lowercase antes del groupBy
# - Limitar el resultado a top 1000 (evitar orderBy global)
# - Verificar el plan para confirmar que hay partial_count

# Medir y comparar ambas versiones
```

**Restricciones:**
1. Implementar ambas versiones
2. Comparar el plan de ejecución (`explain(True)`)
3. Comparar el tiempo y los bytes shuffleados
4. ¿El `orderBy` al final agrega un shuffle? ¿Hay alternativa?

---

## Sección 3.5 — Más Allá de Map/Reduce: el Modelo Generalizado

### Ejercicio 3.5.1 — Las limitaciones de Map/Reduce clásico

**Tipo: Leer/analizar**

El Map/Reduce clásico de Hadoop tiene limitaciones importantes
que motivaron el desarrollo de Spark y Flink:

```
Limitación 1: Solo dos fases (Map y Reduce)
  Los algoritmos iterativos (PageRank, K-means, entrenamiento de ML)
  requieren múltiples rondas de Map/Reduce.
  Cada ronda: leer de disco → procesar → escribir a disco → leer de disco → ...
  Con 100 iteraciones: 100 lecturas y 100 escrituras a disco.

Limitación 2: No hay estado entre jobs
  El estado de una ronda debe escribirse a disco para ser leído en la siguiente.
  Sin memoria compartida entre etapas.

Limitación 3: Solo batch (no streaming)
  Map/Reduce de Hadoop no puede procesar datos que llegan continuamente.

Limitación 4: API de bajo nivel
  Escribir Map/Reduce en Java para operaciones simples requiere mucho código.
  
PageRank en Hadoop MapReduce: ~100 líneas de Java para cada iteración,
  más la lógica de orquestación de múltiples jobs.
PageRank en Spark: ~10 líneas de Python.
```

**Preguntas:**

1. ¿Cómo resuelve Spark la "Limitación 1" (algoritmos iterativos)?

2. ¿Qué innovación de Spark permite evitar leer/escribir a disco entre etapas?

3. ¿Cómo resuelve Flink las limitaciones 1 y 3 simultáneamente?

4. ¿Map/Reduce sigue siendo relevante en 2024? ¿Para qué casos?

5. La "Limitación 4" motivó el desarrollo de Pig Latin, Hive, y finalmente
   Spark SQL. ¿Qué tienen en común estas herramientas como solución?

**Pista:** Spark resuelve la Limitación 1 con el concepto de RDD (Resilient
Distributed Dataset): los datos pueden mantenerse en memoria entre etapas
usando `.cache()` o `.persist()`. En lugar de leer desde HDFS en cada iteración,
el RDD permanece distribuido en la memoria de los executors. Una iteración de
PageRank en Spark: ~1 segundo en memoria vs ~10 minutos en Hadoop (con I/O de disco).
Esto hace que Spark sea 10-100× más rápido para algoritmos iterativos.

---

### Ejercicio 3.5.2 — El DAG generalizado: más de dos fases

Spark generaliza Map/Reduce a un DAG (Directed Acyclic Graph) de operaciones:

```
Map/Reduce clásico:
  [datos] → Map → Shuffle → Reduce → [resultado]
  (siempre exactamente 2 fases)

Spark DAG:
  [datos] → Op1 → Op2 → Shuffle → Op3 → Shuffle → Op4 → [resultado]
  (tantas fases como necesites, con shuffles solo donde son necesarios)

Ejemplo: pipeline de e-commerce
  [ventas] ─────────────────────────────────────────────┐
                                                         join → groupBy → [resultado]
  [clientes] → filter(premium=True) → select(id, segm) ─┘
```

```python
# El mismo pipeline en Spark — el DAG se construye implícitamente:
resultado = (
    df_ventas
    .join(
        df_clientes.filter(F.col("premium") == True).select("id", "segmento"),
        df_ventas["cliente_id"] == df_clientes["id"]
    )
    .groupBy("segmento", F.month("fecha"))
    .agg(F.sum("monto").alias("revenue"))
)
resultado.explain()  # muestra el DAG
```

**Restricciones:**
1. Ejecutar el pipeline y ver el DAG en Spark UI (pestaña "SQL")
2. ¿Cuántos shuffles tiene el DAG?
3. ¿Dónde ocurriría un shuffle adicional si añades `.orderBy("revenue")`?
4. Dibujar el DAG manualmente antes de verlo en Spark UI
   y comparar con el resultado real

---

### Ejercicio 3.5.3 — Map/Reduce en streaming: el modelo de Flink

En Flink, el modelo Map/Reduce se extiende a streams continuos:

```
Map/Reduce en batch (Spark):
  [colección finita] → transformaciones → [resultado finito]
  El pipeline termina cuando se agotan los datos.

Map/Reduce en streaming (Flink):
  [stream infinito] → transformaciones → [stream de resultados]
  El pipeline corre indefinidamente.
  Los "reduces" se hacen sobre ventanas de tiempo.
  
Wordcount en streaming:
  stream de oraciones → flatMap(split) → keyBy(palabra) → window(5min) → sum
  Emite el conteo de cada palabra cada 5 minutos (ventana tumbling).
```

```python
# En PySpark Structured Streaming (micro-batching):
from pyspark.sql import functions as F

df_stream = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "textos") \
    .load()

wordcount_stream = (df_stream
    .select(F.explode(F.split(F.col("value").cast("string"), r"\s+")).alias("palabra"))
    .filter(F.col("palabra") != "")
    .withWatermark("timestamp", "1 minute")
    .groupBy(F.window("timestamp", "5 minutes"), "palabra")
    .count()
)

query = wordcount_stream.writeStream \
    .outputMode("update") \
    .format("console") \
    .start()
```

**Preguntas:**

1. ¿El `groupBy + count` en streaming hace un shuffle real entre micro-batches?
   ¿O el estado se mantiene en memoria?

2. ¿Qué pasa con el estado del wordcount si el stream lleva 24 horas corriendo?
   ¿Cuánta memoria ocupa?

3. ¿Cómo el watermark limita el crecimiento del estado?

4. En el modelo de Flink (streaming puro, no micro-batching), ¿cuándo se emite
   el resultado del wordcount de la ventana de 5 minutos?

---

### Ejercicio 3.5.4 — Map/Reduce en SQL: todo es Map/Reduce

**Tipo: Leer/analizar**

SQL es una abstracción sobre Map/Reduce. Cada cláusula SQL tiene un equivalente:

```
SQL:
  SELECT region, SUM(monto) as total
  FROM transacciones
  WHERE activo = TRUE
  GROUP BY region
  HAVING SUM(monto) > 1000000
  ORDER BY total DESC
  LIMIT 10

Equivalente en Map/Reduce:
  1. WHERE → filter (narrow, sin shuffle)
  2. SELECT columnas + GROUP BY → map a (clave, valor) pairs
  3. GROUP BY + SUM → shuffle + reduce (aquí está el shuffle)
  4. HAVING → filter post-reduce (narrow)
  5. ORDER BY → shuffle de todos los datos (para sort global)
  6. LIMIT → reduce final a N elementos
```

**Preguntas:**

1. Para cada cláusula SQL, indica si genera un shuffle:
   - `WHERE`
   - `GROUP BY`
   - `JOIN ... ON`
   - `ORDER BY`
   - `DISTINCT`
   - `WINDOW FUNCTION (OVER PARTITION BY ... ORDER BY ...)`
   - `LIMIT` (sin ORDER BY)

2. Una query con `ORDER BY` siempre genera un shuffle completo.
   ¿Hay alguna forma de implementar `ORDER BY ... LIMIT 10` con menos shuffle?

3. ¿Cómo implementa BigQuery o Redshift `ORDER BY LIMIT 10` de forma eficiente?

4. Una `WINDOW FUNCTION` como `RANK() OVER (PARTITION BY region ORDER BY monto DESC)`:
   ¿Cuántos shuffles genera?

**Pista:** `ORDER BY ... LIMIT 10` puede implementarse eficientemente como
un "tournament sort": cada nodo encuentra su top-10 local (sin shuffle),
luego el coordinador hace un merge de todos los top-10 locales (solo N×10 filas).
En lugar de ordenar TB de datos globalmente, solo se mueven N×10 filas.
Spark, Flink, y todos los motores SQL modernos hacen esta optimización.

---

### Ejercicio 3.5.5 — El modelo de Beam: unificando batch y streaming

```python
import apache_beam as beam

# En Beam, el mismo código funciona para batch y streaming.
# La diferencia está en la fuente (PCollection bounded vs unbounded).

# Wordcount en batch (archivo):
with beam.Pipeline() as p:
    resultado = (
        p
        | "Leer" >> beam.io.ReadFromText("gs://bucket/corpus/*.txt")
        | "Split" >> beam.FlatMap(str.split)
        | "Pares" >> beam.Map(lambda w: (w, 1))
        | "Contar" >> beam.CombinePerKey(sum)
        | "Escribir" >> beam.io.WriteToText("gs://bucket/wordcount")
    )

# Wordcount en streaming (Kafka):
# El mismo pipeline, diferente fuente:
with beam.Pipeline(options=streaming_options) as p:
    resultado = (
        p
        | "Leer" >> beam.io.ReadFromKafka(
            consumer_config={"bootstrap.servers": "localhost:9092"},
            topics=["textos"]
        )
        | "Split" >> beam.FlatMap(lambda msg: msg.value.decode().split())
        | "Pares" >> beam.Map(lambda w: (w, 1))
        | "Ventana" >> beam.WindowInto(beam.window.FixedWindows(300))
        | "Contar" >> beam.CombinePerKey(sum)
        | "Escribir" >> beam.io.WriteToBigQuery(...)
    )
```

**Preguntas:**

1. ¿Qué parte del pipeline de Beam corresponde al Map de Map/Reduce?
2. ¿Qué parte corresponde al Reduce?
3. ¿Dónde está el shuffle implícito en el pipeline de Beam?
4. ¿Por qué `CombinePerKey` funciona como un reduce con combiner automático?
5. ¿Qué es el "runner" de Beam y cómo decide dónde hacer el shuffle?

---

## Sección 3.6 — Map/Reduce en los Frameworks Modernos

### Ejercicio 3.6.1 — La tabla de equivalencias

**Tipo: Construir/analizar**

Completar la tabla de equivalencias entre Map/Reduce clásico y los frameworks modernos:

```
Concepto MR clásico    Spark RDD      Spark SQL/DF       Beam             Flink
──────────────────────────────────────────────────────────────────────────────────
map                    .map()         .withColumn()      beam.Map()       .map()
flatMap                .flatMap()     .select(explode()) beam.FlatMap()   .flatMap()
filter                 .filter()      .filter()          beam.Filter()    .filter()
groupByKey             .groupByKey()  .groupBy()         beam.GroupByKey  .keyBy()
reduceByKey            .reduceByKey() .agg(F.sum())      beam.Combine...  .reduce()
combiner               automático     HashAggregate(p)   CombinePerKey    .aggregate()
shuffle                Exchange       Exchange           GBK shuffle      network exchange
output                 .saveAs*       .write.*           beam.io.Write*   .addSink()
```

Para cada celda vacía, completar con la API correspondiente del framework.

---

### Ejercicio 3.6.2 — Wordcount en cinco implementaciones

Implementar wordcount en las cinco formas siguientes y comparar:

```python
# 1. Map/Reduce manual en Python puro:
def wordcount_puro(textos: list[str]) -> dict:
    # TODO

# 2. Map/Reduce con multiprocessing:
def wordcount_paralelo(textos: list[str], num_workers: int = 4) -> dict:
    # TODO

# 3. Spark RDD API (el más parecido a Map/Reduce clásico):
def wordcount_spark_rdd(textos_rdd) -> list:
    return (textos_rdd
        .flatMap(str.split)
        .map(lambda w: (w, 1))
        .reduceByKey(lambda a, b: a + b)
        .collect()
    )

# 4. Spark DataFrame API (SQL-like):
def wordcount_spark_df(df) -> object:
    return (df
        .select(F.explode(F.split("value", r"\s+")).alias("palabra"))
        .groupBy("palabra")
        .count()
    )

# 5. Polars:
def wordcount_polars(textos: list[str]) -> pl.DataFrame:
    return (pl.Series("textos", textos)
        .str.split(" ")
        .explode()
        .value_counts()
        .sort("counts", descending=True)
    )
```

**Restricciones:**
1. Implementar las cinco versiones
2. Verificar que producen el mismo resultado
3. Medir el tiempo para 1M, 10M, y 100M palabras
4. Identificar en cuál implementación está más visible el "shuffle"

---

### Ejercicio 3.6.3 — PageRank: donde Map/Reduce clásico falla y Spark gana

PageRank es el algoritmo que ilustra por qué los algoritmos iterativos
son costosos en Hadoop y eficientes en Spark:

```python
# PageRank en Spark (simplificado):
def pagerank_spark(links_rdd, num_iteraciones=10):
    """
    links_rdd: RDD de (url, [url_destino_1, url_destino_2, ...])
    """
    # Inicializar ranks:
    ranks = links_rdd.map(lambda url_links: (url_links[0], 1.0))

    for _ in range(num_iteraciones):
        # Calcular contribuciones:
        contribs = links_rdd.join(ranks).flatMap(
            lambda url_links_rank: [
                (dest, url_links_rank[1][1] / len(url_links_rank[1][0]))
                for dest in url_links_rank[1][0]
            ]
        )
        # Actualizar ranks:
        ranks = contribs.reduceByKey(lambda a, b: a + b).mapValues(
            lambda rank: 0.15 + 0.85 * rank
        )

    return ranks.collect()
```

**Preguntas:**

1. ¿Cuántos shuffles hay en cada iteración de PageRank?

2. ¿Por qué es crítico hacer `.cache()` sobre `links_rdd`?

3. En Hadoop MapReduce, ¿cuántas lecturas/escrituras a disco
   habría para 10 iteraciones?

4. ¿Por qué Spark puede mantener `ranks` en memoria entre iteraciones?

5. ¿Cómo el grafo de PageRank afecta el data skew?
   (pistas: considera urls muy enlazadas vs urls poco enlazadas)

---

### Ejercicio 3.6.4 — Map/Reduce en Polars: sin shuffle explícito

Polars implementa el mismo modelo pero sin la complejidad del shuffle
distribuido — todo ocurre en una sola máquina:

```python
import polars as pl

df = pl.read_parquet("transacciones.parquet")

# Equivalente a Map + GroupBy/Reduce:
resultado = (df
    .lazy()                                        # MAP: lazy evaluation
    .filter(pl.col("activo") == True)              # filter (narrow)
    .with_columns(                                  # map
        (pl.col("monto") * 1.19).alias("monto_iva")
    )
    .group_by("region")                            # GroupBy (shuffle en Spark,
    .agg(                                           # en memoria en Polars)
        pl.sum("monto_iva").alias("total_iva"),
        pl.count().alias("transacciones"),
    )
    .sort("total_iva", descending=True)            # sort
    .collect()                                     # REDUCE: ejecutar el plan
)
```

**Preguntas:**

1. ¿Polars hace un "shuffle" interno para el `group_by`?
   ¿Qué estructura de datos usa en su lugar?

2. ¿Por qué Polars puede ser más rápido que Spark para el mismo job
   si los datos caben en una máquina?

3. ¿Cuándo Polars alcanza su límite y necesitas Spark?

4. `.lazy()` en Polars equivale a ¿qué en Spark?

---

### Ejercicio 3.6.5 — Leer: el paper que cambió todo

**Tipo: Leer/analizar**

Leer la sección 2 y 3 del paper original de MapReduce (Dean & Ghemawat, 2004).

> 📖 Profundizar: *MapReduce: Simplified Data Processing on Large Clusters*,
> Dean & Ghemawat, OSDI 2004. La Sección 2 describe el modelo de programación
> (2 páginas). La Sección 3 describe la implementación incluyendo el shuffle
> (3 páginas). Total: 5 páginas suficientes para entender el paper.

**Preguntas basadas en el paper:**

1. El paper describe que el combiner es opcional. ¿Bajo qué condición
   el paper dice que puede usarse un combiner?

2. El paper describe cómo Google usó Map/Reduce internamente.
   ¿Cuál de sus casos de uso te parece más relevante para data engineering moderno?

3. El paper es de 2004. ¿Qué limitaciones del diseño original
   se volvieron evidentes en los años siguientes?

4. El paper describe que las tareas lentas ("stragglers") son un problema.
   ¿Cómo lo resuelve? ¿Los frameworks modernos usan la misma solución?

5. ¿Qué parte del diseño original de Map/Reduce persiste sin cambios
   en Spark, Beam, y Flink?

**Pista:** Para la pregunta 4: el paper describe el "backup task" o "speculative execution".
Cuando una tarea está tardando más que el promedio (un "straggler"), el sistema
lanza una copia idéntica en otro nodo. Cuando cualquiera de las dos termina,
la otra se cancela. Spark implementa esto con `spark.speculation=true`.
La misma solución persiste 20 años después porque el problema es el mismo:
en un cluster grande, siempre habrá alguna máquina más lenta que las demás.

---

## Sección 3.7 — Cuándo Map/Reduce No Es Suficiente

### Ejercicio 3.7.1 — Algoritmos que no encajan en Map/Reduce

**Tipo: Analizar**

No todos los algoritmos son expresables eficientemente como Map/Reduce.
Para cada algoritmo, evaluar si Map/Reduce es una buena solución:

```
1. Ordenamiento global de 1 TB de datos

2. Búsqueda de camino más corto (Dijkstra) en un grafo de 1B nodos

3. Entrenamiento de una red neuronal (gradient descent iterativo)

4. Cálculo de percentiles exactos sobre 1B valores

5. Detección de fraude en tiempo real (< 100ms por transacción)

6. Joins entre múltiples tablas con dependencias circulares

7. Compresión de video (requiere procesamiento secuencial de frames)

8. Generación de texto con un modelo de lenguaje
```

Para cada uno: ¿es Map/Reduce suficiente? Si no, ¿qué paradigma es más apropiado?

---

### Ejercicio 3.7.2 — Graph processing: más allá de Map/Reduce

El procesamiento de grafos requiere comunicación entre nodos que no sigue
el patrón clave/valor de Map/Reduce:

```python
# Detección de componentes conectados en un grafo:
# Cada nodo necesita saber el estado de sus vecinos,
# que pueden estar en cualquier partición.

# En Spark GraphX (librería de grafos):
from pyspark.sql import SparkSession
from graphframes import GraphFrame

# Crear el grafo:
vertices = spark.createDataFrame([
    ("u1", "Alice"), ("u2", "Bob"), ("u3", "Charlie")
], ["id", "name"])

edges = spark.createDataFrame([
    ("u1", "u2", "follows"), ("u2", "u3", "follows")
], ["src", "dst", "relationship"])

grafo = GraphFrame(vertices, edges)

# Algoritmo: PageRank (ya lo vimos)
pagerank = grafo.pageRank(resetProbability=0.15, maxIter=10)

# Algoritmo: Connected Components
componentes = grafo.connectedComponents()
```

**Preguntas:**

1. ¿Por qué el procesamiento de grafos es difícil de expresar en Map/Reduce?

2. ¿Qué es el modelo BSP (Bulk Synchronous Parallel) y cómo difiere de Map/Reduce?

3. ¿Cuántos shuffles hay por iteración de Connected Components?

4. ¿Para qué tamaño de grafo Spark GraphX es viable?
   ¿Cuándo necesitas una herramienta especializada (Neo4j, TigerGraph)?

> 🔗 Ecosistema: para grafos muy grandes (>10B de aristas), los frameworks
> especializados como GraphX de Spark, Apache Giraph, o bases de datos de
> grafos como TigerGraph son más apropiados que Map/Reduce general.
> No se cubren en este repositorio.

---

### Ejercicio 3.7.3 — Machine learning distribuido: el límite de Map/Reduce

```python
# Gradient Descent en Map/Reduce (simplificado):
# Cada iteración es un Map/Reduce completo:

def entrenar_modelo_mr(datos, num_iteraciones=100):
    parametros = inicializar_parametros()

    for i in range(num_iteraciones):
        # MAP: calcular gradientes en paralelo
        gradientes_parciales = datos.map(
            lambda punto: calcular_gradiente(punto, parametros)
        )

        # REDUCE: promediar gradientes
        gradiente_promedio = gradientes_parciales.reduce(
            lambda g1, g2: sumar_gradientes(g1, g2)
        )
        gradiente_promedio /= len(datos)

        # Actualizar parámetros (en el driver):
        parametros = actualizar_parametros(parametros, gradiente_promedio)

    return parametros
```

**Preguntas:**

1. ¿Dónde están los parámetros del modelo durante el entrenamiento?
   (en el driver, en los workers, o en ambos)

2. ¿Qué pasa si el modelo tiene 10B parámetros (ej: un LLM)?
   ¿Caben en la memoria del driver?

3. ¿Qué es el "parameter server" y cómo resuelve la limitación de los parámetros?

4. ¿En qué se diferencia este approach de cómo PyTorch distribuye el entrenamiento?

5. ¿Map/Reduce es el paradigma correcto para entrenar LLMs?

**Pista:** Para modelos grandes, los parámetros no caben en el driver.
El "parameter server" distribuye los parámetros entre múltiples nodos.
Los workers calculan gradientes y los envían al parameter server,
que actualiza los parámetros y los distribuye de vuelta.
PyTorch usa "Distributed Data Parallel" (DDP) que replica el modelo en
cada GPU y sincroniza los gradientes con All-Reduce (un shuffle especializado).
Map/Reduce general es demasiado ineficiente para esto — la sincronización
de gradientes necesita primitivas de comunicación colectiva (All-Reduce, Broadcast)
que no están en el modelo básico de Map/Reduce.

---

### Ejercicio 3.7.4 — El límite del modelo: cuando necesitas algo distinto

**Tipo: Diseñar**

Para cada caso de uso, determinar si Map/Reduce (o sus derivados Spark/Beam/Flink)
es la herramienta apropiada o si necesitas algo diferente:

```
Caso 1: Procesar 10 TB de logs para extraer métricas de error por servicio.
Caso 2: Recomendar productos a usuarios con < 50ms de latencia.
Caso 3: Detectar anomalías en series temporales de sensores IoT.
Caso 4: Entrenar un modelo de clasificación de imágenes con 1B de parámetros.
Caso 5: Calcular el grafo de dependencias de 1M de paquetes npm.
Caso 6: Procesar transacciones bancarias con exactamente-una-vez.
Caso 7: Buscar documentos similares en una colección de 10M de textos.
Caso 8: Orquestar un workflow de 50 pasos con dependencias complejas.
```

Para cada caso: herramienta recomendada (puede ser Map/Reduce o no)
y justificación en una oración.

---

### Ejercicio 3.7.5 — El modelo mental completo: conectando todo

**Tipo: Integrar**

Este es el ejercicio de cierre de la Parte 1 del repositorio.
Tres capítulos después del Cap.01, el modelo mental está completo.

Retomando la tabla del Cap.01 (§1.5.4), completar ahora con mayor detalle:

```
Concepto de concurrencia → Equivalente en data engineering

Goroutine leak          → Consumer lag creciente
                          (los mensajes se acumulan porque el consumer
                           no puede mantener el ritmo — el equivalente
                           de una goroutine que produce más rápido
                           de lo que el canal puede consumir)

Race condition          → ???
                          (dos jobs escriben al mismo destino
                           sin coordinación — ¿qué pasa?)

Deadlock               → ???
                          (dos stages de Spark esperando datos
                           del otro — ¿puede ocurrir?)

Circuit breaker        → Backpressure en streaming
                          (cuando el consumer no puede seguir el ritmo,
                           ralentiza al producer en lugar de fallar)

Exactly-once           → Exactly-once en Kafka/Flink
                          (mucho más difícil de garantizar en distribuido
                           porque el fallo puede ocurrir entre el
                           procesamiento y el ack)
```

Y la pregunta de cierre:

> El wordcount de Map/Reduce, el pipeline de Spark, el stream de Flink, y
> el DAG de Beam son todos el mismo problema visto con distintos lentes.
>
> ¿Cuál es ese problema?

---

## Resumen del capítulo

**Las cinco ideas que este capítulo debería haber dejado claras:**

```
1. Map es paralelismo perfecto
   Cualquier transformación donde cada elemento es independiente
   puede ejecutarse en paralelo sin coordinación.
   En Spark: narrow transformations. En SQL: WHERE, SELECT.

2. Reduce requiere coordinación (shuffle)
   Para combinar elementos relacionados, necesitas moverlos al mismo nodo.
   El shuffle es el costo inevitable de cualquier GROUP BY, JOIN, ORDER BY.
   Minimizar el shuffle = maximizar el rendimiento.

3. El combiner es pre-reduce local
   Si la función de reduce es asociativa, puedes reducir antes del shuffle.
   reduceByKey() > groupByKey(). partial_sum > sum completo.
   Ahorra tráfico de red sin cambiar el resultado.

4. Todos los frameworks modernos son Map/Reduce generalizado
   Spark, Beam, Flink, Kafka Streams — todos implementan el mismo paradigma
   con distintos tradeoffs de latencia, estado, y garantías.
   El DAG reemplaza las dos fases fijas. El shuffle sigue siendo el centro.

5. Map/Reduce tiene límites
   Grafos, ML distribuido, streaming de baja latencia — requieren
   primitivas adicionales (BSP, All-Reduce, estado persistente).
   Conocer los límites es tan importante como conocer el modelo.
```

**La cadena de causalidad que conecta los tres primeros capítulos:**

```
Cap.01: El framework controla el paralelismo → tú describes QUÉ
  ↓
Cap.02: Los datos en disco son columnar (Parquet) → reduce el I/O
        Los datos en memoria son Arrow → permite operaciones SIMD
        El formato determina cuánto trabajo hay antes del código
  ↓
Cap.03: El trabajo se divide en Map (paralelo) y Reduce (shuffle)
        El shuffle es el cuello de botella
        Minimizar el shuffle = maximizar el rendimiento del pipeline
  ↓
Cap.04: Spark implementa este modelo con particiones, stages, y shuffles
        El Spark UI muestra exactamente dónde está el shuffle y cuánto cuesta
```

Esa cadena es el mapa conceptual del repositorio.
Los siguientes capítulos son implementaciones concretas de ese mapa.
