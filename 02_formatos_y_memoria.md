# Guía de Ejercicios — Cap.02: Formatos y Representación en Memoria

> Este capítulo cubre la capa que todos los frameworks comparten
> pero que casi nadie enseña explícitamente: cómo los datos se representan
> en disco y en memoria, y por qué esa representación determina
> el rendimiento antes de que el framework entre en juego.
>
> Un pipeline lento por formato incorrecto no se arregla con más cores.
> Se arregla eligiendo el formato correcto.

---

## Por qué los formatos importan más de lo que parece

Considera dos archivos con exactamente los mismos datos:

```
ventas.csv (row-oriented, sin compresión):
  user_id,producto_id,monto,region,fecha
  1001,5023,150.00,norte,2024-01-15
  1002,5024,89.50,sur,2024-01-15
  ...
  Tamaño: 10 GB
  Tiempo de lectura para "suma de monto por region": 45 segundos

ventas.parquet (column-oriented, con compresión):
  [mismo contenido, formato binario columnar]
  Tamaño: 800 MB
  Tiempo de lectura para "suma de monto por region": 2 segundos
```

La diferencia no es el framework ni el hardware — es el formato.

El CSV lee 10 GB para extraer solo la columna `monto` y `region`.
El Parquet lee ~160 MB (solo las dos columnas necesarias, comprimidas).

Este capítulo explica por qué, con números reales.

---

## El modelo mental: row vs columnar

```
ROW-ORIENTED (CSV, JSON, Avro, bases de datos OLTP):
  Fila 1: [user_id=1001, producto_id=5023, monto=150.00, region=norte]
  Fila 2: [user_id=1002, producto_id=5024, monto=89.50,  region=sur]
  Fila 3: [user_id=1003, producto_id=5023, monto=220.00, region=norte]

  En disco: todos los campos de fila 1, luego todos los de fila 2...
  Para leer "monto de todas las filas": debes leer TODOS los bytes
  (user_id, producto_id, y region que no necesitas) para llegar a monto.

COLUMNAR (Parquet, ORC, Arrow):
  Columna user_id:     [1001, 1002, 1003, ...]
  Columna producto_id: [5023, 5024, 5023, ...]
  Columna monto:       [150.00, 89.50, 220.00, ...]
  Columna region:      [norte, sur, norte, ...]

  En disco: todos los valores de user_id, luego todos de producto_id...
  Para leer "monto de todas las filas": lees SOLO la columna monto.
  user_id, producto_id, y region ni se tocan.
```

---

## Tabla de contenidos

- [Sección 2.1 — Row vs columnar: la diferencia con números](#sección-21--row-vs-columnar-la-diferencia-con-números)
- [Sección 2.2 — Parquet: el formato estándar de data engineering](#sección-22--parquet-el-formato-estándar-de-data-engineering)
- [Sección 2.3 — Apache Arrow: el formato en memoria](#sección-23--apache-arrow-el-formato-en-memoria)
- [Sección 2.4 — Compresión: el tradeoff CPU vs I/O](#sección-24--compresión-el-tradeoff-cpu-vs-io)
- [Sección 2.5 — Formatos para casos específicos](#sección-25--formatos-para-casos-específicos)
- [Sección 2.6 — Schema evolution: cuando el formato cambia](#sección-26--schema-evolution-cuando-el-formato-cambia)
- [Sección 2.7 — El sistema de e-commerce: decidir los formatos](#sección-27--el-sistema-de-e-commerce-decidir-los-formatos)

---

## Sección 2.1 — Row vs Columnar: la Diferencia con Números

### Ejercicio 2.1.1 — Medir la diferencia de lectura selectiva

**Tipo: Medir**

El beneficio del formato columnar es más grande cuando lees pocas columnas
de un dataset con muchas.

```python
import polars as pl
import pandas as pd
import time
import os

# Generar un dataset de prueba con 10 columnas
# (solo 2 de las cuales se usan en las queries)
def generar_dataset(n_filas: int, ruta_base: str):
    import numpy as np
    rng = np.random.default_rng(42)

    df = pl.DataFrame({
        "user_id":     rng.integers(1, 1_000_000, n_filas),
        "producto_id": rng.integers(1, 100_000, n_filas),
        "monto":       rng.uniform(1, 10_000, n_filas),
        "region":      rng.choice(["norte", "sur", "este", "oeste"], n_filas),
        "canal":       rng.choice(["web", "mobile", "tienda"], n_filas),
        "descuento":   rng.uniform(0, 0.5, n_filas),
        "proveedor_id": rng.integers(1, 500, n_filas),
        "categoria":   rng.choice(["A", "B", "C", "D", "E"], n_filas),
        "pais":        rng.choice(["MX", "CO", "AR", "CL", "PE"], n_filas),
        "timestamp":   rng.integers(1_700_000_000, 1_710_000_000, n_filas),
    })

    # Guardar en distintos formatos:
    df.write_csv(f"{ruta_base}/datos.csv")
    df.write_parquet(f"{ruta_base}/datos.parquet", compression="snappy")
    df.write_parquet(f"{ruta_base}/datos_zstd.parquet", compression="zstd")

    return df

# La query: suma de monto por region (2 columnas de 10)
def query_suma_region_csv(ruta: str) -> float:
    inicio = time.perf_counter()
    df = pd.read_csv(ruta, usecols=["monto", "region"])
    resultado = df.groupby("region")["monto"].sum()
    return time.perf_counter() - inicio

def query_suma_region_parquet(ruta: str) -> float:
    inicio = time.perf_counter()
    df = pl.read_parquet(ruta, columns=["monto", "region"])
    resultado = df.group_by("region").agg(pl.sum("monto"))
    return time.perf_counter() - inicio
```

**Restricciones:**
1. Generar el dataset con 10M filas y medir el tamaño en disco de cada formato
2. Medir el tiempo de la query "suma de monto por region" en CSV y Parquet
3. Repetir con una query que usa las 10 columnas: ¿la ventaja de Parquet se reduce?
4. Medir cuántos bytes se leen del disco en cada caso
   (usar `df.estimated_size()` en Polars o `os.path.getsize` como proxy)
5. Calcular el "speedup" de Parquet sobre CSV para cada query

**Pista:** Para medir bytes leídos del disco (no del cache),
usar `sync && echo 3 > /proc/sys/vm/drop_caches` en Linux antes de cada medición.
En macOS: `sudo purge`. Si no tienes acceso root, ejecutar cada benchmark en
un proceso nuevo para minimizar el cache del SO.

---

### Ejercicio 2.1.2 — El modelo físico del almacenamiento columnar

**Tipo: Leer**

Parquet no simplemente "guarda las columnas una después de la otra".
Tiene una estructura más sofisticada que permite varios tipos de optimización:

```
Estructura de un archivo Parquet:
  ┌──────────────────────────────────────────┐
  │  Magic bytes: PAR1                        │
  ├──────────────────────────────────────────┤
  │  Row Group 0 (ej: primeras 100,000 filas) │
  │  ├── Column Chunk: user_id               │
  │  │   ├── Page 0: [1001, 1002, ..., 1128] │
  │  │   ├── Page 1: [1129, 1130, ..., 1256] │
  │  │   └── ...                             │
  │  ├── Column Chunk: monto                 │
  │  │   ├── Statistics: min=1.0, max=9999.8 │
  │  │   ├── Page 0: [150.0, 89.5, ...]      │
  │  │   └── ...                             │
  │  └── ...más column chunks...             │
  ├──────────────────────────────────────────┤
  │  Row Group 1 (siguientes 100,000 filas)   │
  │  └── ...                                 │
  ├──────────────────────────────────────────┤
  │  Footer (metadata de todo el archivo)     │
  │  - Schema (tipos de cada columna)        │
  │  - Estadísticas por row group y columna  │
  │  - Offsets en disco de cada column chunk │
  └──────────────────────────────────────────┘
  │  Magic bytes: PAR1                        │
  └──────────────────────────────────────────┘
```

**Preguntas:**

1. El footer está al **final** del archivo, no al principio.
   ¿Por qué esta decisión de diseño? ¿Qué implicación tiene para leer
   un archivo Parquet desde S3?

2. Las "statistics" por row group incluyen `min` y `max` de cada columna.
   ¿Cómo usa Spark estas estadísticas para evitar leer row groups completos?

3. Si tienes un archivo Parquet con 10 row groups y haces un filtro
   `monto > 5000`, y las estadísticas muestran que 8 de los 10 row groups
   tienen `max(monto) < 5000`, ¿cuántos row groups se leen?

4. ¿Cuál es el tamaño óptimo de un row group? ¿Por qué importa?

5. ¿Qué ventaja tiene dividir los datos en múltiples archivos Parquet
   en lugar de un único archivo grande?

**Pista:** Para la pregunta 1: al escribir un archivo Parquet de forma streaming
(sin saber el tamaño final), el writer no puede escribir el footer hasta
que termina de escribir los datos. Colocarlo al final es la única opción
práctica. Para leer desde S3: la herramienta necesita hacer dos requests —
primero leer el footer (últimos N bytes), luego leer los column chunks necesarios.
Para archivos muy grandes, este segundo request puede ser a una posición muy
específica del archivo (byte range request).

> 📖 Profundizar: la especificación completa de Parquet está en
> `github.com/apache/parquet-format`. El README del repo explica
> la estructura lógica con más detalle que cualquier tutorial.
> Vale leer las primeras 30 páginas antes del Cap.04.

---

### Ejercicio 2.1.3 — Dictionary encoding: por qué las columnas de baja cardinalidad comprimen tan bien

**Tipo: Leer/calcular**

Una de las razones por las que Parquet comprime tan bien es el
**dictionary encoding**: para columnas con pocos valores únicos
(baja cardinalidad), almacena un diccionario de valores y luego
usa índices enteros en lugar de los valores completos.

```
Columna "region" sin encoding (raw):
  "norte", "sur", "norte", "este", "norte", "sur", "oeste", "norte", ...
  Almacenamiento: 5-6 bytes por valor × 1,000,000 filas = ~5 MB

Columna "region" con dictionary encoding:
  Diccionario: {0: "norte", 1: "sur", 2: "este", 3: "oeste"}
  Datos:        [0, 1, 0, 2, 0, 1, 3, 0, ...]
  Almacenamiento:
    Diccionario: 4 valores × ~6 bytes = 24 bytes (despreciable)
    Datos: 1,000,000 × 2 bits (4 valores → 2 bits suficientes) = 250 KB
  
  Compresión: de 5 MB a 250 KB = ratio 20:1
```

**Preguntas:**

1. Una columna `pais` tiene 250 valores únicos (ISO country codes, 2 chars).
   ¿Cuántos bits por valor necesita el dictionary encoding?
   ¿Cuál es el ratio de compresión aproximado vs almacenar raw?

2. Una columna `user_id` tiene 10 millones de valores únicos.
   ¿Tiene sentido aplicar dictionary encoding? ¿Por qué?

3. ¿Hay un punto donde el dictionary ocupa tanto espacio que
   ya no vale la pena usarlo? ¿Cómo decide Parquet?

4. Una columna `descripcion` (texto libre, 50 chars promedio, alta cardinalidad).
   ¿Qué encoding de Parquet la comprimirá mejor?

5. Calcular el tamaño estimado en Parquet de la columna `region`
   del dataset del Ejercicio 2.1.1 (10M filas, 4 valores únicos)
   con y sin dictionary encoding.

**Pista:** Para la pregunta 3: Parquet usa dictionary encoding por defecto
para columnas con cardinalidad baja. Cuando el diccionario supera un tamaño
umbral (por defecto ~1 MB en los writers más comunes), cambia automáticamente
a plain encoding (sin diccionario). El umbral exacto depende del writer
(PyArrow, Spark, etc.) y puede configurarse.

---

### Ejercicio 2.1.4 — Run-length encoding: el encoding ideal para datos ordenados

**Tipo: Leer/calcular**

El **Run-Length Encoding (RLE)** es eficiente cuando hay muchos valores
consecutivos iguales:

```
Datos sin encoding:
  [norte, norte, norte, norte, norte, sur, sur, sur, ...]
  Con dictionary: [0, 0, 0, 0, 0, 1, 1, 1, ...]

Con RLE:
  [(0, 5), (1, 3), ...]  = "5 veces el valor 0, luego 3 veces el valor 1"
  
  Si los datos están ordenados por región:
    norte: 250,000 filas → (0, 250000) = 1 registro
    sur:   250,000 filas → (1, 250000) = 1 registro
    este:  250,000 filas → (2, 250000) = 1 registro
    oeste: 250,000 filas → (3, 250000) = 1 registro
    
    Total: 4 registros para 1,000,000 filas = compresión masiva
```

**Preguntas:**

1. Si los datos NO están ordenados por región (mezcla aleatoria),
   ¿funciona bien el RLE? ¿Por qué?

2. Un pipeline escribe datos particionados por `fecha` y `region`.
   ¿Cómo afecta esto a la efectividad del RLE en la columna `region`?

3. Además de la columna `region`, ¿qué otras columnas del dataset
   del Ejercicio 2.1.1 se beneficiarían del RLE si los datos están
   ordenados por ellas?

4. El comando `df.sort("region").write_parquet(...)` en Polars:
   ¿cómo afecta al tamaño del archivo resultante?

5. ¿Hay un costo en el pipeline por ordenar los datos antes de escribir?
   ¿Cuándo vale la pena ese costo?

**Pista:** Para la pregunta 5: ordenar 1 TB de datos antes de escribir
puede añadir 10-30 minutos al pipeline pero reducir el tamaño del archivo
en un factor de 2-5×, y reducir el tiempo de lectura de queries futuras
en un factor similar. Si el archivo se lee 1,000 veces durante su vida útil,
el costo de ordenar una vez amortizado es despreciable. Si se lee solo 1-2 veces,
el tradeoff puede no valer la pena.

---

### Ejercicio 2.1.5 — Leer: diagnosticar un archivo Parquet mal escrito

**Tipo: Diagnosticar**

Un data engineer reporta que su archivo Parquet de 5 GB tarda más en leerse
que el CSV equivalente de 40 GB.

Información disponible:

```python
import pyarrow.parquet as pq

metadata = pq.read_metadata("ventas.parquet")
print(metadata)

# Output:
# <pyarrow._parquet.FileMetaData object at 0x...>
#   created_by: parquet-mr version 1.12.0
#   num_columns: 12
#   num_rows: 100000000
#   num_row_groups: 50000         ← !!
#   format_version: 1.0
#   serialized_size: 8422314

# Estadísticas del primer row group:
print(metadata.row_group(0).num_rows)  # → 2000
print(metadata.row_group(0).total_byte_size)  # → 102400 (100 KB)
```

**Preguntas:**

1. ¿Por qué 50,000 row groups para 100M filas es problemático?

2. El tamaño de cada row group es ~100 KB. ¿Cuál es el tamaño "correcto"
   para un row group de Parquet?

3. ¿Qué operación en el pipeline produjo este resultado?
   (pista: `num_rows = 2000` por row group es una pista sobre el batch size)

4. ¿Por qué este archivo es más lento de leer que el CSV equivalente?
   (pista: piensa en el número de operaciones de I/O)

5. ¿Cómo lo arreglarías? ¿Con qué herramienta?

**Pista:** 50,000 row groups significa 50,000 accesos separados al archivo
(uno por row group) para leer una columna completa. Cada acceso tiene overhead
de seek + read de la metadata del row group antes de leer los datos.
El CSV, aunque más grande, es un stream secuencial — un solo acceso.
El origen: el pipeline escribió Parquet en batches de 2,000 filas
(quizás un loop en Python que escribe batch a batch en lugar de escribir todo de una vez).
El fix: reescribir el archivo con row groups de 100,000–1,000,000 filas.
Con PyArrow: `pq.write_table(table, "output.parquet", row_group_size=500_000)`.

---

## Sección 2.2 — Parquet: el Formato Estándar de Data Engineering

### Ejercicio 2.2.1 — Escribir y leer Parquet correctamente

**Tipo: Implementar**

```python
import pyarrow as pa
import pyarrow.parquet as pq
import polars as pl
from pathlib import Path

# Schema explícito (mejor que inferir — evita sorpresas en producción):
schema = pa.schema([
    pa.field("user_id",     pa.int64(),   nullable=False),
    pa.field("producto_id", pa.int32(),   nullable=False),
    pa.field("monto",       pa.float64(), nullable=False),
    pa.field("region",      pa.dictionary(pa.int8(), pa.utf8()), nullable=False),
    pa.field("fecha",       pa.date32(),  nullable=False),
    pa.field("descripcion", pa.utf8(),    nullable=True),
])

# Escribir con configuración explícita:
def escribir_parquet_correcto(df: pl.DataFrame, ruta: str):
    tabla = df.to_arrow()
    pq.write_table(
        tabla,
        ruta,
        schema=schema,
        row_group_size=500_000,        # ~100-500K filas por row group
        compression="zstd",            # mejor ratio que snappy
        compression_level=3,           # equilibrio compresión/velocidad
        write_statistics=True,         # habilitar predicate pushdown
        use_dictionary=True,           # dictionary encoding para baja cardinalidad
        data_page_size=1_048_576,      # 1 MB por page
    )

# Leer solo las columnas necesarias:
def leer_parquet_selectivo(ruta: str, columnas: list[str]) -> pl.DataFrame:
    return pl.read_parquet(ruta, columns=columnas)

# Leer con filtro (predicate pushdown):
def leer_parquet_filtrado(ruta: str, region: str) -> pl.DataFrame:
    return pl.read_parquet(
        ruta,
        filters=[("region", "==", region)]  # pushdown al nivel de row group
    )
```

**Restricciones:**
1. Implementar `escribir_parquet_correcto` y verificar la metadata con `pq.read_metadata`
2. Verificar que `leer_parquet_filtrado` es más rápido que leer todo y filtrar después
3. Comparar el tamaño del archivo con `compression="snappy"` vs `"zstd"` vs `"gzip"`
4. ¿Qué pasa si escribes con un schema y lees sin especificarlo?

**Pista:** Para la pregunta 4: PyArrow lee el schema del footer del archivo —
no necesitas especificarlo al leer. Pero si escribes con `pa.dictionary` para
la columna `region` y luego lees con Pandas, Pandas puede retornar la columna
como `CategoricalDtype` en lugar de `object`. Esto puede causar sorpresas
en código que asume que las columnas de texto son siempre `object`.

---

### Ejercicio 2.2.2 — Predicate pushdown: medir el beneficio real

**Tipo: Medir**

```python
import pyarrow.parquet as pq
import polars as pl
import time

# Escribir un dataset particionado donde las estadísticas son útiles:
# Datos ordenados por fecha → el predicate pushdown es muy efectivo
def preparar_datos_ordenados(ruta: str, n_filas: int = 10_000_000):
    import numpy as np
    rng = np.random.default_rng(42)

    df = pl.DataFrame({
        "fecha":  pl.date_range(
            pl.date(2023, 1, 1), pl.date(2023, 12, 31),
            interval="1s", eager=True
        ).sample(n_filas, with_replacement=True, seed=42),
        "monto":  rng.uniform(0, 10_000, n_filas),
        "region": rng.choice(["norte", "sur", "este", "oeste"], n_filas),
    }).sort("fecha")  # ← ordenar activa el RLE y mejora el pushdown

    df.write_parquet(ruta, statistics=True, row_group_size=100_000)

# Medir con pushdown:
def leer_con_pushdown(ruta: str, fecha_min: str, fecha_max: str) -> tuple:
    inicio = time.perf_counter()
    df = pl.read_parquet(
        ruta,
        filters=[
            ("fecha", ">=", fecha_min),
            ("fecha", "<=", fecha_max),
        ]
    )
    return time.perf_counter() - inicio, len(df)

# Medir sin pushdown (leer todo, luego filtrar):
def leer_sin_pushdown(ruta: str, fecha_min: str, fecha_max: str) -> tuple:
    inicio = time.perf_counter()
    df = pl.read_parquet(ruta)  # lee todo
    df = df.filter(
        (pl.col("fecha") >= fecha_min) & (pl.col("fecha") <= fecha_max)
    )
    return time.perf_counter() - inicio, len(df)
```

**Restricciones:**
1. Medir para una query que selecciona el 1% de los datos
2. Medir para una query que selecciona el 50% de los datos
3. ¿El pushdown sigue siendo útil cuando la selectividad es baja (50%)?
4. Repetir el experimento con datos NO ordenados por fecha:
   ¿cambia el beneficio del pushdown? ¿Por qué?

**Pista:** Con datos ordenados por fecha, los row groups contienen rangos
de fechas que no se solapan — el pushdown puede saltar row groups completos
con solo mirar el `min` y `max` del footer. Con datos desordenados, cada
row group puede contener cualquier fecha — el pushdown no puede saltar
ningún row group (todos tienen el mismo rango estadístico aprox.).

---

### Ejercicio 2.2.3 — Leer metadata sin leer datos

**Tipo: Implementar**

Una de las ventajas del footer de Parquet: puedes obtener información
valiosa sobre el archivo sin leer ni un byte de datos.

```python
import pyarrow.parquet as pq

def inspeccionar_parquet(ruta: str):
    """
    Retorna información sobre un archivo Parquet
    leyendo solo el footer (sin cargar datos).
    """
    metadata = pq.read_metadata(ruta)
    schema   = pq.read_schema(ruta)

    print(f"Filas totales:    {metadata.num_rows:,}")
    print(f"Row groups:       {metadata.num_row_groups}")
    print(f"Columnas:         {metadata.num_columns}")
    print(f"Creado con:       {metadata.created_by}")
    print(f"Schema:")
    for i, campo in enumerate(schema):
        print(f"  {campo.name}: {campo.type}")

    # Estadísticas por row group:
    print("\nEstadísticas por row group (primeros 3):")
    for rg_idx in range(min(3, metadata.num_row_groups)):
        rg = metadata.row_group(rg_idx)
        print(f"\n  Row Group {rg_idx}:")
        print(f"    Filas: {rg.num_rows:,}")
        print(f"    Tamaño: {rg.total_byte_size / 1024:.1f} KB")
        for col_idx in range(metadata.num_columns):
            col = rg.column(col_idx)
            stats = col.statistics
            if stats and stats.has_min_max:
                print(f"    {col.path_in_schema}: "
                      f"min={stats.min}, max={stats.max}, "
                      f"nulls={stats.null_count}")
```

**Restricciones:**
1. Implementar `inspeccionar_parquet` completo
2. Añadir una función `estimar_selectividad(ruta, columna, valor_min, valor_max)`
   que usa las estadísticas para estimar qué porcentaje de los datos
   satisface el filtro, sin leer los datos
3. Implementar `verificar_integridad(ruta)` que detecta row groups
   demasiado pequeños (< 10K filas) o demasiado grandes (> 2M filas)

**Pista:** La función `estimar_selectividad` cuenta cuántos row groups
tienen su `[min, max]` solapado con `[valor_min, valor_max]` y divide
por el total de row groups. Es una estimación — puede sobreestimar
(algunos row groups tienen el rango correcto pero pocas filas que califican)
pero nunca subestima (si el row group no solapa, definitivamente no tiene
filas que califican).

---

### Ejercicio 2.2.4 — Parquet en particiones: organizar para las queries

**Tipo: Implementar**

```python
import polars as pl
from pathlib import Path

def escribir_particionado(df: pl.DataFrame, ruta_base: str,
                          partition_cols: list[str]):
    """
    Escribe un DataFrame particionado en Hive-style:
    ruta_base/col1=val1/col2=val2/part-0001.parquet
    """
    for keys, grupo in df.group_by(partition_cols):
        # Construir el path de la partición:
        ruta_particion = Path(ruta_base)
        for col, val in zip(partition_cols, keys):
            ruta_particion = ruta_particion / f"{col}={val}"
        ruta_particion.mkdir(parents=True, exist_ok=True)

        # Escribir el grupo (sin las columnas de partición — ya están en el path):
        grupo_sin_cols = grupo.drop(partition_cols)
        grupo_sin_cols.write_parquet(
            ruta_particion / "part-0001.parquet"
        )

# Leer con partition pruning:
def leer_particionado(ruta_base: str, filtros: dict) -> pl.DataFrame:
    """
    Lee solo las particiones que satisfacen los filtros.
    filtros: {"region": "norte", "año": 2024}
    """
    ruta = Path(ruta_base)
    partes = []

    for filtro_col, filtro_val in filtros.items():
        ruta = ruta / f"{filtro_col}={filtro_val}"

    # Leer todos los archivos en la ruta filtrada:
    for archivo in ruta.rglob("*.parquet"):
        df = pl.read_parquet(archivo)
        # Añadir las columnas de partición de vuelta:
        for parte in archivo.parents:
            if "=" in parte.name:
                col, val = parte.name.split("=", 1)
                df = df.with_columns(pl.lit(val).alias(col))
        partes.append(df)

    return pl.concat(partes) if partes else pl.DataFrame()
```

**Restricciones:**
1. Implementar `escribir_particionado` y verificar la estructura de directorios
2. Medir el tiempo de `leer_particionado` para un filtro que selecciona
   1 partición de 100 posibles
3. Comparar con leer el archivo completo sin particionamiento
4. ¿Qué problema tiene la implementación actual si hay muchos archivos pequeños?

**Pista:** La implementación actual crea un archivo por partición,
lo cual puede causar el "small files problem" con particionamiento de alta
cardinalidad. En producción, se usa `repartition()` en Spark o `sink_parquet()`
en Polars para controlar el número de archivos por partición.
La regla: apuntar a archivos de 128 MB–1 GB por partición.

---

### Ejercicio 2.2.5 — Leer: comparar Parquet con ORC y Avro

**Tipo: Comparar**

```
Parquet vs ORC vs Avro — cuándo usar cada uno:

PARQUET:
  Columnar, optimizado para analytics (lecturas de columnas específicas)
  Compresión: snappy (rápido), zstd (mejor ratio), gzip (máximo ratio)
  Mejor en: Spark, Presto, Athena, Polars, Pandas con PyArrow
  Estándar de facto para data lakes

ORC (Optimized Row Columnar):
  Columnar, similar a Parquet en principio
  Mejor compresión para ciertos tipos de datos (ACID transactions en Hive)
  Nativo en el ecosistema Hive/Hadoop
  Menos adopción fuera de ese ecosistema
  ACID support: nativo (antes que Delta Lake)

AVRO:
  Row-oriented, schema evolution diseñada desde el inicio
  Schema en JSON, separado de los datos (puede cambiar sin reescribir)
  Mejor para: streaming (Kafka), CDC, inter-service communication
  Malo para: analytics (row-oriented → no hay column pruning)
  Kafka Schema Registry usa Avro por defecto
```

**Preguntas:**

1. Un pipeline lee datos de Kafka (en Avro), los transforma con Spark,
   y los escribe a un data lake para analytics. ¿Qué formato usarías
   para cada paso?

2. Un sistema de CDC (Change Data Capture) captura cambios de PostgreSQL.
   ¿Por qué Avro es frecuentemente el formato elegido para los mensajes?

3. ORC soporta ACID nativo — ¿qué significa esto y por qué Delta Lake
   (sobre Parquet) se volvió más popular que usar ORC con ACID?

4. Si tuvieras que elegir UN solo formato para un data lake nuevo en 2024,
   ¿cuál elegirías? Justifica.

5. ¿Existe algún caso donde CSV sigue siendo el mejor formato?

**Pista:** Para la pregunta 5: CSV sigue siendo válido cuando el destino
es un humano (Excel, Google Sheets) o un sistema que no soporta formatos
binarios (muchas APIs legacy, reportes para sistemas externos).
También para datasets de menos de ~10 MB donde el overhead de Parquet
(leer el footer, inicializar PyArrow) supera el costo de parsear CSV.
Pero para pipelines de datos internos, CSV casi nunca es la respuesta correcta.

> 🔗 Ecosistema: Delta Lake, Iceberg, y Hudi añaden una capa de metadata
> sobre Parquet para soportar ACID, time travel, y schema evolution.
> Son el tema del Cap.08. Si vienes con curiosidad sobre por qué no simplemente
> usar ORC con ACID nativo, el Cap.08 lo explica.

---

## Sección 2.3 — Apache Arrow: el Formato en Memoria

Arrow es el pegamento del ecosistema moderno de data engineering.
Cuando Polars lee un Parquet y lo convierte a Pandas, o cuando PySpark
usa Pandas UDFs, Arrow es el formato intermedio que hace esa conversión eficiente.

```
Sin Arrow (transferencia de datos entre sistemas):
  Sistema A (Java) → serializar a bytes → red/memoria → deserializar → Sistema B (Python)
  Cada sistema tiene su representación interna propia.
  La conversión es costosa: copia de datos + transformación de tipos.

Con Arrow (formato en memoria compartido):
  Sistema A (Java) → Arrow buffer en memoria → Sistema B (Python)
  Ambos sistemas conocen el formato Arrow.
  La "conversión" es exponer el mismo buffer — zero-copy en muchos casos.
```

### Ejercicio 2.3.1 — Arrow como formato en memoria

**Tipo: Implementar/medir**

```python
import pyarrow as pa
import pandas as pd
import polars as pl
import numpy as np
import time

# Crear datos en Arrow nativo:
tabla_arrow = pa.table({
    "user_id": pa.array(range(1_000_000), type=pa.int64()),
    "monto":   pa.array(np.random.uniform(0, 10000, 1_000_000), type=pa.float64()),
    "region":  pa.array(
        np.random.choice(["norte", "sur", "este", "oeste"], 1_000_000),
        type=pa.dictionary(pa.int8(), pa.utf8())
    ),
})

# Conversión Arrow → Pandas:
def medir_conversion_arrow_pandas(tabla: pa.Table):
    inicio = time.perf_counter()
    df_pandas = tabla.to_pandas()
    duracion = time.perf_counter() - inicio
    print(f"Arrow → Pandas: {duracion * 1000:.1f}ms")
    return df_pandas

# Conversión Arrow → Polars:
def medir_conversion_arrow_polars(tabla: pa.Table):
    inicio = time.perf_counter()
    df_polars = pl.from_arrow(tabla)
    duracion = time.perf_counter() - inicio
    print(f"Arrow → Polars: {duracion * 1000:.1f}ms")
    return df_polars

# Conversión Pandas → Arrow (para comparar):
def medir_conversion_pandas_arrow(df: pd.DataFrame):
    inicio = time.perf_counter()
    tabla = pa.Table.from_pandas(df)
    duracion = time.perf_counter() - inicio
    print(f"Pandas → Arrow: {duracion * 1000:.1f}ms")
    return tabla
```

**Restricciones:**
1. Medir las conversiones para 1M, 10M, y 100M filas
2. ¿La conversión es O(n) o O(1)? ¿Por qué?
3. Verificar si la conversión es zero-copy usando `sys.getsizeof()`
   antes y después
4. ¿Qué tipos de datos de Pandas NO son zero-copy al convertir a Arrow?

**Pista:** La conversión Arrow → Polars es frecuentemente zero-copy para
tipos numéricos — Polars simplemente reusa el buffer de Arrow sin copiarlo.
La conversión Arrow → Pandas puede requerir copia si Pandas necesita
un layout de memoria diferente (Pandas usa row-major por defecto para DataFrames,
pero en la práctica, columnas individuales son contiguous arrays igual que Arrow).
Los tipos que rompen el zero-copy: `object` dtype de Pandas (no tiene
equivalente directo en Arrow) y `datetime` con timezone (requiere conversión).

---

### Ejercicio 2.3.2 — Zero-copy: cuándo la conversión no copia datos

**Tipo: Medir/verificar**

```python
import pyarrow as pa
import numpy as np
import ctypes

def verificar_zero_copy():
    """
    Verifica si dos arrays comparten la misma memoria subyacente.
    Si comparten memoria, modificar uno modifica el otro.
    """
    # Array de NumPy:
    arr_numpy = np.array([1, 2, 3, 4, 5], dtype=np.int64)
    
    # Convertir a Arrow (¿zero-copy?):
    arr_arrow = pa.array(arr_numpy)
    
    # Obtener el puntero de memoria de cada uno:
    ptr_numpy = arr_numpy.ctypes.data
    ptr_arrow = arr_arrow.buffers()[1].address  # buffer de datos (índice 1)
    
    print(f"NumPy ptr: {ptr_numpy}")
    print(f"Arrow ptr: {ptr_arrow}")
    print(f"¿Son iguales (zero-copy)? {ptr_numpy == ptr_arrow}")

# La prueba definitiva: modificar el array de NumPy y ver si Arrow cambia
def probar_aliasing():
    arr_numpy = np.array([1, 2, 3, 4, 5], dtype=np.int64)
    arr_arrow = pa.array(arr_numpy)
    
    print(f"Antes: Arrow[0] = {arr_arrow[0]}")
    arr_numpy[0] = 999
    print(f"Después de modificar NumPy: Arrow[0] = {arr_arrow[0]}")
    # Si es zero-copy: Arrow[0] también sería 999
    # Si no es zero-copy: Arrow[0] sigue siendo 1
```

**Restricciones:**
1. Ejecutar `verificar_zero_copy` y documentar el resultado
2. ¿Cambia el resultado si el array de NumPy no es C-contiguous?
   (`arr_numpy = arr_numpy[::-1]` lo hace no-contiguous)
3. ¿Cuándo Arrow garantiza zero-copy y cuándo no?
4. ¿Por qué el zero-copy importa para pipelines de datos a escala?

**Pista:** Arrow garantiza zero-copy cuando el array de NumPy es C-contiguous,
tiene el dtype correcto, y no tiene máscara de nulos (masked array).
Si el array de NumPy tiene gaps en memoria (stride != itemsize), Arrow
debe copiar los datos para tener un buffer contiguo. Para datos de 100 GB,
la diferencia entre zero-copy y una copia completa es la diferencia entre
100ms y 3 minutos.

---

### Ejercicio 2.3.3 — Arrow Flight: datos a alta velocidad por la red

**Tipo: Leer**

Arrow Flight es un protocolo RPC construido sobre gRPC que transfiere
datos en formato Arrow directamente — sin serializar a otro formato.

```python
import pyarrow as pa
import pyarrow.flight as flight

# Servidor que expone datos en formato Arrow:
class ServidorDatos(flight.FlightServerBase):
    def __init__(self, datos: pa.Table, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.datos = datos

    def do_get(self, context, ticket):
        # El cliente pide datos — el servidor los envía en Arrow format
        return flight.RecordBatchStream(self.datos)

    def list_flights(self, context, criteria):
        # Listar qué datos están disponibles:
        descriptor = flight.FlightDescriptor.for_path("ventas")
        info = flight.FlightInfo(
            self.datos.schema,
            descriptor,
            endpoints=[flight.FlightEndpoint("token", ["grpc://localhost:8815"])],
            total_records=len(self.datos),
            total_bytes=self.datos.nbytes,
        )
        yield info

# Cliente:
cliente = flight.connect("grpc://localhost:8815")
lector = cliente.do_get(flight.Ticket("token"))
tabla = lector.read_all()  # recibe Arrow directamente — sin conversión
```

**Preguntas:**

1. ¿Cuál es la ventaja de Arrow Flight sobre una API REST que devuelve JSON
   para transferir 1 GB de datos?

2. ¿En qué escenarios de data engineering usarías Arrow Flight?

3. ¿Arrow Flight es un reemplazo de Kafka para streaming de datos?

4. DuckDB, DataFusion, y algunos data warehouses soportan Arrow Flight.
   ¿Qué posibilidad abre esto para construir pipelines de datos?

**Pista:** La comparación cuantitativa: serializar 1 GB de datos a JSON
puede producir 3-5 GB de texto (los tipos numéricos pierden compresión).
Transmitir 3-5 GB de texto tarda ~30 segundos en una conexión de 1 Gbps.
Con Arrow Flight: se transmiten los ~1 GB en formato binario eficiente
en ~8 segundos. Sin contar la deserialización del JSON (que puede ser
más costosa que la transmisión).

> 🔗 Ecosistema: Arrow Flight SQL extiende Arrow Flight con un protocolo
> estándar para ejecutar queries SQL y recibir resultados en Arrow.
> DuckDB y DataFusion implementan Arrow Flight SQL como interfaz de red.
> Esto permite conectar cualquier cliente Arrow a cualquier servidor
> Arrow Flight SQL — sin drivers específicos por base de datos.

---

### Ejercicio 2.3.4 — Arrow en el contexto de PySpark: las Pandas UDFs

**Tipo: Medir**

El impacto de Arrow en PySpark es más concreto que cualquier teoría:
las Pandas UDFs usan Arrow para eliminar la serialización fila a fila.

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
import pandas as pd
import time

spark = SparkSession.builder \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .getOrCreate()

df = spark.range(0, 1_000_000).toDF("valor")

# UDF escalar (sin Arrow — serialización fila a fila):
@F.udf(returnType=DoubleType())
def raiz_cuadrada_udf(x):
    import math
    return math.sqrt(x)

# Pandas UDF (con Arrow — serialización por batch):
@F.pandas_udf(DoubleType())
def raiz_cuadrada_pandas_udf(serie: pd.Series) -> pd.Series:
    return serie.pow(0.5)  # numpy vectorizado

# Función nativa Spark (sin Python — solo JVM):
def raiz_cuadrada_nativa(col):
    return F.sqrt(col)

# Medir:
def medir(nombre, fn):
    inicio = time.time()
    df.select(fn(F.col("valor"))).count()
    print(f"{nombre}: {time.time() - inicio:.2f}s")

medir("UDF escalar",        lambda c: raiz_cuadrada_udf(c))
medir("Pandas UDF (Arrow)", lambda c: raiz_cuadrada_pandas_udf(c))
medir("Función nativa",     lambda c: raiz_cuadrada_nativa(c))
```

**Restricciones:**
1. Ejecutar el benchmark y documentar los tiempos
2. ¿El speedup de Pandas UDF vs UDF escalar es consistente con
   la teoría del zero-copy? ¿Por qué sí o por qué no?
3. ¿Cuánto overhead añade Arrow al proceso? ¿Es siempre zero?
4. ¿Para qué tipo de UDF la Pandas UDF tiene más ventaja sobre la escalar?

**Pista:** La Pandas UDF tiene mayor ventaja cuando la función Python es rápida
(overhead de inicialización bajo) y el volumen de datos es alto.
Si la función tarda 10ms por elemento, el overhead de Arrow (procesar en batches)
se amortiza fácilmente. Si la función tarda 1µs por elemento y el batch tiene
10,000 elementos, el overhead de convertir el batch de Arrow a Pandas ya es
comparable al cómputo. En ese caso, la función nativa de Spark (que no toca Python)
sigue siendo la mejor opción.

---

### Ejercicio 2.3.5 — Leer: el ecosistema Arrow

**Tipo: Analizar**

Arrow no es solo una librería — es un estándar que conecta el ecosistema:

```
Herramientas que usan Arrow como formato en memoria:

  Pandas (2.0+)  →  puede usar Arrow como backend nativo
  Polars         →  construido sobre Arrow desde el inicio
  DuckDB         →  puede leer/escribir Arrow directamente
  DataFusion     →  motor de ejecución sobre Arrow
  Spark          →  usa Arrow para Pandas UDFs y conversiones
  Dask           →  usa Arrow para datos en disco (parquet)
  cuDF (RAPIDS)  →  Arrow sobre GPU
  
  Conectores:
    PyArrow.flight → transferencia de datos por red
    ADBC           → Arrow Database Connectivity (reemplaza ODBC/JDBC)
    
  Formatos de archivo:
    Parquet (columnar en disco, Arrow en memoria)
    Feather/IPC    (Arrow en disco — lectura inmediata sin conversión)
```

**Preguntas:**

1. Si Polars, DuckDB, y Pandas 2.0 todos usan Arrow, ¿qué pasa
   cuando conviertes un DataFrame entre ellos?

2. ¿Qué es Feather/Arrow IPC y cuándo es mejor que Parquet?

3. ADBC (Arrow Database Connectivity) pretende reemplazar JDBC/ODBC.
   ¿Cuál es la ventaja específica para data engineering?

4. `cuDF` es Arrow sobre GPU. ¿Qué tipo de operaciones se benefician
   más de GPU en data engineering?

5. Si el ecosistema Arrow se convierte en el estándar universal,
   ¿qué problema de data engineering resuelve fundamentalmente?

**Pista:** Para la pregunta 2: la conversión entre herramientas que todas usan
Arrow como formato interno es frecuentemente zero-copy o casi zero-copy.
El "problema de los 1000 conversiones" (cada tool tiene su propio formato,
convertir entre ellos es costoso) es exactamente lo que Arrow intenta resolver.
Si todos hablan Arrow, el "costo de conversión" desaparece.

> 📖 Profundizar: el paper *Apache Arrow and the "10 Things I Hate About pandas"*
> (Wes McKinney, 2017) explica la motivación original de Arrow desde la perspectiva
> del creador de Pandas. Es una lectura corta y directa sobre por qué los formatos
> importan en la práctica.

---

## Sección 2.4 — Compresión: el Tradeoff CPU vs I/O

### Ejercicio 2.4.1 — Medir el tradeoff compresión/velocidad

**Tipo: Medir**

```python
import polars as pl
import numpy as np
import time
import os

def benchmark_compresion(df: pl.DataFrame, codec: str, level: int = None):
    """
    Mide: tamaño en disco, tiempo de escritura, tiempo de lectura.
    """
    ruta = f"/tmp/benchmark_{codec}.parquet"

    kwargs = {"compression": codec}
    if level is not None:
        kwargs["compression_level"] = level

    # Escritura:
    inicio = time.perf_counter()
    df.write_parquet(ruta, **kwargs)
    t_escritura = time.perf_counter() - inicio

    tamaño = os.path.getsize(ruta)

    # Lectura (con cache frío — ejecutar en proceso separado para rigor):
    inicio = time.perf_counter()
    df_leido = pl.read_parquet(ruta)
    t_lectura = time.perf_counter() - inicio

    print(f"{codec:10} level={level or 'default':3} | "
          f"Tamaño: {tamaño/1e6:6.1f} MB | "
          f"Escritura: {t_escritura:.2f}s | "
          f"Lectura: {t_lectura:.2f}s")

    return tamaño, t_escritura, t_lectura
```

**Restricciones:**
1. Ejecutar el benchmark con: `uncompressed`, `snappy`, `gzip`, `zstd` (levels 1, 3, 9), `lz4`
2. Para un dataset de texto (alta compresibilidad) vs numérico (baja compresibilidad)
3. ¿Cuándo `gzip` es mejor que `zstd`? ¿Cuándo es peor?
4. Para un pipeline I/O-bound (red lenta a S3), ¿qué codec elegirías?
   ¿Y para un pipeline CPU-bound?

**La tabla a completar:**

```
Codec       Nivel   Tamaño (MB)   Escritura (s)   Lectura (s)   Ratio vs raw
──────────────────────────────────────────────────────────────────────────────
sin compres  -      ???           ???             ???           1.0×
snappy       -      ???           ???             ???           ???
lz4          -      ???           ???             ???           ???
zstd         1      ???           ???             ???           ???
zstd         3      ???           ???             ???           ???
zstd         9      ???           ???             ???           ???
gzip         6      ???           ???             ???           ???
```

**Pista:** Para entornos de producción con almacenamiento en S3 o HDFS:
zstd nivel 3 es frecuentemente el mejor balance — buen ratio de compresión,
velocidad de descompresión rápida (más importante que la compresión),
y amplio soporte en todos los lectores modernos. Snappy es más rápido
pero con peor ratio. gzip tiene mejor ratio que snappy pero es mucho más
lento en descompresión — penaliza la lectura. Para datos que se leen frecuentemente,
la velocidad de descompresión importa más que el ratio.

---

### Ejercicio 2.4.2 — La compresión y el paralelismo

**Tipo: Leer**

Un aspecto no obvio de la compresión: algunos codecs son "splittable"
(pueden dividirse en bloques independientes para procesamiento paralelo)
y otros no.

```
Compresión splittable (cada bloque es independiente):
  snappy, lz4, zstd, bzip2
  Spark puede asignar diferentes bloques a diferentes workers.
  Un archivo grande puede procesarse en paralelo.

Compresión NO splittable:
  gzip, xz
  Un archivo .csv.gz COMPLETO debe leerse por un solo worker.
  No importa cuántos workers tengas — uno hace todo el trabajo.

Parquet y la compresión:
  La compresión en Parquet es por columna-dentro-del-row-group.
  Siempre es splittable a nivel de row group (independientemente del codec).
  Por eso un .parquet.snappy no tiene el mismo problema que un .csv.gz.
```

**Preguntas:**

1. Tienes 1 TB de datos en un archivo `datos.csv.gz` (comprimido con gzip).
   ¿Cuántos workers de Spark pueden leer este archivo en paralelo?

2. Los mismos datos en `datos.parquet` con compresión gzip por columna.
   ¿Cuántos workers pueden leerlo en paralelo?

3. ¿Por qué gzip sigue usándose en archivos CSV si no es splittable?

4. Un pipeline genera archivos Parquet comprimidos con snappy.
   ¿Cómo afecta el codec al número de tasks de Spark para leer el archivo?

5. ¿Qué recomendarías a un equipo que tiene 5 años de datos históricos
   en formato `.csv.gz` que quiere mejorar el rendimiento de sus pipelines?

**Pista:** Para la pregunta 3: gzip es el codec más universalmente disponible
en sistemas UNIX (está en cada servidor), y produce archivos más pequeños
que snappy/lz4. Para datos que se intercambian con sistemas externos
(partners comerciales, APIs legacy), gzip es el mínimo común denominador.
Para pipelines internos modernos, no hay razón para usarlo.

---

### Ejercicio 2.4.3 — Estimar el tamaño de los datos antes de escribirlos

**Tipo: Calcular**

Antes de ejecutar un pipeline que produce un archivo grande,
vale la pena estimar el tamaño del output.

```python
def estimar_tamaño_parquet(
    n_filas: int,
    schema: dict[str, str],  # {"columna": "tipo"}
    cardinalidades: dict[str, int],  # {"columna": n_valores_únicos}
    codec: str = "zstd",
) -> dict:
    """
    Estimación aproximada del tamaño de un archivo Parquet.
    No es precisa — es un orden de magnitud.
    """
    tamaño_raw = 0

    for col, tipo in schema.items():
        bytes_por_valor = {
            "int32": 4, "int64": 8,
            "float32": 4, "float64": 8,
            "date32": 4,
            "string_10":  10,   # estimado
            "string_50":  50,   # estimado
            "string_200": 200,  # estimado
        }.get(tipo, 8)

        tamaño_raw += n_filas * bytes_por_valor

    # Factor de compresión estimado por codec y cardinalidad:
    # (muy aproximado — depende de los datos reales)
    factor_compresion = {
        "snappy": 0.4,
        "zstd":   0.25,
        "gzip":   0.2,
    }.get(codec, 0.5)

    # Columnas con baja cardinalidad comprimen mejor:
    ajuste_cardinalidad = sum(
        0.5 if card < 100 else 1.0
        for card in cardinalidades.values()
    ) / len(cardinalidades)

    tamaño_estimado = tamaño_raw * factor_compresion * ajuste_cardinalidad

    return {
        "raw_bytes": tamaño_raw,
        "estimado_bytes": int(tamaño_estimado),
        "estimado_mb": tamaño_estimado / 1e6,
        "ratio_estimado": tamaño_raw / tamaño_estimado,
    }
```

**Restricciones:**
1. Usar `estimar_tamaño_parquet` para el dataset del sistema de e-commerce
2. Comparar la estimación con el archivo real generado en los ejercicios anteriores
3. ¿En qué factor puede equivocarse la estimación? ¿Qué datos la hacen más imprecisa?

---

### Ejercicio 2.4.4 — Leer: el pipeline que se quedó sin espacio en disco

**Tipo: Diagnosticar**

Un job de Spark falló con:
```
java.io.IOException: No space left on device
  at java.io.FileOutputStream.write0(Native Method)
  at ...SpillWriter...
```

El equipo tenía 500 GB libres en el cluster y el input del job era 200 GB.

**Información adicional:**
```python
# El job:
df = spark.read.parquet("s3://input/datos/")  # 200 GB
df_resultado = (df
    .groupBy("user_id")
    .agg(F.collect_list("evento").alias("eventos"))
)
df_resultado.write.parquet("s3://output/")
```

```
Estadísticas del job antes de fallar:
  Stage 0 (lectura): completado, output = 200 GB
  Stage 1 (shuffle para groupBy): 
    Input: 200 GB
    Shuffle write: 600 GB  ← !!
    Disco local usado: 480 GB (de 500 GB disponibles)
    Estado: FAILED
```

**Preguntas:**

1. ¿Por qué el shuffle write es 600 GB si el input es 200 GB?
   ¿Dónde se crearon los 400 GB adicionales?

2. ¿Qué hace `collect_list` que es especialmente problemático aquí?

3. ¿Por qué el spill al disco local usa casi todo el espacio disponible?

4. Propón tres soluciones posibles, ordenadas de más a menos impacto.

5. ¿Existe una versión de este job que no use `collect_list` pero
   produzca el mismo resultado?

**Pista:** `collect_list("evento")` agrupa todos los eventos de cada usuario
en un array. En memoria, ese array debe caber en el executor. En el shuffle,
todos los eventos de cada user_id viajan al mismo executor (para poder agruparse).
Si hay usuarios con muchos eventos (skew), el executor recibe una cantidad
desproporcionada de datos. El shuffle write > input puede ocurrir porque:
los datos se replican (cada fila puede ir a múltiples destinos en algunos shuffles)
o porque la serialización del shuffle añade overhead de metadatos.

---

### Ejercicio 2.4.5 — Elegir el codec correcto para el caso de uso

**Tipo: Diseñar**

Para cada caso, elige el codec de compresión y justifica:

```
1. Archivos Parquet en un data warehouse interno.
   Se leen muchas veces al día por distintos usuarios.
   El almacenamiento es barato (S3).
   Los workers tienen CPUs modernas.

2. Archivos CSV que se envían a un partner externo por SFTP.
   El partner tiene sistemas legacy que solo conocen gzip.
   El tamaño del archivo importa (tiene un límite de 2 GB por archivo).

3. Archivos intermedios en un pipeline Spark con muchos shuffles.
   Se escriben y se borran dentro del mismo job.
   La velocidad de escritura/lectura es crítica.

4. Archivos de logs para archivado a largo plazo (5+ años).
   Se leerán raramente (solo en investigaciones forenses).
   El costo de almacenamiento importa.

5. Archivos Parquet para un feature store de ML.
   Se leen intensamente durante el entrenamiento de modelos.
   Los modelos leen solo 5-10 features de 200 disponibles.
```

---

## Sección 2.5 — Formatos para Casos Específicos

### Ejercicio 2.5.1 — JSON y JSONL: cuándo son inevitables

**Tipo: Comparar/medir**

```python
import polars as pl
import json
import time
import os

# JSONL (JSON Lines) — un JSON por línea:
# {"user_id": 1001, "evento": "click", "datos": {"producto": 5023, "precio": 150.0}}
# {"user_id": 1002, "evento": "compra", "datos": {"producto": 5024, "monto": 89.5}}

# Leer JSONL con Polars:
def leer_jsonl(ruta: str) -> pl.DataFrame:
    return pl.read_ndjson(ruta)

# El problema: JSON con esquema anidado
datos_anidados = [
    {
        "user_id": i,
        "eventos": [
            {"tipo": "click", "producto_id": i * 2},
            {"tipo": "vista", "producto_id": i * 3},
        ],
        "metadata": {"fuente": "web", "version": "2.1"},
    }
    for i in range(100_000)
]
```

**Preguntas:**

1. ¿Cuál es la diferencia entre JSON y JSONL (JSON Lines)?
   ¿Por qué JSONL es mejor para procesamiento en paralelo?

2. Para el dataset `datos_anidados`, ¿cómo lo convertirías a
   formato tabular (flat) para almacenar en Parquet?

3. Medir el tamaño de 1M filas en JSONL vs Parquet.
   ¿Cuál es el factor de diferencia?

4. ¿Cuándo es correcto usar JSONL como formato de almacenamiento?
   (Pista: piensa en semi-structured data)

5. Un API REST devuelve datos en JSON con estructura variable
   (diferentes campos según el tipo de evento). ¿Cómo lo manejarías
   en un pipeline de datos?

**Pista:** Para la pregunta 5: la estrategia común es almacenar el campo
variable como una columna de tipo `string` (el JSON crudo) o como `map<string, string>`
en Parquet. Cuando necesitas acceder a los campos internos, los parseas en el momento
de la query. Delta Lake y Hudi soportan semi-structured data con queries sobre
campos JSON usando notación de punto: `SELECT datos:producto FROM tabla`.

---

### Ejercicio 2.5.2 — Feather/Arrow IPC: el formato para intercambio rápido

**Tipo: Medir**

```python
import polars as pl
import pyarrow as pa
import pyarrow.feather as feather
import time

# Feather v2 = Arrow IPC format
# Diseñado para lectura rápida, no para almacenamiento a largo plazo

def comparar_parquet_vs_feather(df: pl.DataFrame):
    # Escribir:
    df.write_parquet("/tmp/datos.parquet", compression="zstd")
    feather.write_feather(df.to_arrow(), "/tmp/datos.feather",
                          compression="zstd")

    # Leer (con cache frío):
    inicio = time.perf_counter()
    df_parquet = pl.read_parquet("/tmp/datos.parquet")
    t_parquet = time.perf_counter() - inicio

    inicio = time.perf_counter()
    df_feather = pl.from_arrow(feather.read_table("/tmp/datos.feather"))
    t_feather = time.perf_counter() - inicio

    print(f"Parquet  → lectura: {t_parquet:.3f}s")
    print(f"Feather  → lectura: {t_feather:.3f}s")
    print(f"Speedup Feather: {t_parquet/t_feather:.1f}×")
```

**Preguntas:**

1. ¿Por qué Feather es más rápido de leer que Parquet?
2. ¿Cuándo usarías Feather en lugar de Parquet?
3. ¿Por qué Feather NO es el formato estándar de data lakes?
4. ¿Qué es el "Arrow IPC format" y cuál es la relación con Feather?

**Pista:** Feather/Arrow IPC puede leerse sin deserialización significativa —
el archivo en disco tiene exactamente el layout de memoria que Arrow usa.
Parquet tiene un layout optimizado para compresión y predicate pushdown
pero requiere más trabajo para convertir a la representación en memoria.
Feather es ideal para: caché de datos intermedios en pipelines locales,
archivos temporales que se leen muchas veces en el mismo proceso,
y comunicación entre procesos en la misma máquina.

---

### Ejercicio 2.5.3 — CSV: cuándo todavía es la respuesta correcta

**Tipo: Analizar**

A pesar de sus limitaciones, CSV sigue siendo relevante en data engineering.

```python
# Cuándo CSV es la única opción:
casos_csv = {
    "interoperabilidad_externa": """
        Un banco envía transacciones en CSV cada noche.
        El sistema de destino no soporta Parquet.
        No puedes cambiar ninguno de los dos sistemas.
        → CSV es obligatorio aquí.
    """,
    "inspeccion_humana": """
        El equipo de finanzas quiere revisar el reporte en Excel.
        Excel puede abrir CSV directamente.
        Parquet requiere una herramienta especial.
        → CSV para el reporte final, Parquet para el procesamiento.
    """,
    "datasets_pequeños": """
        Un archivo de configuración con 50 filas y 5 columnas.
        El overhead de PyArrow para leer Parquet es comparable
        al tiempo de leer el CSV.
        → CSV es más simple y suficiente.
    """,
}

# Estrategias para trabajar con CSV eficientemente:
def leer_csv_eficientemente(ruta: str, columnas_necesarias: list[str]) -> pl.DataFrame:
    """
    Lee solo las columnas necesarias del CSV para reducir memoria.
    """
    return pl.read_csv(ruta, columns=columnas_necesarias)

def csv_a_parquet(ruta_csv: str, ruta_parquet: str,
                  chunk_size: int = 1_000_000):
    """
    Convierte CSV a Parquet procesando en chunks para no llenar la memoria.
    """
    writer = None
    for chunk in pl.read_csv_batched(ruta_csv, batch_size=chunk_size):
        if writer is None:
            writer = chunk.to_arrow()
        else:
            writer = pa.concat_tables([writer, chunk.to_arrow()])

    import pyarrow.parquet as pq
    pq.write_table(writer, ruta_parquet, compression="zstd")
```

**Restricciones:**
1. Implementar `csv_a_parquet` de forma eficiente para un CSV de 100 GB
   (sin cargar todo en memoria)
2. ¿Cuánto tiempo toma la conversión? ¿Vale la pena el one-time cost?
3. Diseñar la estrategia para una empresa que recibe 50 CSVs/día de distintos
   proveedores y los consolida en un data lake

**Pista:** Para la conversión de 100 GB de CSV a Parquet sin OOM:
usar `read_csv_batched` en Polars o `pd.read_csv(chunksize=N)` en Pandas.
El truco es escribir al Parquet incrementalmente — no acumular todos los chunks
en memoria antes de escribir. `pyarrow.parquet.ParquetWriter` permite
escribir row group a row group: abrir el writer, escribir un row group por chunk,
cerrar el writer al final.

---

### Ejercicio 2.5.4 — Delta Lake y Iceberg: Parquet con superpoderes

**Tipo: Leer**

Delta Lake e Apache Iceberg no son formatos nuevos — son una capa de
metadata sobre Parquet que añade capacidades que Parquet por sí solo no tiene.

```
Parquet solo:
  ✓ Columnar, comprimido, eficiente
  ✗ Sin transacciones ACID
  ✗ Sin time travel
  ✗ Schema evolution limitada
  ✗ Sin soporte nativo para UPDATE/DELETE
  ✗ Sin gestión de pequeños archivos

Delta Lake = Parquet + transaction log:
  ✓ Todo lo de Parquet
  ✓ Transacciones ACID (múltiples writers seguros)
  ✓ Time travel (leer el estado en cualquier momento pasado)
  ✓ Schema evolution (añadir/cambiar columnas sin reescribir)
  ✓ UPDATE/DELETE/MERGE eficientes
  ✓ Automatic file compaction

Estructura en disco de una tabla Delta:
  mi_tabla/
  ├── _delta_log/
  │   ├── 00000000000000000000.json  ← commit 0: CREATE TABLE
  │   ├── 00000000000000000001.json  ← commit 1: INSERT 1M filas
  │   ├── 00000000000000000002.json  ← commit 2: DELETE WHERE region='sur'
  │   └── ...
  ├── part-00001-abc123.parquet      ← datos reales
  ├── part-00002-def456.parquet
  └── ...
```

**Preguntas:**

1. ¿Cómo Delta Lake implementa el "time travel" usando el transaction log?

2. Si dos pipelines escriben a la misma tabla Delta simultáneamente,
   ¿cómo resuelve los conflictos?

3. ¿Qué es el "small files problem" en Delta Lake y cómo lo resuelve
   el "automatic file compaction" (OPTIMIZE)?

4. Apache Iceberg es el competidor principal de Delta Lake.
   ¿Cuáles son las diferencias clave?

5. ¿Cuándo usarías un archivo Parquet simple en lugar de una tabla Delta?

**Pista:** El time travel en Delta Lake: cada commit en el `_delta_log` lista
exactamente qué archivos se añadieron y cuáles se removieron.
Para leer el estado en el commit 5, Delta lee los commits 0 a 5 y calcula
el conjunto de archivos activos en ese momento. Los archivos nunca se borran
inmediatamente — solo se "demarcan" como inactivos en el log.
El `VACUUM` los borra físicamente después de un período de retención.

> 📖 Profundizar: la documentación de Delta Lake en `docs.delta.io` tiene
> una sección "How Delta Lake Works" que explica el transaction log con
> ejemplos concretos. Recomendado antes del Cap.08.

---

### Ejercicio 2.5.5 — Elegir el formato para cada componente del sistema

**Tipo: Diseñar**

Para el sistema de e-commerce del repositorio, elegir el formato
para cada componente y justificar:

```
Componente → Formato a elegir + justificación

1. Eventos de Kafka (en tránsito, topic de clicks)
   → ???

2. Raw data lake (los eventos tal como llegaron, para replay)
   → ???

3. Tabla de hechos procesada (ventas enriquecidas, se consulta frecuentemente)
   → ???

4. Tabla de dimensiones (catálogo de productos, 1M filas, actualizado c/hora)
   → ???

5. Archivos intermedios de Spark (spill, shuffle temporal)
   → ???

6. Caché de features para modelo de ML (se lee miles de veces/día)
   → ???

7. Reporte mensual enviado al equipo de finanzas (Excel-compatible)
   → ???

8. Archive de logs (se guarda 7 años, se lee raramente)
   → ???
```

---

## Sección 2.6 — Schema Evolution: Cuando el Formato Cambia

### Ejercicio 2.6.1 — El problema de la evolución del schema

**Tipo: Leer**

Los datos evolucionan. Las columnas se añaden, se renombran, cambian de tipo.
Un formato que no soporta esta evolución obliga a reescribir todos los datos históricos.

```python
# Schema versión 1 (enero 2024):
schema_v1 = pa.schema([
    pa.field("user_id", pa.int64()),
    pa.field("monto",   pa.float64()),
    pa.field("region",  pa.utf8()),
])

# Schema versión 2 (marzo 2024) — añade columna, cambia tipo:
schema_v2 = pa.schema([
    pa.field("user_id",  pa.int64()),
    pa.field("monto",    pa.float64()),
    pa.field("region",   pa.utf8()),
    pa.field("moneda",   pa.utf8()),     # NUEVA — ¿qué valor tienen los datos v1?
    pa.field("descuento", pa.float32()), # NUEVA
])

# Schema versión 3 (junio 2024) — renombra una columna:
schema_v3 = pa.schema([
    pa.field("user_id",    pa.int64()),
    pa.field("monto_usd",  pa.float64()),  # RENOMBRADA de "monto"
    pa.field("region",     pa.utf8()),
    pa.field("moneda",     pa.utf8()),
    pa.field("descuento",  pa.float32()),
])
```

**Preguntas:**

1. Si tienes archivos Parquet con schema v1, v2, y v3 mezclados en el mismo
   directorio, ¿qué pasa cuando Spark los lee con `spark.read.parquet("dir/")`?

2. ¿Cuál de los tres cambios de schema es más problemático? ¿Por qué?
   - Añadir una columna nueva
   - Cambiar el tipo de una columna (float32 → float64)
   - Renombrar una columna

3. Delta Lake maneja la schema evolution con `mergeSchema`.
   ¿Cómo funciona para columnas nuevas? ¿Y para columnas renombradas?

4. ¿Cómo Avro maneja la evolución del schema que Parquet no puede?

5. Diseñar una estrategia de versionado de schemas para un sistema
   que tiene 5 años de datos históricos y el schema cambia 3-4 veces al año.

**Pista:** Spark puede leer archivos Parquet con schemas distintos mezclados
si se activa `spark.sql.parquet.mergeSchema = true` (o `mergeSchema=true`
en las opciones de lectura). Infiere el schema unión de todos los archivos —
las columnas que no existen en un archivo se rellenan con `null`.
El renombrado es el cambio más problemático porque los datos con el nombre
viejo y los datos con el nombre nuevo son incompatibles — Spark los trataría
como dos columnas distintas: `monto` (con valores, solo en archivos viejos)
y `monto_usd` (con valores, solo en archivos nuevos).

---

### Ejercicio 2.6.2 — Implementar schema evolution con Delta Lake

**Tipo: Implementar**

```python
from delta import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Crear tabla con schema v1:
df_v1 = spark.createDataFrame([
    (1001, 150.0, "norte"),
    (1002, 89.5, "sur"),
], ["user_id", "monto", "region"])

df_v1.write.format("delta").save("/tmp/ventas_delta")

# Añadir columna nueva (schema evolution):
df_v2 = spark.createDataFrame([
    (1003, 200.0, "este", "USD"),
    (1004, 175.5, "oeste", "MXN"),
], ["user_id", "monto", "region", "moneda"])

# Esto fallará sin mergeSchema:
# df_v2.write.format("delta").mode("append").save("/tmp/ventas_delta")

# Esto funciona con schema evolution:
df_v2.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .save("/tmp/ventas_delta")

# Leer: las filas v1 tienen NULL en la columna "moneda"
spark.read.format("delta").load("/tmp/ventas_delta").show()
```

**Restricciones:**
1. Implementar el ejemplo completo
2. Verificar que las filas v1 tienen `null` en la columna `moneda`
3. Implementar un UPDATE que rellena los nulls con un valor por defecto
4. ¿Cómo manejarías el renombrado de `monto` a `monto_usd`?

**Pista:** Delta Lake no soporta directamente el renombrado de columnas
en versiones antiguas. La estrategia: añadir `monto_usd` como columna nueva,
copiar los valores de `monto` a `monto_usd`, y luego marcar `monto` como
deprecated (o eliminarla si los lectores ya están actualizados).
En Delta Lake 2.0+ existe `ALTER TABLE RENAME COLUMN` que actualiza el log
sin reescribir los datos — pero los archivos Parquet subyacentes siguen
teniendo el nombre viejo (se usa un mapeo en el log).

---

### Ejercicio 2.6.3 — Avro y schema registry: evolución coordinada

**Tipo: Leer**

Kafka con Avro y Schema Registry resuelve el problema de la evolución
del schema para eventos en streaming:

```
Sin Schema Registry:
  Productor: escribe Avro con schema v2
  Consumidor: tiene hardcodeado schema v1
  Resultado: el consumidor no puede deserializar → error en producción

Con Schema Registry:
  Registry guarda todas las versiones del schema.
  Cada mensaje incluye el ID del schema que usó para serializar.
  El consumidor pide al Registry el schema correcto para deserializar.
  Las versiones deben ser compatibles (backward/forward compatible).
```

```python
# Con confluent-kafka y schema registry:
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer

schema_registry_client = SchemaRegistryClient({"url": "http://localhost:8081"})

# Schema v1:
schema_v1_str = """
{
  "type": "record",
  "name": "Venta",
  "fields": [
    {"name": "user_id", "type": "long"},
    {"name": "monto",   "type": "double"}
  ]
}
"""

# Schema v2 — compatible con v1 (añade campo con default):
schema_v2_str = """
{
  "type": "record",
  "name": "Venta",
  "fields": [
    {"name": "user_id", "type": "long"},
    {"name": "monto",   "type": "double"},
    {"name": "moneda",  "type": "string", "default": "USD"}
  ]
}
"""
# ↑ El "default" hace este schema backward-compatible:
# un consumidor con schema v1 puede leer mensajes v2 (ignora "moneda")
# un consumidor con schema v2 puede leer mensajes v1 ("moneda" = "USD" por defecto)
```

**Preguntas:**

1. ¿Qué es "backward compatibility" y "forward compatibility" en schema evolution?

2. ¿Qué cambio haría el schema v2 INCOMPATIBLE con el v1?

3. ¿Por qué añadir un campo sin `default` rompe la backward compatibility?

4. En el contexto de Kafka, ¿quién es el "productor" y quién el "consumidor"
   en términos de schema evolution?

5. ¿Cómo la Schema Registry previene que un productor publique
   un schema incompatible sin darse cuenta?

**Pista:** Schema Registry tiene modos de compatibilidad configurables:
`BACKWARD` (consumidores nuevos pueden leer mensajes viejos),
`FORWARD` (consumidores viejos pueden leer mensajes nuevos),
`FULL` (ambos). El modo `BACKWARD` es el más común —
los consumidores suelen actualizarse antes que los productores.
Cuando el productor intenta registrar un schema nuevo, Schema Registry
verifica la compatibilidad y rechaza el registro si viola la regla.

> 📖 Profundizar: la documentación de Confluent Schema Registry tiene
> una guía de "Schema Evolution and Compatibility" con ejemplos concretos
> de qué cambios son compatibles y cuáles no. Referencia útil antes del Cap.09.

---

## Sección 2.7 — El Sistema de E-commerce: Decidir los Formatos

### Ejercicio 2.7.1 — Auditoría de formatos del sistema actual

**Tipo: Diseñar**

El sistema de e-commerce hereda la siguiente infraestructura de datos:

```
Estado actual (heredado, "así siempre fue"):
  
  PostgreSQL → exporta tablas a CSV cada noche → S3
    s3://data/productos/productos_20240115.csv    (1 GB)
    s3://data/usuarios/usuarios_20240115.csv      (5 GB)
    s3://data/ventas/ventas_20240115.csv.gz       (15 GB comprimido)
  
  Kafka → consumidor escribe JSON a S3 (un archivo por mensaje)
    s3://events/clicks/2024/01/15/14/event_a1b2c3.json
    s3://events/clicks/2024/01/15/14/event_d4e5f6.json
    ... (200,000 archivos por hora)
  
  Pipeline Spark → lee CSV de S3, genera reportes en CSV
    s3://reportes/revenue_diario.csv
    s3://reportes/top_productos.csv
```

**Restricciones:** Para cada fuente de datos, identificar:
1. Los problemas del formato actual
2. El formato recomendado
3. El impacto estimado en rendimiento (cuantitativo si es posible)
4. El plan de migración sin downtime

**Pista:** El caso de los 200,000 archivos JSON por hora en S3 es el
"small files problem" en su forma más extrema. Leer esos archivos con Spark
requiere 200,000 operaciones LIST de S3 (costosas en tiempo y en dinero)
antes de siquiera empezar a leer datos. La solución estándar: un proceso
de "compaction" que consolida los archivos pequeños en Parquet cada hora
— el costo de la compactación se amortiza en todas las lecturas futuras.

---

### Ejercicio 2.7.2 — Diseñar el schema del sistema

**Tipo: Diseñar**

Diseñar el schema completo para las tablas principales del sistema:

```python
import pyarrow as pa

# Tabla de hechos: ventas
schema_ventas = pa.schema([
    # Completar con tipos apropiados:
    pa.field("venta_id",    ???),
    pa.field("user_id",     ???),
    pa.field("producto_id", ???),
    pa.field("monto",       ???),  # ¿float32 o float64? ¿Por qué?
    pa.field("moneda",      ???),  # ¿utf8 o dictionary?
    pa.field("region",      ???),  # 4 valores posibles
    pa.field("canal",       ???),  # "web", "mobile", "tienda"
    pa.field("fecha",       ???),  # ¿date32 o timestamp?
    pa.field("procesado_at",???),  # timestamp de cuando se procesó
])

# Tabla de dimensiones: productos
schema_productos = pa.schema([
    pa.field("producto_id",  ???),
    pa.field("nombre",       ???),
    pa.field("descripcion",  ???),  # texto largo, nullable
    pa.field("categoria",    ???),
    pa.field("precio_base",  ???),
    pa.field("activo",       ???),
    pa.field("creado_at",    ???),
    pa.field("actualizado_at",???),
])

# Tabla de eventos: clicks (semi-structured)
schema_clicks = pa.schema([
    pa.field("evento_id",    ???),
    pa.field("user_id",      ???),
    pa.field("session_id",   ???),
    pa.field("producto_id",  ???),  # nullable (no todos los clicks son en productos)
    pa.field("url",          ???),
    pa.field("timestamp",    ???),
    pa.field("metadata",     ???),  # JSON crudo para campos variables
])
```

**Restricciones:**
1. Completar los schemas con los tipos correctos y justificar cada elección
2. ¿Qué columnas usan `dictionary` encoding y por qué?
3. ¿Cuál es el particionamiento correcto para cada tabla?

---

### Ejercicio 2.7.3 — Calcular el costo de almacenamiento

**Tipo: Calcular**

Con los schemas del ejercicio anterior y los volúmenes esperados:

```
Volúmenes:
  Ventas: 1,000 transacciones/segundo = 86.4M/día
  Clicks: 100,000 eventos/segundo = 8,640M/día
  Productos: 1M productos, actualización diaria completa
```

Calcular para 1 año de datos:
1. Tamaño raw (sin compresión, en bytes por fila × filas)
2. Tamaño estimado en Parquet con zstd
3. Costo mensual en S3 Standard ($0.023/GB)
4. ¿Vale la pena usar compresión más agresiva (zstd nivel 9)?
5. ¿Cuánto ahorras con Parquet vs CSV para las ventas?

---

### Ejercicio 2.7.4 — El pipeline de migración de formatos

**Tipo: Implementar**

Implementar el pipeline que migra el CSV heredado a Parquet particionado:

```python
import polars as pl
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq

def migrar_csv_a_parquet(
    ruta_csv: str,
    ruta_parquet_base: str,
    schema: pa.Schema,
    partition_cols: list[str],
    chunk_size: int = 1_000_000,
):
    """
    Migra un archivo CSV grande a Parquet particionado.
    Procesa en chunks para no llenar la memoria.
    Es idempotente: si se interrumpe, puede retomarse.
    """
    ruta_progreso = Path(ruta_parquet_base) / "_migration_progress.json"

    # Retomar desde donde se dejó si existe el archivo de progreso:
    chunk_inicial = 0
    if ruta_progreso.exists():
        import json
        progreso = json.loads(ruta_progreso.read_text())
        chunk_inicial = progreso.get("ultimo_chunk_completado", 0) + 1
        print(f"Retomando desde chunk {chunk_inicial}")

    for i, chunk in enumerate(
        pl.read_csv_batched(ruta_csv, batch_size=chunk_size)
    ):
        if i < chunk_inicial:
            continue  # saltar chunks ya procesados

        # Validar schema:
        chunk_arrow = chunk.to_arrow().cast(schema)

        # Escribir particionado:
        pq.write_to_dataset(
            chunk_arrow,
            root_path=ruta_parquet_base,
            partition_cols=partition_cols,
            existing_data_behavior="overwrite_or_ignore",
        )

        # Guardar progreso:
        import json
        ruta_progreso.write_text(json.dumps({"ultimo_chunk_completado": i}))
        print(f"Chunk {i} completado ({len(chunk):,} filas)")

    ruta_progreso.unlink()  # eliminar archivo de progreso al terminar
    print("Migración completada")
```

**Restricciones:**
1. Implementar el pipeline completo
2. Simular una interrupción a mitad y verificar que se retoma correctamente
3. Verificar que el output es válido con `pq.read_metadata`
4. Medir cuánto tiempo toma migrar 10 GB de CSV

---

### Ejercicio 2.7.5 — Leer: la decisión que más afecta el rendimiento futuro

**Tipo: Reflexionar/sintetizar**

Al final de este capítulo, una pregunta de síntesis:

Un data engineer senior dice:
*"Antes de escribir una sola línea de Spark o Flink, la decisión más
importante que tomas es el formato del dato y cómo está particionado.
Si eso está mal, puedes tirar el resto."*

Con lo que aprendiste en este capítulo:

1. ¿Estás de acuerdo con esa afirmación?
   Da un ejemplo concreto donde el formato incorrecto hace que
   el código correcto falle en producción.

2. ¿Qué cuatro preguntas harías sobre los datos antes de decidir el formato?

3. Hay una decisión de formato que casi nunca puede cambiarse sin
   reescribir todos los datos históricos. ¿Cuál es?

4. Si pudieras cambiar solo una cosa del sistema CSV heredado del
   Ejercicio 2.7.1, ¿cuál cambiarías primero? ¿Por qué?

5. El Cap.01 terminó con: *"La decisión más importante es: cuánta latencia
   estoy dispuesto a pagar por cuánta completitud de datos."*
   Este capítulo tiene su propia decisión fundamental.
   ¿Cuál es?

---

## Resumen del capítulo

**Los cinco conceptos que determinan el rendimiento antes de escribir código:**

```
1. Columnar vs row: lee solo lo que necesitas
   Para analytics (pocas columnas, muchas filas): columnar (Parquet).
   Para transacciones (todas las columnas, pocas filas): row (PostgreSQL).

2. Estadísticas y predicate pushdown: no leas lo que no necesitas
   Los min/max por row group permiten saltar bloques completos.
   Funciona mejor cuando los datos están ordenados por la columna filtrada.

3. Dictionary encoding: los datos categóricos comprimen dramáticamente
   4 valores únicos en 1M filas: de 5 MB a 250 KB.
   El beneficio desaparece con alta cardinalidad.

4. Compresión: el tradeoff es CPU vs I/O, no solo tamaño
   En pipelines I/O-bound: más compresión = más rápido (menos bytes a leer).
   En pipelines CPU-bound: menos compresión = más rápido (menos CPU en decode).
   zstd nivel 3 es el punto óptimo para la mayoría de los casos.

5. Arrow: el formato en memoria que conecta el ecosistema
   Polars, Pandas, DuckDB, Spark, DataFusion: todos hablan Arrow.
   La conversión entre ellos es frecuentemente zero-copy.
   Sin Arrow, cada conversión es una copia completa de los datos.
```

**La pregunta clave para cualquier decisión de formato:**

> ¿Cuál es el patrón de acceso? ¿Quién leerá estos datos, cuántas veces,
> y qué columnas necesitará?

El formato óptimo para "escribir muchas veces, leer pocas" es diferente al
de "escribir una vez, leer miles de veces". Parquet optimiza para el segundo caso.
Avro optimiza para escritura frecuente con schema evolution.
Arrow IPC optimiza para lectura en el mismo proceso.

No existe el formato universal — existe el formato correcto para el caso de uso.
