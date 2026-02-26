# Guía de Ejercicios — Cap.10: Apache Beam — El Modelo Unificado

> Beam resuelve un problema que los otros frameworks ignoran:
> ¿Por qué tienes que reescribir tu pipeline cuando cambias de batch a streaming?
>
> En Spark, escribes un pipeline batch con DataFrames y otro streaming
> con Structured Streaming — misma lógica, APIs distintas.
> En Flink, similar.
>
> Beam dice: la lógica es la misma. El "runner" (Spark, Flink, Dataflow, local)
> decide cómo ejecutarla. Escribes una vez, ejecutas en cualquier lugar.
>
> El costo de esa abstracción es real: Beam tiene más capas que los otros.
> Pero cuando el portability importa más que el rendimiento máximo,
> Beam es la elección correcta.

---

## El modelo mental: pipelines como grafos de transformaciones

```
En Spark:                           En Beam:
  df = spark.read.parquet(...)        p = beam.Pipeline()
  resultado = df                      resultado = (
    .filter(...)                          p
    .groupBy(...)                         | "Leer"   >> beam.io.ReadFromParquet(...)
    .agg(...)                             | "Filtrar" >> beam.Filter(...)
  resultado.write.parquet(...)           | "Agrupar" >> beam.GroupByKey()
                                         | "Agregar" >> beam.CombinePerKey(sum)
                                         | "Escribir" >> beam.io.WriteToParquet(...)
                                     )
                                     p.run()
```

```
La diferencia central:

Spark separa batch y streaming:
  df = spark.read.parquet(...)           ← batch
  df = spark.readStream.format(...)      ← streaming
  APIs diferentes, planes diferentes

Beam unifica:
  p | "Leer" >> beam.io.ReadFromParquet(...)   ← batch (PCollection bounded)
  p | "Leer" >> beam.io.ReadFromKafka(...)     ← streaming (PCollection unbounded)
  El resto del pipeline es idéntico en ambos casos.
```

---

## Los cuatro conceptos que definen Beam

```
1. PCollection: la colección de datos (puede ser bounded o unbounded)
   Bounded: tamaño conocido (un archivo, una tabla) → batch
   Unbounded: flujo infinito (Kafka, Pub/Sub) → streaming

2. PTransform: una transformación sobre una PCollection
   Primitivas: ParDo (map/filter), GroupByKey (shuffle), Combine, Flatten, Partition
   Composites: combinaciones de primitivas que forman operaciones de alto nivel

3. Pipeline: el grafo de PTransforms
   No se ejecuta hasta p.run()
   El runner optimiza y ejecuta el plan

4. Runner: el motor de ejecución
   DirectRunner: local, para desarrollo y testing
   SparkRunner: Spark como motor
   FlinkRunner: Flink como motor
   DataflowRunner: Google Cloud Dataflow (managed, serverless)
```

---

## Tabla de contenidos

- [Sección 10.1 — El modelo de programación: PCollections y PTransforms](#sección-101--el-modelo-de-programación-pcollections-y-ptransforms)
- [Sección 10.2 — Transforms primitivos: ParDo, GroupByKey, Combine](#sección-102--transforms-primitivos-pardo-groupbykey-combine)
- [Sección 10.3 — Windowing: el tiempo en Beam](#sección-103--windowing-el-tiempo-en-beam)
- [Sección 10.4 — Watermarks y eventos tardíos](#sección-104--watermarks-y-eventos-tardíos)
- [Sección 10.5 — Runners: ejecutar el mismo pipeline en distintos motores](#sección-105--runners-ejecutar-el-mismo-pipeline-en-distintos-motores)
- [Sección 10.6 — IO connectors: leer y escribir en el mundo real](#sección-106--io-connectors-leer-y-escribir-en-el-mundo-real)
- [Sección 10.7 — Cuándo Beam y cuándo las alternativas](#sección-107--cuándo-beam-y-cuándo-las-alternativas)

---

## Sección 10.1 — El Modelo de Programación: PCollections y PTransforms

### Ejercicio 10.1.1 — El primer pipeline: wordcount

```python
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# El pipeline más clásico: wordcount
def wordcount_pipeline(ruta_entrada: str, ruta_salida: str):
    
    # Opciones del pipeline:
    options = PipelineOptions([
        "--runner=DirectRunner",           # ejecutar localmente
        "--direct_num_workers=4",          # 4 workers locales
    ])
    
    with beam.Pipeline(options=options) as p:
        resultado = (
            p
            | "Leer texto"     >> beam.io.ReadFromText(ruta_entrada)
            | "Dividir en palabras" >> beam.FlatMap(str.split)
            | "Filtrar vacíos"  >> beam.Filter(lambda w: len(w) > 0)
            | "Crear pares"    >> beam.Map(lambda w: (w.lower(), 1))
            | "Contar"         >> beam.CombinePerKey(sum)
            | "Formatear"      >> beam.Map(lambda kv: f"{kv[0]}: {kv[1]}")
            | "Escribir"       >> beam.io.WriteToText(ruta_salida)
        )
    
    # Al salir del `with`, el pipeline se ejecuta (p.run() + p.wait_until_finish())
    print("Pipeline completado")

wordcount_pipeline("corpus/*.txt", "resultado/wordcount")
```

**Preguntas:**

1. ¿Por qué cada paso tiene un nombre como `"Leer texto"` y `"Contar"`?
   ¿Qué pasa si dos pasos tienen el mismo nombre?

2. ¿`beam.io.ReadFromText()` produce una PCollection bounded o unbounded?
   ¿Y `beam.io.ReadFromKafka()`?

3. ¿El pipeline se ejecuta línea a línea (como Pandas) o todo de una vez
   al final (como Spark lazy)?

4. ¿Dónde ocurre el shuffle en este pipeline?
   ¿Qué transform lo genera?

5. ¿Cuántas PCollections hay en este pipeline?
   (Pista: cada `|` crea una nueva)

**Pista:** El nombre de cada step tiene dos usos:
(1) aparece en el grafo del pipeline (para debugging y monitoreo),
(2) es el identificador único del step — no puede haber dos steps con el mismo nombre
en el mismo scope. Beam lanza `ValueError` si detecta nombres duplicados.
El shuffle ocurre en `CombinePerKey` (que internamente hace GroupByKey + aggregate).
Hay 6 PCollections intermedias: una después de cada transform.

---

### Ejercicio 10.1.2 — Bounded vs Unbounded: el mismo pipeline para batch y streaming

```python
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

def pipeline_metricas(modo: str = "batch"):
    """
    El mismo pipeline funciona en batch y en streaming.
    Solo cambia la fuente de datos y las opciones.
    """
    options = PipelineOptions()
    
    if modo == "streaming":
        options.view_as(StandardOptions).streaming = True
    
    with beam.Pipeline(options=options) as p:
        
        if modo == "batch":
            # Fuente bounded (archivo Parquet):
            eventos = (
                p
                | "Leer Parquet" >> beam.io.ReadFromParquet("eventos/*.parquet")
            )
        else:
            # Fuente unbounded (Kafka):
            eventos = (
                p
                | "Leer Kafka" >> beam.io.ReadFromKafka(
                    consumer_config={"bootstrap.servers": "localhost:9092"},
                    topics=["eventos"],
                )
                | "Deserializar" >> beam.Map(
                    lambda msg: json.loads(msg[1].decode())
                )
            )
        
        # A partir de aquí, el pipeline es IDÉNTICO para batch y streaming:
        resultado = (
            eventos
            | "Filtrar activos" >> beam.Filter(lambda e: e.get("activo", False))
            | "Extraer región"  >> beam.Map(lambda e: (e["region"], e["monto"]))
            | "Revenue por región" >> beam.CombinePerKey(sum)
            | "Formatear"       >> beam.Map(lambda kv: {"region": kv[0], "revenue": kv[1]})
        )
        
        if modo == "batch":
            resultado | "Escribir" >> beam.io.WriteToText("resultado/batch")
        else:
            resultado | "A BigQuery" >> beam.io.WriteToBigQuery(
                "proyecto:dataset.tabla",
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            )
```

**Preguntas:**

1. ¿Las PCollections `eventos` en batch y streaming son del mismo tipo?
   ¿Qué diferencia tienen internamente?

2. ¿Por qué `beam.CombinePerKey(sum)` funciona en batch pero en streaming
   necesita windowing para tener sentido?

3. ¿Qué pasa si intentas ejecutar el pipeline de streaming con `DirectRunner`?
   ¿Y con `SparkRunner`?

4. Si la lógica de negocio cambia (por ejemplo, añadir un filtro por monto mínimo),
   ¿en cuántos lugares del código hay que cambiarlo?

5. ¿Cuál es la limitación más importante de la abstracción de Beam
   para el modo streaming?

**Pista:** En streaming, `CombinePerKey(sum)` sin windowing acumularía
todos los valores desde el inicio del pipeline — el resultado crecería
indefinidamente sin emitir resultados intermedios. El windowing (§10.3)
resuelve esto: agrupa los eventos en ventanas de tiempo y emite resultados
al cerrar cada ventana. El mismo `CombinePerKey` funciona dentro de una ventana.

---

### Ejercicio 10.1.3 — PCollections: inmutabilidad y ramificación

```python
import apache_beam as beam

# Las PCollections son inmutables — cada transform crea una nueva.
# Esto permite ramificar el pipeline: la misma PCollection puede ser
# input de múltiples transforms.

with beam.Pipeline() as p:
    
    # Una fuente, múltiples ramas:
    todos_los_eventos = (
        p
        | "Leer" >> beam.io.ReadFromParquet("eventos.parquet")
    )
    
    # Rama 1: métricas de compras
    compras = (
        todos_los_eventos
        | "Filtrar compras" >> beam.Filter(lambda e: e["tipo"] == "compra")
        | "Revenue" >> beam.Map(lambda e: (e["region"], e["monto"]))
        | "Revenue total" >> beam.CombinePerKey(sum)
        | "Escribir revenue" >> beam.io.WriteToText("resultado/revenue")
    )
    
    # Rama 2: conteo de usuarios únicos
    usuarios_unicos = (
        todos_los_eventos
        | "Extraer usuarios" >> beam.Map(lambda e: (e["region"], e["user_id"]))
        | "Usuarios por región" >> beam.combiners.Count.PerKey()
        | "Escribir usuarios" >> beam.io.WriteToText("resultado/usuarios")
    )
    
    # Rama 3: eventos anómalos (monto > 10000)
    anomalias = (
        todos_los_eventos
        | "Filtrar anomalías" >> beam.Filter(lambda e: e["monto"] > 10000)
        | "Alertar" >> beam.Map(lambda e: f"ANOMALÍA: user={e['user_id']}, monto={e['monto']}")
        | "Escribir alertas" >> beam.io.WriteToText("resultado/alertas")
    )
```

**Preguntas:**

1. ¿`todos_los_eventos` se lee del disco una vez o tres veces
   (una por cada rama)?

2. ¿Las tres ramas se ejecutan en paralelo o secuencialmente?

3. Si `Filtrar compras` reduce los datos al 20%, ¿el runner puede
   optimizar la lectura para leer solo ese 20%?

4. ¿Cómo combinas (merge) dos ramas en una sola PCollection?
   ¿Cuál es el transform para eso?

5. ¿Puedes crear un grafo cíclico en Beam? ¿Por qué "A" en DAG?

**Pista:** Para combinar dos PCollections en una:
```python
combinada = (
    (pc1, pc2)
    | "Combinar" >> beam.Flatten()
)
```
`Flatten` es el union de Beam — concatena múltiples PCollections del mismo tipo.
Para joins entre PCollections de distintos tipos, el pattern es usar
`CoGroupByKey` (el join de Beam, similar al `join` de Spark).

---

### Ejercicio 10.1.4 — Composite transforms: encapsular lógica reutilizable

```python
import apache_beam as beam

class NormalizarEventos(beam.PTransform):
    """
    Composite transform que encapsula una secuencia de transformaciones.
    Reutilizable en múltiples pipelines.
    """
    
    def __init__(self, monto_minimo: float = 0.0):
        self.monto_minimo = monto_minimo
    
    def expand(self, pcoll):
        """
        El método expand() es donde defines las transformaciones.
        Recibe una PCollection y retorna una PCollection.
        """
        return (
            pcoll
            | "Filtrar nulos" >> beam.Filter(
                lambda e: e.get("monto") is not None and e.get("user_id") is not None
            )
            | "Filtrar monto mínimo" >> beam.Filter(
                lambda e: e["monto"] >= self.monto_minimo
            )
            | "Normalizar campos" >> beam.Map(lambda e: {
                **e,
                "region": e.get("region", "desconocida").lower().strip(),
                "user_id": str(e["user_id"]),
                "monto": float(e["monto"]),
            })
        )

# Usar el composite transform como si fuera un transform primitivo:
with beam.Pipeline() as p:
    eventos_limpios = (
        p
        | "Leer" >> beam.io.ReadFromParquet("eventos.parquet")
        | "Normalizar" >> NormalizarEventos(monto_minimo=1.0)
    )
    
    # Los composite transforms se pueden anidar:
    class Pipeline Completo(beam.PTransform):
        def expand(self, p):
            return (
                p
                | "Leer" >> beam.io.ReadFromParquet("eventos.parquet")
                | "Normalizar" >> NormalizarEventos(monto_minimo=1.0)
                | "Agrupar" >> beam.Map(lambda e: (e["region"], e["monto"]))
                | "Revenue" >> beam.CombinePerKey(sum)
            )
```

**Restricciones:**
1. Implementar `NormalizarEventos` completo y verificar con datos sucios
2. Implementar un composite transform `EnriquecerConCatalogo` que hace
   un join con un catálogo de productos
3. ¿Los composite transforms se pueden testar independientemente?
   ¿Cómo se escribe un test unitario para un PTransform?

---

### Ejercicio 10.1.5 — Leer: el modelo de ejecución de Beam

**Tipo: Analizar**

Analizar el siguiente pipeline y responder las preguntas sobre su ejecución:

```python
import apache_beam as beam

with beam.Pipeline() as p:
    ventas = (
        p
        | beam.io.ReadFromParquet("ventas/*.parquet")  # 500 GB, 1000 archivos
    )
    
    clientes = (
        p
        | beam.io.ReadFromParquet("clientes.parquet")  # 200 MB
    )
    
    resultado = (
        {"ventas": ventas, "clientes": clientes}
        | beam.CoGroupByKey()
        | beam.Map(lambda kv: procesar(kv))
        | beam.io.WriteToParquet("resultado/")
    )
```

**Preguntas:**

1. ¿`beam.CoGroupByKey()` es equivalente al JOIN de SQL? ¿A cuál tipo de JOIN?

2. ¿Cuántos shuffles tiene este pipeline?

3. ¿El runner puede hacer broadcast join si `clientes.parquet` es pequeño?
   ¿Depende del runner?

4. ¿Los 1000 archivos de `ventas/*.parquet` se leen en paralelo?
   ¿Con cuántos workers?

5. ¿Qué información necesita el runner para decidir el grado de paralelismo?

**Pista:** `CoGroupByKey` recibe un diccionario de PCollections con la misma key
y agrupa todos los valores de todas las PCollections por key. Para el join:
`kv = (key, {"ventas": [venta_1, venta_2], "clientes": [cliente_1]})`.
El resultado es un full outer join por defecto — aparecen todas las keys
de todas las PCollections. Si `clientes` no tiene una key, aparece con lista vacía.

---

## Sección 10.2 — Transforms Primitivos: ParDo, GroupByKey, Combine

### Ejercicio 10.2.1 — ParDo: el transform más flexible

```python
import apache_beam as beam
from apache_beam import DoFn

# ParDo es el transform más general — equivale al map/filter/flatMap de Spark.
# La diferencia: puede producir 0, 1, o N elementos por elemento de entrada.
# También puede tener estado y temporizadores (stateful ParDo).

class ExtraerEventosValidos(DoFn):
    """
    DoFn: la función que ejecuta ParDo.
    """
    
    def __init__(self, schema_esperado: list[str]):
        self.schema_esperado = schema_esperado
    
    def setup(self):
        """Llamado una vez por worker al inicializar. Útil para conexiones."""
        self.errores = 0
    
    def process(self, elemento, timestamp=DoFn.TimestampParam):
        """
        Llamado una vez por elemento.
        
        Puede usar:
        - yield: emitir un elemento al output principal
        - yield beam.pvalue.TaggedOutput("nombre", valor): output secundario
        - return: no emitir nada (equivale a filter)
        """
        if not isinstance(elemento, dict):
            yield beam.pvalue.TaggedOutput("errores", {
                "error": "no es dict",
                "elemento": str(elemento),
                "timestamp": float(timestamp),
            })
            return
        
        campos_faltantes = [c for c in self.schema_esperado if c not in elemento]
        if campos_faltantes:
            yield beam.pvalue.TaggedOutput("errores", {
                "error": f"campos faltantes: {campos_faltantes}",
                "elemento": elemento,
            })
            return
        
        # Elemento válido — emitir al output principal:
        yield elemento
    
    def teardown(self):
        """Llamado al finalizar el worker."""
        print(f"Worker procesó con {self.errores} errores")

# Usar con outputs múltiples (tagged outputs):
with beam.Pipeline() as p:
    resultados = (
        p
        | "Leer" >> beam.io.ReadFromText("datos.jsonl")
        | "Parsear" >> beam.Map(json.loads)
        | "Validar" >> beam.ParDo(
            ExtraerEventosValidos(["id", "user_id", "monto"]),
        ).with_outputs("errores", main="validos")
    )
    
    validos = resultados.validos
    errores = resultados.errores
    
    validos | "Procesar" >> beam.Map(procesar_evento)
    errores | "Log errores" >> beam.io.WriteToText("errores/")
```

**Preguntas:**

1. ¿Cuál es la diferencia entre `beam.Map` y `beam.ParDo`?
   ¿Cuándo necesitas uno vs el otro?

2. ¿El método `setup()` se llama una vez por elemento o una vez por worker?
   ¿Qué tipos de recursos inicializarías ahí?

3. ¿Los outputs secundarios (tagged outputs) tienen algún costo adicional
   en el runner?

4. ¿Un `DoFn` puede tener estado (compartido entre llamadas al mismo worker)?
   ¿Y estado compartido entre todos los workers?

5. ¿`yield beam.pvalue.TaggedOutput(...)` puede usarse para múltiples outputs
   en el mismo `process()` call?

**Pista:** `beam.Map(fn)` = `beam.ParDo(lambda element: [fn(element)])` — es azúcar
sintáctico para el caso más simple. `beam.ParDo` es necesario cuando:
(1) quieres outputs secundarios, (2) necesitas estado entre elementos,
(3) necesitas `setup()/teardown()` para recursos externos, o
(4) la función puede producir 0 o múltiples outputs (`beam.FlatMap` también).
El estado en `DoFn` está particionado por key — no es global entre todos los workers.

---

### Ejercicio 10.2.2 — GroupByKey vs CombinePerKey: el tradeoff de Beam

```python
import apache_beam as beam
from apache_beam.transforms.combinefn_lifecycle import CallableWrapperCombineFn
import statistics

# GroupByKey: agrupa todos los valores de una key — puede ser costoso en memoria
with beam.Pipeline() as p:
    resultado_gbk = (
        p
        | "Datos" >> beam.Create([("a", 1), ("b", 2), ("a", 3), ("b", 4)])
        | "GroupByKey" >> beam.GroupByKey()
        # resultado: [("a", [1, 3]), ("b", [2, 4])]
    )

# CombinePerKey: reduce localmente antes del shuffle (como combiner en Hadoop)
with beam.Pipeline() as p:
    resultado_combine = (
        p
        | "Datos" >> beam.Create([("a", 1), ("b", 2), ("a", 3), ("b", 4)])
        | "CombinePerKey" >> beam.CombinePerKey(sum)
        # resultado: [("a", 4), ("b", 6)]
    )

# CombineFn personalizado: para agregaciones complejas
class MedianCombineFn(beam.CombineFn):
    """
    Calcula la mediana de forma distribuida.
    
    Importante: la mediana no es directamente combinable (necesita todos los valores).
    Este CombineFn acumula todos los valores — no tan eficiente como sum/count
    pero más flexible.
    """
    
    def create_accumulator(self):
        return []  # lista de valores
    
    def add_input(self, accumulator, input):
        return accumulator + [input]
    
    def merge_accumulators(self, accumulators):
        # Combinar todas las listas parciales:
        resultado = []
        for acc in accumulators:
            resultado.extend(acc)
        return resultado
    
    def extract_output(self, accumulator):
        if not accumulator:
            return None
        return statistics.median(accumulator)
    
    def compact(self, accumulator):
        # Opcional: reducir el acumulador para menos transferencia de datos
        # Para mediana exacta: no podemos reducir sin perder precisión
        return accumulator

# Usar:
with beam.Pipeline() as p:
    medianas = (
        p
        | "Datos" >> beam.io.ReadFromParquet("montos.parquet")
        | "Pares" >> beam.Map(lambda e: (e["region"], e["monto"]))
        | "Mediana" >> beam.CombinePerKey(MedianCombineFn())
    )
```

**Preguntas:**

1. ¿Por qué `CombinePerKey(sum)` es más eficiente que `GroupByKey` + `Map(sum)`?
   ¿Qué hace internamente el runner con el `CombineFn`?

2. ¿La `MedianCombineFn` puede ser optimizada con un `compact()` efectivo?
   ¿Qué dato compacto representaría el estado de una mediana parcial?

3. ¿`merge_accumulators` en la `MedianCombineFn` es asociativo?
   ¿Puede el runner llamarlo en cualquier orden?

4. Para un dataset con data skew severo (una key tiene el 80% de los datos),
   ¿`CombinePerKey` ayuda más o menos que con datos uniformes?

5. ¿Cuándo es inevitable usar `GroupByKey` (no puede reemplazarse con `CombinePerKey`)?

**Pista:** `CombinePerKey` tiene un "lifting" automático: el runner aplica
`add_input` localmente en cada worker (fase partial) y luego `merge_accumulators`
para combinar los resultados de distintos workers (fase final). Es exactamente
el patrón combiner del Cap.03. Para la mediana, no hay una representación
compacta del estado parcial que permita calcular la mediana exacta al final.
La aproximación: usar el percentil de TDigest o HyperLogLog, que sí tienen
estados combinables en tamaño constante.

---

### Ejercicio 10.2.3 — CoGroupByKey: joins en Beam

```python
import apache_beam as beam

# CoGroupByKey es el join general de Beam.
# Une múltiples PCollections por key.

with beam.Pipeline() as p:
    ventas = (
        p
        | "Ventas" >> beam.Create([
            ("user_1", {"monto": 100, "producto": "A"}),
            ("user_2", {"monto": 200, "producto": "B"}),
            ("user_1", {"monto": 150, "producto": "C"}),
        ])
    )
    
    clientes = (
        p
        | "Clientes" >> beam.Create([
            ("user_1", {"nombre": "Alice", "region": "norte"}),
            ("user_2", {"nombre": "Bob", "region": "sur"}),
            ("user_3", {"nombre": "Charlie", "region": "este"}),  # sin ventas
        ])
    )
    
    # CoGroupByKey une por key:
    resultado = (
        {"ventas": ventas, "clientes": clientes}
        | "Join" >> beam.CoGroupByKey()
        # resultado por key:
        # ("user_1", {"ventas": [{"monto":100,...}, {"monto":150,...}], "clientes": [{"nombre":"Alice",...}]})
        # ("user_2", {"ventas": [{"monto":200,...}], "clientes": [{"nombre":"Bob",...}]})
        # ("user_3", {"ventas": [], "clientes": [{"nombre":"Charlie",...}]})
    )
    
    # Post-procesar el resultado del CoGroupByKey:
    def procesar_join(elemento):
        key, grupos = elemento
        ventas_usuario = grupos["ventas"]
        clientes_usuario = grupos["clientes"]
        
        if not clientes_usuario:
            return None  # usuario sin info de cliente — descartamos
        
        cliente = clientes_usuario[0]
        total_ventas = sum(v["monto"] for v in ventas_usuario)
        
        return {
            "user_id": key,
            "nombre": cliente["nombre"],
            "region": cliente["region"],
            "num_compras": len(ventas_usuario),
            "total_gasto": total_ventas,
        }
    
    enriquecido = (
        resultado
        | "Procesar join" >> beam.Map(procesar_join)
        | "Filtrar None"  >> beam.Filter(lambda x: x is not None)
    )
```

**Preguntas:**

1. ¿El `CoGroupByKey` en Beam es un inner join, left join, o full outer join?

2. ¿Si `ventas` tiene 10 millones de filas y `clientes` tiene 100,000,
   Beam puede hacer broadcast join automáticamente?

3. ¿Cuántos shuffles genera `CoGroupByKey`?

4. ¿Qué pasa con las keys que están en `ventas` pero no en `clientes`?
   ¿Y las que están en `clientes` pero no en `ventas`?

5. ¿Cómo implementarías un inner join (solo keys en ambas colecciones)
   usando `CoGroupByKey`?

**Pista:** `CoGroupByKey` es un full outer join — retorna todas las keys
de todas las PCollections de entrada. Para las keys que no están en alguna
PCollection, la lista correspondiente está vacía. Para implementar inner join:
filtrar los resultados donde alguna de las listas esté vacía.
El runner puede optimizar a broadcast join si detecta que una de las PCollections
es pequeña (depende del runner — DataflowRunner sí, DirectRunner no).

---

### Ejercicio 10.2.4 — Stateful DoFn: estado por key

```python
import apache_beam as beam
from apache_beam import DoFn
from apache_beam.transforms.userstate import BagStateSpec, CombiningValueStateSpec
from apache_beam.coders import VarIntCoder

class DetectorFraude(DoFn):
    """
    DoFn stateful: mantiene el estado de las últimas N transacciones por usuario.
    
    IMPORTANTE: el estado es por key (user_id) y está particionado entre workers.
    No es estado global — es estado local a cada partición.
    
    Requiere que la PCollection esté keyed (KV pairs).
    """
    
    # Definir el estado que se mantiene entre elementos:
    HISTORIAL = BagStateSpec("historial", VarIntCoder())  # bag de enteros
    TOTAL_GASTO = CombiningValueStateSpec("total", VarIntCoder(), sum)
    
    def process(
        self,
        elemento,
        historial=DoFn.StateParam(HISTORIAL),
        total_gasto=DoFn.StateParam(TOTAL_GASTO),
        timestamp=DoFn.TimestampParam,
    ):
        user_id, transaccion = elemento
        monto = transaccion["monto"]
        
        # Añadir al historial:
        historial.add(monto)
        total_gasto.add(monto)
        
        # Leer el historial:
        ultimas_transacciones = list(historial.read())
        total = total_gasto.read()
        
        # Detectar anomalía: si el monto > 3× el promedio histórico:
        if len(ultimas_transacciones) >= 5:
            promedio = total / len(ultimas_transacciones)
            if monto > 3 * promedio:
                yield {
                    "user_id": user_id,
                    "monto_actual": monto,
                    "promedio_historico": promedio,
                    "alerta": "FRAUDE_POTENCIAL",
                }

# Usar:
with beam.Pipeline(options=PipelineOptions(["--streaming"])) as p:
    transacciones = (
        p
        | beam.io.ReadFromKafka(...)
        | beam.Map(lambda msg: (msg["user_id"], msg))  # hacer keyed
    )
    
    alertas = (
        transacciones
        | "Detectar fraude" >> beam.ParDo(DetectorFraude())
    )
```

**Preguntas:**

1. ¿El estado en `BagStateSpec` está en memoria o en disco?
   ¿Qué pasa si el estado crece demasiado?

2. ¿El estado de `user_123` puede estar en dos workers simultáneamente?
   ¿Por qué no?

3. ¿Cómo limitas el historial a las últimas N transacciones
   si `BagStateSpec` acumula indefinidamente?

4. ¿Los stateful DoFns funcionan en modo batch (PCollection bounded)?

5. ¿Cómo se maneja el estado si el pipeline se reinicia?
   ¿Se pierde o se recupera?

**Pista:** El estado está en el backend de estado configurado (por defecto en memoria
para DirectRunner, en RocksDB para Flink, en Bigtable/Firestore para Dataflow).
Para limitar el historial: usar `TimerSpec` para limpiar el estado periódicamente,
o mantener manualmente solo los últimos N valores al leer el bag.
En modo batch, los stateful DoFns funcionan pero son menos naturales —
en batch suele ser más eficiente usar GroupByKey + procesamiento secuencial.

---

### Ejercicio 10.2.5 — Leer: el wordcount completo con todas las optimizaciones

**Tipo: Comparar**

Comparar cuatro implementaciones del wordcount y analizar las diferencias:

```python
import apache_beam as beam

# Versión 1: usando primitivos básicos
def wordcount_v1(p):
    return (
        p
        | beam.io.ReadFromText("corpus.txt")
        | beam.FlatMap(str.split)
        | beam.Map(lambda w: (w, 1))
        | beam.GroupByKey()
        | beam.Map(lambda kv: (kv[0], sum(kv[1])))
    )

# Versión 2: usando CombinePerKey (más eficiente)
def wordcount_v2(p):
    return (
        p
        | beam.io.ReadFromText("corpus.txt")
        | beam.FlatMap(str.split)
        | beam.Map(lambda w: (w, 1))
        | beam.CombinePerKey(sum)
    )

# Versión 3: usando combiners.Count
def wordcount_v3(p):
    return (
        p
        | beam.io.ReadFromText("corpus.txt")
        | beam.FlatMap(str.split)
        | beam.combiners.Count.PerElement()
    )

# Versión 4: usando transforms de más alto nivel
def wordcount_v4(p):
    return (
        p
        | beam.io.ReadFromText("corpus.txt")
        | "Tokenizar" >> beam.FlatMap(lambda l: l.lower().split())
        | "Filtrar stop words" >> beam.Filter(lambda w: w not in STOP_WORDS)
        | "Normalizar" >> beam.Map(lambda w: w.strip(".,!?"))
        | beam.combiners.Count.PerElement()
    )
```

**Preguntas:**

1. ¿Cuántos shuffles tiene cada versión? ¿Cuál tiene menos?

2. ¿`GroupByKey()` en la Versión 1 es equivalente a `CombinePerKey(sum)` en la Versión 2?
   ¿Cuál es más eficiente y por qué?

3. ¿`beam.combiners.Count.PerElement()` internamente usa `CombinePerKey`?

4. ¿La Versión 4 produce el mismo resultado que las otras
   con los mismos datos de entrada? ¿Por qué puede diferir?

5. ¿Cuándo preferirías `GroupByKey` sobre `CombinePerKey`?

---

## Sección 10.3 — Windowing: el Tiempo en Beam

### Ejercicio 10.3.1 — El problema del tiempo en streaming

```
Sin windowing en streaming:
  CombinePerKey(sum) acumularía TODOS los valores desde el inicio del pipeline.
  El resultado nunca se emitiría (o se emitiría al cerrar el pipeline — nunca en streaming).
  
Con windowing:
  Los eventos se agrupan en ventanas de tiempo.
  Al cerrar cada ventana, se emite el resultado parcial.
  Los eventos tardíos se manejan con watermarks y grace periods.
```

```python
import apache_beam as beam
from apache_beam.transforms.window import FixedWindows, SlidingWindows, Sessions
from apache_beam.transforms.trigger import AfterWatermark, AfterProcessingTime, AccumulationMode
import apache_beam.transforms.window as window

# Tipos de ventanas:

# Tumbling (Fixed): sin solapamiento, sin estado entre ventanas
p | beam.WindowInto(FixedWindows(60))  # ventanas de 60 segundos

# Hopping (Sliding): con solapamiento
p | beam.WindowInto(SlidingWindows(
    size=60,    # tamaño de la ventana: 60 segundos
    period=10,  # nueva ventana cada 10 segundos
    # cada evento pertenece a 6 ventanas distintas
))

# Session: basada en inactividad
p | beam.WindowInto(Sessions(
    gap_size=300,  # nueva sesión si hay 5 minutos de inactividad
))

# Global: toda la PCollection en una sola ventana (batch)
p | beam.WindowInto(window.GlobalWindows())
```

**Preguntas:**

1. Un evento con timestamp 14:05:30 cae en:
   - ¿Qué ventana tumbling de 5 minutos? (14:00-14:05 o 14:05-14:10)
   - ¿Cuántas ventanas hopping de 10min con período de 5min?
   - ¿Cuál sesión, si el evento previo del usuario fue a las 14:04?

2. ¿Por qué las ventanas de sesión son más costosas de implementar
   que las tumbling?

3. Para calcular "clicks por usuario en los últimos 30 minutos"
   (una métrica que siempre mira los últimos 30 minutos):
   ¿tumbling, hopping, o sesión?

4. ¿Las ventanas se aplican por clave (by key) o globalmente?
   ¿Todos los usuarios comparten la misma ventana?

5. ¿Cuándo tiene sentido usar múltiples tipos de ventana en el mismo pipeline?

**Pista:** Un evento con timestamp 14:05:30 en una ventana tumbling de 5 minutos:
la ventana es `[14:05:00, 14:10:00)` — el timestamp se redondea al inicio de su ventana.
Para la ventana hopping de 10min con período de 5min: el evento pertenece a 2 ventanas
(`[14:00:00, 14:10:00)` y `[14:05:00, 14:15:00)`). Esto significa que el mismo evento
se procesa dos veces — el costo del solapamiento.

---

### Ejercicio 10.3.2 — Implementar métricas por ventana

```python
import apache_beam as beam
from apache_beam.transforms.window import FixedWindows
import apache_beam.transforms.window as window
import json

def pipeline_metricas_por_minuto(
    bootstrap_servers: str,
    topic: str,
    tabla_bq: str,
):
    """
    Calcula revenue por región cada 60 segundos.
    """
    options = PipelineOptions([
        "--streaming",
        "--runner=DataflowRunner",
        "--project=mi-proyecto",
        "--region=us-central1",
    ])
    
    with beam.Pipeline(options=options) as p:
        eventos = (
            p
            | "Leer Kafka" >> beam.io.ReadFromKafka(
                consumer_config={"bootstrap.servers": bootstrap_servers},
                topics=[topic],
                # Usar el timestamp del mensaje como event time:
                with_metadata=True,
            )
            | "Deserializar" >> beam.Map(
                lambda record: json.loads(record[1].decode())
            )
        )
        
        revenue_por_ventana = (
            eventos
            | "Asignar ventana" >> beam.WindowInto(
                FixedWindows(60),  # ventanas de 60 segundos
                trigger=AfterWatermark(
                    early=AfterProcessingTime(30),  # resultados tempranos cada 30s
                    late=AfterCount(1),              # resultado cuando llega evento tardío
                ),
                accumulation_mode=AccumulationMode.DISCARDING,  # no acumular entre panes
            )
            | "Pares"  >> beam.Map(lambda e: (e["region"], e["monto"]))
            | "Revenue" >> beam.CombinePerKey(sum)
            | "Añadir metadata de ventana" >> beam.ParDo(AñadirMetadataVentana())
            | "Escribir BQ" >> beam.io.WriteToBigQuery(tabla_bq)
        )

class AñadirMetadataVentana(beam.DoFn):
    """Añadir el rango de la ventana a cada elemento."""
    
    def process(self, elemento, window=beam.DoFn.WindowParam):
        key, valor = elemento
        yield {
            "region": key,
            "revenue": valor,
            "ventana_inicio": window.start.to_rfc3339(),
            "ventana_fin": window.end.to_rfc3339(),
        }
```

**Restricciones:**
1. Implementar el pipeline completo
2. ¿Qué significa `AccumulationMode.DISCARDING`? ¿Cuándo usarías `ACCUMULATING`?
3. ¿Los "early results" (resultados tempranos antes del watermark) son parciales
   o completos?
4. ¿Cuántas veces puede emitirse el resultado de una ventana de 60 segundos?

---

### Ejercicio 10.3.3 — Event time vs Processing time: la diferencia importa

```
Event time: cuándo ocurrió el evento (timestamp en el payload del mensaje)
Processing time: cuándo el pipeline lo procesa (ahora)

Ejemplo de evento tardío:
  14:05:30 — el evento ocurre en el dispositivo del usuario
  14:06:15 — el dispositivo envía el evento a Kafka (45 segundos de delay)
  14:07:00 — el pipeline lo procesa (85 segundos después del evento)
  
Con ventanas de event time:
  El evento pertenece a la ventana 14:05:00-14:10:00 (según cuándo ocurrió)
  
Con ventanas de processing time:
  El evento pertenece a la ventana 14:05:00-14:10:00 o 14:10:00-14:15:00
  (según cuándo el pipeline lo procesó — no determinista)
```

```python
import apache_beam as beam
from apache_beam.transforms.window import FixedWindows

# Por defecto, Beam usa el timestamp del mensaje (event time):
eventos | beam.WindowInto(FixedWindows(60))

# Para usar processing time explícitamente:
from apache_beam.transforms.trigger import AfterProcessingTime
eventos | beam.WindowInto(
    FixedWindows(60),
    trigger=AfterProcessingTime(60),
)

# Para Kafka: Beam usa el timestamp del mensaje como event time:
beam.io.ReadFromKafka(
    ...,
    # timestamp_policy=... puede configurarse para usar offset o timestamp del header
)
```

**Preguntas:**

1. ¿Para un sistema de facturación, qué tipo de tiempo es correcto:
   event time o processing time? ¿Por qué?

2. ¿Para un dashboard de "eventos en tiempo real" (lo que está pasando ahora),
   qué tipo de tiempo es más apropiado?

3. Si tienes un flujo de dispositivos IoT con relojes desincronizados,
   ¿qué problemas causa el event time? ¿Cómo lo manejas?

4. ¿Processing time es más simple de implementar que event time?
   ¿Tiene desventajas?

5. ¿Puede el mismo pipeline usar event time para algunas ventanas
   y processing time para otras?

---

### Ejercicio 10.3.4 — Ventanas de sesión: agrupar por inactividad

```python
import apache_beam as beam
from apache_beam.transforms.window import Sessions

# Ventanas de sesión: una sesión termina cuando no hay eventos
# durante `gap_size` segundos.

def calcular_sesiones_usuario(eventos_keyed):
    """
    Calcular métricas por sesión de usuario.
    
    Una sesión = secuencia de eventos con menos de 5 minutos entre ellos.
    Si el usuario no hace nada en 5 minutos → nueva sesión.
    """
    return (
        eventos_keyed  # PCollection de (user_id, evento)
        | "Ventanas sesión" >> beam.WindowInto(
            Sessions(gap_size=300),  # 5 minutos de inactividad → nueva sesión
            trigger=AfterWatermark(),
            accumulation_mode=AccumulationMode.DISCARDING,
        )
        | "Métricas sesión" >> beam.CombinePerKey(
            SesionCombineFn()
        )
    )

class SesionCombineFn(beam.CombineFn):
    """Calcula métricas de una sesión de usuario."""
    
    def create_accumulator(self):
        return {
            "num_eventos": 0,
            "monto_total": 0.0,
            "tipos_evento": set(),
            "compro": False,
        }
    
    def add_input(self, acc, evento):
        return {
            "num_eventos": acc["num_eventos"] + 1,
            "monto_total": acc["monto_total"] + evento.get("monto", 0),
            "tipos_evento": acc["tipos_evento"] | {evento["tipo"]},
            "compro": acc["compro"] or evento["tipo"] == "compra",
        }
    
    def merge_accumulators(self, acumuladores):
        resultado = self.create_accumulator()
        for acc in acumuladores:
            resultado["num_eventos"] += acc["num_eventos"]
            resultado["monto_total"] += acc["monto_total"]
            resultado["tipos_evento"] |= acc["tipos_evento"]
            resultado["compro"] = resultado["compro"] or acc["compro"]
        return resultado
    
    def extract_output(self, acc):
        return {
            **acc,
            "tipos_evento": list(acc["tipos_evento"]),
        }
```

**Preguntas:**

1. ¿Las ventanas de sesión son por user_id o globales para todos los usuarios?

2. Si user_1 no tiene actividad por 6 minutos y luego tiene un click,
   ¿el click pertenece a la sesión anterior o a una nueva?

3. ¿Cuánta memoria ocupa mantener las ventanas de sesión para 1M usuarios activos?

4. ¿Las sesiones tienen duración máxima? ¿Qué pasa si un usuario hace
   un click cada 4 minutos durante 10 horas?

---

### Ejercicio 10.3.5 — Leer: elegir el tipo de ventana para cada caso

**Tipo: Analizar**

Para cada métrica de negocio, elegir el tipo de ventana más apropiado:

```
1. Revenue total por día (para reportes diarios)
2. Usuarios activos en los últimos 5 minutos (sliding window KPI)
3. Tasa de conversión por sesión de usuario
4. Alertas de anomalía: si hay 10 errores en 1 minuto
5. Acumulado de ventas desde el inicio del mes (siempre creciente)
6. Media móvil de precios de los últimos 7 días
7. Tiempo promedio entre la primera visita y la compra
```

Para cada una: tipo de ventana, tamaño/período, y qué timestamp usar.

---

## Sección 10.4 — Watermarks y Eventos Tardíos

### Ejercicio 10.4.1 — El watermark: cuándo cerrar una ventana

```
El watermark responde: "¿Con qué seguridad puede el sistema afirmar
que todos los eventos hasta el tiempo T ya llegaron?"

Si el watermark está en T=14:10:00:
  El sistema cree que no llegarán más eventos con timestamp < 14:10:00.
  Las ventanas que terminan antes de 14:10:00 pueden cerrarse.
  
Watermark ideal (event time = processing time):
  Solo posible si todos los eventos llegan al instante.
  
Watermark heurístico (basado en retraso observado):
  El sistema observa: "los últimos N eventos tuvieron un retraso promedio de X segundos"
  Watermark = processing_time - max_observed_lag
  
Watermark conservador vs agresivo:
  Conservador: watermark avanza lento → pocas ventanas tardías pero resultados más lentos
  Agresivo: watermark avanza rápido → más ventanas tardías pero resultados más rápidos
```

```python
import apache_beam as beam
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.trigger import (
    AfterWatermark, AfterProcessingTime, AfterCount, AccumulationMode
)

# Configurar cómo se manejan los eventos tardíos:
pipeline | beam.WindowInto(
    FixedWindows(60),
    # Trigger: cuándo emitir resultados
    trigger=AfterWatermark(
        # Emitir resultados tempranos (antes del watermark):
        early=AfterProcessingTime(30),   # cada 30 segundos de processing time
        # Emitir resultado adicional cuando llega un evento tardío:
        late=AfterCount(1),              # al llegar cada evento tardío
    ),
    # Cómo tratar los resultados tardíos:
    allowed_lateness=60,  # permitir eventos con hasta 60s de retraso
    # Si el evento llega después del allowed_lateness: se descarta (o se pone en side output)
    
    # Acumulación: cómo combinar el resultado tardío con el previo
    accumulation_mode=AccumulationMode.ACCUMULATING,  # el resultado tardío acumula
    # AccumulationMode.DISCARDING: el resultado tardío reemplaza al anterior
)
```

**Preguntas:**

1. ¿Qué pasa con un evento que llega 2 minutos tarde si `allowed_lateness=60s`?

2. ¿`AccumulationMode.ACCUMULATING` vs `DISCARDING` — cuál genera más escrituras
   al sink?

3. Si el watermark de Kafka está basado en el timestamp del mensaje,
   ¿qué pasa si un producer produce mensajes con timestamps del pasado?
   (por ejemplo, un reprocesamiento de datos históricos)

4. ¿Cómo el runner calcula el watermark para un topic de Kafka con
   múltiples particiones?

5. ¿Los "early results" son siempre parciales? ¿Pueden ser completos?

**Pista:** El watermark para múltiples particiones de Kafka es el mínimo
del watermark de todas las particiones. Si una partición está inactiva
(sin mensajes nuevos), su watermark no avanza, bloqueando el watermark global.
Esto es el "idle partition problem" — las ventanas no cierran si hay una
partición silenciosa. La solución: configurar `idle_strategy` para avanzar
el watermark de particiones inactivas después de un timeout.

---

### Ejercicio 10.4.2 — Implementar: pipeline resistente a eventos tardíos

```python
import apache_beam as beam
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.trigger import (
    AfterWatermark, AfterCount, AccumulationMode, Repeatedly, Always
)

class PipelineMétricasRobusto:
    """
    Pipeline que maneja eventos tardíos correctamente.
    
    Estrategia:
    1. Emitir resultado preliminar cuando el watermark pasa el fin de la ventana
    2. Reemitir resultado actualizado cuando llegan eventos tardíos
    3. Descarta eventos que llegan más de 5 minutos tarde
    """
    
    @staticmethod
    def construir(p):
        return (
            p
            | "Leer" >> beam.io.ReadFromKafka(
                consumer_config={"bootstrap.servers": "localhost:9092"},
                topics=["transacciones"],
            )
            | "Deserializar" >> beam.Map(lambda m: json.loads(m[1].decode()))
            | "Ventanas" >> beam.WindowInto(
                FixedWindows(60),
                trigger=AfterWatermark(
                    late=AfterCount(1),
                ),
                allowed_lateness=300,  # 5 minutos de gracia
                accumulation_mode=AccumulationMode.ACCUMULATING,
            )
            | "Pares" >> beam.Map(lambda e: (e["region"], e["monto"]))
            | "Revenue" >> beam.CombinePerKey(sum)
            | "Con pane info" >> beam.ParDo(AñadirInfoPane())
            | "Escribir" >> beam.io.WriteToBigQuery("metricas.revenue_por_ventana")
        )

class AñadirInfoPane(beam.DoFn):
    """Añadir información sobre si el resultado es preliminar o final."""
    
    def process(self, elemento, window=beam.DoFn.WindowParam, pane_info=beam.DoFn.PaneInfoParam):
        key, valor = elemento
        yield {
            "region": key,
            "revenue": valor,
            "ventana_inicio": window.start.to_rfc3339(),
            "ventana_fin": window.end.to_rfc3339(),
            "es_final": pane_info.is_last,         # ¿es el último resultado para esta ventana?
            "es_tardio": pane_info.is_first is False,  # ¿llegó después del watermark?
            "indice_pane": pane_info.index,        # cuántos resultados se han emitido para esta ventana
        }
```

**Restricciones:**
1. Implementar el pipeline completo
2. Simular eventos tardíos y verificar que se re-emiten resultados
3. ¿Cuántos registros puede generar una sola ventana en BigQuery?
4. Implementar deduplicación en el sink para manejar los múltiples "panes"

---

### Ejercicio 10.4.3 — Leer: el watermark que no avanzaba

**Tipo: Diagnosticar**

Un pipeline de Beam tiene una latencia creciente. Las ventanas de 1 minuto
no se están emitiendo después de 2-3 minutos; en cambio, tardan 10-15 minutos.
Los logs muestran:

```
[INFO] Watermark at 14:05:00 (current time: 14:17:42)
[INFO] Watermark at 14:05:00 (current time: 14:18:15)
[INFO] Watermark at 14:05:00 (current time: 14:19:01)
[WARN] Partition 3 has not received messages in 720 seconds
```

**Preguntas:**

1. ¿Por qué el watermark está bloqueado en 14:05:00 aunque el tiempo de
   procesamiento es 14:17-14:19?

2. ¿Cuál es la relación entre la Partition 3 inactiva y el watermark bloqueado?

3. ¿Cómo el watermark de Kafka se calcula cuando hay múltiples particiones?

4. ¿Qué solución implementarías para que el watermark avance
   aunque haya particiones inactivas?

5. ¿Este problema ocurriría si usas processing time en lugar de event time?

---

### Ejercicio 10.4.4 — Triggers: controlar cuándo se emiten resultados

```python
from apache_beam.transforms.trigger import (
    AfterWatermark, AfterProcessingTime, AfterCount,
    Repeatedly, OrFinally, AccumulationMode
)

# Trigger 1: solo al cerrar la ventana (default)
trigger_simple = AfterWatermark()

# Trigger 2: resultados tempranos frecuentes + resultado final
trigger_con_previos = AfterWatermark(
    early=Repeatedly(AfterProcessingTime(10)),  # cada 10s de processing time
    late=AfterCount(100),                        # cuando llegan 100 eventos tardíos
)

# Trigger 3: emitir siempre que lleguen 1000 elementos
trigger_por_count = Repeatedly(AfterCount(1000))

# Trigger 4: por count O por tiempo, lo que ocurra primero
trigger_hibrido = Repeatedly(
    AfterCount(1000).or_finally(AfterProcessingTime(5))
)
```

**Preguntas:**

1. ¿`Repeatedly(AfterCount(1000))` emite el resultado de la ventana actual
   cada 1000 elementos aunque la ventana no haya cerrado?

2. ¿Cuándo usarías `trigger_por_count` sobre `trigger_con_previos`?

3. ¿El trigger afecta al watermark o el watermark afecta al trigger?

4. ¿`AccumulationMode.DISCARDING` significa que los eventos se descartan,
   o que el acumulador se descarta después de cada emisión?

---

### Ejercicio 10.4.5 — El pipeline de fraude en tiempo real con watermarks

**Tipo: Implementar**

Implementar un pipeline de detección de fraude que:
1. Lee transacciones de Kafka (event time: timestamp de la transacción)
2. Para cada usuario, detecta si hay más de 3 transacciones en 5 minutos
3. Emite alertas con latencia < 30 segundos desde el evento
4. Maneja eventos tardíos (dispositivos que sincronizan después de estar offline)

Especificar: tipo de ventana, trigger, allowed_lateness, y accumulation_mode.
Justificar cada elección.

---

## Sección 10.5 — Runners: Ejecutar el Mismo Pipeline en Distintos Motores

### Ejercicio 10.5.1 — DirectRunner: desarrollo y testing

```python
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, DirectOptions
import unittest

# DirectRunner: ejecutar localmente, sin infraestructura
options = PipelineOptions([
    "--runner=DirectRunner",
    "--direct_num_workers=4",         # 4 threads locales
    "--direct_running_mode=multi_threading",  # threads vs processes
])

# Testing con TestPipeline:
class WordcountTest(unittest.TestCase):
    
    def test_wordcount(self):
        """Test unitario para el pipeline de wordcount."""
        
        with beam.Pipeline() as p:
            resultado = (
                p
                | "Crear" >> beam.Create(["hola mundo", "hola beam"])
                | "Split" >> beam.FlatMap(str.split)
                | "Contar" >> beam.combiners.Count.PerElement()
            )
            
            # Verificar el resultado:
            assert_that(
                resultado,
                equal_to([("hola", 2), ("mundo", 1), ("beam", 1)]),
            )
    
    def test_wordcount_con_parquet(self):
        """Test con archivo real usando TestStream."""
        from apache_beam.testing.test_stream import TestStream
        
        stream = (
            TestStream()
            .add_elements(["hola mundo"])
            .add_elements(["hola beam"])
            .advance_watermark_to_infinity()
        )
        
        with beam.Pipeline() as p:
            resultado = (
                p
                | stream
                | beam.FlatMap(str.split)
                | beam.combiners.Count.PerElement()
            )
            assert_that(resultado, equal_to([...]))
```

**Preguntas:**

1. ¿`DirectRunner` garantiza el mismo comportamiento que `DataflowRunner`
   o `SparkRunner`? ¿Qué diferencias puede haber?

2. ¿`TestStream` permite simular el avance del tiempo en streaming?
   ¿Cómo usarías `advance_watermark_to` para testear ventanas?

3. ¿`assert_that` verifica el orden de los elementos o solo el conjunto?

4. ¿Puedes usar `DirectRunner` para pipelines en producción?
   ¿Cuándo sería aceptable?

---

### Ejercicio 10.5.2 — SparkRunner: Beam sobre Spark

```python
# Usar Spark como runner para un pipeline de Beam:
from apache_beam.options.pipeline_options import PipelineOptions

options = PipelineOptions([
    "--runner=SparkRunner",
    "--spark_master=spark://localhost:7077",
    "--spark_executor_cores=4",
    "--spark_executor_memory=4g",
    "--spark_num_executors=10",
])

# El pipeline es IDÉNTICO — solo cambian las options:
with beam.Pipeline(options=options) as p:
    resultado = (
        p
        | beam.io.ReadFromParquet("s3://datos/ventas/*.parquet")
        | beam.Map(lambda e: (e["region"], e["monto"]))
        | beam.CombinePerKey(sum)
        | beam.io.WriteToParquet("s3://resultado/")
    )
```

**Preguntas:**

1. ¿El SparkRunner traduce el grafo de Beam a un DAG de Spark exactamente?
   ¿O hay un overhead de traducción?

2. ¿Las optimizaciones de Spark (AQE, broadcast join, etc.) se aplican
   cuando usas SparkRunner?

3. ¿El SparkRunner soporta todos los features de Beam
   (stateful DoFns, watermarks, etc.)?

4. ¿Cuándo tiene sentido usar SparkRunner sobre Spark directamente?

5. ¿Cuándo DataflowRunner es mejor que SparkRunner?

**Pista:** El SparkRunner traduce el grafo de Beam a operaciones de Spark RDD,
no a DataFrames/Dataset. Por eso las optimizaciones de Catalyst (predicate pushdown,
AQE) NO se aplican automáticamente. El overhead de traducción es real — en benchmarks,
el mismo pipeline en SparkRunner puede ser 2-3× más lento que el mismo pipeline
escrito directamente en Spark. El valor del SparkRunner es la portabilidad, no
el rendimiento máximo.

---

### Ejercicio 10.5.3 — DataflowRunner: managed y serverless

```python
# Google Cloud Dataflow: runner managed que escala automáticamente
from apache_beam.options.pipeline_options import (
    PipelineOptions, GoogleCloudOptions, WorkerOptions
)

options = PipelineOptions()
gcp = options.view_as(GoogleCloudOptions)
gcp.project = "mi-proyecto"
gcp.region = "us-central1"
gcp.job_name = "pipeline-e-commerce"
gcp.staging_location = "gs://mi-bucket/staging"
gcp.temp_location = "gs://mi-bucket/temp"
options.view_as(StandardOptions).runner = "DataflowRunner"

workers = options.view_as(WorkerOptions)
workers.machine_type = "n1-standard-4"
workers.max_num_workers = 100    # autoscaling hasta 100 workers
workers.num_workers = 10         # comenzar con 10

# El pipeline — idéntico:
with beam.Pipeline(options=options) as p:
    resultado = (
        p
        | "Leer" >> beam.io.ReadFromBigQuery(
            query="SELECT * FROM `proyecto.dataset.tabla` WHERE fecha='2024-01-15'",
            use_standard_sql=True,
        )
        | "Procesar" >> beam.Map(procesar_evento)
        | "Escribir" >> beam.io.WriteToBigQuery(
            "proyecto:dataset.resultado",
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        )
    )
```

**Preguntas:**

1. ¿DataflowRunner requiere que el código del pipeline esté en un contenedor?
   ¿O se despliega automáticamente?

2. ¿El autoscaling de Dataflow ajusta el número de workers basándose en qué métrica?

3. ¿Cuánto tiempo tarda en arrancar un job de Dataflow? ¿Y un job de Spark en EMR?

4. ¿DataflowRunner soporta streaming con latencias menores a 1 segundo?

5. ¿Cuándo el costo de Dataflow es menor que el de un cluster de Spark en EMR
   para el mismo workload?

---

### Ejercicio 10.5.4 — FlinkRunner: streaming con estado persistente

```python
# FlinkRunner: mejor runner para streaming con estado complejo
from apache_beam.options.pipeline_options import PipelineOptions

options = PipelineOptions([
    "--runner=FlinkRunner",
    "--flink_master=localhost:8081",
    "--parallelism=16",
    "--max_parallelism=32",
    "--checkpointing_interval=60000",    # checkpoint cada 60 segundos
    "--checkpointing_mode=EXACTLY_ONCE",
])

# Para pipelines con stateful DoFns y ventanas complejas,
# FlinkRunner es más robusto que SparkRunner:
with beam.Pipeline(options=options) as p:
    transacciones = (
        p
        | "Leer Kafka" >> beam.io.ReadFromKafka(
            consumer_config={"bootstrap.servers": "localhost:9092"},
            topics=["transacciones"],
        )
    )
    
    alertas_fraude = (
        transacciones
        | "Keyed" >> beam.Map(lambda m: (m["user_id"], m))
        | "Detectar fraude" >> beam.ParDo(DetectorFraudeStateful())
    )
```

**Preguntas:**

1. ¿El checkpointing de Flink (a través de FlinkRunner) es equivalente
   al checkpointing de Spark Structured Streaming?

2. ¿FlinkRunner soporta el exactly-once de Kafka a Kafka a través de Beam?

3. ¿Cuándo FlinkRunner es mejor que DataflowRunner para streaming?

4. ¿El estado de los stateful DoFns se guarda en el checkpoint de Flink?

---

### Ejercicio 10.5.5 — Comparar runners para el mismo pipeline

**Tipo: Analizar**

Para un pipeline de streaming que:
- Lee de Kafka: 50,000 eventos/segundo
- Detecta anomalías con estado por usuario (30 minutos de historial)
- Emite alertas con latencia < 500ms
- Requiere tolerancia a fallos (exactamente-una-vez)

Comparar DirectRunner, SparkRunner, FlinkRunner, y DataflowRunner en:
1. Latencia mínima alcanzable
2. Tolerancia a fallos (¿qué garantías ofrece cada runner?)
3. Facilidad de operación
4. Costo estimado (propia infraestructura vs managed)

Crear una tabla de decisión con los 4 runners y 4 criterios.

---

## Sección 10.6 — IO Connectors: Leer y Escribir en el Mundo Real

### Ejercicio 10.6.1 — Leer desde múltiples fuentes

```python
import apache_beam as beam

with beam.Pipeline() as p:
    # Parquet con schema inference:
    desde_parquet = p | beam.io.ReadFromParquet("datos/*.parquet")
    
    # Avro:
    desde_avro = p | beam.io.ReadFromAvro("eventos/*.avro")
    
    # BigQuery (tabla completa):
    desde_bq_tabla = p | beam.io.ReadFromBigQuery(
        table="proyecto:dataset.tabla"
    )
    
    # BigQuery (query):
    desde_bq_query = p | beam.io.ReadFromBigQuery(
        query="SELECT * FROM `proyecto.dataset.tabla` WHERE fecha='2024-01-15'",
        use_standard_sql=True,
    )
    
    # Pub/Sub (streaming):
    desde_pubsub = p | beam.io.ReadFromPubSub(
        subscription="projects/proyecto/subscriptions/mi-sub",
        with_attributes=True,
    )
    
    # Kafka (streaming):
    desde_kafka = p | beam.io.ReadFromKafka(
        consumer_config={"bootstrap.servers": "localhost:9092"},
        topics=["eventos"],
        with_metadata=True,
    )
    
    # Texto (batch):
    desde_texto = p | beam.io.ReadFromText("logs/*.txt")
    
    # Crear elementos en memoria (para testing):
    en_memoria = p | beam.Create([1, 2, 3, 4, 5])
```

**Restricciones:**
1. Implementar un pipeline que une tres fuentes distintas usando `CoGroupByKey`
2. ¿Qué tipo de join hace `CoGroupByKey` cuando las keys no están en todas las fuentes?
3. ¿Hay IO connectors para S3, HDFS, y bases de datos SQL en Beam?
   ¿Cómo accedes a ellos?

---

### Ejercicio 10.6.2 — Escribir a múltiples sinks

```python
import apache_beam as beam

with beam.Pipeline() as p:
    datos_procesados = (
        p
        | "Leer" >> beam.io.ReadFromParquet("entrada/*.parquet")
        | "Procesar" >> beam.Map(procesar)
    )
    
    # Escribir a Parquet:
    datos_procesados | beam.io.WriteToParquet(
        "gs://mi-bucket/resultado/",
        schema=SCHEMA_ARROW,
        file_name_suffix=".parquet",
        num_shards=100,  # número de archivos de salida
    )
    
    # Escribir a BigQuery (append):
    datos_procesados | beam.io.WriteToBigQuery(
        "proyecto:dataset.tabla",
        schema=SCHEMA_BQ,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    )
    
    # Escribir a Kafka:
    datos_procesados | beam.io.WriteToKafka(
        producer_config={"bootstrap.servers": "localhost:9092"},
        topic="resultados-procesados",
        key_serializer=lambda k: k.encode(),
        value_serializer=lambda v: json.dumps(v).encode(),
    )
    
    # Escribir a texto (para debugging):
    datos_procesados | beam.io.WriteToText(
        "gs://mi-bucket/debug/",
        file_name_suffix=".txt",
        shard_name_template="",  # sin numeración de shards
    )
```

**Preguntas:**

1. Si el pipeline falla a mitad de la escritura a Parquet (50 de 100 archivos
   escritos), ¿qué pasa cuando reinicias? ¿Los 50 archivos existentes
   se duplican o se sobrescriben?

2. ¿Cómo implementa Beam la escritura "atómica" a Parquet?
   ¿Usa archivos temporales?

3. `num_shards=100` en WriteToParquet genera exactamente 100 archivos.
   ¿Cuántos archivos generaría con `num_shards=0`?

4. ¿WriteToBigQuery en streaming tiene las mismas garantías que en batch?

---

### Ejercicio 10.6.3 — Custom IO: leer desde una fuente no estándar

```python
import apache_beam as beam
from apache_beam.io import iobase
from apache_beam.io.range_trackers import OffsetRangeTracker

class PostgreSQLSource(iobase.BoundedSource):
    """
    Fuente personalizada que lee de PostgreSQL de forma paralela.
    
    Divide la tabla en ranges de IDs para paralelización.
    """
    
    def __init__(self, dsn: str, tabla: str, columna_id: str = "id"):
        self.dsn = dsn
        self.tabla = tabla
        self.columna_id = columna_id
    
    def estimate_size(self) -> int:
        """Estimar el tamaño total para el runner."""
        with psycopg2.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {self.tabla}")
                count = cur.fetchone()[0]
                return count * 100  # ~100 bytes por fila estimado
    
    def get_range_tracker(self, start_position, stop_position):
        """Crear un tracker para el rango de IDs [start, stop)."""
        if start_position is None:
            start_position = 0
        if stop_position is None:
            stop_position = self._max_id()
        return OffsetRangeTracker(start_position, stop_position)
    
    def read(self, range_tracker):
        """Leer filas en el rango asignado."""
        start = range_tracker.start_position()
        stop = range_tracker.stop_position()
        
        with psycopg2.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT * FROM {self.tabla} WHERE {self.columna_id} >= %s AND {self.columna_id} < %s",
                    (start, stop)
                )
                for fila in cur:
                    if not range_tracker.try_claim(fila[0]):
                        return
                    yield dict(zip([d[0] for d in cur.description], fila))
    
    def split(self, desired_bundle_size, start_position=None, stop_position=None):
        """Dividir en bundles para paralelización."""
        max_id = self._max_id()
        bundle_size = desired_bundle_size // 100  # rows per bundle
        
        for start in range(0, max_id, bundle_size):
            yield iobase.SourceBundle(
                weight=bundle_size,
                source=self,
                start_position=start,
                stop_position=min(start + bundle_size, max_id),
            )

# Usar:
with beam.Pipeline() as p:
    datos = (
        p
        | "Leer Postgres" >> beam.io.Read(
            PostgreSQLSource("postgresql://localhost/db", "ventas")
        )
    )
```

**Restricciones:**
1. Implementar `PostgreSQLSource` completo con `_max_id()`
2. Verificar que la lectura paralela produce el mismo resultado que la secuencial
3. ¿Cómo implementarías un `UnboundedSource` (streaming) para leer nuevas
   filas de PostgreSQL a medida que se insertan?

---

### Ejercicio 10.6.4 — FileIO: lectura y escritura dinámica de archivos

```python
import apache_beam as beam
from apache_beam.io import fileio

# Leer archivos y preservar el nombre del archivo:
with beam.Pipeline() as p:
    archivos = (
        p
        | "Matchear archivos" >> fileio.MatchFiles("datos/*.parquet")
        | "Leer archivos"    >> fileio.ReadMatches()
        | "Procesar"         >> beam.Map(lambda archivo: {
            "nombre": archivo.metadata.path,
            "tamaño": archivo.metadata.size_in_bytes,
            "contenido": archivo.read(),
        })
    )

# Escribir archivos dinámicamente (un archivo por key):
with beam.Pipeline() as p:
    datos_por_region = (
        p
        | "Datos" >> beam.io.ReadFromParquet("entrada/*.parquet")
        | "Agrupar" >> beam.Map(lambda e: (e["region"], e))
    )
    
    datos_por_region | fileio.WriteToFiles(
        path="gs://mi-bucket/por_region/",
        destination=lambda e: e[0],  # la region como subdirectorio
        sink=fileio.TextSink(),
        file_naming=fileio.default_file_naming(".txt"),
    )
```

**Preguntas:**

1. ¿`fileio.MatchFiles` soporta wildcards para S3 y GCS además de disco local?

2. ¿`WriteToFiles` con `destination=lambda e: e[0]` escribe en paralelo
   a todos los directorios de destino?

3. ¿Cómo implementarías escritura a Parquet con `fileio.WriteToFiles`?

---

### Ejercicio 10.6.5 — Leer: el connector de BigQuery y sus modos de lectura

**Tipo: Analizar**

```python
# Modo 1: EXPORT (default hasta Beam 2.30)
# Exporta la tabla a GCS temporal y luego la lee
beam.io.ReadFromBigQuery(
    table="proyecto:dataset.tabla",
    method=beam.io.ReadFromBigQuery.Method.EXPORT,
)

# Modo 2: DIRECT_READ (BigQuery Storage API)
# Lee directamente de BigQuery sin exportar
beam.io.ReadFromBigQuery(
    table="proyecto:dataset.tabla",
    method=beam.io.ReadFromBigQuery.Method.DIRECT_READ,
)
```

**Preguntas:**

1. ¿Por qué el modo `EXPORT` puede ser más lento que `DIRECT_READ`?

2. ¿`DIRECT_READ` tiene predicate pushdown?
   ¿Qué pasa con `WHERE fecha='2024-01-15'` en cada modo?

3. ¿Cuándo el modo `EXPORT` sería preferible a `DIRECT_READ`?

4. ¿El modo de lectura afecta al costo en GCP?

---

## Sección 10.7 — Cuándo Beam y Cuándo las Alternativas

### Ejercicio 10.7.1 — El árbol de decisión para pipelines de datos

```
¿El mismo pipeline debe ejecutarse en batch Y en streaming?
  Sí → Beam (portabilidad entre modos es la principal ventaja)
  No → continuar...

¿Necesitas portabilidad entre runners (Spark, Flink, Dataflow)?
  Sí → Beam
  No → ¿cuál runner usarás?
    Spark → Spark directamente (más optimizado que SparkRunner)
    Flink → Flink directamente (Cap.11) o Beam + FlinkRunner
    Google Cloud → Dataflow directamente (sin Beam) o Beam + DataflowRunner

¿El equipo prefiere SQL sobre código?
  Sí → Apache Beam SQL, Spark SQL, o Flink SQL
  No → continuar con la API de DataFrame/PTransform

¿Los datos son < 1 TB y no necesitas streaming?
  Sí → Polars o Spark local (mucho más simple que Beam)
  No → Beam, Spark, o Flink según el caso
```

**Restricciones:**
1. Para cada nodo del árbol, añadir un ejemplo concreto
2. ¿Hay casos donde ninguna de estas opciones es la correcta?
3. ¿El árbol cambia si el equipo tiene experiencia previa con algún framework?

---

### Ejercicio 10.7.2 — Beam vs Spark Structured Streaming

Para un pipeline de métricas en tiempo real (latencia 5-10 segundos):

```
Beam + FlinkRunner:
  + Portable: puede moverse a Dataflow o Spark sin reescribir
  + Model unificado batch/streaming
  + Mejor abstracción para watermarks y ventanas complejas
  - Más verboso que Spark SS para casos simples
  - Menos optimizaciones automáticas que Spark AQE
  - Comunidad más pequeña, menos documentación en español

Spark Structured Streaming:
  + Más simple para casos comunes (SQL sobre streams)
  + AQE y optimizaciones automáticas
  + Mayor adopción, más soporte de comunidad
  + Mejor integración con el ecosistema Spark (MLlib, Delta Lake)
  - No portable: el código no funciona en Flink o Dataflow
  - Micro-batch (no streaming puro) → latencia mínima ~100ms
  - API separada para batch y streaming (aunque similar)
```

**Preguntas:**

1. Para un equipo que ya usa Spark para batch, ¿tiene sentido adoptar
   Beam para streaming?

2. ¿La portabilidad de Beam es una ventaja real en la práctica?
   ¿Con qué frecuencia las empresas cambian de runner?

3. ¿Beam es más difícil de aprender que Spark Structured Streaming?
   ¿Por qué?

4. ¿Para detección de fraude en tiempo real con latencia < 500ms,
   cuál elegirías? Justifica.

---

### Ejercicio 10.7.3 — Beam en GCP: el caso de uso principal

```python
# El caso de uso donde Beam brilla más:
# Pipeline que se ejecuta como batch programado Y como streaming continuo.
# Ejemplo: procesar eventos del día (batch) y procesar nuevos eventos (streaming).

# Batch (backfill histórico):
batch_options = PipelineOptions([
    "--runner=DataflowRunner",
    "--project=mi-proyecto",
    "--region=us-central1",
])
batch_options.view_as(StandardOptions).streaming = False

# Streaming (tiempo real):
stream_options = PipelineOptions([
    "--runner=DataflowRunner",
    "--project=mi-proyecto",
    "--region=us-central1",
    "--streaming",
])

# El pipeline — IDÉNTICO para ambos modos:
def crear_pipeline(opciones, fuente):
    with beam.Pipeline(options=opciones) as p:
        datos = p | fuente  # la fuente es diferente
        
        resultado = (
            datos
            | "Procesar" >> beam.Map(procesar_evento)
            | "Agrupar" >> beam.Map(lambda e: (e["region"], e["monto"]))
            | "Revenue" >> beam.CombinePerKey(sum)
        )
        
        resultado | "Escribir BigQuery" >> beam.io.WriteToBigQuery(...)

# Batch con datos históricos:
crear_pipeline(
    batch_options,
    beam.io.ReadFromBigQuery(query="SELECT * FROM tabla WHERE fecha='2024-01-14'"),
)

# Streaming con datos nuevos:
crear_pipeline(
    stream_options,
    beam.io.ReadFromPubSub(subscription="..."),
)
```

**Preguntas:**

1. ¿Existe algún caso donde este pattern de "mismo pipeline para batch y streaming"
   falle debido a diferencias semánticas entre los dos modos?

2. ¿Dataflow gestiona el escalado automáticamente para el modo batch
   igual que para el modo streaming?

3. ¿La Lambda Architecture (batch layer + speed layer) es más o menos
   compleja que este approach con Beam?

---

### Ejercicio 10.7.4 — Implementar: el pipeline de e-commerce en Beam

```python
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

def pipeline_ecommerce(modo: str, opciones_extra: dict = None):
    """
    Pipeline de analytics del e-commerce.
    Funciona en batch (análisis histórico) y streaming (tiempo real).
    
    Modo batch: leer de Parquet en GCS, escribir a BigQuery
    Modo streaming: leer de Pub/Sub, escribir a BigQuery en tiempo real
    """
    options = PipelineOptions()
    
    if modo == "streaming":
        options.view_as(StandardOptions).streaming = True
    
    with beam.Pipeline(options=options) as p:
        
        # Fuente de datos (diferente según modo):
        if modo == "batch":
            eventos_raw = (
                p
                | "Leer GCS" >> beam.io.ReadFromParquet(
                    "gs://datos-ecommerce/eventos/fecha=*/",
                )
            )
        else:
            eventos_raw = (
                p
                | "Leer Pub/Sub" >> beam.io.ReadFromPubSub(
                    subscription="projects/mi-proyecto/subscriptions/eventos-sub",
                    with_attributes=True,
                )
                | "Deserializar" >> beam.Map(
                    lambda msg: json.loads(msg.data.decode())
                )
            )
        
        # A partir de aquí: IDÉNTICO para batch y streaming
        eventos_limpios = (
            eventos_raw
            | "Limpiar" >> NormalizarEventos(monto_minimo=0.01)
        )
        
        # Métrica 1: revenue por región
        revenue = (
            eventos_limpios
            | "Filtrar compras" >> beam.Filter(lambda e: e["tipo"] == "compra")
            | "Par región-monto" >> beam.Map(lambda e: (e["region"], e["monto"]))
            | "Revenue por región" >> beam.CombinePerKey(sum)
        )
        
        # Métrica 2: usuarios activos por segmento
        usuarios_activos = (
            eventos_limpios
            | "Par segmento-usuario" >> beam.Map(lambda e: (e.get("segmento", "unknown"), e["user_id"]))
            | "Usuarios únicos" >> beam.combiners.Count.PerKey()
        )
        
        # Escribir resultados:
        revenue | "Revenue a BQ" >> beam.io.WriteToBigQuery(
            "mi-proyecto:metricas.revenue_por_region",
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        )
        
        usuarios_activos | "Usuarios a BQ" >> beam.io.WriteToBigQuery(
            "mi-proyecto:metricas.usuarios_activos",
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        )

# Ejecutar en batch (backfill del día anterior):
pipeline_ecommerce("batch")

# Ejecutar en streaming (producción continua):
pipeline_ecommerce("streaming")
```

**Restricciones:**
1. Implementar el pipeline completo con al menos 3 métricas
2. Añadir windowing en modo streaming (ventanas de 5 minutos)
3. Implementar testing con `TestStream`
4. ¿Qué partes del pipeline son diferentes entre batch y streaming?
   ¿Pueden unificarse completamente?

---

### Ejercicio 10.7.5 — El resumen: Beam en el ecosistema

**Tipo: Integrar**

Reflexionar sobre el rol de Beam en el stack del repositorio:

```
Cap.09 (Kafka):     El bus de datos — transporta y retiene mensajes.
Cap.10 (Beam):      El modelo unificado — mismo código para batch y streaming.
Cap.11 (Spark SS):  El motor de streaming más usado — mejor integración con Spark.
Cap.12 (Flink):     El motor de streaming más potente — estado complejo, bajo retraso.

¿Por qué los cuatro y no solo uno?
  Kafka es el bus, los otros tres son motores de procesamiento.
  No compiten directamente — son herramientas para distintos contextos.
  
  Beam brilla: portabilidad, batch+streaming unificado, GCP ecosystem.
  Spark SS brilla: equipos ya en Spark, casos de uso de batch+streaming simples.
  Flink brilla: baja latencia, estado complejo, exactly-once crítico.
```

**Preguntas:**

1. ¿Beam "ganará" sobre Spark SS y Flink en los próximos años,
   o cada uno tiene un nicho estable?

2. Si tuvieras que elegir solo uno de los tres para un equipo nuevo,
   ¿cuál elegirías y por qué?

3. ¿Apache Beam es más o menos relevante en 2024 que hace 3 años?
   ¿Qué factores influyen?

4. ¿Existe una alternativa emergente a Beam, Spark SS, y Flink
   que valga mencionar?

---

## Resumen del capítulo

**Las cinco ideas que definen el modelo de Beam:**

```
1. PCollection + PTransform: datos + transformaciones inmutables
   La inmutabilidad permite la ramificación del pipeline y
   el re-uso sin efectos secundarios. Cada transform crea
   una nueva PCollection, nunca modifica la existente.

2. Runners intercambiables: mismo código, distinto motor
   DirectRunner para desarrollo, SparkRunner para escalar,
   FlinkRunner para streaming robusto, DataflowRunner para managed.
   La portabilidad tiene un costo: menos optimizaciones automáticas.

3. Event time + Watermarks: el tiempo correcto
   Event time garantiza resultados correctos aunque los eventos lleguen
   desordenados. Los watermarks son la estimación de cuándo el sistema
   puede cerrar una ventana con confianza razonable.

4. Windows + Triggers: cuándo agregar y cuándo emitir
   Las ventanas definen QUÉ datos agrupar (por tiempo).
   Los triggers definen CUÁNDO emitir el resultado de cada ventana.
   Allowed lateness define QUÉ hacer con los eventos que llegan tarde.

5. Composite transforms: encapsular para reutilizar
   El `PTransform` personalizado es la unidad de modularidad de Beam.
   Un pipeline bien diseñado es una composición de transforms
   testeables y reutilizables.
```

**El criterio de decisión en una línea:**

> Si necesitas el mismo pipeline para batch y streaming, o si necesitas
> portabilidad entre runners, Beam es la respuesta. Si solo necesitas
> streaming en un runner específico, usa ese runner directamente.

**Conexión con Cap.11 (Spark Structured Streaming):**

> Spark SS implementa un subconjunto del modelo de Beam:
> micro-batching sobre streams, ventanas de tiempo, watermarks.
> Pero lo hace con la API y las optimizaciones de Spark — más simple
> para equipos que ya conocen Spark, y más rápido en muchos benchmarks.
> El Cap.11 cubre los mismos conceptos de esta sección desde
> la perspectiva del ecosistema Spark.
