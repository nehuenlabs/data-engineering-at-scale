# data-engineering-at-scale

> Guía de ejercicios para procesar datos a escala — desde una máquina hasta
> clusters distribuidos, desde batch hasta streaming en tiempo real.

Este repositorio es el complemento práctico de
[concurrencia](../concurrencia/README.md).
Donde ese repo pregunta *¿cómo coordino la ejecución concurrente?*,
este pregunta *¿cómo proceso datos que no caben en una máquina,
o que no dejan de llegar?*

---

## La pregunta que organiza todo

En data engineering hay exactamente **un tradeoff central** que se repite
en cada herramienta, cada decisión de arquitectura, y cada incidente de producción:

```
Latencia  ←————————————————————→  Throughput

Procesar cada evento en 10ms       Procesar 10 TB en 2 horas
requiere sacrificar throughput.    requiere sacrificar latencia.
```

Todos los frameworks de este repositorio son posiciones distintas en ese espectro:

```
Polars / DataFusion          Kafka Streams        Apache Flink
(batch, una máquina,    (streaming embebido,   (streaming distribuido,
 máxima eficiencia)      baja latencia)         estado complejo)

        Apache Spark                 Apache Beam
  (batch y micro-batch,        (modelo unificado,
   ecosistema completo)         portable entre runners)
```

Entender dónde está cada herramienta en ese espectro —
y por qué está ahí — es el objetivo de este repositorio.

---

## Prerequisitos

Este repositorio asume que dominas los conceptos de:

- **Concurrencia** — goroutines, canales, locks, races, deadlocks.
  Si no, el [repo de concurrencia](../concurrencia/README.md)
  es el punto de partida.

- **Python básico** — los ejercicios usan Python como lenguaje principal.
  Los capítulos de lenguajes (Parte 4) cubren Java, Scala, y Rust
  en el contexto de data engineering.

- **SQL** — las queries de los ejercicios asumen familiaridad con
  SELECT, GROUP BY, JOIN, y window functions básicas.

---

## Cómo usar este repositorio

Cada ejercicio tiene un **tipo** que indica qué se espera:

```
Implementar   →  escribir código desde cero o completar un skeleton
Leer          →  analizar código o métricas dado y responder preguntas
Diagnosticar  →  dado un síntoma (log, dashboard, error), encontrar la causa
Diseñar       →  proponer una arquitectura o estrategia, justificarla
Medir         →  ejecutar un experimento y analizar los resultados
Comparar      →  evaluar dos o más approaches con criterios explícitos
```

Los ejercicios de tipo **Leer** y **Diagnosticar** son tan importantes
como los de **Implementar**. En producción, la mayoría del tiempo
se pasa diagnosticando pipelines lentos o incorrectos — no escribiendo
código nuevo.

### Referencias en los ejercicios

A lo largo del repositorio encontrarás tres tipos de notas:

```
> 📖 Profundizar: — paper o libro que es la fuente canónica del concepto.

> ⚙️ Versión: — cuando el comportamiento depende de la versión exacta
               del framework o de la configuración del cluster.

> 🔗 Ecosistema: — herramienta relacionada relevante en producción
                  que no se cubre en profundidad aquí.
```

---

## Estructura del repositorio

```
Parte 1 — El modelo mental (Cap.01–03)
  Fundamentos compartidos por todos los frameworks.
  Sin entender estos tres capítulos, los demás no tienen contexto.

Parte 2 — Batch processing (Cap.04–08)
  Procesar datos que ya existen — archivos, bases de datos, objetos en S3.

Parte 3 — Stream processing (Cap.09–12)
  Procesar datos que están llegando ahora mismo.

Parte 4 — Lenguajes y ecosistemas (Cap.13–16)
  El mismo problema visto desde Python, Java/Scala, y Rust.

Parte 5 — En producción (Cap.17–20)
  Orquestar, observar, testear, y mantener pipelines de datos.
```

---

## Tabla de capítulos

### Parte 1 — El modelo mental

| Cap. | Título | Descripción |
|------|--------|-------------|
| 01 | [De concurrencia a datos distribuidos](01_de_concurrencia_a_datos.md) | El puente conceptual. Por qué los frameworks de datos invierten el modelo de concurrencia. |
| 02 | [Formatos y representación en memoria](02_formatos_y_memoria.md) | Row vs columnar, Apache Arrow, Parquet, compresión. La base que todos los frameworks comparten. |
| 03 | [El modelo Map/Reduce](03_mapreduce.md) | No como tecnología sino como paradigma. La abstracción que subyace a Spark, Beam, y Hadoop. |

### Parte 2 — Batch processing

| Cap. | Título | Descripción |
|------|--------|-------------|
| 04 | [Spark — modelo de ejecución y diagnóstico](04_spark_modelo_ejecucion.md) | DAG, stages, shuffles, Spark UI. Cómo Spark planifica y ejecuta el trabajo. |
| 05 | [Spark — optimización avanzada](05_spark_optimizacion.md) | Data skew, broadcast joins, AQE, caché, configuración de memoria. |
| 06 | [Polars — paralelismo sin JVM](06_polars.md) | El modelo de Rust aplicado a data frames. Cuándo supera a Spark y cuándo no. |
| 07 | [DataFusion — SQL distribuido en Rust](07_datafusion.md) | El motor embebido. Arrow como formato unificador del ecosistema Rust. |
| 08 | [El lakehouse — Delta Lake, Iceberg, Hudi](08_lakehouse.md) | ACID sobre object storage. Time travel, schema evolution, upserts a escala. |

### Parte 3 — Stream processing

| Cap. | Título | Descripción |
|------|--------|-------------|
| 09 | [Kafka — el log distribuido](09_kafka.md) | Productores, consumidores, particiones, offsets, consumer groups. |
| 10 | [Apache Beam — el modelo unificado](10_beam.md) | PCollections, DoFns, windowing, watermarks. Batch y streaming con el mismo código. |
| 11 | [Spark Structured Streaming](11_spark_streaming.md) | Micro-batching, triggers, foreachBatch, integración con Delta Lake. |
| 12 | [Flink — cuando Spark Streaming no es suficiente](12_flink.md) | Streaming puro, estado complejo, exactly-once, latencia < 100ms. |

### Parte 4 — Lenguajes y ecosistemas

| Cap. | Título | Descripción |
|------|--------|-------------|
| 13 | [Python — PySpark, Arrow, y la GIL en data engineering](13_python.md) | UDFs, Pandas UDFs, el rol de la GIL, el ecosistema PyData. |
| 14 | [Scala — el lenguaje nativo de Spark](14_scala.md) | Por qué Spark se escribe en Scala. Tipos, pattern matching, implicits en el contexto de datos. |
| 15 | [Java — Beam, Kafka Streams, ecosistema empresarial](15_java.md) | Stream processing en Java. Cuándo la verbosidad de Java es una ventaja. |
| 16 | [Rust — DataFusion, Polars, Delta-rs](16_rust.md) | El ownership aplicado a data engineering. Por qué Rust está ganando terreno en el ecosistema. |

### Parte 5 — En producción

| Cap. | Título | Descripción |
|------|--------|-------------|
| 17 | [Orquestación — Airflow, Dagster, Prefect](17_orquestacion.md) | DAGs de pipelines, dependencias, reintentos, backfill. |
| 18 | [Observabilidad de pipelines de datos](18_observabilidad.md) | Métricas, logs, trazabilidad de datos (data lineage), alertas. |
| 19 | [Testing de pipelines](19_testing.md) | Unit tests, integration tests, contract tests, testing de streaming. |
| 20 | [El sistema completo](20_sistema_completo.md) | Integrando todo: de la fuente de datos al dashboard, con resiliencia y observabilidad. |

---

## El hilo conductor

A lo largo del repositorio construimos el mismo sistema de ejemplo:
una **plataforma de analytics de e-commerce** con:

- Eventos de clicks y compras (streaming, Kafka)
- Catálogo de productos (batch, PostgreSQL)
- Métricas de negocio (revenue, conversión, fraude)
- Dashboard en tiempo real y reportes históricos

El mismo sistema se implementa incrementalmente en cada parte:
- Parte 1: entender el modelo de datos
- Parte 2: pipeline batch con Spark y Polars
- Parte 3: pipeline streaming con Kafka y Flink
- Parte 4: el mismo pipeline en distintos lenguajes
- Parte 5: el sistema completo en producción

Al final del repositorio, tienes un sistema funcionando de principio a fin
— no ejercicios aislados.

---

## Lenguajes y versiones de referencia

```
Python    3.11+     (PySpark, Beam, Polars, DataFusion)
Java      17 LTS    (Beam, Kafka Streams, Flink)
Scala     2.13      (Spark nativo)
Rust      1.75+     (Polars, DataFusion, Delta-rs)

Apache Spark         3.5.x
Apache Kafka         3.6.x
Apache Beam          2.52.x
Apache Flink         1.18.x
Polars               0.20.x
Apache DataFusion    35.x
Delta Lake           3.x
```

> ⚙️ Versión: los ejercicios se verificaron con las versiones listadas.
> El comportamiento de AQE (Spark), el estado en Flink, y las APIs de Polars
> cambian entre versiones menores. Antes de ejecutar en producción,
> verificar el changelog de la versión específica de tu entorno.

---

## Relación con otros repositorios

```
concurrencia/
  ↓ prerequisito conceptual
data-engineering-at-scale/   ← este repositorio
  ↓ aplicación práctica
[algoritmos/]                (repo de algoritmos y estructuras de datos)
```

Los conceptos del repo de concurrencia que más aparecen aquí:

| Concurrencia | Equivalente en data engineering |
|---|---|
| Goroutine / Thread | Task de Spark / Flink operator |
| Canal de Go | Kafka topic / Beam PCollection |
| Mutex / Lock | Optimistic concurrency control en Delta Lake |
| Race condition | Inconsistencia en replicación eventual |
| Deadlock | Shuffle deadlock en Spark (ver Cap.04) |
| Goroutine leak | Consumer lag sin límite en Kafka (ver Cap.09) |
| Circuit breaker | Backpressure en stream processing (ver Cap.12) |

---

## Convenciones del repositorio

```python
# Los ejemplos de código son runnable cuando es posible.
# Los que requieren un cluster están marcados con:
# [REQUIERE CLUSTER] — necesita Spark/Flink/Kafka corriendo

# Los fragmentos de diagnóstico muestran output real:
# >>> df.explain()
# == Physical Plan ==
# ...

# Las métricas de performance son orientativas (hardware de referencia:
# laptop con 16GB RAM, 8 cores, SSD NVMe).
# Los tiempos absolutos variarán; los relativos son representativos.
```

---

*Este repositorio se construye incrementalmente.
Cada capítulo referencia los anteriores — leer en orden la primera vez.*
