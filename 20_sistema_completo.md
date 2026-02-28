# Guía de Ejercicios — Cap.20: El Sistema Completo

> Este no es un capítulo nuevo. Es la culminación de todo el libro.
>
> Has aprendido formatos y memoria (Cap.02), el modelo MapReduce (Cap.03),
> Spark (Cap.04-05), Polars y DataFusion (Cap.06-07), el lakehouse (Cap.08),
> Kafka (Cap.09), Beam (Cap.10), Spark Streaming (Cap.11), Flink (Cap.12),
> los lenguajes (Cap.13-16), la orquestación (Cap.17),
> la observabilidad (Cap.18), y el testing (Cap.19).
>
> Ahora la pregunta es: ¿cómo se conectan todas estas piezas
> en un sistema real que funciona las 24 horas del día,
> los 7 días de la semana, sin que nadie lo toque?
>
> La respuesta no es elegir la herramienta perfecta.
> Es entender los tradeoffs de cada decisión,
> construir para fallar gracefully,
> y operar con la confianza que solo dan
> la observabilidad y el testing.
>
> Este capítulo construye el sistema de e-commerce completo
> — el hilo conductor del libro — de principio a fin.

---

## El modelo mental: un sistema de datos es un sistema distribuido

```
Lo que parece:

  "Un pipeline que lee de PostgreSQL, transforma en Spark,
   y carga a BigQuery."

Lo que realmente es:

  ┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐
  │PostgreSQL│────→│   S3     │────→│  Spark   │────→│ BigQuery │
  │ (OLTP)   │     │(Data Lake)│     │(Compute) │     │  (DWH)   │
  └──────────┘     └──────────┘     └──────────┘     └──────────┘
       ↑                ↑                ↑                ↑
    red de            red de          red de           red de
    producción        AWS/GCP         cluster          Google
       │                │                │                │
    puede              puede           puede            puede
    caerse             caerse          caerse           caerse

  Cada flecha es una llamada de red que puede:
  - Fallar silenciosamente
  - Retornar datos parciales
  - Tardar 10× más de lo esperado
  - Retornar datos en un schema inesperado

Un sistema de datos en producción es un sistema distribuido.
Se aplican las mismas reglas:
  - Todo lo que puede fallar, fallará
  - Las fallas parciales son peores que las fallas totales
  - La consistencia es costosa y a veces imposible
  - La idempotencia es la única defensa confiable
```

```
El sistema de e-commerce — visión completa:

  FUENTES                    INGESTA               ALMACENAMIENTO
  ────────                   ───────                ──────────────
  PostgreSQL ──batch──→ Airflow extract ──→ S3 raw/ (Parquet)
  (ventas, clientes)                               ↓
                                              Delta Lake (ACID)
  Kafka ──streaming──→ Flink/Beam ──→ S3 streaming/ (Parquet)
  (clicks, eventos)                            ↓
                                          Delta Lake (merge)
  API REST ──cada 6h──→ Airflow sync ──→ S3 dim/ (Parquet)
  (inventario, precios)

  TRANSFORMACIÓN              SERVING                CONSUMO
  ──────────────              ───────                ───────
  dbt + Spark ──→ staging/ ──→ marts/ ──→ BigQuery ──→ Looker dashboards
  (Dagster assets)    ↓                               ↓
                  quality checks               analistas SQL
                  (Great Expectations)
                      ↓
                  ML features ──→ MLflow ──→ inference ──→ API de
                  (feature store)              (batch)      recomendaciones

  OPERACIÓN
  ─────────
  Orquestación: Airflow (batch) + Kubernetes (streaming)
  Observabilidad: Prometheus + Grafana + structlog + OpenLineage
  Testing: pytest + Spark local + Testcontainers + CI/CD
  Alertas: PagerDuty (P1) + Slack (P2/P3)
```

---

## Tabla de contenidos

- [Sección 20.1 — La arquitectura de referencia: decisiones y tradeoffs](#sección-201--la-arquitectura-de-referencia-decisiones-y-tradeoffs)
- [Sección 20.2 — El pipeline batch: de PostgreSQL a BigQuery](#sección-202--el-pipeline-batch-de-postgresql-a-bigquery)
- [Sección 20.3 — El pipeline streaming: de Kafka al dashboard en tiempo real](#sección-203--el-pipeline-streaming-de-kafka-al-dashboard-en-tiempo-real)
- [Sección 20.4 — La capa de transformación: dbt, Spark, y el lakehouse](#sección-204--la-capa-de-transformación-dbt-spark-y-el-lakehouse)
- [Sección 20.5 — El sistema operado: orquestación, observabilidad, y testing juntos](#sección-205--el-sistema-operado-orquestación-observabilidad-y-testing-juntos)
- [Sección 20.6 — Modos de fallo: qué se rompe y cómo se recupera](#sección-206--modos-de-fallo-qué-se-rompe-y-cómo-se-recupera)
- [Sección 20.7 — Evolución del sistema: del MVP al sistema maduro](#sección-207--evolución-del-sistema-del-mvp-al-sistema-maduro)

---

## Sección 20.1 — La Arquitectura de Referencia: Decisiones y Tradeoffs

### Ejercicio 20.1.1 — Leer: las decisiones de arquitectura y sus razones

**Tipo: Leer**

```
Cada decisión de arquitectura del sistema de e-commerce tiene una razón
y un tradeoff. No existe la arquitectura perfecta — existe la arquitectura
adecuada para tus constraints.

  Decisión 1: PostgreSQL como fuente OLTP
  ────────────────────────────────────────
  ¿Por qué?  El equipo de backend ya usa PostgreSQL.
             No vamos a cambiar la base de datos de producción
             para que el pipeline de datos sea más fácil.
  Tradeoff:  El extract de PostgreSQL es un full scan diario.
             CDC (Change Data Capture) sería más eficiente,
             pero requiere Debezium + Kafka Connect — más complejidad.
  Alternativa evaluada: CDC con Debezium.
  Decisión: empezar con batch extract; migrar a CDC si el volumen
            supera 10M filas/día o la latencia batch no es aceptable.

  Decisión 2: S3 como data lake (Parquet + Delta Lake)
  ────────────────────────────────────────────────────
  ¿Por qué?  S3 es el almacenamiento más barato y duradero.
             Parquet + Delta Lake da ACID sobre S3.
             Cualquier motor (Spark, Polars, DuckDB) puede leer Parquet.
  Tradeoff:  S3 tiene alta latencia para lecturas pequeñas.
             No es ideal para queries interactivas.
  Alternativa evaluada: BigQuery directamente como data lake.
  Decisión: S3 para raw/staging, BigQuery para marts/serving.
            Separar almacenamiento de compute.

  Decisión 3: Kafka para eventos de streaming
  ────────────────────────────────────────────
  ¿Por qué?  El equipo de backend ya produce eventos a Kafka.
             Kafka es el estándar para streaming a escala.
  Tradeoff:  Kafka requiere operación (ZooKeeper/KRaft, brokers, particiones).
             Managed Kafka (MSK, Confluent) reduce el costo operativo.
  Alternativa evaluada: Pub/Sub (GCP), Kinesis (AWS).
  Decisión: Kafka porque ya existe en la infraestructura.

  Decisión 4: Flink para streaming, Spark para batch
  ──────────────────────────────────────────────────
  ¿Por qué?  Flink: latencia sub-segundo, estado complejo, exactly-once.
             Spark: ecosistema completo, batch a escala, ML integrado.
  Tradeoff:  Dos motores de compute = más operación, más conocimiento.
  Alternativa evaluada: solo Spark (con Structured Streaming para streaming).
  Decisión: Flink para streaming real-time, Spark para batch y ML.
            Si el equipo es pequeño, empezar solo con Spark.

  Decisión 5: Airflow para orquestación
  ──────────────────────────────────────
  ¿Por qué?  Estándar de facto. El equipo ya lo conoce. Managed (MWAA).
  Tradeoff:  UI anticuada, XCom limitado, parseo lento con muchos DAGs.
  Alternativa evaluada: Dagster.
  Decisión: Airflow porque ya está en producción.
            Evaluar Dagster para nuevos proyectos.
```

**Preguntas:**

1. ¿Cada decisión tiene un "trigger de revisión" (cuándo reconsiderar)?

2. ¿Documentar las alternativas evaluadas tiene valor?

3. ¿Un Architecture Decision Record (ADR) es el formato adecuado
   para documentar estas decisiones?

4. ¿Un equipo de 3 data engineers puede operar
   Kafka + Flink + Spark + Airflow simultáneamente?

5. ¿Cuál de las 5 decisiones cambiarías si empezaras desde cero en 2025?

---

### Ejercicio 20.1.2 — Leer: la topología de datos del sistema

**Tipo: Leer**

```
El flujo de datos completo — con cada formato y protocolo:

  PostgreSQL (rows)
       │
       │ JDBC (SQL query, batch diario)
       ▼
  Airflow Worker (Python, psycopg2)
       │
       │ boto3 (S3 PutObject, Parquet via PyArrow)
       ▼
  S3: raw/ventas/fecha=2024-03-14/ (Parquet, Snappy compression)
       │
       │ Spark read (S3A connector, Parquet reader)
       ▼
  Spark Cluster (DataFrames en memoria, Tungsten format)
       │
       │ Delta Lake write (Parquet + transaction log)
       ▼
  S3: staging/ventas_validadas/ (Delta Lake, Z-ordered by region)
       │
       │ dbt + Spark SQL (read Delta, write BigQuery)
       ▼
  BigQuery: analytics.metricas_diarias (columnar, partitioned by fecha)
       │
       │ BigQuery API (REST, OAuth2)
       ▼
  Looker Dashboard (SQL query cada 5 minutos, cached)

  ───────────────────────────────────────────────────────

  Kafka Topic: ecommerce.clicks (Avro, Schema Registry)
       │
       │ Kafka Consumer Protocol (binary, partitioned)
       ▼
  Flink Cluster (DataStream, event time processing)
       │
       │ Flink Kafka Sink (Avro serialization)
       ▼
  Kafka Topic: ecommerce.metricas_realtime (Avro)
       │
       │ Kafka Connect BigQuery Sink (JSON/Avro → BigQuery)
       ▼
  BigQuery: realtime.metricas_clicks (streaming buffer)
       │
       ▼
  Looker Dashboard (real-time panel, refresh 30 segundos)

Cada flecha es una conversión de formato:
  rows → Parquet → Arrow → Tungsten → Delta → columnar BigQuery
  Avro → bytes → DataStream → Avro → JSON → columnar BigQuery

El costo de cada conversión es CPU + memoria + I/O.
Minimizar conversiones = maximizar throughput (Cap.02).
```

**Preguntas:**

1. ¿Cuántas conversiones de formato hay en el pipeline batch?
   ¿Cuáles se podrían eliminar?

2. ¿El streaming tiene menos o más conversiones que el batch?

3. ¿Arrow como formato intermedio universal reduciría las conversiones?

4. ¿El connector S3A de Spark es un cuello de botella conocido?

5. ¿BigQuery streaming buffer vs BigQuery load job —
   cuál para el output de Flink?

---

### Ejercicio 20.1.3 — Diagrama: el grafo de dependencias del sistema

**Tipo: Diseñar**

```
Dependencias del sistema (qué depende de qué):

  Capa 0: Infraestructura (no la controlas)
    AWS (S3, EC2, VPC, IAM)
    GCP (BigQuery, GCS)
    Confluent Cloud (Kafka)

  Capa 1: Almacenamiento (pasivo, no ejecuta)
    S3 buckets (raw, staging, marts)
    BigQuery datasets (analytics, realtime)
    Delta Lake transaction logs

  Capa 2: Compute (activo, ejecuta transformaciones)
    Spark cluster (EMR / Dataproc / Databricks)
    Flink cluster (Kubernetes)
    Airflow (MWAA / Composer)

  Capa 3: Aplicación (pipelines que tú escribes)
    DAG: ecommerce_batch_daily
    DAG: ecommerce_streaming_monitor
    DAG: ml_feature_engineering
    DAG: ml_inference_daily
    Flink job: realtime_metrics
    dbt project: ecommerce_transforms

  Capa 4: Consumo (stakeholders)
    Looker dashboards
    Jupyter notebooks (data science)
    API de recomendaciones (ML serving)
    Reportes automáticos (email)

Si la Capa 0 falla (AWS outage), TODO falla.
Si la Capa 2 falla (Spark cluster down), los datos siguen en S3
  pero no se transforman — Capa 4 ve datos stale.
Si la Capa 3 falla (DAG error), solo ese pipeline se afecta.

El sistema está diseñado para que las fallas en capas superiores
NO propaguen a capas inferiores (decoupling vía storage).
```

**Preguntas:**

1. ¿El decoupling por almacenamiento (S3 como buffer)
   es el patrón más importante del sistema?

2. ¿Un cloud provider outage es un riesgo real?
   ¿Multi-cloud es la solución?

3. ¿Cuántas capas de infraestructura puede manejar un equipo de 5?

4. ¿Managed services (EMR, MWAA, Confluent) reducen las capas
   que debes operar?

5. ¿Serverless (Lambda, Cloud Functions) elimina la Capa 2?

---

### Ejercicio 20.1.4 — El data model: raw, staging, marts

```sql
-- El sistema sigue el modelo medallion (bronze, silver, gold)
-- adaptado a nombres descriptivos:

-- RAW (bronze): datos tal como llegan de la fuente.
-- No se transforma nada. Es el "backup" de la fuente.
-- Si la fuente desaparece, raw tiene todos los datos históricos.

-- S3: raw/ventas/fecha=2024-03-14/*.parquet
-- Schema: idéntico al schema de PostgreSQL
CREATE TABLE raw.ventas (
    order_id        STRING,
    user_id         STRING,
    product_id      STRING,
    monto           DECIMAL(10,2),
    descuento       DECIMAL(10,2),
    region          STRING,
    timestamp_venta TIMESTAMP,
    fecha           DATE           -- partición
);

-- STAGING (silver): datos validados y normalizados.
-- Quality checks aplicados. Schema estable.
-- Idempotente: la misma fecha reprocesada da el mismo resultado.

CREATE TABLE staging.ventas_validadas (
    order_id        STRING NOT NULL,
    user_id         STRING NOT NULL,
    product_id      STRING NOT NULL,
    monto_bruto     DECIMAL(10,2) NOT NULL,
    descuento       DECIMAL(10,2) DEFAULT 0.00,
    monto_neto      DECIMAL(10,2) NOT NULL,  -- monto_bruto - descuento
    region          STRING NOT NULL,
    timestamp_venta TIMESTAMP NOT NULL,
    fecha           DATE NOT NULL,
    _loaded_at      TIMESTAMP,     -- cuándo se cargó este registro
    _source_hash    STRING         -- hash para deduplicación
);

-- MARTS (gold): datos optimizados para consumo.
-- Agregados, denormalizados, listos para dashboards y análisis.

CREATE TABLE marts.metricas_diarias (
    fecha               DATE NOT NULL,
    region              STRING NOT NULL,
    segmento_cliente    STRING,
    revenue_bruto       DECIMAL(12,2),
    revenue_neto        DECIMAL(12,2),
    descuentos_total    DECIMAL(12,2),
    transacciones       INT64,
    usuarios_unicos     INT64,
    ticket_promedio     DECIMAL(10,2),
    tasa_conversion     FLOAT64,
    -- Metadata:
    _updated_at         TIMESTAMP,
    _pipeline_version   STRING
)
PARTITION BY fecha
CLUSTER BY region;
```

**Preguntas:**

1. ¿Por qué mantener el raw layer si el staging tiene los mismos datos
   pero validados?

2. ¿`_loaded_at` y `_source_hash` en staging — ¿para qué sirven?

3. ¿PARTITION BY fecha + CLUSTER BY region en BigQuery
   es la estrategia correcta para este uso?

4. ¿`_pipeline_version` en marts permite debugging de regresiones?

5. ¿El modelo medallion es el único patrón?
   ¿Cuándo no funciona?

---

### Ejercicio 20.1.5 — Analizar: el costo del sistema completo

**Tipo: Analizar**

```
Estimación de costos mensuales del sistema de e-commerce
(10M eventos/día, 1M filas batch/día, equipo de 5 data engineers):

  Almacenamiento:
    S3 (10 TB raw + staging):             $230/mes
    BigQuery storage (2 TB marts):        $40/mes
    Delta Lake (transaction logs):        incluido en S3
    Total almacenamiento:                 ~$270/mes

  Compute:
    Spark cluster (EMR, 4 nodos, 8h/día): $1,200/mes
    Flink cluster (EKS, 2 pods 24/7):     $400/mes
    Airflow (MWAA, medium):               $350/mes
    Total compute:                        ~$1,950/mes

  Streaming:
    Kafka (MSK, 3 brokers):               $600/mes
    Schema Registry (Confluent):          $150/mes
    Total streaming:                      ~$750/mes

  Serving:
    BigQuery queries (~50 TB scanned/mes): $250/mes
    Looker (5 usuarios):                  $500/mes
    Total serving:                        ~$750/mes

  Observabilidad:
    Datadog (5 hosts + logs):             $500/mes
    PagerDuty (1 team):                   $50/mes
    Total observabilidad:                 ~$550/mes

  TOTAL:                                  ~$4,270/mes (~$51K/año)

  Costo del equipo (5 data engineers):    ~$75K/mes (USA avg)
  → La infraestructura es <6% del costo del equipo.
  → Optimizar el equipo importa más que optimizar la factura de cloud.
```

**Preguntas:**

1. ¿$51K/año en infraestructura es razonable para este sistema?

2. ¿Dónde está el costo más grande que se podría reducir?

3. ¿Spot instances / preemptible VMs para Spark reducen el costo?

4. ¿Serverless (Glue, BigQuery slots) es más barato que clusters dedicados?

5. ¿El 6% de infraestructura vs equipo aplica a empresas más grandes?

---

## Sección 20.2 — El Pipeline Batch: de PostgreSQL a BigQuery

### Ejercicio 20.2.1 — Implementar: el extract completo

**Tipo: Implementar**

```python
# El extract de PostgreSQL a S3 — production-ready.
# Requisitos:
# - Idempotente (safe to re-run)
# - Partitioned by fecha
# - Schema validation
# - Métricas y logging
# - Manejo de errores con reintentos

import polars as pl
import structlog
from datetime import date
from typing import Optional

log = structlog.get_logger()

class PostgresExtractor:
    """Extraer datos de PostgreSQL y escribir a S3 como Parquet."""

    def __init__(self, conn_string: str, s3_bucket: str):
        self.conn_string = conn_string
        self.s3_bucket = s3_bucket

    def extraer_ventas(self, fecha: date) -> dict:
        """Extraer ventas de un día específico."""
        log.info("extract_started", tabla="ventas", fecha=str(fecha))

        query = f"""
            SELECT order_id, user_id, product_id, monto, descuento,
                   region, timestamp_venta, fecha
            FROM ventas
            WHERE fecha = '{fecha}'
        """

        df = pl.read_database(query, self.conn_string)
        log.info("extract_query_completed", filas=len(df), fecha=str(fecha))

        # Validar schema:
        expected_columns = {
            "order_id", "user_id", "product_id", "monto",
            "descuento", "region", "timestamp_venta", "fecha"
        }
        actual_columns = set(df.columns)
        if actual_columns != expected_columns:
            missing = expected_columns - actual_columns
            extra = actual_columns - expected_columns
            raise ValueError(f"Schema mismatch. Missing: {missing}, Extra: {extra}")

        # Quality check básico:
        if len(df) == 0:
            log.warning("extract_empty", fecha=str(fecha))

        # Escribir a S3:
        s3_path = f"{self.s3_bucket}/raw/ventas/fecha={fecha}/"
        df.write_parquet(f"{s3_path}/data.parquet")
        log.info("extract_written", path=s3_path, filas=len(df),
                 bytes=df.estimated_size())

        return {
            "filas": len(df),
            "path": s3_path,
            "fecha": str(fecha),
            "columns": list(df.columns),
        }
```

**Preguntas:**

1. ¿Este extract es idempotente? ¿Qué pasa si lo ejecutas dos veces?

2. ¿`read_database` con una query parametrizada es seguro (SQL injection)?

3. ¿Escribir un solo archivo Parquet o múltiples archivos particionados?

4. ¿El extract debería usar COPY de PostgreSQL en vez de una query?

5. ¿CDC (Debezium) sería mejor para tablas con >10M filas/día?

---

### Ejercicio 20.2.2 — Implementar: la transformación en Spark + dbt

**Tipo: Implementar**

```python
# La transformación tiene dos fases:
# Fase 1: Spark (limpieza pesada, join con dimensiones, write Delta Lake)
# Fase 2: dbt (agregaciones SQL, métricas de negocio, write BigQuery)

# Fase 1: Spark
from pyspark.sql import SparkSession, functions as F

def transformar_ventas_spark(spark: SparkSession, fecha: str):
    """Limpiar y enriquecer ventas, escribir a staging (Delta Lake)."""
    # Leer raw:
    ventas = spark.read.parquet(f"s3://data-lake/raw/ventas/fecha={fecha}/")
    clientes = spark.read.parquet("s3://data-lake/dim/clientes/")
    productos = spark.read.parquet("s3://data-lake/dim/productos/")

    # Limpiar:
    ventas_clean = ventas \
        .filter(F.col("monto") > 0) \
        .filter(F.col("user_id").isNotNull()) \
        .withColumn("monto_neto", F.col("monto") - F.coalesce(F.col("descuento"), F.lit(0))) \
        .withColumn("_loaded_at", F.current_timestamp()) \
        .withColumn("_source_hash", F.sha2(F.concat_ws("|",
            F.col("order_id"), F.col("user_id"), F.col("monto")), 256))

    # Enriquecer con dimensiones:
    enriched = ventas_clean \
        .join(clientes.select("user_id", "segmento", "pais"), on="user_id", how="left") \
        .join(productos.select("product_id", "categoria", "marca"), on="product_id", how="left")

    # Escribir a staging (Delta Lake, idempotente con replaceWhere):
    enriched.write \
        .format("delta") \
        .mode("overwrite") \
        .option("replaceWhere", f"fecha = '{fecha}'") \
        .save("s3://data-lake/staging/ventas_validadas/")

    return {"filas_input": ventas.count(), "filas_output": enriched.count(),
            "filas_dropped": ventas.count() - ventas_clean.count()}
```

```sql
-- Fase 2: dbt (models/marts/metricas_diarias.sql)

{{ config(
    materialized='incremental',
    unique_key=['fecha', 'region', 'segmento_cliente'],
    partition_by={'field': 'fecha', 'data_type': 'date'},
    cluster_by=['region'],
) }}

WITH ventas AS (
    SELECT * FROM {{ ref('stg_ventas_validadas') }}
    {% if is_incremental() %}
    WHERE fecha > (SELECT MAX(fecha) FROM {{ this }})
    {% endif %}
),

metricas AS (
    SELECT
        fecha,
        region,
        segmento AS segmento_cliente,
        SUM(monto_neto) AS revenue_neto,
        SUM(monto) AS revenue_bruto,
        SUM(COALESCE(descuento, 0)) AS descuentos_total,
        COUNT(*) AS transacciones,
        COUNT(DISTINCT user_id) AS usuarios_unicos,
        ROUND(AVG(monto_neto), 2) AS ticket_promedio,
        CURRENT_TIMESTAMP() AS _updated_at,
        '{{ var("pipeline_version", "unknown") }}' AS _pipeline_version
    FROM ventas
    GROUP BY 1, 2, 3
)

SELECT * FROM metricas
```

**Preguntas:**

1. ¿Spark para limpieza + dbt para agregación — es la separación correcta?
   ¿Cuándo usarías solo dbt?

2. ¿`replaceWhere` en Delta Lake garantiza idempotencia?

3. ¿`is_incremental()` en dbt procesa solo los datos nuevos?
   ¿Qué pasa si necesitas re-procesar un día antiguo?

4. ¿El join con dimensiones debería ser broadcast join o shuffle join?

5. ¿`_pipeline_version` como variable de dbt — ¿cómo se pasa desde Airflow?

---

### Ejercicio 20.2.3 — Implementar: el DAG de Airflow completo

**Tipo: Implementar**

```python
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator

@dag(
    dag_id="ecommerce_batch_daily",
    schedule="0 3 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
        "on_failure_callback": alerta_fallo,
    },
    tags=["ecommerce", "batch", "daily", "production"],
)
def ecommerce_batch():

    @task()
    def verificar_fuentes(**context):
        """Health check: ¿PostgreSQL y S3 están disponibles?"""
        fecha = context["data_interval_start"].strftime("%Y-%m-%d")
        check_postgres_health()
        check_s3_health()
        return {"fecha": fecha, "fuentes_ok": True}

    @task()
    def extraer_ventas(metadata: dict):
        """Extraer ventas de PostgreSQL a S3."""
        extractor = PostgresExtractor(conn_string=POSTGRES_CONN, s3_bucket=S3_BUCKET)
        return extractor.extraer_ventas(date.fromisoformat(metadata["fecha"]))

    @task()
    def extraer_clientes():
        """Extraer tabla de clientes (dimensión)."""
        extractor = PostgresExtractor(conn_string=POSTGRES_CONN, s3_bucket=S3_BUCKET)
        return extractor.extraer_clientes()

    @task()
    def quality_gate_raw(metadata_ventas: dict):
        """Verificar calidad de datos raw."""
        if metadata_ventas["filas"] < 1000:
            raise ValueError(f"Solo {metadata_ventas['filas']} ventas — mínimo 1000")
        if metadata_ventas["filas"] > 10_000_000:
            raise ValueError(f"{metadata_ventas['filas']} ventas — posible duplicación")
        return {**metadata_ventas, "quality_ok": True}

    transformar_spark = SparkSubmitOperator(
        task_id="transformar_spark",
        application="/opt/spark-jobs/transformar_ventas.py",
        conf={"spark.sql.shuffle.partitions": "100"},
        application_args=["--fecha", "{{ data_interval_start | ds }}"],
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/dbt && dbt run --select tag:daily "
                     "--vars '{\"fecha\": \"{{ data_interval_start | ds }}\", "
                     "\"pipeline_version\": \"v2.3.1\"}'",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/dbt && dbt test --select tag:daily",
    )

    @task()
    def quality_gate_final(**context):
        """Verificar calidad del output final en BigQuery."""
        fecha = context["data_interval_start"].strftime("%Y-%m-%d")
        resultado = bq_client.query(f"""
            SELECT COUNT(*) as n, SUM(revenue_neto) as revenue
            FROM analytics.metricas_diarias WHERE fecha = '{fecha}'
        """).result().to_dataframe()
        revenue = resultado["revenue"].iloc[0]
        if revenue == 0:
            raise ValueError(f"Revenue = $0 para {fecha}")
        return {"filas": int(resultado["n"].iloc[0]), "revenue": float(revenue)}

    @task()
    def notificar(quality: dict, **context):
        fecha = context["data_interval_start"].strftime("%Y-%m-%d")
        enviar_slack(f"Pipeline batch {fecha} completado. "
                     f"Revenue: ${quality['revenue']:,.2f}")

    # Grafo de dependencias:
    fuentes = verificar_fuentes()
    ventas = extraer_ventas(fuentes)
    clientes = extraer_clientes()
    quality_raw = quality_gate_raw(ventas)

    quality_raw >> transformar_spark >> dbt_run >> dbt_test
    clientes >> transformar_spark

    quality_final = quality_gate_final()
    dbt_test >> quality_final >> notificar(quality_final)

ecommerce_batch()
```

**Preguntas:**

1. ¿Este DAG cubre todos los pasos necesarios para producción?

2. ¿Dos quality gates (raw y final) es suficiente o necesitas más?

3. ¿SparkSubmitOperator vs PythonOperator — cuál para ejecutar Spark?

4. ¿El DAG es idempotente? ¿Qué paso no lo es?

5. ¿El DAG maneja correctamente un backfill de 30 días?

---

### Ejercicio 20.2.4 — Medir: performance del pipeline batch

**Tipo: Medir**

```
Benchmark del pipeline batch (datos reales del sistema de e-commerce):

  Volumen: 5M filas/día de ventas, 500K clientes, 50K productos

  Paso                    Duración    Filas/sec    Notas
  ─────────────────────   ─────────   ─────────    ─────────────
  Extract PostgreSQL       3 min       28K/s       JDBC, full scan
  Write Parquet S3         1 min       83K/s       Snappy, 128MB blocks
  Spark read raw           30 sec      167K/s      S3A, 10 partitions
  Spark transform          2 min       42K/s       join + filter + enrichment
  Delta Lake write         1 min       83K/s       replaceWhere, 1 partition
  dbt run (BigQuery)       4 min       21K/s       incremental, 3 models
  dbt test                 1 min       —           5 tests
  Quality checks           30 sec      —           3 queries BigQuery
  ─────────────────────   ─────────
  TOTAL                    ~13 min

  SLA: completar antes de las 6:00 UTC (3 horas de margen)
  Buffer: el pipeline tarda 13 min pero el SLA es 3 horas
          → margen para reintentos, slowness, y crecimiento de datos

  Proyección de crecimiento:
  Si los datos crecen 2× al año:
  - Año 1: 5M filas → 13 min
  - Año 2: 10M filas → ~25 min (sub-lineal gracias a Spark scaling)
  - Año 3: 20M filas → ~45 min (todavía dentro del SLA)
  - Año 4: 40M filas → ~80 min (empezamos a acercarnos al SLA)
  → Trigger de revisión: cuando el pipeline supere 1 hora.
```

**Preguntas:**

1. ¿13 minutos es rápido o lento para 5M filas?

2. ¿El cuello de botella es el extract (JDBC) o dbt (BigQuery)?

3. ¿La proyección de crecimiento sub-lineal asume scaling de Spark?

4. ¿Cuándo migrar de batch extract a CDC (Debezium)?

5. ¿Si el SLA baja de 3 horas a 30 minutos,
   qué partes del pipeline optimizas primero?

---

### Ejercicio 20.2.5 — Diagnosticar: el pipeline batch falla un lunes a las 3am

**Tipo: Diagnosticar**

```
Incidente: lunes 2024-03-18, 03:00 UTC.
El DAG ecommerce_batch_daily falla.

Alerta recibida (Slack #data-alerts, 03:15 UTC):
  "❌ ecommerce_batch_daily.extraer_ventas falló después de 3 reintentos.
   Error: psycopg2.OperationalError: connection refused
   Último éxito: 2024-03-17 03:45 UTC"

Log de Airflow:
  [2024-03-18 03:00:15] INFO - extract_started tabla=ventas fecha=2024-03-17
  [2024-03-18 03:00:16] ERROR - connection to server at "prod-db.internal"
                                 refused. Is the server running?
  [2024-03-18 03:05:16] INFO - Retry 1/3 in 5 minutes
  [2024-03-18 03:10:17] ERROR - connection refused (same error)
  [2024-03-18 03:15:17] INFO - Retry 2/3 in 10 minutes
  [2024-03-18 03:25:18] ERROR - connection refused (same error)
  [2024-03-18 03:30:18] INFO - Retry 3/3 in 20 minutes
  [2024-03-18 03:50:19] ERROR - connection refused (same error)
  [2024-03-18 03:50:20] ERROR - Task failed after 3 retries

  Investigación:
  - PostgreSQL está caído → mantenimiento programado del DBA (ventana: 02:00-04:00)
  - El DBA no notificó al equipo de data engineering
  - El pipeline tiene que esperar hasta que PostgreSQL vuelva

  ¿Qué haces?
```

**Preguntas:**

1. ¿El pipeline debería esperar (sensor) o fallar y re-ejecutar después?

2. ¿Un calendario de mantenimiento compartido habría prevenido esto?

3. ¿El exponential backoff de los reintentos fue adecuado?

4. ¿Cómo re-ejecutas el pipeline del lunes cuando PostgreSQL vuelve?

5. ¿Qué acción de postmortem propones para evitar que se repita?

---

## Sección 20.3 — El Pipeline Streaming: de Kafka al Dashboard en Tiempo Real

### Ejercicio 20.3.1 — Implementar: el Flink job de métricas real-time

**Tipo: Implementar**

```python
# Pipeline de Flink (representado en Python para legibilidad,
# implementado en Java para producción — ver Cap.15):

# Kafka (clicks) → Flink → métricas por ventana → Kafka (métricas) → BigQuery

# Lógica del Flink job:
# 1. Consumir eventos de click de Kafka
# 2. Parsear y validar (descartar malformados)
# 3. Asignar event time (timestamp del evento, no de procesamiento)
# 4. Ventana tumbling de 1 minuto
# 5. Agregar: clicks por página, por región
# 6. Ventana sliding de 5 minutos
# 7. Agregar: usuarios únicos, conversión (clicks → compras)
# 8. Escribir a Kafka topic de salida
# 9. Kafka Connect → BigQuery (streaming insert)

# Schema del evento de entrada (Avro):
# {
#   "user_id": "alice",
#   "event_type": "click" | "purchase" | "add_to_cart",
#   "page": "/product/123",
#   "product_id": "prod-123",
#   "monto": 0.0 (para clicks) | 150.0 (para purchases),
#   "region": "norte",
#   "timestamp": 1710400000000  (epoch millis)
# }

# Schema del evento de salida:
# {
#   "window_start": "2024-03-14T10:00:00Z",
#   "window_end": "2024-03-14T10:01:00Z",
#   "region": "norte",
#   "clicks": 1234,
#   "purchases": 56,
#   "add_to_cart": 189,
#   "revenue": 8400.0,
#   "conversion_rate": 0.045,
#   "unique_users": 890
# }
```

**Preguntas:**

1. ¿Por qué ventana tumbling de 1 minuto Y sliding de 5 minutos?
   ¿Qué aporta cada una?

2. ¿Event time con watermark de cuántos segundos de delay?

3. ¿Qué pasa con eventos que llegan 5 minutos tarde?

4. ¿Kafka Connect BigQuery Sink garantiza exactly-once?

5. ¿El Flink job necesita ser reiniciado para cambiar
   la configuración de ventanas?

---

### Ejercicio 20.3.2 — Reconciliación: batch vs streaming

**Tipo: Implementar**

```python
# El batch pipeline calcula métricas diarias con datos completos.
# El streaming pipeline calcula métricas por minuto con datos parciales.
# Deben coincidir al final del día.

@task()
def reconciliar_batch_streaming(**context):
    """Verificar que batch y streaming coinciden."""
    fecha = context["data_interval_start"].strftime("%Y-%m-%d")

    # Revenue del batch:
    batch = bq_client.query(f"""
        SELECT SUM(revenue_neto) as revenue_batch
        FROM analytics.metricas_diarias
        WHERE fecha = '{fecha}'
    """).result().to_dataframe()

    # Revenue del streaming (sumando todas las ventanas del día):
    streaming = bq_client.query(f"""
        SELECT SUM(revenue) as revenue_streaming
        FROM realtime.metricas_clicks
        WHERE DATE(window_start) = '{fecha}'
        AND event_type = 'purchase'
    """).result().to_dataframe()

    revenue_batch = batch["revenue_batch"].iloc[0]
    revenue_streaming = streaming["revenue_streaming"].iloc[0]
    diff_pct = abs(revenue_batch - revenue_streaming) / revenue_batch * 100

    log.info("reconciliation_completed",
             fecha=fecha,
             batch=revenue_batch,
             streaming=revenue_streaming,
             diff_pct=round(diff_pct, 2))

    # Tolerancia: 2% de diferencia es aceptable
    # (streaming puede perder eventos late, batch es completo)
    if diff_pct > 5:
        raise ValueError(
            f"Reconciliación falla: batch=${revenue_batch:.2f} vs "
            f"streaming=${revenue_streaming:.2f} (diff={diff_pct:.1f}%)")
    elif diff_pct > 2:
        log.warning("reconciliation_drift", diff_pct=diff_pct)

    return {"diff_pct": diff_pct, "batch": revenue_batch,
            "streaming": revenue_streaming}
```

**Preguntas:**

1. ¿2% de diferencia entre batch y streaming es aceptable?

2. ¿La diferencia siempre será streaming < batch
   (por eventos late no procesados)?

3. ¿La reconciliación debe ejecutarse cada día
   o solo cuando hay sospecha de problemas?

4. ¿Si la diferencia es >5%, ¿qué datos son correctos: batch o streaming?

5. ¿Lambda Architecture vs Kappa Architecture — ¿cuál es este sistema?

---

### Ejercicio 20.3.3 — El dashboard: batch + streaming en una sola vista

**Tipo: Diseñar**

```
El dashboard de revenue del e-commerce tiene dos fuentes:
  - Métricas batch (diarias, completas, hasta ayer)
  - Métricas streaming (por minuto, parciales, hoy)

  ┌──────────────────────────────────────────────────────────┐
  │  Revenue Dashboard                                        │
  │                                                           │
  │  Revenue total (últimos 7 días):                          │
  │  ┌────────────────────────────────────────────────────┐  │
  │  │ $$$                                           ╱    │  │
  │  │         ╱╲          ╱╲                       ╱     │  │
  │  │ batch  ╱  ╲        ╱  ╲       ╱╲           ╱ hoy  │  │
  │  │ ──────╱────╲──────╱────╲─────╱──╲─────────╱  (RT) │  │
  │  │  Lun   Mar   Mie   Jue   Vie   Sab  Dom   Lun    │  │
  │  └────────────────────────────────────────────────────┘  │
  │                                                           │
  │  El segmento de "hoy" viene del streaming (parcial).     │
  │  Los días anteriores vienen del batch (completos).        │
  │  Indicador: "Datos de hoy: actualizados hace 45 seg"     │
  │  Indicador: "Datos históricos: actualizados 03:45 UTC"   │
  └──────────────────────────────────────────────────────────┘

  Query del dashboard (Looker):
    SELECT fecha, SUM(revenue_neto) AS revenue
    FROM analytics.metricas_diarias  -- batch, hasta ayer
    WHERE fecha >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    GROUP BY fecha

    UNION ALL

    SELECT DATE(window_start) AS fecha, SUM(revenue) AS revenue
    FROM realtime.metricas_clicks    -- streaming, hoy
    WHERE DATE(window_start) = CURRENT_DATE()
    GROUP BY fecha
```

**Preguntas:**

1. ¿UNION ALL entre batch y streaming es el patrón correcto?

2. ¿El "hoy" del streaming se reemplaza por el batch mañana?

3. ¿El indicador de freshness es visible para los analistas?

4. ¿Un analista puede confiar en el número de "hoy"?

5. ¿El dashboard necesita explicar la diferencia entre batch y streaming?

---

### Ejercicio 20.3.4 — Escalar el streaming: de 10K a 1M eventos/segundo

**Tipo: Analizar**

```
El sistema procesa 10M eventos/día (~115 eventos/segundo).
¿Qué pasa si el negocio crece 100× (1B eventos/día, ~11,500 eventos/segundo)?

  Kafka:
    115 ev/s → 3 particiones, 3 brokers → OK
    11,500 ev/s → 30+ particiones, 6+ brokers → scaling horizontal
    Acción: añadir brokers, aumentar particiones, ajustar replication

  Flink:
    115 ev/s → parallelism=2, 1 TaskManager → OK
    11,500 ev/s → parallelism=20+, 5+ TaskManagers → scaling horizontal
    Acción: aumentar parallelism, más TaskManagers, state backend RocksDB
    Riesgo: checkpoints más lentos (más state), más GC pressure

  BigQuery (streaming insert):
    115 ev/s → streaming buffer OK
    11,500 ev/s → batching necesario (BigQuery Storage Write API)
    Acción: agrupar inserts cada 10 segundos, usar batch loading

  Costo (aproximado):
    115 ev/s: ~$750/mes (streaming infra)
    11,500 ev/s: ~$5,000/mes
    Scaling es ~7× costo para 100× throughput → sub-lineal, eficiente
```

**Preguntas:**

1. ¿11,500 ev/s es mucho para Flink? ¿Cuál es el límite práctico?

2. ¿El scaling de Kafka particiones requiere downtime?

3. ¿Flink auto-scaling (reactive mode) elimina la necesidad
   de provisionar manualmente?

4. ¿BigQuery Storage Write API es significativamente más eficiente
   que streaming inserts?

5. ¿A qué escala dejas de usar Flink + BigQuery
   y empiezas a considerar otras arquitecturas?

---

### Ejercicio 20.3.5 — Diagnosticar: consumer lag creciente a las 2pm

**Tipo: Diagnosticar**

```
Síntoma: alerta de Grafana a las 14:05 UTC.
"Kafka consumer lag > 50,000 mensajes para consumer group flink-metricas"

Contexto:
  - Consumer lag a las 13:55: 200 mensajes (normal)
  - Consumer lag a las 14:00: 10,000 mensajes
  - Consumer lag a las 14:05: 50,000 mensajes (y creciendo)
  - Throughput de entrada: 150 ev/s (normal)
  - Throughput de salida del consumer: 50 ev/s (1/3 del normal)

Métricas de Flink:
  - isBackPressured: true (operador: "calcular_metricas")
  - checkpointDuration: 45 seconds (normal: 5 seconds)
  - stateSize: 2.5 GB (ayer: 500 MB)

¿Qué pasó? ¿Cómo lo arreglas?
```

**Preguntas:**

1. ¿Backpressure en "calcular_metricas" + state creciente
   sugiere qué tipo de problema?

2. ¿State de 500 MB a 2.5 GB en un día es un leak o es esperado?

3. ¿Checkpoint de 45 segundos es peligroso? ¿Qué pasa si falla?

4. ¿Puedes escalar Flink sin reiniciar el job?

5. ¿La acción inmediata es escalar o investigar el root cause?

**Pista:** State que crece 5× en un día generalmente indica un state leak:
el operador acumula estado sin limpiarlo. Causes comunes: un TTL no configurado
en state descriptors, o un join entre un stream y una tabla donde la tabla
no tiene evicción. La acción inmediata es aumentar parallelism para comprar tiempo.
La acción de raíz es añadir TTL al state y verificar la lógica del join.

---

## Sección 20.4 — La Capa de Transformación: dbt, Spark, y el Lakehouse

### Ejercicio 20.4.1 — El proyecto dbt del e-commerce

**Tipo: Implementar**

```
Estructura del proyecto dbt:

  dbt_ecommerce/
  ├── dbt_project.yml
  ├── models/
  │   ├── staging/
  │   │   ├── stg_ventas_validadas.sql     (vista sobre Delta Lake)
  │   │   ├── stg_clientes.sql             (dimensión)
  │   │   └── stg_productos.sql            (dimensión)
  │   ├── intermediate/
  │   │   ├── int_ventas_enriquecidas.sql  (join ventas + clientes + productos)
  │   │   └── int_sesiones_usuario.sql     (sessionization de clicks)
  │   └── marts/
  │       ├── metricas_diarias.sql         (revenue por región)
  │       ├── metricas_producto.sql        (top productos)
  │       ├── cohortes_usuario.sql         (retención por cohorte)
  │       └── funnel_conversion.sql        (click → cart → purchase)
  ├── tests/
  │   └── assert_revenue_not_negative.sql
  ├── macros/
  │   └── quality_checks.sql
  └── seeds/
      └── regiones_validas.csv

  # Total: ~15 modelos, 5 stages, 3 marts
  # DAG de dbt:
  #   stg_ventas → int_ventas_enriquecidas → metricas_diarias
  #   stg_clientes ─────────────────────────↗
  #   stg_productos ────────────────────────↗
```

**Preguntas:**

1. ¿15 modelos es un proyecto dbt pequeño, mediano, o grande?

2. ¿staging → intermediate → marts es siempre la estructura correcta?

3. ¿Las vistas de staging sobre Delta Lake tienen performance aceptable?

4. ¿Materializar como tabla o vista en cada capa?

5. ¿dbt puede leer directamente de Delta Lake en S3?

---

### Ejercicio 20.4.2 — Delta Lake: ACID sobre el data lake

```python
# Delta Lake en el sistema de e-commerce:
# - raw/: Parquet puro (no Delta) — inmutable, append-only
# - staging/: Delta Lake — ACID, time travel, schema enforcement
# - marts/: BigQuery — optimizado para queries analíticas

# Operaciones Delta Lake en el sistema:

# 1. MERGE (upsert): actualizar clientes existentes, insertar nuevos
from delta.tables import DeltaTable

delta_clientes = DeltaTable.forPath(spark, "s3://data-lake/staging/clientes/")

delta_clientes.alias("target") \
    .merge(
        nuevos_clientes.alias("source"),
        "target.user_id = source.user_id"
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

# 2. Time Travel: consultar datos como estaban ayer
df_ayer = spark.read.format("delta") \
    .option("timestampAsOf", "2024-03-13") \
    .load("s3://data-lake/staging/ventas_validadas/")

# 3. Schema Evolution: añadir columna sin romper pipelines downstream
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# 4. VACUUM: limpiar archivos obsoletos (liberar espacio)
delta_ventas = DeltaTable.forPath(spark, "s3://data-lake/staging/ventas_validadas/")
delta_ventas.vacuum(168)  # retener 7 días de history
```

**Preguntas:**

1. ¿MERGE en Delta Lake es idempotente?

2. ¿Time travel de 7 días es suficiente para debugging?

3. ¿Schema evolution automática es peligrosa en producción?

4. ¿VACUUM debe ejecutarse en un DAG separado?

5. ¿Delta Lake vs Iceberg — ¿cuál para este sistema en 2025?

---

### Ejercicio 20.4.3 — Feature engineering para ML

**Tipo: Implementar**

```python
# El sistema de e-commerce alimenta un modelo de ML para recomendaciones.
# Features calculadas diariamente:

@dg.asset(
    description="Features de usuario para el modelo de recomendaciones",
    deps=[ventas_validadas, sesiones_usuario],
    group_name="ml_features",
    partitions_def=daily_partitions,
)
def user_features(context, ventas_validadas, sesiones_usuario):
    """Calcular features por usuario."""
    features = ventas_validadas.group_by("user_id").agg([
        # Comportamiento de compra:
        pl.count().alias("total_compras_30d"),
        pl.sum("monto_neto").alias("revenue_total_30d"),
        pl.mean("monto_neto").alias("ticket_promedio_30d"),
        pl.std("monto_neto").alias("ticket_std_30d"),
        pl.n_unique("product_id").alias("productos_unicos_30d"),
        pl.n_unique("categoria").alias("categorias_30d"),

        # Temporalidad:
        pl.max("timestamp_venta").alias("ultima_compra"),
        pl.min("timestamp_venta").alias("primera_compra"),

        # Preferencias:
        pl.col("region").mode().first().alias("region_preferida"),
        pl.col("categoria").mode().first().alias("categoria_preferida"),
    ])

    # Añadir features de sesión:
    session_features = sesiones_usuario.group_by("user_id").agg([
        pl.mean("session_duration_min").alias("duracion_sesion_promedio"),
        pl.mean("pages_viewed").alias("paginas_por_sesion"),
        pl.sum("add_to_cart_count").alias("total_add_to_cart_30d"),
    ])

    return features.join(session_features, on="user_id", how="left")
```

**Preguntas:**

1. ¿Estas features son suficientes para un modelo de recomendaciones?

2. ¿Features calculadas diariamente o en tiempo real?

3. ¿Un feature store (Feast, Tecton) es necesario para este sistema?

4. ¿El feature engineering debe estar en dbt (SQL) o en Dagster (Python)?

5. ¿Cómo testeas que las features son correctas?

---

### Ejercicio 20.4.4 — Analizar: cuándo no necesitas Spark

```
No todo necesita Spark. La decisión depende del volumen:

  < 1 GB:    Polars en un solo nodo.
             100× más rápido de desarrollar que Spark.
             Sin JVM, sin cluster, sin configuración.
             → La mayoría de los pipelines del e-commerce
               caen en esta categoría.

  1-100 GB:  Polars o DuckDB en un nodo grande (32GB+ RAM).
             Todavía más rápido que Spark para este volumen.
             → El extract diario de 5M filas ≈ 500 MB.
               No necesita Spark.

  100 GB - 1 TB: Spark empieza a justificarse.
             El overhead de setup (cluster, JVM, shuffle)
             se amortiza con el volumen.
             → El backfill de 1 año ≈ 180 GB. Aquí sí Spark.

  > 1 TB:    Spark es necesario.
             Un solo nodo no puede manejar este volumen
             eficientemente.

  La decisión del e-commerce:
    Daily batch (500 MB): Polars sería suficiente.
    Usamos Spark porque:
    1. El equipo ya lo conoce
    2. El backfill de 1 año sí necesita Spark
    3. El feature engineering con joins complejos escala mejor en Spark
    → Si empezáramos hoy, haríamos el daily con Polars
      y Spark solo para backfill y ML.
```

**Preguntas:**

1. ¿Es un error usar Spark para 500 MB de datos diarios?

2. ¿Polars + DuckDB para daily + Spark para backfill es más complejo
   (dos motores) o más eficiente?

3. ¿BigQuery SQL directamente (sin Spark ni Polars) es una opción
   para transformaciones simples?

4. ¿La tendencia en 2025 es menos Spark y más motores ligeros?

5. ¿Si eliminas Spark del daily batch, ¿cuánto reduces el costo?

---

### Ejercicio 20.4.5 — Implementar: el lakehouse completo

**Tipo: Implementar**

```python
# Implementar el lakehouse del sistema de e-commerce:
#
# 1. Raw layer: Parquet en S3, particionado por fecha
#    - ventas (daily, PostgreSQL)
#    - eventos (streaming, Kafka → Parquet via Flink)
#    - clientes (daily, PostgreSQL)
#    - productos (daily, API REST)
#
# 2. Staging layer: Delta Lake en S3
#    - ventas_validadas (MERGE, schema enforcement)
#    - clientes_dim (SCD Type 2 via MERGE)
#    - productos_dim (overwrite daily)
#    - eventos_sessionized (sessionización de clicks)
#
# 3. Marts layer: BigQuery (materialized by dbt)
#    - metricas_diarias (revenue, conversión, usuarios)
#    - metricas_producto (top productos, categorías)
#    - cohortes_usuario (retención, LTV)
#    - user_features (ML feature store)

# Requisitos:
# - Schema enforcement en staging (rechazar datos inválidos)
# - Time travel de 7 días en staging
# - VACUUM semanal
# - Lineage completo (OpenLineage)
```

**Restricciones:**
1. Implementar el schema de cada capa
2. Implementar MERGE para clientes (SCD Type 2)
3. Implementar los quality checks entre capas
4. Calcular el storage cost por capa

---

## Sección 20.5 — El Sistema Operado: Orquestación, Observabilidad, y Testing Juntos

### Ejercicio 20.5.1 — La operación diaria: qué hace el on-call

```
Día típico del on-call de data engineering:

  03:00 UTC — Pipeline batch arranca (automático).
  03:45 UTC — Pipeline completa. Slack: "✅ batch completado".
              El on-call no se despierta.

  06:00 UTC — Analistas llegan. Dashboards actualizados. Sin incidentes.

  09:30 UTC — Slack de un analista: "¿Por qué el revenue de Brasil
              bajó 30% ayer?" El on-call verifica:
              1. Pipeline ejecutó OK → sí
              2. Quality checks pasaron → sí
              3. Lineage muestra → datos de Brasil llegaron correctamente
              4. Distribución → no hay anomalía estadística
              → Conclusión: la caída es real (no un bug). Escalar a negocio.

  14:05 UTC — Alerta P2: "Consumer lag > 10,000 para flink-metricas".
              El on-call investiga:
              1. Grafana → backpressure en el operador de join
              2. Flink metrics → state size creció 3×
              3. Root cause: un nuevo tipo de evento sin TTL configurado
              → Fix: añadir TTL, reiniciar Flink con savepoint
              → Tiempo de resolución: 35 minutos

  17:00 UTC — Review del día: 0 incidentes P1, 1 incidente P2 resuelto.
              Escribir postmortem del P2 con action items.

  Herramientas usadas durante el día:
    Airflow UI, Grafana, Slack, PagerDuty, BigQuery console,
    Flink Dashboard, kubectl (para Flink en K8s), git (para el fix)
```

**Preguntas:**

1. ¿Un on-call de data engineering es comparable a un on-call de SRE?

2. ¿"Revenue de Brasil bajó 30%" — ¿es responsabilidad del data engineer
   o del analista determinar si es un bug?

3. ¿El postmortem del P2 debe incluir el fix de código?

4. ¿Cuántos incidentes P2 por semana son aceptables?

5. ¿Un equipo de 5 personas necesita rotación de on-call?

---

### Ejercicio 20.5.2 — Data SLOs: definir qué significa "el sistema funciona"

```python
# SLOs (Service Level Objectives) para el sistema de datos:

DATA_SLOS = {
    # Freshness: ¿los datos están actualizados?
    "batch_freshness": {
        "metric": "tiempo desde última actualización de marts.metricas_diarias",
        "target": "< 6 horas en el 99% de los días",
        "measurement": """
            SELECT DATE_DIFF(CURRENT_TIMESTAMP(), MAX(_updated_at), HOUR)
            FROM analytics.metricas_diarias
        """,
    },
    "streaming_freshness": {
        "metric": "lag del dashboard de real-time",
        "target": "< 2 minutos en el 99.9% del tiempo",
        "measurement": "max(kafka_consumer_lag_seconds)",
    },

    # Quality: ¿los datos son correctos?
    "data_quality_score": {
        "metric": "porcentaje de quality checks que pasan",
        "target": "> 99.5% de checks pasan por día",
        "measurement": """
            SELECT COUNTIF(passed) / COUNT(*) * 100
            FROM metadata.quality_results
            WHERE checked_at >= CURRENT_DATE()
        """,
    },

    # Completeness: ¿tenemos todos los datos?
    "batch_completeness": {
        "metric": "filas procesadas vs filas esperadas",
        "target": "> 95% de las filas esperadas en el 99% de los días",
        "measurement": """
            SELECT actual_rows / expected_rows * 100
            FROM metadata.pipeline_metrics
            WHERE pipeline_name = 'ecommerce_batch_daily'
        """,
    },

    # Reconciliation: ¿batch y streaming coinciden?
    "batch_streaming_reconciliation": {
        "metric": "diferencia porcentual entre batch y streaming revenue",
        "target": "< 3% de diferencia en el 95% de los días",
        "measurement": "abs(batch_revenue - streaming_revenue) / batch_revenue * 100",
    },
}
```

**Preguntas:**

1. ¿Estos SLOs son realistas para un equipo que recién empieza?

2. ¿Cómo mides el SLO de freshness de forma automatizada?

3. ¿99.5% de quality checks pasan — ¿qué haces con el 0.5% que falla?

4. ¿Los SLOs deben ser visibles para stakeholders no-técnicos?

5. ¿Un error budget (como en SRE) aplica a data SLOs?

---

### Ejercicio 20.5.3 — El test suite completo del sistema

**Tipo: Implementar**

```python
# El test suite del sistema de e-commerce en CI:
#
# tests/
# ├── unit/                          (~30 tests, <10 seg)
# │   ├── test_revenue_calculation.py
# │   ├── test_validation_rules.py
# │   └── test_data_transformations.py
# ├── contracts/                     (~15 tests, <5 seg)
# │   ├── test_schema_ventas.py
# │   ├── test_schema_metricas.py
# │   └── test_avro_compatibility.py
# ├── component/                     (~20 tests, <60 seg)
# │   ├── test_spark_transforms.py
# │   ├── test_beam_pipeline.py
# │   └── test_polars_transforms.py
# ├── dag_validation/                (~10 tests, <15 seg)
# │   ├── test_dag_parsing.py
# │   └── test_dag_structure.py
# ├── integration/                   (~10 tests, <120 seg)
# │   ├── test_postgres_extract.py
# │   ├── test_kafka_produce_consume.py
# │   └── test_delta_lake_merge.py
# └── e2e/                           (~5 tests, staging only)
#     ├── test_pipeline_batch.py
#     └── test_reconciliation.py
#
# Total: ~90 tests
# CI time: ~3 minutos (sin E2E)
# Pre-deploy: ~10 minutos (con E2E en staging)
```

**Preguntas:**

1. ¿90 tests son suficientes para un sistema de esta complejidad?

2. ¿La distribución 30/15/20/10/10/5 entre capas es la correcta?

3. ¿3 minutos de CI es rápido o lento?

4. ¿Los E2E tests solo antes de deploy — ¿es suficiente?

5. ¿Quién escribe los tests: el data engineer que construye el pipeline
   o un equipo de QA?

---

### Ejercicio 20.5.4 — Disaster recovery: el peor escenario

**Tipo: Diseñar**

```
Escenario: se borran accidentalmente los datos de staging en Delta Lake.
Un ingeniero ejecutó `rm -rf s3://data-lake/staging/` por error.

¿Cómo te recuperas?

  Opción 1: S3 versioning
    Si S3 versioning está habilitado, los objetos borrados
    pueden recuperarse restaurando la versión anterior.
    → RPO (Recovery Point Objective): 0 (ningún dato perdido)
    → RTO (Recovery Time Objective): minutos a horas

  Opción 2: Delta Lake time travel
    Si el transaction log de Delta Lake no se borró,
    puedes restaurar a un estado anterior.
    → Pero si se borró el directorio completo, no hay log.

  Opción 3: Re-procesar desde raw
    El raw layer es inmutable y no fue afectado.
    Re-ejecutar el pipeline de staging desde raw.
    → RPO: 0 (raw tiene todo)
    → RTO: horas (re-procesar todo el histórico)

  Opción 4: Backup cross-region
    Si hay una copia en otra región de AWS,
    copiar de vuelta.
    → RPO: depende de la frecuencia del backup
    → RTO: horas (transferencia entre regiones)

  Prevención:
  1. S3 versioning habilitado
  2. IAM policies que restrinjan delete en buckets de producción
  3. MFA delete para buckets críticos
  4. Backup cross-region para raw (el dato original es irremplazable)
  5. Alertas de CloudTrail para operaciones destructivas en S3
```

**Preguntas:**

1. ¿S3 versioning es suficiente como disaster recovery?

2. ¿IAM policies que prohíban `s3:DeleteObject` en producción
   son prácticas?

3. ¿Cuánto cuesta re-procesar 1 año de staging desde raw?

4. ¿Un backup cross-region de 10 TB es costoso?

5. ¿El raw layer nunca debe borrarse?

---

### Ejercicio 20.5.5 — Implementar: runbook del sistema completo

**Tipo: Implementar**

```markdown
# Crear el runbook maestro del sistema de e-commerce:
#
# 1. Arquitectura: diagrama del sistema (copiar de la sección 20.1)
# 2. Contactos: owner de cada pipeline, escalamiento
# 3. SLOs: targets y cómo medirlos
# 4. Incidentes comunes:
#    a. Pipeline batch falla → diagnóstico + resolución
#    b. Consumer lag creciente → diagnóstico + resolución
#    c. Revenue = $0 → diagnóstico + resolución
#    d. Schema change en upstream → diagnóstico + resolución
#    e. Spark OOM → diagnóstico + resolución
#    f. Flink checkpoint timeout → diagnóstico + resolución
# 5. Procedimientos:
#    a. Backfill de N días
#    b. Actualizar Flink job sin pérdida de state
#    c. Escalar Kafka particiones
#    d. Rotación de credenciales
#    e. Disaster recovery (borrado accidental)
```

**Restricciones:**
1. Cada incidente con pasos de diagnóstico numerados
2. Cada procedimiento con comandos exactos (no genéricos)
3. Tiempos estimados de resolución por tipo de incidente
4. Links a dashboards, logs, y documentación relevante

---

## Sección 20.6 — Modos de Fallo: Qué Se Rompe y Cómo Se Recupera

### Ejercicio 20.6.1 — Catálogo de fallos: los 10 más comunes

**Tipo: Leer**

```
Los 10 fallos más comunes en el sistema de e-commerce
(ordenados por frecuencia):

  #1. Fuente no disponible (PostgreSQL en mantenimiento)
      Frecuencia: 1-2×/mes
      Impacto: pipeline batch falla, datos de ayer no disponibles
      Recovery: esperar + re-ejecutar automático (sensor o manual trigger)

  #2. Schema change en upstream (columna renombrada)
      Frecuencia: 1×/mes
      Impacto: pipeline falla o produce datos incorrectos
      Recovery: actualizar mapping + backfill del día afectado

  #3. Data skew en Spark (una partición 100× más grande)
      Frecuencia: 2-3×/mes
      Impacto: pipeline lento, timeout, o OOM
      Recovery: broadcast join, salting, o repartition

  #4. Consumer lag en Kafka (Flink no puede seguir el ritmo)
      Frecuencia: 1-2×/mes
      Impacto: dashboard real-time muestra datos atrasados
      Recovery: escalar parallelism, optimizar operador lento

  #5. Credenciales expiradas (token de servicio)
      Frecuencia: cada 90 días si no hay rotación automática
      Impacto: pipeline falla, todos los reintentos fallan
      Recovery: rotar credenciales + re-ejecutar

  #6. Datos duplicados (at-least-once delivery)
      Frecuencia: raro pero impactante
      Impacto: métricas infladas (revenue 2×)
      Recovery: deduplicación con _source_hash + backfill

  #7. Network timeout (S3, BigQuery, Kafka)
      Frecuencia: 2-3×/mes
      Impacto: tarea falla, reintento normalmente exitoso
      Recovery: automático (reintentos del orquestador)

  #8. Out of disk (Delta Lake transaction log crece)
      Frecuencia: raro si VACUUM ejecuta regularmente
      Impacto: Spark no puede escribir, pipeline falla
      Recovery: VACUUM manual + aumentar disco

  #9. Dependency conflict (librería Python incompatible)
      Frecuencia: en cada deploy si no hay tests
      Impacto: DAG no parsea, scheduler se confunde
      Recovery: revertir deploy + fix de dependencias

  #10. Clock skew (event time vs processing time)
       Frecuencia: raro
       Impacto: ventanas de Flink calculan mal
       Recovery: NTP sync + verificar watermark config
```

**Preguntas:**

1. ¿Los fallos #1 y #7 (fuente no disponible, network timeout)
   se resuelven automáticamente con reintentos?

2. ¿El fallo #2 (schema change) es prevenible con contract tests?

3. ¿Cuál de los 10 fallos causa más daño al negocio?

4. ¿Un "chaos engineering" para data pipelines tiene sentido?

5. ¿Cuántos de estos 10 fallos has experimentado personalmente?

---

### Ejercicio 20.6.2 — Fallo cascada: cuándo un problema propaga

**Tipo: Diagnosticar**

```
Escenario de fallo cascada:

  09:00 — El equipo de backend deploya un cambio:
          la columna "region" ahora puede ser NULL
          (antes tenía NOT NULL constraint).

  03:00 (día siguiente) — Pipeline batch arranca.
         Extract lee datos con region=NULL (300 filas de 15000).
         Quality gate pasa (min_filas=1000 → OK con 15000).
         Transform: groupBy("region") agrupa los nulls como un grupo.
         Métricas: una región "null" aparece en marts.metricas_diarias.

  06:00 — Dashboard de Looker muestra una región desconocida.
          Pero nadie lo nota todavía.

  09:30 — El modelo de ML entrena con user_features.
          region_preferida = NULL para 2% de los usuarios.
          El modelo interpreta NULL como una categoría válida.

  10:00 — El API de recomendaciones sirve recomendaciones
          basadas en el modelo entrenado con datos incorrectos.
          2% de los usuarios reciben recomendaciones aleatorias.

  14:00 — El equipo de producto nota que la conversión bajó.
          Escala a data engineering.

  14:30 — Data engineering investiga. Encuentra los NULLs.
          Han pasado 35 horas desde el deploy que causó el problema.
          5 sistemas afectados: extract, staging, marts, ML, API.

  Costo del incidente:
  - 8 horas de investigación (3 ingenieros)
  - Backfill de 2 días de datos
  - Re-entrenamiento del modelo
  - Recomendaciones incorrectas durante 1 día
  - Confianza del equipo de producto erosionada
```

**Preguntas:**

1. ¿En qué punto del cascade debería haberse detectado el problema?

2. ¿Un NOT NULL check en el quality gate habría prevenido todo?

3. ¿Contract testing entre backend y data engineering
   habría detectado el cambio antes del deploy?

4. ¿El modelo de ML debería tener su propio quality gate
   para features de input?

5. ¿35 horas de "tiempo hasta detección" es aceptable?
   ¿Cuál debería ser el target?

---

### Ejercicio 20.6.3 — Implementar: circuit breakers para el sistema

**Tipo: Implementar**

```python
# Circuit breaker: detener el pipeline automáticamente
# si las condiciones no son seguras para procesar.

class CircuitBreaker:
    """Verificar condiciones antes de ejecutar el pipeline."""

    def __init__(self, checks: list):
        self.checks = checks

    def verify(self, context: dict) -> dict:
        results = {}
        for check in self.checks:
            try:
                passed = check.verify(context)
                results[check.name] = {"passed": passed, "error": None}
            except Exception as e:
                results[check.name] = {"passed": False, "error": str(e)}

        all_passed = all(r["passed"] for r in results.values())
        if not all_passed:
            failed = {k: v for k, v in results.items() if not v["passed"]}
            raise CircuitBreakerOpen(f"Checks failed: {failed}")
        return results


# Checks del circuit breaker:
class SourceAvailableCheck:
    name = "source_available"
    def verify(self, ctx):
        return ping_postgres(ctx["postgres_conn"])

class MinRowsCheck:
    name = "min_rows_yesterday"
    def verify(self, ctx):
        count = count_rows(ctx["tabla"], ctx["fecha"])
        return count >= ctx.get("min_rows", 1000)

class SchemaUnchangedCheck:
    name = "schema_unchanged"
    def verify(self, ctx):
        current = get_schema_hash(ctx["tabla"])
        expected = ctx.get("expected_schema_hash")
        return current == expected


# Uso en el pipeline:
breaker = CircuitBreaker([
    SourceAvailableCheck(),
    MinRowsCheck(),
    SchemaUnchangedCheck(),
])

@task()
def pre_flight_check(**context):
    breaker.verify({
        "postgres_conn": POSTGRES_CONN,
        "tabla": "ventas",
        "fecha": context["data_interval_start"].strftime("%Y-%m-%d"),
        "min_rows": 1000,
        "expected_schema_hash": "abc123...",
    })
```

**Preguntas:**

1. ¿El circuit breaker debe fallar el pipeline o saltarse el día?

2. ¿`SchemaUnchangedCheck` requiere mantener un hash de referencia.
   ¿Dónde se almacena? ¿Cómo se actualiza?

3. ¿El circuit breaker es una tarea de Airflow o un wrapper del pipeline?

4. ¿Cuántos checks son suficientes? ¿Demasiados checks bloquean todo?

5. ¿El circuit breaker puede tener su propio dashboard?

---

### Ejercicio 20.6.4 — Graceful degradation: el sistema que funciona parcialmente

```
Principio: es mejor tener datos parciales que no tener datos.

Escenario: el extract de clientes falla (API down).
  Opción A: fallar todo el pipeline.
            → Métricas no disponibles. Dashboard vacío.
  Opción B: continuar con datos de clientes de ayer.
            → Métricas disponibles. Segmento puede estar desactualizado.
            → Alertar que se usaron datos stale.

Implementar graceful degradation en el pipeline:

  @task()
  def extraer_clientes_con_fallback(**context):
      try:
          return extraer_clientes_frescos()
      except Exception as e:
          log.warning("clientes_fallback", error=str(e),
                      accion="usando datos de ayer")
          enviar_alerta(f"⚠️ Usando clientes de ayer: {e}")
          return cargar_clientes_de_ayer()

Reglas de graceful degradation:
  1. Las DIMENSIONES pueden usar datos stale (24h max)
  2. Los HECHOS (ventas) NO pueden usar datos stale (deben ser del día)
  3. Si los hechos fallan → fallar el pipeline (datos incorrectos)
  4. Si una dimensión falla → usar la versión anterior + alertar
  5. El dashboard debe indicar "datos parciales" si aplica
```

**Preguntas:**

1. ¿Datos stale son peores que no tener datos?

2. ¿24 horas de stale para dimensiones es aceptable?
   ¿Y para hechos?

3. ¿El dashboard debe mostrar un banner "datos parciales"?

4. ¿Graceful degradation complica el testing?

5. ¿El analista sabe que los datos de hoy usan clientes de ayer?

---

### Ejercicio 20.6.5 — Post-incidente: el proceso de mejora continua

**Tipo: Implementar**

```markdown
# Template de postmortem para incidentes de datos:

## Incidente: [título descriptivo]
## Fecha: [fecha]
## Severidad: P1 / P2 / P3
## Duración: [tiempo desde detección hasta resolución]

### Timeline
- HH:MM — [qué pasó]
- HH:MM — [quién hizo qué]

### Root Cause
[Descripción técnica de la causa raíz]

### Impacto
- Datos afectados: [qué tablas, qué fechas]
- Usuarios afectados: [quién vio datos incorrectos]
- Duración del impacto: [cuánto tiempo los datos estuvieron mal]
- Business impact: [decisiones tomadas con datos incorrectos]

### Detection
- ¿Cómo se detectó? [alerta automática / reporte manual]
- ¿Cuánto tardó en detectarse? [MTTD]
- ¿La detección fue adecuada? [sí/no, por qué]

### Resolution
- ¿Qué se hizo para resolverlo? [pasos]
- ¿Cuánto tardó? [MTTR]
- ¿Se hizo backfill? [sí/no, qué fechas]

### Action Items
| # | Acción | Owner | Prioridad | Fecha límite |
|---|--------|-------|-----------|--------------|
| 1 | [acción preventiva] | [persona] | P1 | [fecha] |
| 2 | [mejora de detección] | [persona] | P2 | [fecha] |

### Lessons Learned
[Qué aprendimos que aplica más allá de este incidente]
```

**Restricciones:**
1. Escribir un postmortem completo para el fallo cascada del ejercicio 20.6.2
2. Definir al menos 5 action items concretos
3. Calcular MTTD y MTTR
4. ¿Cuáles de los action items previenen este tipo de incidente por completo?

---

## Sección 20.7 — Evolución del Sistema: del MVP al Sistema Maduro

### Ejercicio 20.7.1 — Leer: el roadmap de madurez de data engineering

**Tipo: Leer**

```
Fase 1: MVP (mes 1-3)
  ─────────────────────
  Stack: cron + Python scripts + PostgreSQL → CSV → BigQuery
  Orquestación: cron (sin orquestador real)
  Testing: manual ("lo ejecuto y verifico")
  Observabilidad: print() + email si falla
  Equipo: 1 data engineer
  Costo: ~$200/mes

  Lo que funciona: los datos llegan al dashboard.
  Lo que falla: nadie sabe cuándo fallan los datos.

Fase 2: Primer orquestador (mes 3-6)
  ──────────────────────────────────
  Stack: Airflow + Python + Spark + S3 + BigQuery
  Orquestación: Airflow (managed)
  Testing: DAG validation + unit tests básicos
  Observabilidad: Airflow UI + Slack alerts
  Equipo: 2 data engineers
  Costo: ~$1,500/mes

  Lo que funciona: pipelines programados, reintentos, alertas.
  Lo que falla: datos incorrectos sin detectar.

Fase 3: Calidad y confianza (mes 6-12)
  ─────────────────────────────────────
  Stack: + dbt + Delta Lake + Great Expectations
  Orquestación: Airflow + Dagster (para dbt)
  Testing: unit + component + contracts + CI/CD
  Observabilidad: Grafana + structlog + quality checks
  Equipo: 3-4 data engineers
  Costo: ~$3,000/mes

  Lo que funciona: datos validados, lineage, confianza.
  Lo que falla: streaming no existe todavía.

Fase 4: Real-time y ML (mes 12-18)
  ─────────────────────────────────
  Stack: + Kafka + Flink + MLflow + feature store
  Orquestación: Airflow (batch) + K8s (streaming)
  Testing: + streaming tests + E2E staging
  Observabilidad: + Prometheus + PagerDuty + runbooks
  Equipo: 5+ data engineers
  Costo: ~$5,000/mes

  Lo que funciona: el sistema completo de este capítulo.
  Lo que falla: complejidad alta, necesitas especialización.

Fase 5: Platform engineering (mes 18+)
  ─────────────────────────────────────
  Stack: self-service para analistas y data scientists
  Orquestación: platform team mantiene la infra
  Testing: automated test generation, chaos engineering
  Observabilidad: data SLOs, error budgets, automated remediation
  Equipo: platform team + domain teams
  Costo: ~$10,000+/mes

  Lo que funciona: los data engineers no operan — construyen.
  Lo que falla: nada si la inversión en platform es correcta.
```

**Preguntas:**

1. ¿Puedes saltar fases (ir directo de MVP a Fase 4)?

2. ¿La Fase 2 es donde la mayoría de los equipos se quedan?

3. ¿La Fase 5 (platform engineering) es realista para empresas medianas?

4. ¿El costo de $5,000/mes para la Fase 4 es bajo
   comparado con el valor que genera?

5. ¿Cuánto tiempo toma pasar de Fase 1 a Fase 4?

---

### Ejercicio 20.7.2 — Qué construirías diferente si empezaras hoy

**Tipo: Analizar**

```
Decisiones que tomaríamos diferente en 2025:

  1. Polars en vez de Spark para daily batch
     El volumen diario (500 MB) no justifica un cluster Spark.
     Polars en un nodo es 10× más rápido de desarrollar.
     Spark solo para backfill y ML.

  2. Dagster en vez de Airflow para proyectos nuevos
     El modelo de assets es superior para data engineering.
     Airflow sigue siendo válido si ya lo tienes.

  3. DuckDB para análisis ad-hoc en vez de BigQuery
     Para queries de exploración, DuckDB sobre Parquet en S3
     es gratuito y más rápido para datasets < 100 GB.

  4. Iceberg en vez de Delta Lake
     El ecosistema open-source de Iceberg creció significativamente.
     Mejor portabilidad entre motores.

  5. OpenLineage desde el día 1
     El lineage es más valioso cuanto antes lo implementas.
     Retrofitting lineage en un sistema existente es doloroso.

  6. Contract tests antes de más quality checks
     Prevenir es más barato que detectar.
     Un schema contract previene el 50% de los incidentes.

Lo que NO cambiaríamos:
  - Kafka para streaming (el estándar sigue firme)
  - Flink para stream processing complejo (sin rival real)
  - dbt para transformaciones SQL (la herramienta perfecta para esto)
  - S3 como data lake (barato, duradero, universal)
```

**Preguntas:**

1. ¿Las 6 decisiones alternativas son mejoras claras
   o solo preferencias?

2. ¿"Polars para daily, Spark para backfill" añade complejidad
   de dos motores?

3. ¿Iceberg vs Delta Lake — ¿hay un ganador claro en 2025?

4. ¿OpenLineage desde el día 1 es realista cuando estás en la Fase 1?

5. ¿Qué tecnología nueva (no mencionada) podría cambiar
   estas decisiones en 2026?

---

### Ejercicio 20.7.3 — El sistema del futuro: tendencias en data engineering

**Tipo: Analizar**

```
Tendencias que están cambiando data engineering en 2024-2025:

  1. Lakehouse universal
     Delta Lake, Iceberg, Hudi convergen.
     Un solo formato abierto + múltiples motores (Spark, Polars,
     DuckDB, DataFusion) leyendo del mismo storage.
     → El data lake se convierte en el data warehouse.

  2. Motores embebidos (DuckDB, DataFusion, Polars)
     Para datasets que caben en un nodo grande (< 1 TB),
     no necesitas un cluster distribuido.
     → La mayoría de las cargas de trabajo son < 1 TB.

  3. Orquestación basada en assets (Dagster model)
     De "ejecutar tareas en orden" a "declarar qué datos deben existir".
     Airflow está adoptando este modelo (Datasets, AIP-72).

  4. Data contracts como estándar
     Definir y versionar los schemas entre equipos.
     Prevenir en vez de detectar.

  5. AI-assisted data engineering
     LLMs para generar transformaciones SQL, debug de pipelines,
     anomaly detection automática, y documentación.
     → No reemplaza al data engineer — lo potencia.

  6. Streaming como default
     En vez de batch primero + streaming después,
     empezar con streaming y derivar batch como snapshot.
     → Kappa Architecture gana tracción.
```

**Preguntas:**

1. ¿El lakehouse universal es realidad o marketing?

2. ¿DuckDB puede reemplazar a Spark para el 80% de las cargas?

3. ¿AI-assisted data engineering elimina la necesidad de aprender SQL?

4. ¿Streaming como default es práctico o prematuro?

5. ¿Cuáles de estas tendencias estarán consolidadas en 2027?

---

### Ejercicio 20.7.4 — Tu roadmap personal como data engineer

**Tipo: Analizar**

```
Después de completar este libro, tu mapa de conocimiento es:

  ✅ Fundamentos: formatos, MapReduce, el modelo mental
  ✅ Batch: Spark, Polars, DataFusion, lakehouse
  ✅ Streaming: Kafka, Beam, Spark Streaming, Flink
  ✅ Lenguajes: Python, Scala, Java, Rust en contexto de datos
  ✅ Operación: orquestación, observabilidad, testing

  ¿Qué sigue?

  Profundizar en especialización:
    - ML Engineering: MLflow, feature stores, model serving
    - Analytics Engineering: dbt avanzado, semantic layer, metrics store
    - Platform Engineering: Kubernetes, Terraform, internal tools
    - Data Governance: lineage, cataloging, privacy, compliance

  Profundizar en fundamentos:
    - Sistemas distribuidos: Designing Data-Intensive Applications (Kleppmann)
    - Bases de datos: Database Internals (Petrov)
    - Streaming: Streaming Systems (Akidau, Chernyak, Lax)
    - Infraestructura: Site Reliability Engineering (Google)
```

**Preguntas:**

1. ¿Cuál especialización tiene más demanda en el mercado actual?

2. ¿Designing Data-Intensive Applications sigue siendo
   el libro más importante para data engineers?

3. ¿Un data engineer necesita saber Kubernetes?

4. ¿La especialización es necesaria o es mejor ser generalista?

5. ¿Cuál es el siguiente proyecto que construirías
   para aplicar lo que aprendiste?

---

### Ejercicio 20.7.5 — Implementar: el checklist de producción definitivo

**Tipo: Implementar**

```markdown
# Checklist: ¿está tu pipeline listo para producción?

## Funcionalidad
- [ ] El pipeline produce resultados correctos con datos de test
- [ ] El pipeline maneja edge cases (nulls, vacíos, duplicados)
- [ ] El pipeline es idempotente (safe to re-run)
- [ ] El pipeline maneja schema evolution gracefully

## Testing
- [ ] Unit tests para lógica de transformación
- [ ] Component tests con Spark/Polars local
- [ ] Contract tests para input/output schemas
- [ ] DAG validation tests
- [ ] Integration tests (al menos contra PostgreSQL/Kafka)
- [ ] CI/CD pipeline que ejecuta todos los tests

## Orquestación
- [ ] DAG definido con schedule, retries, y timeouts
- [ ] Owner y tags asignados
- [ ] SLA definido y verificable
- [ ] Backfill probado para al menos 7 días
- [ ] max_active_runs = 1 (o justificación para > 1)

## Observabilidad
- [ ] Logging estructurado (structlog, JSON)
- [ ] Métricas custom (filas, bytes, duración, drop_rate)
- [ ] Quality checks (freshness, volume, schema, distribution)
- [ ] Dashboard operativo en Grafana
- [ ] Alertas P1/P2/P3 configuradas con canales y owners

## Resiliencia
- [ ] Circuit breaker para fuentes externas
- [ ] Graceful degradation para dimensiones
- [ ] Reintentos con exponential backoff
- [ ] Manejo explícito de credenciales expiradas
- [ ] Runbook documentado para fallos comunes

## Documentación
- [ ] README con arquitectura y dependencias
- [ ] Data model documentado (raw, staging, marts)
- [ ] Lineage visible (OpenLineage, dbt docs, o manual)
- [ ] Postmortem template disponible
- [ ] Onboarding doc para nuevo miembro del equipo
```

**Restricciones:**
1. Evaluar el sistema de e-commerce del libro contra este checklist
2. Identificar los ítems que faltan o son débiles
3. Priorizar los 5 más importantes para implementar primero
4. ¿El checklist es demasiado exigente para un equipo de 3 personas?

---

## Resumen del capítulo (y del libro)

**El sistema completo: lo que construiste**

```
A lo largo de 20 capítulos, construiste incrementalmente
un sistema de analytics de e-commerce con:

  Parte 1 (Cap.01-03): El modelo mental
    Formatos columnar, Arrow, Parquet, MapReduce.
    La base que todos los frameworks comparten.

  Parte 2 (Cap.04-08): Batch processing
    Spark, Polars, DataFusion, Delta Lake.
    Procesar datos que ya existen.

  Parte 3 (Cap.09-12): Stream processing
    Kafka, Beam, Spark Streaming, Flink.
    Procesar datos que están llegando.

  Parte 4 (Cap.13-16): Lenguajes
    Python, Scala, Java, Rust.
    El mismo problema, distintas herramientas.

  Parte 5 (Cap.17-20): En producción
    Orquestación, observabilidad, testing, el sistema completo.
    Operar lo que construiste.
```

**El principio que une todo el libro:**

```
Data engineering no es elegir el framework correcto.
Es entender el tradeoff central:

  Latencia  ←————————————————————→  Throughput

Y construir un sistema que:
  1. Procesa los datos correctos
  2. En el tiempo correcto
  3. Con la confianza suficiente
  4. A un costo razonable
  5. Y que puedas operar a las 3am sin pánico

Los frameworks cambian cada 3 años.
Los principios de este libro duran 30.

  "No importa si usas Spark o Polars,
   Airflow o Dagster, Kafka o Pub/Sub.
   Importa que entiendas por qué existen,
   qué problema resuelven,
   y cuándo usar cada uno."
```

**Lo que sigue:**

```
Este repositorio se construye incrementalmente.
Cada capítulo referencia los anteriores.

Si llegaste hasta aquí, tienes el modelo mental
para construir y operar sistemas de datos a escala.

El siguiente paso no es leer otro libro.
Es construir algo.

  → Toma un dataset público (NYC taxi, GitHub archive, Wikipedia dumps).
  → Construye un pipeline batch + streaming.
  → Ponlo en producción (aunque sea en tu laptop con Docker).
  → Rómpelo. Arréglalo. Documenta qué aprendiste.

Eso es data engineering.
```
