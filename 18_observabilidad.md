# Guía de Ejercicios — Cap.18: Observabilidad de Pipelines de Datos

> Un pipeline que ejecuta exitosamente y produce datos incorrectos
> es más peligroso que uno que falla — porque nadie se entera.
>
> La orquestación (Cap.17) responde "¿el pipeline ejecutó?".
> La observabilidad responde "¿el pipeline produjo datos correctos,
> a tiempo, y en el volumen esperado?".
>
> En sistemas distribuidos, observabilidad significa tres cosas:
> métricas, logs, y traces. En data engineering, hay una cuarta:
> la calidad de los datos mismos.
>
> No puedes operar lo que no puedes ver.
> Y en data engineering, "ver" no es solo ver si el job corrió —
> es ver si la tabla de revenue tiene 0 filas,
> si el campo `monto` tiene valores negativos que no deberían existir,
> si los datos de ayer llegaron 3 horas tarde,
> y si el pipeline que tardaba 20 minutos ahora tarda 2 horas.

---

## El modelo mental: los cuatro pilares de la observabilidad de datos

```
Observabilidad clásica (SRE/DevOps):          Observabilidad de datos:

  1. Métricas (Prometheus/Datadog)               1. Métricas de pipeline
     CPU, memoria, latencia, throughput              duración, filas procesadas,
                                                     bytes leídos/escritos

  2. Logs (ELK/CloudWatch)                       2. Logs de ejecución
     errores, warnings, stack traces                 errores de parsing, schemas
                                                     inválidos, registros descartados

  3. Traces (Jaeger/Zipkin)                      3. Data lineage
     request → servicio A → servicio B               tabla X → transformación → tabla Y
     latencia por hop                                ¿de dónde vienen estos datos?

  4. (no existe equivalente clásico)             4. Data quality
                                                     freshness: ¿los datos son recientes?
                                                     volume: ¿hay la cantidad esperada?
                                                     schema: ¿las columnas son correctas?
                                                     distribution: ¿los valores son normales?

El cuarto pilar es lo que hace la observabilidad de datos DIFERENTE
de la observabilidad de software. Un microservicio que retorna 200 OK
con datos incorrectos es invisible para Prometheus.
Un pipeline que completa en verde pero escribe 0 filas
es invisible para Airflow.
```

```
¿Quién necesita ver qué?

  Data Engineer:
    "¿El pipeline ejecutó? ¿Cuánto tardó? ¿Hubo errores?"
    → Airflow UI, logs, métricas de ejecución

  Analytics Engineer:
    "¿Los datos del modelo de dbt son frescos y correctos?"
    → Data quality checks, freshness monitors, schema tests

  Data Analyst / Stakeholder:
    "¿Puedo confiar en el dashboard de hoy?"
    → Data freshness indicator, quality score, SLA status

  On-call Engineer:
    "¿Qué se rompió, por qué, y cómo lo arreglo?"
    → Alertas, logs, lineage (¿qué upstream falló?)

  Cada persona necesita una vista distinta de la misma realidad.
  La observabilidad es el sistema que une todas esas vistas.
```

---

## Tabla de contenidos

- [Sección 18.1 — Métricas de pipelines: qué medir y por qué](#sección-181--métricas-de-pipelines-qué-medir-y-por-qué)
- [Sección 18.2 — Logging estructurado para data engineering](#sección-182--logging-estructurado-para-data-engineering)
- [Sección 18.3 — Data lineage: de dónde vienen los datos](#sección-183--data-lineage-de-dónde-vienen-los-datos)
- [Sección 18.4 — Data quality: el pilar que falta](#sección-184--data-quality-el-pilar-que-falta)
- [Sección 18.5 — Dashboards operativos y alertas](#sección-185--dashboards-operativos-y-alertas)
- [Sección 18.6 — Observabilidad de Spark, Flink, y Kafka](#sección-186--observabilidad-de-spark-flink-y-kafka)
- [Sección 18.7 — El e-commerce observable: integrando los cuatro pilares](#sección-187--el-e-commerce-observable-integrando-los-cuatro-pilares)

---

## Sección 18.1 — Métricas de Pipelines: Qué Medir y Por Qué

### Ejercicio 18.1.1 — Leer: las métricas que importan

**Tipo: Leer**

```
Las métricas de un pipeline de datos se dividen en tres categorías:

  1. MÉTRICAS DE EJECUCIÓN (¿el pipeline corrió bien?)
     ─────────────────────────────────────────────────
     pipeline_duration_seconds        cuánto tardó el pipeline completo
     task_duration_seconds            cuánto tardó cada tarea
     task_retries_total               cuántos reintentos hubo
     task_failures_total              cuántas tareas fallaron
     pipeline_success_rate            % de runs exitosos (últimos 30 días)
     scheduler_lag_seconds            tiempo entre "scheduled" y "running"

  2. MÉTRICAS DE DATOS (¿los datos son correctos?)
     ─────────────────────────────────────────────────
     rows_processed_total             filas leídas/escritas
     rows_dropped_total               filas descartadas (parsing, quality)
     bytes_read_total                 volumen de datos leídos
     bytes_written_total              volumen de datos escritos
     null_rate_per_column             % de nulls por columna
     schema_changes_detected          cambios de schema detectados

  3. MÉTRICAS DE NEGOCIO (¿el resultado tiene sentido?)
     ─────────────────────────────────────────────────
     daily_revenue_total              revenue calculado hoy
     unique_users_count               usuarios únicos procesados
     average_order_value              ticket promedio
     revenue_vs_yesterday_pct         cambio respecto al día anterior

  La trampa: la mayoría de los equipos solo miden la categoría 1.
  Saben que el pipeline corrió, pero no saben si los datos son correctos.
  Las categorías 2 y 3 son las que previenen el incidente del "$0 en el dashboard".
```

**Preguntas:**

1. ¿Las métricas de ejecución son responsabilidad del orquestador
   o del código del pipeline?

2. ¿Las métricas de datos deben almacenarse en Prometheus/Datadog
   o en una tabla del data warehouse?

3. ¿`rows_dropped_total` debería generar una alerta?
   ¿A partir de qué porcentaje?

4. ¿`revenue_vs_yesterday_pct` es una métrica de observabilidad
   o una métrica de negocio? ¿Hay diferencia?

5. ¿Qué métrica agregarías que no está en la lista?

---

### Ejercicio 18.1.2 — Instrumentar un pipeline con métricas custom

```python
# Emitir métricas desde un pipeline de datos usando structlog + StatsD.

import structlog
import time
from dataclasses import dataclass, field
from typing import Optional

@dataclass
class PipelineMetrics:
    """Acumulador de métricas para un run del pipeline."""
    pipeline_name: str
    run_date: str
    start_time: float = field(default_factory=time.time)
    rows_read: int = 0
    rows_written: int = 0
    rows_dropped: int = 0
    bytes_read: int = 0
    bytes_written: int = 0
    errors: list = field(default_factory=list)
    step_durations: dict = field(default_factory=dict)

    def record_step(self, step_name: str, duration: float, rows: int = 0):
        self.step_durations[step_name] = duration
        self.rows_read += rows

    def record_drop(self, count: int, reason: str):
        self.rows_dropped += count
        self.errors.append({"type": "drop", "count": count, "reason": reason})

    @property
    def duration_total(self) -> float:
        return time.time() - self.start_time

    @property
    def drop_rate(self) -> float:
        total = self.rows_read + self.rows_dropped
        return self.rows_dropped / total if total > 0 else 0.0

    def to_dict(self) -> dict:
        return {
            "pipeline": self.pipeline_name,
            "run_date": self.run_date,
            "duration_seconds": round(self.duration_total, 2),
            "rows_read": self.rows_read,
            "rows_written": self.rows_written,
            "rows_dropped": self.rows_dropped,
            "drop_rate": round(self.drop_rate, 4),
            "bytes_read": self.bytes_read,
            "bytes_written": self.bytes_written,
            "steps": self.step_durations,
            "errors": self.errors,
        }


# Uso en un pipeline:
log = structlog.get_logger()

def ejecutar_pipeline(fecha: str):
    metrics = PipelineMetrics(pipeline_name="ecommerce_daily", run_date=fecha)

    # Paso 1: Extraer
    t0 = time.time()
    df = extraer_ventas(fecha)
    metrics.record_step("extraer", time.time() - t0, rows=len(df))

    # Paso 2: Validar
    t0 = time.time()
    df_valid, df_invalid = validar(df)
    metrics.record_step("validar", time.time() - t0)
    if len(df_invalid) > 0:
        metrics.record_drop(len(df_invalid), "schema_validation_failed")

    # Paso 3: Transformar
    t0 = time.time()
    resultado = transformar(df_valid)
    metrics.record_step("transformar", time.time() - t0)
    metrics.rows_written = len(resultado)

    # Emitir métricas:
    log.info("pipeline_completed", **metrics.to_dict())

    # Enviar a StatsD/Prometheus:
    statsd.gauge("pipeline.duration", metrics.duration_total,
                 tags=[f"pipeline:{metrics.pipeline_name}"])
    statsd.gauge("pipeline.rows_written", metrics.rows_written,
                 tags=[f"pipeline:{metrics.pipeline_name}"])
    statsd.gauge("pipeline.drop_rate", metrics.drop_rate,
                 tags=[f"pipeline:{metrics.pipeline_name}"])

    return metrics
```

**Preguntas:**

1. ¿StatsD vs Prometheus push gateway — cuál para métricas de pipeline?

2. ¿Las métricas deben emitirse durante el pipeline o al final?

3. ¿`drop_rate` de 0.1% es aceptable? ¿Y 5%? ¿Depende del pipeline?

4. ¿Guardar las métricas en una tabla de BigQuery además de Prometheus
   tiene valor? ¿Para qué?

5. ¿Cómo instrumentas un pipeline de Spark distribuido
   donde las métricas están en los executors?

---

### Ejercicio 18.1.3 — Métricas de Airflow: qué expone el scheduler

```python
# Airflow expone métricas vía StatsD de forma nativa.
# Configurar en airflow.cfg:
# [metrics]
# statsd_on = True
# statsd_host = statsd-server
# statsd_port = 8125
# statsd_prefix = airflow

# Métricas clave que Airflow emite automáticamente:
# ─────────────────────────────────────────────────────
# airflow.dag.{dag_id}.duration              duración del DagRun
# airflow.dag_processing.total_parse_time    tiempo de parseo de DAGs
# airflow.scheduler.tasks.running            tareas en ejecución
# airflow.scheduler.tasks.starving           tareas esperando slot
# airflow.pool.open_slots.{pool}             slots disponibles en el pool
# airflow.executor.queued_tasks              tareas en cola
# airflow.executor.running_tasks             tareas ejecutando
# airflow.ti.finish.{dag_id}.{task_id}.{state}  estado final de la tarea

# Dashboard de Grafana para Airflow:
# Panel 1: Pipeline Duration (time series)
#   query: airflow.dag.ecommerce_daily.duration
#   alert: si > 2× el promedio de los últimos 7 días

# Panel 2: Tasks Starving (gauge)
#   query: airflow.scheduler.tasks.starving
#   alert: si > 10 tareas esperando por más de 5 minutos

# Panel 3: DAG Parse Time (time series)
#   query: airflow.dag_processing.total_parse_time
#   alert: si > 30 segundos (indica DAGs pesados)

# Panel 4: Pool Utilization (gauge)
#   query: airflow.pool.open_slots.default / total_slots
#   alert: si utilización > 90% por más de 10 minutos
```

**Preguntas:**

1. ¿`tasks.starving` es la métrica más importante para detectar
   cuellos de botella en Airflow?

2. ¿El parse time de DAGs afecta directamente la latencia de scheduling?

3. ¿Las métricas de Airflow incluyen el tiempo de ejecución
   de las tareas mismas, o solo el overhead del scheduler?

4. ¿Managed Airflow (MWAA, Composer) expone las mismas métricas StatsD?

5. ¿Dagster y Prefect tienen métricas equivalentes?

---

### Ejercicio 18.1.4 — Detectar anomalías en métricas de pipeline

```python
import numpy as np
from datetime import datetime, timedelta

# Detectar si la métrica de hoy es anómala respecto al histórico.
# No hardcodear umbrales — usar estadísticas del propio pipeline.

def detectar_anomalia(
    valor_actual: float,
    historico: list[float],
    metodo: str = "zscore",
    umbral: float = 3.0,
) -> dict:
    """
    Detectar si el valor actual es anómalo respecto al histórico.

    Métodos:
    - zscore: |valor - media| / std > umbral
    - iqr: valor < Q1 - 1.5*IQR o valor > Q3 + 1.5*IQR
    - pct_change: |cambio porcentual vs ayer| > umbral (en %)
    """
    historico = np.array(historico)

    if metodo == "zscore":
        media = np.mean(historico)
        std = np.std(historico)
        if std == 0:
            return {"anomalo": valor_actual != media, "metodo": "zscore",
                    "detalle": "std=0, comparación exacta"}
        zscore = abs(valor_actual - media) / std
        return {"anomalo": zscore > umbral, "metodo": "zscore",
                "zscore": round(zscore, 2), "media": round(media, 2)}

    elif metodo == "iqr":
        q1, q3 = np.percentile(historico, [25, 75])
        iqr = q3 - q1
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        return {"anomalo": valor_actual < lower or valor_actual > upper,
                "metodo": "iqr", "rango": [round(lower, 2), round(upper, 2)]}

    elif metodo == "pct_change":
        ayer = historico[-1]
        if ayer == 0:
            return {"anomalo": valor_actual != 0, "metodo": "pct_change"}
        cambio = abs(valor_actual - ayer) / ayer * 100
        return {"anomalo": cambio > umbral, "metodo": "pct_change",
                "cambio_pct": round(cambio, 2)}


# Ejemplo: verificar revenue diario
historico_30d = [12500, 13200, 11800, 14500, 12900, 13100, ...]  # últimos 30 días
resultado = detectar_anomalia(valor_actual=150, historico=historico_30d)
# {"anomalo": True, "zscore": 8.7, "media": 12850}
# Revenue de $150 cuando el promedio es $12,850 → claramente anómalo
```

**Preguntas:**

1. ¿Z-score es robusto para datos con estacionalidad
   (ej: revenue de fin de semana vs entre semana)?

2. ¿IQR es mejor que z-score para detectar anomalías en datos con outliers?

3. ¿Cuántos días de histórico son suficientes? ¿7? ¿30? ¿90?

4. ¿La detección de anomalías debe ejecutarse dentro del pipeline
   o en un sistema separado?

5. ¿Monte Carlo Simulation (Cap.18 de otro dominio) aplica
   para predecir valores esperados de métricas?

**Pista:** Para datos con estacionalidad (fin de semana vs laborable),
usa ventanas por día de la semana: compara el lunes de hoy con los últimos
4 lunes, no con los últimos 30 días. Para holidays, necesitas un calendario
de excepciones. Ningún método estadístico simple maneja esto bien —
en la práctica, la mayoría de los equipos usan z-score con ventanas
por día de semana + un diccionario de excepciones conocidas.

---

### Ejercicio 18.1.5 — Implementar: tabla de métricas del pipeline

**Tipo: Implementar**

```python
# Diseñar una tabla que almacene las métricas de cada run del pipeline.
# Esta tabla es consultable por SQL → dashboards, análisis de tendencias.

# Schema:
# pipeline_metrics (
#   run_id            STRING    -- UUID del run
#   pipeline_name     STRING    -- "ecommerce_daily"
#   run_date          DATE      -- fecha de datos procesados
#   started_at        TIMESTAMP -- cuándo inició
#   completed_at      TIMESTAMP -- cuándo terminó (NULL si falló)
#   status            STRING    -- "success", "failed", "running"
#   duration_seconds  FLOAT     -- duración total
#   rows_read         INT64     -- filas leídas
#   rows_written      INT64     -- filas escritas
#   rows_dropped      INT64     -- filas descartadas
#   drop_rate         FLOAT     -- rows_dropped / (rows_read + rows_dropped)
#   bytes_read        INT64     -- bytes leídos
#   bytes_written     INT64     -- bytes escritos
#   error_message     STRING    -- mensaje de error (NULL si éxito)
#   step_durations    JSON      -- {"extraer": 45.2, "transformar": 120.5}
#   metadata          JSON      -- cualquier contexto adicional
# )

# Queries útiles sobre esta tabla:
# 1. Tendencia de duración del pipeline (últimos 30 días)
# 2. Top 5 pipelines más lentos esta semana
# 3. Pipelines con drop_rate > 1%
# 4. Tasa de éxito por pipeline (últimos 7 días)
# 5. Alertar si duration > 2× promedio_historico
```

**Restricciones:**
1. Implementar la tabla y la lógica de inserción
2. Implementar las 5 queries listadas
3. Crear un check que detecte anomalías en duración y drop_rate
4. ¿BigQuery, PostgreSQL, o la metadata DB de Airflow? Justificar.

---

## Sección 18.2 — Logging Estructurado para Data Engineering

### Ejercicio 18.2.1 — Leer: por qué print() no es logging

**Tipo: Leer**

```
print() vs logging estructurado:

  print():
    print(f"Procesando ventas del 2024-03-14, encontré 15432 filas")
    # Output: Procesando ventas del 2024-03-14, encontré 15432 filas

    Problemas:
    - No tiene timestamp (¿cuándo ocurrió?)
    - No tiene nivel (¿es info, warning, error?)
    - No es parseable (¿cómo buscas "filas > 10000" en 1M de logs?)
    - No tiene contexto (¿qué pipeline? ¿qué run? ¿qué tarea?)

  structlog (JSON):
    log.info("ventas_procesadas", fecha="2024-03-14", filas=15432,
             pipeline="ecommerce_daily", run_id="abc-123")
    # Output:
    # {"event": "ventas_procesadas", "fecha": "2024-03-14",
    #  "filas": 15432, "pipeline": "ecommerce_daily",
    #  "run_id": "abc-123", "timestamp": "2024-03-15T03:12:45Z",
    #  "level": "info"}

    Ventajas:
    - Timestamp automático
    - Nivel explícito (info/warning/error)
    - Parseable (JSON → buscar con jq, CloudWatch Insights, BigQuery)
    - Contexto automático (pipeline, run_id, tarea)
    - Correlación (buscar TODOS los logs del run abc-123)

En data engineering, logs estructurados permiten:
  - "¿Cuántas filas procesó cada run de la última semana?"
    → SELECT filas FROM logs WHERE event = 'ventas_procesadas'
  - "¿Qué runs tuvieron errores de parsing?"
    → SELECT * FROM logs WHERE level = 'error' AND event LIKE '%parsing%'
  - "¿El pipeline de hoy procesó menos filas que ayer?"
    → Comparar filas entre dos runs
```

**Preguntas:**

1. ¿structlog vs logging estándar de Python — cuál para data engineering?

2. ¿Los logs de un pipeline de Spark van a stdout de los executors?
   ¿Cómo los centralizas?

3. ¿JSON logs son legibles para humanos?
   ¿Cómo balanceas legibilidad y parseabilidad?

4. ¿Cuánto log es demasiado? ¿Un log por fila procesada es viable?

5. ¿Los logs de Airflow y los logs del pipeline se mezclan?
   ¿Cómo los separas?

---

### Ejercicio 18.2.2 — Configurar structlog para pipelines de datos

```python
import structlog
import logging
import sys

def configurar_logging(pipeline_name: str, run_id: str):
    """Configurar structlog con contexto automático del pipeline."""

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
    )

    # Bind contexto global (aparece en TODOS los logs):
    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(
        pipeline=pipeline_name,
        run_id=run_id,
    )


# Uso:
configurar_logging("ecommerce_daily", "run-2024-03-14-abc123")
log = structlog.get_logger()

log.info("pipeline_started", fecha="2024-03-14")
# {"event": "pipeline_started", "fecha": "2024-03-14",
#  "pipeline": "ecommerce_daily", "run_id": "run-2024-03-14-abc123",
#  "timestamp": "2024-03-15T03:00:01Z", "level": "info"}

log.info("step_completed", step="extraer", rows=15432, duration_s=45.2)
# {"event": "step_completed", "step": "extraer", "rows": 15432,
#  "duration_s": 45.2, "pipeline": "ecommerce_daily", ...}

try:
    resultado = transformar(df)
except Exception as e:
    log.error("step_failed", step="transformar", error=str(e), exc_info=True)
    raise
```

**Preguntas:**

1. ¿`contextvars` de structlog funciona correctamente con multiprocessing?
   ¿Y con threads?

2. ¿`JSONRenderer` es para producción y `ConsoleRenderer` para desarrollo?

3. ¿Los logs deben ir a stdout, a un archivo, o a ambos?

4. ¿Cómo evitas que un log con `exc_info=True` exponga datos sensibles
   en los stack traces?

5. ¿structlog puede integrarse con el logging de Airflow?

---

### Ejercicio 18.2.3 — Log levels: cuándo usar cada uno

```python
import structlog
log = structlog.get_logger()

# DEBUG: información detallada para debugging (NO en producción por defecto)
log.debug("query_ejecutada", sql="SELECT COUNT(*) FROM ventas WHERE ...",
          parametros={"fecha": "2024-03-14"})

# INFO: eventos normales del pipeline (el "heartbeat" del sistema)
log.info("step_completed", step="extraer", rows=15432, duration_s=45.2)
log.info("quality_check_passed", check="min_rows", expected=100, actual=15432)

# WARNING: algo inesperado pero no fatal (el pipeline continúa)
log.warning("rows_dropped", count=23, reason="null_user_id",
            drop_rate=0.0015)
log.warning("slow_query", query="groupby_region", duration_s=180,
            expected_s=60)
log.warning("schema_change_detected", columna_nueva="discount_code",
            tipo="string")

# ERROR: algo falló (la tarea puede reintentar o fallar)
log.error("step_failed", step="transformar",
          error="OutOfMemoryError: Java heap space",
          rows_input=15432, heap_max_mb=4096)

# CRITICAL: el pipeline no puede continuar, intervención humana necesaria
log.critical("data_corruption_detected",
             tabla="metricas_diarias",
             detalle="revenue negativo en 500 registros",
             accion="pipeline detenido, se requiere investigación")
```

**Preguntas:**

1. ¿WARNING para `rows_dropped` o solo si supera un umbral?

2. ¿Un `schema_change_detected` es WARNING o ERROR?
   ¿Depende del contexto?

3. ¿Cuántos logs INFO por minuto son aceptables
   antes de que la factura de CloudWatch sea un problema?

4. ¿ERROR debe siempre acompañarse de un stack trace?

5. ¿Cómo defines la política de log levels para un equipo de 5 personas?

---

### Ejercicio 18.2.4 — Centralizar logs: ELK, CloudWatch, y alternativas

```python
# Patrón: enviar logs a un sistema centralizado para búsqueda y alertas.

# Opción 1: CloudWatch (AWS)
# Los logs de Airflow en MWAA van automáticamente a CloudWatch.
# Los logs del pipeline necesitan un handler:
import watchtower
import logging

cloudwatch_handler = watchtower.CloudWatchLogHandler(
    log_group="/data-engineering/pipelines",
    stream_name="ecommerce-daily-{date}",
)
logging.getLogger().addHandler(cloudwatch_handler)

# Buscar en CloudWatch Insights:
# fields @timestamp, @message
# | filter pipeline = "ecommerce_daily"
# | filter level = "error"
# | sort @timestamp desc
# | limit 50

# Opción 2: ELK Stack (Elasticsearch + Logstash + Kibana)
# Los logs JSON de structlog se envían a Logstash via Filebeat.
# Kibana permite dashboards y alertas sobre los logs.

# Opción 3: Datadog / Grafana Loki
# Datadog: integración directa con structlog via ddtrace.
# Loki: alternativa ligera a Elasticsearch, integrada con Grafana.

# Opción 4: BigQuery como log sink
# Para análisis SQL sobre logs históricos:
# Los logs JSON se escriben a un topic de Pub/Sub → BigQuery.
# Ventaja: consultas SQL sobre meses de logs.
# Desventaja: latencia de minutos (no real-time).
```

**Preguntas:**

1. ¿CloudWatch Insights vs Elasticsearch — cuál para buscar logs
   de pipelines de datos?

2. ¿Grafana Loki es suficiente para un equipo de 10 data engineers?

3. ¿Logs en BigQuery tiene sentido para auditoría y compliance?

4. ¿El costo de almacenar logs es significativo?
   ¿Cuál es la política de retención razonable?

5. ¿Los logs de Spark executors deben ir al mismo sistema
   que los logs de Airflow?

---

### Ejercicio 18.2.5 — Implementar: correlacionar logs entre componentes

**Tipo: Implementar**

```python
# Problema: un pipeline involucra Airflow, Spark, y BigQuery.
# Cada componente genera sus propios logs.
# ¿Cómo correlacionas los logs de una ejecución específica?

# Solución: propagar un trace_id a través de todos los componentes.

# Airflow task:
def extraer_ventas(**context):
    run_id = context["run_id"]
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id

    # Generar trace_id único para esta ejecución:
    trace_id = f"{dag_id}-{run_id}-{task_id}"

    # Pasar a Spark:
    spark = SparkSession.builder \
        .config("spark.app.name", f"extraer-{trace_id}") \
        .getOrCreate()
    spark.sparkContext.setLocalProperty("trace_id", trace_id)

    # Pasar a BigQuery:
    job_config = bigquery.QueryJobConfig(
        labels={"trace_id": trace_id}
    )
```

**Restricciones:**
1. Implementar propagación de trace_id entre Airflow → Spark → BigQuery
2. Implementar una query que busque todos los logs de un trace_id
3. ¿OpenTelemetry es una alternativa? ¿Está maduro para data engineering?
4. ¿El trace_id debe incluirse en los datos mismos (como una columna)?

---

## Sección 18.3 — Data Lineage: de Dónde Vienen los Datos

### Ejercicio 18.3.1 — Leer: qué es lineage y por qué importa

**Tipo: Leer**

```
Data lineage responde dos preguntas:

  1. Upstream (¿de dónde vienen?):
     "La tabla metricas_diarias — ¿de qué fuentes se alimenta?"
     → PostgreSQL.ventas + Kafka.eventos + S3.clientes

  2. Downstream (¿a quién afecta?):
     "Si cambio la tabla ventas_raw — ¿qué dashboards se rompen?"
     → metricas_diarias → dashboard_revenue → reporte_ejecutivo

Niveles de lineage:

  Nivel 1: Table-level lineage
    ventas_raw → metricas_diarias → dashboard
    "Esta tabla viene de esta otra tabla"
    → La mayoría de las herramientas hacen esto

  Nivel 2: Column-level lineage
    ventas_raw.monto → metricas_diarias.revenue (via SUM)
    ventas_raw.region → metricas_diarias.region (passthrough)
    "Esta columna viene de esta otra columna, con esta transformación"
    → Más difícil, requiere parsear SQL o analizar el código

  Nivel 3: Row-level lineage
    La fila 42 de metricas_diarias viene de las filas 100-150 de ventas_raw
    "Este registro específico viene de estos registros específicos"
    → Muy costoso, solo para auditoría regulatoria (ej: finanzas)

¿Cuándo el lineage salva el día?

  Escenario: el dashboard de revenue muestra números incorrectos desde el martes.
  Sin lineage: "¿Qué cambió? ¿Fue la extracción? ¿La transformación?
                ¿Un cambio de schema en PostgreSQL? ¿Un deploy de dbt?"
  Con lineage: "metricas_diarias depende de ventas_raw,
                que depende del extract de PostgreSQL.
                El martes se modificó la columna 'descuento' en PostgreSQL.
                El extract no falla pero el cálculo de revenue no incluye descuentos."
```

**Preguntas:**

1. ¿Lineage de tablas es suficiente para la mayoría de los casos?
   ¿Cuándo necesitas lineage de columnas?

2. ¿dbt genera lineage automáticamente?

3. ¿El lineage de Dagster (asset dependencies) es equivalente
   al lineage de una herramienta dedicada como OpenLineage?

4. ¿El lineage captura transformaciones en Spark
   (no solo SQL)?

5. ¿Lineage y data catalog son lo mismo?

---

### Ejercicio 18.3.2 — OpenLineage: el estándar abierto de lineage

```python
# OpenLineage es el estándar abierto para emitir y consumir eventos de lineage.
# Integraciones: Airflow, Spark, dbt, Flink, Great Expectations.

# Un evento OpenLineage tiene esta estructura:
import json

evento_lineage = {
    "eventType": "COMPLETE",
    "eventTime": "2024-03-15T03:45:00Z",
    "run": {
        "runId": "run-2024-03-14-abc123",
    },
    "job": {
        "namespace": "ecommerce",
        "name": "calcular_metricas_diarias",
    },
    "inputs": [
        {
            "namespace": "bigquery",
            "name": "proyecto.raw.ventas",
            "facets": {
                "schema": {
                    "fields": [
                        {"name": "user_id", "type": "STRING"},
                        {"name": "monto", "type": "FLOAT64"},
                        {"name": "region", "type": "STRING"},
                    ]
                },
                "dataQualityMetrics": {
                    "rowCount": 15432,
                    "bytes": 2_500_000,
                }
            }
        }
    ],
    "outputs": [
        {
            "namespace": "bigquery",
            "name": "proyecto.analytics.metricas_diarias",
            "facets": {
                "schema": {
                    "fields": [
                        {"name": "region", "type": "STRING"},
                        {"name": "revenue", "type": "FLOAT64"},
                        {"name": "transacciones", "type": "INT64"},
                    ]
                },
                "dataQualityMetrics": {
                    "rowCount": 5,
                }
            }
        }
    ]
}

# Airflow emite estos eventos automáticamente si instalas:
# pip install openlineage-airflow

# Marquez (https://marquezproject.ai) es el servidor open-source
# que recolecta y visualiza eventos OpenLineage.
```

**Preguntas:**

1. ¿OpenLineage captura el lineage automáticamente o requiere instrumentación?

2. ¿Marquez vs Datahub vs Amundsen — cuál para lineage open-source?

3. ¿OpenLineage funciona con Dagster y Prefect?

4. ¿Los "facets" de OpenLineage pueden incluir data quality metrics?

5. ¿Column-level lineage está soportado en OpenLineage?

---

### Ejercicio 18.3.3 — Lineage en la práctica: rastrear un error hasta la fuente

**Tipo: Diagnosticar**

```
Escenario: el analista reporta que el revenue de la región "centro"
cayó 80% el día 14 de marzo.

Con lineage, el proceso de investigación es:

  1. Buscar en Marquez: "metricas_diarias, fecha=2024-03-14, region=centro"
     → Input: ventas_raw (15432 filas), clientes_dim (50000 filas)

  2. Verificar ventas_raw para región centro:
     → Solo 200 filas para "centro" (normalmente son 3000)
     → El problema está en la extracción, no en la transformación

  3. Buscar upstream de ventas_raw:
     → Fuente: PostgreSQL, tabla "orders", schema "production"
     → El extract del 14 de marzo usó un filtro incorrecto

  4. Verificar el extract:
     → El filtro era WHERE region = 'centro'
     → Pero el 13 de marzo, el equipo de backend renombró
       la región "centro" a "central" en PostgreSQL
     → El extract obtuvo solo los 200 registros legacy

  5. Fix: actualizar el filtro a WHERE region IN ('centro', 'central')
     → Re-ejecutar el backfill del 14 de marzo
     → Verificar que las métricas se corrigen

Sin lineage, el paso 1-3 habrían tardado horas en vez de minutos.
```

**Preguntas:**

1. ¿Este tipo de investigación es posible solo con Airflow logs?

2. ¿Notificaciones proactivas ("el schema de PostgreSQL cambió")
   habrían prevenido el incidente?

3. ¿Column-level lineage habría acelerado la investigación?

4. ¿Cómo automatizas la detección de "region renombrada en upstream"?

5. ¿El lineage debe ser parte del pipeline o un sistema paralelo?

---

### Ejercicio 18.3.4 — Implementar: lineage casero con metadata tables

**Tipo: Implementar**

```python
# Si no tienes OpenLineage/Marquez, puedes implementar lineage básico
# con una tabla de metadata:

# pipeline_lineage (
#   run_id            STRING
#   pipeline_name     STRING
#   run_date          DATE
#   input_table       STRING    -- "raw.ventas"
#   input_row_count   INT64
#   input_schema_hash STRING    -- hash del schema para detectar cambios
#   output_table      STRING    -- "analytics.metricas_diarias"
#   output_row_count  INT64
#   transformation    STRING    -- "groupby_region_sum_monto"
#   created_at        TIMESTAMP
# )

# Registrar lineage en cada paso del pipeline:
def registrar_lineage(run_id, pipeline, input_table, output_table,
                      input_rows, output_rows, transformation):
    bq_client.query(f"""
        INSERT INTO metadata.pipeline_lineage
        VALUES ('{run_id}', '{pipeline}', CURRENT_DATE(),
                '{input_table}', {input_rows}, ...,
                '{output_table}', {output_rows},
                '{transformation}', CURRENT_TIMESTAMP())
    """)

# Query: "¿de dónde viene la tabla metricas_diarias?"
# SELECT * FROM metadata.pipeline_lineage
# WHERE output_table = 'analytics.metricas_diarias'
# ORDER BY created_at DESC
```

**Restricciones:**
1. Implementar la tabla y la función de registro
2. Implementar una query recursiva que trace el lineage completo
   (output → input → input del input → ...)
3. Implementar detección de schema changes (comparar hash del schema)
4. ¿Cuándo este enfoque casero es suficiente y cuándo necesitas OpenLineage?

---

### Ejercicio 18.3.5 — Data catalog: descubribilidad de datos

**Tipo: Analizar**

```
Data catalog vs data lineage:

  Lineage: "¿de dónde viene este dato?"
  Catalog: "¿qué datos tenemos y qué significan?"

  Un data catalog es un inventario de todos los datasets,
  con metadata: descripción, owner, schema, tags, freshness.

Herramientas:
  Open-source: Datahub (LinkedIn), Amundsen (Lyft), Apache Atlas
  Managed: Google Data Catalog, AWS Glue Data Catalog, Alation

  Datahub incluye:
  - Catalog (inventario)
  - Lineage (dependencias)
  - Quality (tests)
  - Governance (ownership, PII tags)

Ejemplo de metadata en un catalog:
  tabla: analytics.metricas_diarias
  descripción: "Revenue y transacciones por región, calculado diariamente"
  owner: data-engineering@empresa.com
  freshness: última actualización hace 2 horas
  schema: region (STRING), revenue (FLOAT64), transacciones (INT64)
  tags: [pii:no, tier:gold, domain:ecommerce]
  lineage: depende de raw.ventas, dim.clientes
  quality: 5/5 tests pasando
```

**Preguntas:**

1. ¿Un data catalog es necesario para un equipo de 5 personas?
   ¿A partir de cuántas tablas vale la pena?

2. ¿dbt docs genera un catalog básico?

3. ¿Datahub vs Amundsen — cuál recomiendas en 2024?

4. ¿El catalog debe ser mantenido manualmente o generado automáticamente?

5. ¿Tags de PII en el catalog ayudan con compliance (GDPR, etc.)?

---

## Sección 18.4 — Data Quality: el Pilar que Falta

### Ejercicio 18.4.1 — Leer: las cinco dimensiones de la calidad de datos

**Tipo: Leer**

```
Las cinco dimensiones (el "framework" de data quality):

  1. FRESHNESS (frescura)
     "¿Los datos son recientes?"
     Esperado: la tabla de métricas se actualiza antes de las 6:00 UTC.
     Check: MAX(updated_at) > NOW() - INTERVAL 6 HOURS

  2. VOLUME (volumen)
     "¿Hay la cantidad esperada de datos?"
     Esperado: entre 10,000 y 100,000 filas por día.
     Check: COUNT(*) BETWEEN 10000 AND 100000

  3. SCHEMA (estructura)
     "¿Las columnas y tipos son correctos?"
     Esperado: columna 'monto' es FLOAT64, no STRING.
     Check: column_type('monto') == 'FLOAT64'

  4. DISTRIBUTION (distribución)
     "¿Los valores tienen sentido?"
     Esperado: monto > 0, region IN ('norte','sur','este','oeste').
     Check: MIN(monto) >= 0 AND COUNT(DISTINCT region) <= 10

  5. UNIQUENESS (unicidad)
     "¿Hay duplicados?"
     Esperado: cada (user_id, timestamp) es único.
     Check: COUNT(*) == COUNT(DISTINCT user_id, timestamp)

Cada dimensión se puede monitorear de forma independiente,
y cada una puede tener su propio umbral de alerta.

Herramientas:
  - Great Expectations: framework Python, declarativo, amplio
  - dbt tests: SQL-based, integrado con dbt
  - Soda: YAML-based, cloud y open-source
  - Elementary: monitoring de dbt nativo
  - Monte Carlo / Bigeye: SaaS, zero-config anomaly detection
```

**Preguntas:**

1. ¿Las cinco dimensiones son exhaustivas?
   ¿Qué dimensión falta?

2. ¿Freshness es la dimensión más fácil de monitorear?

3. ¿Distribution checks pueden detectar data drift
   (cambios graduales que no son errores)?

4. ¿Uniqueness checks deben ejecutarse ANTES o DESPUÉS de cargar datos?

5. ¿Data quality es responsabilidad del data engineer
   o del analytics engineer?

---

### Ejercicio 18.4.2 — Great Expectations: data quality como código

```python
import great_expectations as gx

# Great Expectations define "expectativas" sobre los datos.
# Una expectativa = una afirmación que debe ser verdadera.

# Crear un contexto:
context = gx.get_context()

# Definir un datasource (conexión a los datos):
datasource = context.sources.add_pandas("mi_datasource")
asset = datasource.add_dataframe_asset("ventas")

# Definir expectativas (suite):
suite = context.add_or_update_expectation_suite("ventas_quality")

# Expectativa 1: la tabla no está vacía
suite.add_expectation(
    gx.expectations.ExpectTableRowCountToBeBetween(min_value=100, max_value=1_000_000)
)

# Expectativa 2: no hay nulls en user_id
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column="user_id")
)

# Expectativa 3: monto es positivo
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeBetween(
        column="monto", min_value=0.01, max_value=1_000_000
    )
)

# Expectativa 4: region es un valor conocido
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeInSet(
        column="region", value_set=["norte", "sur", "este", "oeste", "centro"]
    )
)

# Expectativa 5: no hay duplicados en (user_id, timestamp)
suite.add_expectation(
    gx.expectations.ExpectCompoundColumnsToBeUnique(column_list=["user_id", "timestamp"])
)

# Ejecutar validación:
batch = asset.get_batch(dataframe=df_ventas)
results = batch.validate(suite)

if not results.success:
    fallos = [r for r in results.results if not r.success]
    for f in fallos:
        print(f"FALLO: {f.expectation_config.expectation_type} — {f.result}")
    raise ValueError(f"{len(fallos)} quality checks fallaron")
```

**Preguntas:**

1. ¿Great Expectations ejecuta las validaciones en memoria o en la DB?

2. ¿Las expectativas deben definirse en código o en YAML?

3. ¿Cuántas expectativas por tabla es razonable? ¿5? ¿50? ¿500?

4. ¿Great Expectations genera reportes HTML? ¿Son útiles?

5. ¿Cómo integras Great Expectations en un DAG de Airflow?

---

### Ejercicio 18.4.3 — dbt tests: quality integrado en las transformaciones

```sql
-- dbt tests: validaciones declaradas en YAML junto al modelo.

-- models/analytics/metricas_diarias.sql
SELECT
    fecha,
    region,
    SUM(monto) AS revenue,
    COUNT(DISTINCT user_id) AS usuarios_unicos,
    COUNT(*) AS transacciones
FROM {{ ref('stg_ventas') }}
GROUP BY 1, 2

-- models/analytics/schema.yml
version: 2
models:
  - name: metricas_diarias
    description: "Métricas diarias de revenue por región"
    columns:
      - name: fecha
        tests:
          - not_null
          - unique  -- combinado con region para PK
      - name: region
        tests:
          - not_null
          - accepted_values:
              values: ['norte', 'sur', 'este', 'oeste', 'centro']
      - name: revenue
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: ">= 0"
      - name: usuarios_unicos
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: "> 0"

    # Tests a nivel de tabla:
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns: [fecha, region]
      - dbt_utils.expression_is_true:
          expression: "transacciones >= usuarios_unicos"
          # No puede haber más usuarios únicos que transacciones

-- Ejecutar tests: dbt test --select metricas_diarias
-- Los tests fallidos bloquean el pipeline (en el DAG de Airflow/Dagster)
```

**Preguntas:**

1. ¿dbt tests ejecutan después de que el modelo se materializa
   o pueden ejecutar como pre-check?

2. ¿`dbt_utils.expression_is_true` puede expresar cualquier validación SQL?

3. ¿Los tests de dbt reemplazan a Great Expectations?
   ¿O son complementarios?

4. ¿Elementary (herramienta de monitoring para dbt) añade
   anomaly detection automática sobre los tests de dbt?

5. ¿Cuántos tests de dbt tiene un proyecto típico de 50 modelos?

---

### Ejercicio 18.4.4 — Freshness monitoring: los datos más peligrosos son los stale

```python
# Freshness: verificar que los datos se actualizan a tiempo.
# Datos stale = datos del día anterior presentados como si fueran de hoy.

# Patrón 1: query directa
def verificar_freshness(tabla: str, columna_tiempo: str,
                        max_age_hours: int = 6) -> bool:
    query = f"""
        SELECT
            MAX({columna_tiempo}) AS ultimo_dato,
            TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX({columna_tiempo}), HOUR) AS age_hours
        FROM `{tabla}`
    """
    result = bq_client.query(query).result().to_dataframe()
    age = result["age_hours"].iloc[0]

    if age > max_age_hours:
        log.warning("datos_stale", tabla=tabla, age_hours=age,
                    max_allowed=max_age_hours)
        return False
    return True

# Patrón 2: dbt source freshness
# En sources.yml:
# sources:
#   - name: raw
#     tables:
#       - name: ventas
#         loaded_at_field: updated_at
#         freshness:
#           warn_after: {count: 6, period: hour}
#           error_after: {count: 12, period: hour}
#
# Ejecutar: dbt source freshness

# Patrón 3: check periódico con Airflow Sensor
from airflow.sensors.sql import SqlSensor

freshness_check = SqlSensor(
    task_id="verificar_freshness_ventas",
    conn_id="bigquery",
    sql="""
        SELECT COUNT(*) FROM `raw.ventas`
        WHERE DATE(updated_at) = CURRENT_DATE()
        HAVING COUNT(*) > 0
    """,
    timeout=3600,
    poke_interval=300,
)
```

**Preguntas:**

1. ¿Freshness check antes o después de la transformación?

2. ¿Datos de 6 horas de antigüedad son "frescos" para un dashboard de revenue?
   ¿Y para detección de fraude?

3. ¿`dbt source freshness` es la forma más simple de monitorear freshness?

4. ¿Cómo manejas freshness para tablas que se actualizan irregularmente
   (ej: catálogo de productos)?

5. ¿Un dashboard debe mostrar "datos actualizados hace X horas"
   como indicador de confianza?

---

### Ejercicio 18.4.5 — Implementar: quality framework integrado en el pipeline

**Tipo: Implementar**

```python
# Diseñar un framework de quality checks que:
# 1. Ejecute ANTES de cargar datos (pre-check)
# 2. Ejecute DESPUÉS de cargar datos (post-check)
# 3. Bloquee el pipeline si checks críticos fallan
# 4. Alerte pero no bloquee si checks no-críticos fallan
# 5. Registre resultados en una tabla de histórico

# Schema para la tabla de histórico:
# quality_results (
#   check_id        STRING    -- "ventas_min_rows"
#   run_id          STRING
#   run_date        DATE
#   table_name      STRING    -- "raw.ventas"
#   check_type      STRING    -- "volume", "freshness", "schema", etc.
#   severity        STRING    -- "critical", "warning", "info"
#   passed          BOOLEAN
#   expected_value  STRING    -- ">=100"
#   actual_value    STRING    -- "15432"
#   message         STRING    -- detalle del resultado
#   checked_at      TIMESTAMP
# )

# Con este histórico puedes:
# - Ver la tasa de éxito de cada check en el tiempo
# - Detectar checks que fallan intermitentemente
# - Generar reportes de quality para stakeholders
```

**Restricciones:**
1. Implementar al menos 10 checks cubriendo las 5 dimensiones
2. Implementar la lógica de bloqueo (critical) vs alerta (warning)
3. Implementar la tabla de histórico y queries de análisis
4. Integrar como tarea de Airflow entre extract y transform

---

## Sección 18.5 — Dashboards Operativos y Alertas

### Ejercicio 18.5.1 — El dashboard operativo: qué debe mostrar

**Tipo: Diseñar**

```
Un dashboard operativo de pipelines NO es un dashboard de negocio.
Es la vista que el on-call engineer mira a las 3am
cuando recibe una alerta de PagerDuty.

Panel 1: Estado actual de pipelines (traffic light)
  ┌─────────────────────────┬──────────┬──────────┐
  │ Pipeline                │ Estado   │ Última   │
  ├─────────────────────────┼──────────┼──────────┤
  │ ecommerce_daily         │ 🟢 OK    │ 03:45    │
  │ fraud_detection         │ 🟡 SLOW  │ 03:30    │
  │ inventory_sync          │ 🔴 FAIL  │ 02:15    │
  │ ml_inference            │ 🟢 OK    │ 04:00    │
  └─────────────────────────┴──────────┴──────────┘

Panel 2: Duración del pipeline (time series, últimos 30 días)
  [gráfico: línea con banda de ±2σ, punto rojo si fuera de banda]

Panel 3: Filas procesadas (time series)
  [gráfico: barras por día, rojo si < mínimo esperado]

Panel 4: Quality checks (stacked bar)
  [gráfico: checks passed vs failed por día]

Panel 5: Freshness (gauge por tabla)
  ventas_raw:      actualizado hace 2h  [🟢]
  metricas_diarias: actualizado hace 3h  [🟢]
  clientes_dim:    actualizado hace 25h [🔴]

Panel 6: Alertas activas
  [lista de alertas no resueltas con timestamp y severidad]
```

**Preguntas:**

1. ¿Grafana o Looker para el dashboard operativo?

2. ¿El dashboard operativo debe ser separado del dashboard de negocio?

3. ¿Cuántos paneles son demasiados? ¿Qué eliminarías?

4. ¿El dashboard debe tener un botón de "re-ejecutar pipeline"?

5. ¿Los stakeholders no-técnicos necesitan acceso
   al dashboard operativo?

---

### Ejercicio 18.5.2 — Alertas que no se ignoran

```python
# El problema #1 de las alertas: alert fatigue.
# Si alertas por todo, la gente ignora las alertas.

# Reglas para alertas que se respetan:

# 1. Cada alerta debe ser actionable
# MAL:  "El pipeline tardó más de lo normal" → ¿y qué hago?
# BIEN: "El pipeline tardó 3× más de lo normal.
#        Paso más lento: 'transformar' (120min vs 40min normal).
#        Posible causa: data skew en la región 'norte'.
#        Acción: verificar distribución de datos."

# 2. Cada alerta debe tener un owner claro
# MAL:  alerta a #general → nadie responde
# BIEN: alerta a #data-oncall con @mention del owner del pipeline

# 3. Severidades deben significar algo
# P1 (PagerDuty, wake someone up):
#    - Dashboard de revenue muestra $0
#    - Pipeline crítico no completó antes del SLA
# P2 (Slack, responder en 1 hora):
#    - Quality check warning
#    - Pipeline 2× más lento de lo normal
# P3 (Slack, responder en 1 día):
#    - Schema change detectado
#    - Drop rate > 0.5% pero < 2%

# Implementar una alerta P1:
def alerta_revenue_cero(context):
    fecha = context["data_interval_start"].strftime("%Y-%m-%d")
    revenue = bq_client.query(f"""
        SELECT COALESCE(SUM(revenue), 0) AS total
        FROM analytics.metricas_diarias
        WHERE fecha = '{fecha}'
    """).result().to_dataframe()["total"].iloc[0]

    if revenue == 0:
        enviar_pagerduty(
            severity="critical",
            summary=f"Revenue = $0 para {fecha}",
            dedup_key=f"revenue-zero-{fecha}",
            details={
                "pipeline": "ecommerce_daily",
                "fecha": fecha,
                "accion": "Verificar extracción de ventas y quality checks",
                "runbook": "https://wiki.empresa.com/runbooks/revenue-zero",
            }
        )
```

**Preguntas:**

1. ¿`dedup_key` en PagerDuty previene alertas duplicadas?
   ¿Cómo funciona?

2. ¿Un runbook link en la alerta reduce el MTTR (mean time to resolve)?

3. ¿Cuántas alertas P1 por semana son aceptables?
   ¿Y cuántas P2?

4. ¿Las alertas deben auto-resolverse cuando el problema se corrige?

5. ¿OpsGenie vs PagerDuty — cuál para data engineering?

---

### Ejercicio 18.5.3 — Anomaly detection automática vs reglas manuales

```python
# Dos enfoques para detectar problemas:

# Enfoque 1: Reglas manuales (explícitas)
REGLAS = {
    "revenue_min": lambda revenue: revenue > 1000,
    "revenue_max": lambda revenue: revenue < 1_000_000,
    "filas_min": lambda rows: rows > 100,
    "drop_rate_max": lambda rate: rate < 0.05,
    "duracion_max": lambda dur: dur < 7200,
}

# Ventajas: predecibles, fáciles de entender, sin falsos positivos
# Desventajas: no detectan anomalías nuevas, requieren mantenimiento,
#              los umbrales se desactualizan

# Enfoque 2: Anomaly detection automática (estadística)
def detectar_anomalias_automaticas(tabla, columna, dias_historico=30):
    """Detectar automáticamente valores fuera de lo normal."""
    historico = bq_client.query(f"""
        SELECT {columna}, fecha
        FROM {tabla}
        WHERE fecha BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL {dias_historico} DAY)
              AND CURRENT_DATE()
        ORDER BY fecha
    """).result().to_dataframe()

    media = historico[columna].mean()
    std = historico[columna].std()
    ultimo = historico[columna].iloc[-1]

    zscore = abs(ultimo - media) / std if std > 0 else 0
    return {
        "anomalo": zscore > 3,
        "valor": ultimo,
        "media_historica": media,
        "zscore": zscore,
    }

# Ventajas: detecta anomalías que no esperabas, se adapta automáticamente
# Desventajas: falsos positivos, requiere histórico, difícil de debuggear

# Enfoque 3: Combinar ambos (recomendado)
# - Reglas manuales para checks críticos conocidos (revenue > 0)
# - Anomaly detection para detectar lo inesperado (drift gradual)
```

**Preguntas:**

1. ¿Monte Carlo (SaaS) usa anomaly detection automática?
   ¿Funciona bien en la práctica?

2. ¿Cuántos falsos positivos son aceptables para anomaly detection?

3. ¿La estacionalidad (fin de semana, holidays) rompe la detección
   automática?

4. ¿ML-based anomaly detection (isolation forest, etc.)
   es overkill para métricas de pipeline?

5. ¿El enfoque combinado es más trabajo que solo reglas manuales?
   ¿Vale la pena?

---

### Ejercicio 18.5.4 — Runbooks: qué hacer cuando la alerta suena

```markdown
# Runbook: Revenue = $0 en el dashboard
# Owner: data-engineering@empresa.com
# Severidad: P1
# SLA de resolución: 1 hora

## Síntoma
Dashboard de revenue muestra $0 para el día de ayer.

## Diagnóstico (seguir en orden)

### Paso 1: ¿El pipeline ejecutó?
- Abrir Airflow UI → DAG `ecommerce_daily` → último run
- Si el run está en "failed" → ir al Paso 2
- Si el run está en "success" → ir al Paso 3

### Paso 2: Pipeline falló
- Abrir logs de la tarea fallida
- Errores comunes:
  - `ConnectionRefusedError` → PostgreSQL caído → contactar DBA
  - `OutOfMemoryError` → escalar el Spark cluster o reducir partitions
  - `SchemaValidationError` → schema cambió upstream → verificar con equipo backend
- Re-ejecutar: `airflow dags trigger ecommerce_daily -e 2024-03-14`

### Paso 3: Pipeline completó pero datos incorrectos
- Verificar tabla `raw.ventas`:
  ```sql
  SELECT COUNT(*), MIN(fecha), MAX(fecha) FROM raw.ventas
  WHERE fecha = '2024-03-14'
  ```
- Si COUNT = 0 → la extracción no trajo datos → verificar filtro de fecha
- Si COUNT > 0 pero revenue = 0 → verificar campo `monto`:
  ```sql
  SELECT AVG(monto), MIN(monto), MAX(monto) FROM raw.ventas
  WHERE fecha = '2024-03-14'
  ```

### Paso 4: Escalar si no se resuelve en 30 minutos
- Notificar en #data-incidents con contexto
- Contactar al tech lead de data engineering
```

**Preguntas:**

1. ¿Cuántos runbooks necesita un sistema con 20 pipelines?

2. ¿Los runbooks deben estar en Confluence, GitHub, o en la propia alerta?

3. ¿Un runbook puede automatizarse parcialmente?
   (ej: el paso de diagnóstico como un script)

4. ¿Quién escribe los runbooks: el que creó el pipeline
   o el on-call?

5. ¿Los runbooks deben actualizarse después de cada incidente?

---

### Ejercicio 18.5.5 — Implementar: sistema de alertas con contexto

**Tipo: Implementar**

```python
# Implementar un sistema de alertas que:
# 1. Detecte problemas (reglas + anomalías)
# 2. Enriquezca la alerta con contexto (logs, lineage, queries de diagnóstico)
# 3. Envíe la alerta al canal correcto (Slack, PagerDuty, email)
# 4. Incluya un link al runbook correspondiente
# 5. Se auto-resuelva cuando el problema se corrige

# Bonus: implementar un "incident timeline" que registre:
# - Cuándo se detectó el problema
# - Cuándo se alertó
# - Cuándo se empezó a investigar
# - Cuándo se resolvió
# → Calcular MTTD (mean time to detect) y MTTR (mean time to resolve)
```

**Restricciones:**
1. Implementar al menos 5 reglas de detección
2. Implementar enriquecimiento automático con queries SQL
3. Implementar envío a Slack con bloques formateados
4. Implementar la tabla de incident timeline

---

## Sección 18.6 — Observabilidad de Spark, Flink, y Kafka

### Ejercicio 18.6.1 — Spark UI y métricas: diagnosticar un job lento

```python
# Spark expone métricas vía:
# 1. Spark UI (web): jobs, stages, tasks, storage, executors
# 2. Spark Metrics (Prometheus/Graphite): métricas custom
# 3. SparkListener (programático): eventos en tiempo real

# Métricas clave de Spark para diagnosticar:
# ─────────────────────────────────────────────────
# executor.cpuTime             CPU usado por executor
# executor.runTime             tiempo total de ejecución
# executor.memoryUsed          memoria usada
# executor.diskBytesSpilled    datos spilled a disco (MAL)
# stage.shuffleReadBytes       bytes leídos en shuffle
# stage.shuffleWriteBytes      bytes escritos en shuffle
# task.garbageCollectionTime   tiempo en GC por task

# Si diskBytesSpilled > 0: los datos no caben en memoria
# → Aumentar spark.executor.memory o reducir el partition count

# Si shuffleReadBytes es muy alto: el shuffle domina
# → Verificar si hay data skew (una partición >> otras)
# → Considerar broadcast join si una tabla es pequeña

# Si garbageCollectionTime > 10% del runtime: GC problem
# → Aumentar memoria o reducir el tamaño de objetos

# Emitir métricas custom desde PySpark:
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

# Accumulator como métrica custom:
registros_invalidos = sc.accumulator(0)

def procesar_fila(fila):
    if fila.monto < 0:
        registros_invalidos.add(1)
        return None
    return fila

# Al final del job:
print(f"Registros inválidos: {registros_invalidos.value}")
statsd.gauge("spark.custom.registros_invalidos", registros_invalidos.value)
```

**Preguntas:**

1. ¿Spark UI está disponible después de que el job termina?
   ¿Cómo preservas la información?

2. ¿`diskBytesSpilled > 0` siempre es un problema?
   ¿O hay casos donde es aceptable?

3. ¿Los accumulators de Spark son equivalentes a métricas de Prometheus?

4. ¿Spark History Server es suficiente como observabilidad de Spark?

5. ¿Databricks tiene observabilidad de Spark superior a open-source?

---

### Ejercicio 18.6.2 — Flink metrics: observar un job de streaming

```java
// Flink expone métricas vía su sistema de métricas integrado.
// Se integra con Prometheus, Datadog, Graphite, etc.

// Métricas clave de Flink:
// ─────────────────────────────────────────────────
// numRecordsIn / numRecordsOut    throughput del operador
// numRecordsInPerSecond           throughput por segundo
// currentInputWatermark           watermark actual
// numLateRecordsDropped           eventos late descartados
// checkpointDuration              duración del checkpoint
// checkpointSize                  tamaño del state checkpointed
// isBackPressured                 ¿el operador está en backpressure?
// busyTimeMsPerSecond             % de tiempo que el operador está busy

// Dashboard de Grafana para Flink:
// Panel 1: Throughput (records/sec) per operator
// Panel 2: Latencia end-to-end (event time - processing time)
// Panel 3: Checkpoint duration y size (detectar state growth)
// Panel 4: Backpressure map (qué operador es el cuello de botella)
// Panel 5: Watermark lag (qué tan "atrás" está el watermark)

// Alerta crítica: checkpoint falla
// Si Flink no puede completar un checkpoint, no puede recuperarse de fallos.
// Si checkpointDuration > timeout → el job no tiene exactly-once.

// Alerta warning: backpressure sostenido
// Backpressure por > 5 minutos → el pipeline no puede mantenerse al día
// → escalar parallelism o optimizar el operador lento
```

**Preguntas:**

1. ¿Backpressure en Flink es equivalente a consumer lag en Kafka?

2. ¿Checkpoint size creciendo linealmente indica un memory leak en state?

3. ¿`numLateRecordsDropped` debería ser 0 siempre?
   ¿Cuánto es aceptable?

4. ¿El Flink Dashboard (UI web) es suficiente o necesitas Grafana?

5. ¿Cómo observas un job de Flink en Kubernetes
   que se reinicia automáticamente?

---

### Ejercicio 18.6.3 — Kafka monitoring: consumer lag y más allá

```python
# Kafka expone métricas vía JMX (Java Management Extensions).
# Las más importantes para data engineering:

# CONSUMER LAG: la métrica #1 de Kafka
# lag = offset más reciente del topic - offset del consumer group
# lag = "cuántos mensajes no se han procesado"

# Si lag crece → el consumer no puede mantenerse al día
# Si lag = 0 → el consumer está al día
# Si lag oscila → el consumer tiene throughput variable

# Herramientas para monitorear Kafka:
# - Burrow (LinkedIn): consumer lag monitoring dedicado
# - Kafka-exporter: exportar métricas a Prometheus
# - Confluent Control Center: UI comercial completa
# - AKHQ / Kafdrop: UIs open-source ligeras

# Métricas clave:
# ─────────────────────────────────────────────────
# consumer_lag                    mensajes sin procesar por partition
# consumer_lag_seconds            lag en tiempo (no solo offsets)
# bytes_in_per_sec                throughput de entrada al cluster
# bytes_out_per_sec               throughput de salida
# under_replicated_partitions     particiones sin réplicas suficientes
# request_latency_avg             latencia promedio de requests
# active_controller_count         debe ser exactamente 1

# Alerta P1: under_replicated_partitions > 0
# → El cluster está perdiendo redundancia. Datos en riesgo.

# Alerta P2: consumer_lag_seconds > 300 (5 minutos)
# → El consumer está 5 minutos atrás. El dashboard real-time no es real-time.
```

**Preguntas:**

1. ¿Consumer lag en offsets vs consumer lag en segundos — cuál es más útil?

2. ¿Un consumer lag de 1000 mensajes es mucho o poco?
   ¿Depende del throughput?

3. ¿`under_replicated_partitions > 0` siempre merece P1?

4. ¿Burrow es mejor que el consumer lag nativo de Kafka?

5. ¿Cómo monitoras un cluster de Kafka con 100 topics
   y 50 consumer groups sin ahogarte en métricas?

---

### Ejercicio 18.6.4 — End-to-end latency: medir desde el evento hasta el dashboard

```python
# La métrica más importante que nadie mide:
# ¿Cuánto tiempo pasa desde que ocurre un evento
# hasta que aparece en el dashboard?

# Pipeline: evento → Kafka → Flink → BigQuery → Looker

# Medir cada hop:
# 1. event_time → kafka_timestamp:     latencia de producción
# 2. kafka_timestamp → flink_processed: latencia de consumo
# 3. flink_processed → bigquery_loaded: latencia de escritura
# 4. bigquery_loaded → dashboard_shown: latencia de refresh

# Implementar con timestamps embebidos:
import time
import json

# Productor (al generar el evento):
evento = {
    "user_id": "alice",
    "monto": 150.0,
    "event_time": time.time(),  # timestamp del evento real
    "produced_at": time.time(), # timestamp de producción a Kafka
}
producer.send("ventas", json.dumps(evento))

# Consumer (al procesar en Flink/Spark):
def procesar(evento):
    consumed_at = time.time()
    latencia_kafka = consumed_at - evento["produced_at"]
    latencia_total = consumed_at - evento["event_time"]
    statsd.timing("pipeline.latency.kafka", latencia_kafka * 1000)
    statsd.timing("pipeline.latency.total", latencia_total * 1000)

# Dashboard: mostrar p50, p95, p99 de latencia end-to-end
```

**Preguntas:**

1. ¿End-to-end latency de 30 segundos es aceptable
   para un dashboard de revenue?

2. ¿El refresh rate del dashboard (ej: 1 minuto)
   domina la latencia end-to-end?

3. ¿Cómo mides latencia end-to-end si no controlas todos los hops?

4. ¿La latencia de producción (event → Kafka) es responsabilidad
   del data engineer o del equipo de backend?

5. ¿p99 de latencia es más útil que p50 para detectar problemas?

---

### Ejercicio 18.6.5 — Implementar: dashboard de observabilidad del stack completo

**Tipo: Implementar**

```python
# Diseñar el dashboard de Grafana que muestre:
# 1. Airflow: estado de DAGs, duración, queue time, SLA compliance
# 2. Spark: job duration, shuffle, spill, GC time
# 3. Flink: throughput, latency, checkpoint, backpressure
# 4. Kafka: consumer lag, throughput, under-replicated partitions
# 5. Data quality: checks passed/failed, freshness, anomalies
# 6. End-to-end: latencia del evento al dashboard

# Para cada sección, definir:
# - Queries de Prometheus/CloudWatch
# - Umbrales de alerta (P1, P2, P3)
# - Panel type (time series, gauge, table, heatmap)
```

**Restricciones:**
1. Diseñar al menos 15 paneles distribuidos en las 6 secciones
2. Definir las alertas para cada panel con severidad y canal
3. Incluir un "health score" global (0-100) calculado de las métricas
4. ¿Cómo evitas que el dashboard sea demasiado ruidoso?

---

## Sección 18.7 — El E-commerce Observable: Integrando los Cuatro Pilares

### Ejercicio 18.7.1 — Leer: la arquitectura de observabilidad completa

**Tipo: Leer**

```
El sistema de e-commerce con observabilidad integrada:

  ┌────────────────────────────────────────────────────────────┐
  │                    PIPELINES                                │
  │  Airflow DAGs │ Spark Jobs │ Flink Streaming │ dbt Models  │
  └───────┬───────────┬────────────┬─────────────┬────────────┘
          │           │            │             │
  ┌───────▼───────────▼────────────▼─────────────▼────────────┐
  │                 INSTRUMENTACIÓN                             │
  │  structlog    │ Spark Metrics │ Flink Metrics │ OpenLineage │
  │  StatsD       │ Accumulators  │ JMX           │ GX/dbt tests│
  └───────┬───────────┬────────────┬─────────────┬────────────┘
          │           │            │             │
  ┌───────▼───────────▼────────────▼─────────────▼────────────┐
  │              ALMACENAMIENTO DE SEÑALES                      │
  │  Prometheus  │ CloudWatch │ Marquez │ BigQuery (quality)    │
  └───────┬───────────┬────────────┬─────────────┬────────────┘
          │           │            │             │
  ┌───────▼───────────▼────────────▼─────────────▼────────────┐
  │              VISUALIZACIÓN Y ALERTAS                        │
  │  Grafana dashboards │ PagerDuty alerts │ Slack notifications│
  │  Datahub catalog    │ Runbooks wiki    │ Incident timeline  │
  └────────────────────────────────────────────────────────────┘

Los cuatro pilares en acción para un incidente:

  1. MÉTRICAS detectan: "pipeline_duration 3× más lento que lo normal"
  2. ALERTAS notifican: PagerDuty → on-call engineer
  3. LOGS diagnostican: "step 'transformar' tardó 120min por data skew"
  4. LINEAGE localiza: "la tabla raw.ventas tiene distribución anómala
                        porque PostgreSQL cambió el encoding de región"
  5. QUALITY confirma: "el quality check 'region_in_set' falló para
                        30% de los registros"

  Sin observabilidad: "algo está lento, no sé qué ni por qué"
  Con observabilidad: "sé exactamente qué pasó, por qué, y cómo arreglarlo"
```

**Preguntas:**

1. ¿La instrumentación debe ser responsabilidad de cada data engineer
   o de un equipo de platform engineering?

2. ¿El costo de la observabilidad (Datadog, Grafana Cloud, etc.)
   es significativo respecto al costo del pipeline?

3. ¿Cuánto tiempo invierte un equipo de 5 personas
   en mantener la observabilidad?

4. ¿La observabilidad de datos está madura en 2024
   o todavía es "estado del arte"?

5. ¿Un startup necesita los cuatro pilares desde el día 1?

---

### Ejercicio 18.7.2 — Incident management: del alerta al postmortem

```
Proceso de gestión de incidentes para pipelines de datos:

  1. DETECCIÓN (automática)
     Alerta: "revenue = $0 para 2024-03-14"
     Fuente: quality check en el pipeline post-transform
     Severidad: P1 (PagerDuty)

  2. TRIAGE (on-call, < 5 minutos)
     On-call acknowledges en PagerDuty.
     Abre #incident-2024-0314 en Slack.
     Sigue el runbook "revenue-zero".

  3. DIAGNÓSTICO (< 30 minutos)
     Runbook paso 1: ¿pipeline ejecutó? → Sí, success.
     Runbook paso 2: ¿ventas_raw tiene datos? → Sí, 15000 filas.
     Runbook paso 3: ¿monto tiene valores válidos? → NO.
       → AVG(monto) = 0. Todos los montos son 0.
     Runbook paso 4: ¿la fuente (PostgreSQL) tiene montos correctos? → Sí.
     → El extract lee la columna incorrecta (un deploy cambió el nombre).

  4. RESOLUCIÓN (< 1 hora)
     Fix: actualizar el mapping de columnas en el extract.
     Re-ejecutar backfill del 14 de marzo.
     Verificar que revenue > 0.
     Cerrar incidente en Slack.

  5. POSTMORTEM (< 48 horas)
     Root cause: deploy de backend renombró columna sin notificar a data eng.
     Acciones:
     - Añadir schema validation en el extract (detectar cambios de columna).
     - Añadir contrato entre backend y data eng para cambios de schema.
     - Añadir alerta de anomalía de distribución (AVG(monto) = 0).
     Timeline: detección 03:45, resolución 04:30, MTTR = 45 minutos.
```

**Preguntas:**

1. ¿45 minutos de MTTR es bueno o malo para un pipeline P1?

2. ¿El postmortem debe ser blameless (sin culpar a nadie)?

3. ¿Cuántos action items del postmortem se implementan realmente?

4. ¿Los postmortems deben ser públicos para todo el equipo de engineering?

5. ¿Un schema contract entre backend y data engineering
   es implementable en la práctica?

---

### Ejercicio 18.7.3 — Implementar: observabilidad del sistema de e-commerce

**Tipo: Implementar**

```python
# Implementar observabilidad completa para el sistema de e-commerce:
#
# 1. Instrumentar el pipeline batch (Airflow + Spark):
#    - Métricas custom (duración, filas, bytes)
#    - Logging estructurado con trace_id
#    - Quality checks (5 dimensiones)
#
# 2. Instrumentar el pipeline streaming (Flink):
#    - Consumer lag monitoring
#    - End-to-end latency measurement
#    - Checkpoint monitoring
#
# 3. Configurar alertas:
#    - P1: revenue = 0, pipeline fail después de retries
#    - P2: pipeline lento, quality check warning
#    - P3: schema change, freshness borderline
#
# 4. Crear dashboard de Grafana:
#    - Health score global
#    - Métricas por pipeline
#    - Quality trends
#    - Incident timeline

# Bonus: implementar "data SLOs" (Service Level Objectives para datos):
# - freshness SLO: 99% de los días, los datos están disponibles antes de 6am
# - quality SLO: 99.9% de los registros pasan todos los quality checks
# - completeness SLO: 95% de los registros esperados están presentes
```

**Restricciones:**
1. Implementar la instrumentación para batch y streaming
2. Configurar al menos 10 alertas con severidad y canal
3. Diseñar el dashboard con al menos 12 paneles
4. Calcular data SLOs sobre el histórico simulado

---

### Ejercicio 18.7.4 — El costo de no observar: calcular el ROI

**Tipo: Analizar**

```
Calcular el ROI de la observabilidad:

  Costo de la observabilidad:
    - Herramientas: Datadog ~$200/mes, Grafana Cloud ~$50/mes
    - Tiempo de setup: 2 semanas de un data engineer
    - Mantenimiento: 2 horas/semana
    - Total año 1: ~$20,000

  Costo de NO tener observabilidad (estimado):
    - Incidente tipo "$0 en dashboard": 4 horas de 3 ingenieros = 12h
      × $150/hora × 2 incidentes/mes = $3,600/mes = $43,200/año
    - Datos incorrectos no detectados: decisiones de negocio erróneas
      × impacto estimado = difícil de cuantificar pero potencialmente enorme
    - Tiempo de debugging sin herramientas: 2× más que con herramientas
      × frecuencia de problemas = $20,000+/año en productividad

  ROI estimado: ($63,000 - $20,000) / $20,000 = 215%
  (sin contar decisiones de negocio erróneas por datos incorrectos)
```

**Preguntas:**

1. ¿El cálculo de ROI es convincente para un CTO?

2. ¿Observabilidad open-source (Prometheus + Grafana + Marquez)
   reduce significativamente el costo?

3. ¿Cuál es el costo de Datadog para un equipo de 10 data engineers
   con 50 pipelines?

4. ¿El costo más grande es la herramienta o el tiempo del equipo?

5. ¿Empezar con observabilidad mínima y crecer
   es mejor que implementar todo de una vez?

---

### Ejercicio 18.7.5 — El ecosystem fit: observabilidad en el stack de 2024

**Tipo: Analizar**

```
Estado del ecosistema en 2024:

  Métricas y dashboards:
    Prometheus + Grafana (open-source, estándar)
    Datadog (SaaS, todo-en-uno, caro)
    CloudWatch (AWS, integrado, limitado)

  Logging:
    ELK Stack (open-source, complejo)
    Grafana Loki (open-source, simple)
    CloudWatch Logs (AWS, integrado)
    Datadog Logs (SaaS, caro pero completo)

  Lineage:
    OpenLineage + Marquez (open-source, estándar emergente)
    Datahub (open-source, completo)
    Atlan / Alation (SaaS, enterprise)

  Data quality:
    Great Expectations (open-source, Python)
    dbt tests (open-source, SQL)
    Soda (open-source + cloud)
    Monte Carlo / Bigeye (SaaS, anomaly detection)
    Elementary (open-source, dbt-native)

  Recomendación para un equipo nuevo:
    Mínimo viable: structlog + Grafana + dbt tests
    Medio: + Prometheus + Great Expectations + OpenLineage
    Completo: + Datahub + PagerDuty + Monte Carlo
```

**Preguntas:**

1. ¿Un equipo puede empezar solo con dbt tests y Grafana?

2. ¿Monte Carlo (SaaS) justifica el costo respecto a Great Expectations (free)?

3. ¿Datahub reemplaza a OpenLineage + Marquez?

4. ¿La observabilidad de datos se consolidará en una sola herramienta?

5. ¿Para el sistema de e-commerce del libro, ¿cuál es la recomendación final?

---

## Resumen del capítulo

**Observabilidad de datos: los cuatro pilares**

```
Pilar 1: Métricas
  ¿Cuánto tardó? ¿Cuántas filas? ¿Cuántos errores?
  → Prometheus, StatsD, custom metrics en BigQuery
  → Detectar anomalías automáticamente (zscore, IQR)

Pilar 2: Logs
  ¿Qué pasó exactamente? ¿En qué orden? ¿Cuál fue el error?
  → structlog (JSON), CloudWatch/Loki/ELK
  → Correlación con trace_id entre componentes

Pilar 3: Lineage
  ¿De dónde vienen los datos? ¿A quién afecta un cambio?
  → OpenLineage, Marquez, Datahub
  → Table-level para operación, column-level para auditoría

Pilar 4: Data Quality
  ¿Los datos son correctos, frescos, completos?
  → Great Expectations, dbt tests, Soda
  → Las 5 dimensiones: freshness, volume, schema, distribution, uniqueness
```

**El principio de la observabilidad de datos:**

```
La observabilidad no previene los problemas.
Los hace visibles antes de que el negocio se entere.

  Sin observabilidad:
    Problema ocurre → nadie se entera → stakeholder reporta
    → investigación manual → fix → "¿cuánto tiempo estuvo mal?"

  Con observabilidad:
    Problema ocurre → alerta automática → diagnóstico guiado
    → fix → postmortem → prevención

  La diferencia no es técnica — es de confianza.
  Un equipo de datos con buena observabilidad
  puede decir "nuestros datos son correctos con 99.9% de confianza".
  Un equipo sin observabilidad dice "creemos que está bien".
```

**Conexión con el Cap.19 (Testing):**

> La observabilidad detecta problemas en producción.
> El testing previene que esos problemas lleguen a producción.
> La observabilidad dice "el revenue de ayer es $0 — algo está mal".
> Los tests dicen "este cambio habría producido $0 — no lo deployemos".
> El Cap.19 explora cómo testear pipelines de datos
> antes de que lleguen al mundo real.
