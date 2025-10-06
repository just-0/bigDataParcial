#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
============================================================================
JOB 5: SISTEMA DE RECOMENDACIÓN CON SPARK ALS
============================================================================
Objetivo: Entrenar modelo ALS para recomendar canciones a usuarios
Input: listening_clean (Parquet en S3)
Output: Modelo ALS + recomendaciones + métricas
============================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, stddev, min as spark_min, max as spark_max
from pyspark.sql.types import IntegerType
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
import sys

# ============================================================================
# CONFIGURACIÓN DE SPARK
# ============================================================================

print("=" * 80)
print("INICIANDO JOB 5: SISTEMA DE RECOMENDACIÓN")
print("=" * 80)

spark = SparkSession.builder \
    .appName("MAESTRO-Job5-Recommender-ALS") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Paths en S3
S3_BUCKET = "s3://emr-logs-1758750407/music-data"
INPUT_LISTENING = f"{S3_BUCKET}/cleaned/listening/"
OUTPUT_MODEL = f"{S3_BUCKET}/models/als_model/"
OUTPUT_RECOMMENDATIONS = f"{S3_BUCKET}/recommendations/"
OUTPUT_METRICS = f"{S3_BUCKET}/metrics/"

print("\n📂 Configuración de rutas:")
print(f"   Input: {INPUT_LISTENING}")
print(f"   Modelo: {OUTPUT_MODEL}")
print(f"   Recomendaciones: {OUTPUT_RECOMMENDATIONS}")


# ============================================================================
# PASO 1: CARGAR Y EXPLORAR DATOS
# ============================================================================

print("\n" + "=" * 80)
print("PASO 1: CARGA Y EXPLORACIÓN DE DATOS")
print("=" * 80)

# Cargar listening_clean
df_listening = spark.read.parquet(INPUT_LISTENING)

print(f"\n✓ Datos cargados: {df_listening.count():,} registros")
print("\nEsquema:")
df_listening.printSchema()

# Estadísticas básicas
print("\n📊 Estadísticas de interacciones:")
df_listening.select(
    count("*").alias("total_interactions"),
    count("user_id").alias("distinct_users"),
    count("track_id").alias("distinct_tracks"),
    avg("total_playcount").alias("avg_playcount"),
    stddev("total_playcount").alias("stddev_playcount"),
    spark_min("total_playcount").alias("min_playcount"),
    spark_max("total_playcount").alias("max_playcount")
).show()

# Distribución de interacciones por usuario
print("\n📈 Distribución de canciones por usuario:")
user_songs = df_listening.groupBy("user_id").agg(
    count("track_id").alias("num_songs"),
    avg("total_playcount").alias("avg_plays")
)
user_songs.describe().show()


# ============================================================================
# PASO 2: PREPARACIÓN DE DATOS PARA ALS
# ============================================================================

print("\n" + "=" * 80)
print("PASO 2: PREPARACIÓN DE DATOS")
print("=" * 80)

# ALS requiere IDs numéricos, así que creamos índices
from pyspark.ml.feature import StringIndexer

print("\n⚙️  Creando índices numéricos para usuarios y canciones...")

# Indexar usuarios
user_indexer = StringIndexer(inputCol="user_id", outputCol="user_idx")
user_model = user_indexer.fit(df_listening)
df_indexed = user_model.transform(df_listening)

# Indexar tracks
track_indexer = StringIndexer(inputCol="track_id", outputCol="track_idx")
track_model = track_indexer.fit(df_indexed)
df_indexed = track_model.transform(df_indexed)

# Seleccionar columnas necesarias y convertir a int
df_als = df_indexed.select(
    col("user_idx").cast(IntegerType()).alias("user"),
    col("track_idx").cast(IntegerType()).alias("item"),
    col("total_playcount").cast(IntegerType()).alias("rating")
)

print(f"✓ Datos indexados: {df_als.count():,} registros")
print("\nMuestra de datos preparados:")
df_als.show(10)

# Filtrar usuarios/canciones con muy pocas interacciones (mejora calidad)
print("\n🔍 Aplicando filtros de calidad...")

min_user_interactions = 5  # Usuario debe tener al menos 5 canciones
min_item_interactions = 5  # Canción debe tener al menos 5 usuarios

user_counts = df_als.groupBy("user").count().filter(col("count") >= min_user_interactions)
item_counts = df_als.groupBy("item").count().filter(col("count") >= min_item_interactions)

df_filtered = df_als.join(user_counts.select("user"), "user") \
                    .join(item_counts.select("item"), "item")

print(f"✓ Después de filtrado: {df_filtered.count():,} registros")
print(f"   Usuarios únicos: {df_filtered.select('user').distinct().count():,}")
print(f"   Canciones únicas: {df_filtered.select('item').distinct().count():,}")


# ============================================================================
# PASO 3: DIVISIÓN TRAIN/TEST
# ============================================================================

print("\n" + "=" * 80)
print("PASO 3: DIVISIÓN TRAIN/TEST")
print("=" * 80)

# Split 80/20
(training, test) = df_filtered.randomSplit([0.8, 0.2], seed=42)

print(f"\n📊 Distribución de datos:")
print(f"   Training: {training.count():,} registros ({training.count()/df_filtered.count()*100:.1f}%)")
print(f"   Test: {test.count():,} registros ({test.count()/df_filtered.count()*100:.1f}%)")


# ============================================================================
# PASO 4: ENTRENAMIENTO DEL MODELO ALS
# ============================================================================

print("\n" + "=" * 80)
print("PASO 4: ENTRENAMIENTO DEL MODELO ALS")
print("=" * 80)

print("\n⚙️  Configurando modelo ALS...")

# Configuración del modelo
als = ALS(
    maxIter=10,
    rank=10,  # Número de factores latentes
    regParam=0.1,  # Regularización
    userCol="user",
    itemCol="item",
    ratingCol="rating",
    coldStartStrategy="drop",  # Ignorar usuarios/items nuevos en test
    implicitPrefs=False,  # Ratings explícitos (playcount)
    nonnegative=True  # Factores no negativos
)

print("\n🚀 Entrenando modelo ALS...")
print("   (esto puede tardar 3-5 minutos)")

model = als.fit(training)

print("✓ Modelo entrenado exitosamente!")


# ============================================================================
# PASO 5: EVALUACIÓN DEL MODELO
# ============================================================================

print("\n" + "=" * 80)
print("PASO 5: EVALUACIÓN DEL MODELO")
print("=" * 80)

# Predicciones en test set
print("\n📊 Generando predicciones en test set...")
predictions = model.transform(test)
predictions_clean = predictions.filter(col("prediction").isNotNull())

print(f"✓ Predicciones generadas: {predictions_clean.count():,}")

# Evaluar RMSE (Root Mean Square Error)
evaluator = RegressionEvaluator(
    metricName="rmse",
    labelCol="rating",
    predictionCol="prediction"
)

rmse = evaluator.evaluate(predictions_clean)
print(f"\n📈 RMSE del modelo: {rmse:.4f}")

# Evaluar MAE (Mean Absolute Error)
evaluator_mae = RegressionEvaluator(
    metricName="mae",
    labelCol="rating",
    predictionCol="prediction"
)
mae = evaluator_mae.evaluate(predictions_clean)
print(f"📈 MAE del modelo: {mae:.4f}")

# Guardar métricas
metrics_data = [("RMSE", rmse), ("MAE", mae)]
df_metrics = spark.createDataFrame(metrics_data, ["metric", "value"])
df_metrics.coalesce(1).write.mode("overwrite").parquet(f"{OUTPUT_METRICS}/model_evaluation/")
print(f"\n✓ Métricas guardadas en {OUTPUT_METRICS}/model_evaluation/")


# ============================================================================
# PASO 6: GENERAR RECOMENDACIONES
# ============================================================================

print("\n" + "=" * 80)
print("PASO 6: GENERACIÓN DE RECOMENDACIONES")
print("=" * 80)

# Top 10 recomendaciones para cada usuario
print("\n⚙️  Generando top 10 recomendaciones por usuario...")
user_recs = model.recommendForAllUsers(10)
print(f"✓ Recomendaciones generadas para {user_recs.count():,} usuarios")

# Guardar recomendaciones
user_recs.write.mode("overwrite").parquet(f"{OUTPUT_RECOMMENDATIONS}/user_recommendations/")
print(f"✓ Guardadas en {OUTPUT_RECOMMENDATIONS}/user_recommendations/")

# Ejemplo de recomendaciones
print("\n📋 Ejemplo de recomendaciones (primeros 5 usuarios):")
user_recs.show(5, truncate=False)

# Top 10 usuarios similares para cada canción
print("\n⚙️  Generando top 10 usuarios por canción...")
item_recs = model.recommendForAllItems(10)
print(f"✓ Recomendaciones generadas para {item_recs.count():,} canciones")

item_recs.write.mode("overwrite").parquet(f"{OUTPUT_RECOMMENDATIONS}/item_recommendations/")
print(f"✓ Guardadas en {OUTPUT_RECOMMENDATIONS}/item_recommendations/")


# ============================================================================
# PASO 7: CASOS DE USO PRÁCTICOS
# ============================================================================

print("\n" + "=" * 80)
print("PASO 7: CASOS DE USO Y ANÁLISIS")
print("=" * 80)

# Recuperar mapeos originales
user_labels = user_model.labels
track_labels = track_model.labels

# Crear DataFrames con los mapeos
df_user_mapping = spark.createDataFrame(
    [(i, user_id) for i, user_id in enumerate(user_labels)],
    ["user_idx", "user_id"]
)

df_track_mapping = spark.createDataFrame(
    [(i, track_id) for i, track_id in enumerate(track_labels)],
    ["track_idx", "track_id"]
)

# Guardar mapeos para uso futuro
df_user_mapping.write.mode("overwrite").parquet(f"{OUTPUT_MODEL}/user_mapping/")
df_track_mapping.write.mode("overwrite").parquet(f"{OUTPUT_MODEL}/track_mapping/")
print("✓ Mapeos guardados para uso posterior")

# Ejemplo: Recomendaciones para usuarios específicos
print("\n📌 EJEMPLO: Recomendaciones para usuarios de muestra")

# Tomar 5 usuarios aleatorios
sample_users = df_filtered.select("user").distinct().limit(5)
sample_recs = model.recommendForUserSubset(sample_users, 5)

print("\nTop 5 recomendaciones para usuarios de muestra:")
sample_recs.show(truncate=False)


# ============================================================================
# PASO 8: ESTADÍSTICAS FINALES
# ============================================================================

print("\n" + "=" * 80)
print("PASO 8: ESTADÍSTICAS FINALES")
print("=" * 80)

# Calcular estadísticas de las recomendaciones
print("\n📊 Análisis de recomendaciones:")

# Distribución de scores de recomendación
from pyspark.sql.functions import explode, col as F_col

user_recs_exploded = user_recs.select(
    F_col("user"),
    explode(F_col("recommendations")).alias("rec")
).select(
    F_col("user"),
    F_col("rec.item").alias("item"),
    F_col("rec.rating").alias("score")
)

print("\nDistribución de scores de recomendación:")
user_recs_exploded.select("score").describe().show()

# Guardar recomendaciones en formato legible
user_recs_exploded.write.mode("overwrite").parquet(
    f"{OUTPUT_RECOMMENDATIONS}/user_recs_exploded/"
)
print(f"✓ Recomendaciones en formato expandido guardadas")


# ============================================================================
# RESUMEN FINAL
# ============================================================================

print("\n" + "=" * 80)
print("JOB 5 COMPLETADO EXITOSAMENTE")
print("=" * 80)

print(f"""
📊 RESUMEN DEL MODELO:
   • Algoritmo: ALS (Alternating Least Squares)
   • Factores latentes (rank): 10
   • Iteraciones: 10
   • RMSE: {rmse:.4f}
   • MAE: {mae:.4f}
   
📁 ARCHIVOS GENERADOS:
   • Modelo ALS: {OUTPUT_MODEL}
   • Recomendaciones por usuario: {OUTPUT_RECOMMENDATIONS}/user_recommendations/
   • Recomendaciones por canción: {OUTPUT_RECOMMENDATIONS}/item_recommendations/
   • Métricas: {OUTPUT_METRICS}/model_evaluation/
   • Mapeos: {OUTPUT_MODEL}/user_mapping/ y track_mapping/

✅ El modelo está listo para generar recomendaciones personalizadas!
""")

spark.stop()
print("Spark session finalizada.")
