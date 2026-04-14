# =================================================
# 1. IMPORTAR LIBRERÍAS
# =================================================
from pyspark.sql import SparkSession, functions as F
# =================================================
# 2. CREAR SESIÓN SPARK
# =================================================
spark = SparkSession.builder \
    .appName("Colombianos_Detenidos_Exterior") \
    .getOrCreate()
# =================================================
# 3. CARGAR DATASET
# =================================================
file_path = "hdfs://localhost:9000/Tarea3.ProcesamientoDatos/e97j-vuf7.csv"

df = spark.read.csv(file_path, header=True, inferSchema=True)

df.printSchema()
print("===DATOS ORIGINALES===")
df.show(15)
#=================================================
# 4. LIMPIEZA DE DATOS Y TRANFORMACION DE DATOS
#=================================================

df_limpieza = (
    df
    .withColumnRenamed("PAIS PRISIÓN", "pais_prision")
    .withColumnRenamed("GÉNERO", "genero")
    .withColumnRenamed("SITUACIÓN JURÍDICA", "situacion_juridica")
    .withColumnRenamed("FECHA PUBLICACIÓN", "fecha_publicacion")
    .withColumnRenamed("EXTRADITADO Y O REPATRIADO", "estado_retorno")
    .withColumnRenamed("GRUPO EDAD", "grupo_edad")
    .withColumnRenamed("CANTIDAD", "cantidad")
    .withColumnRenamed("DELITO", "delito")
    .dropna(subset=["pais_prision", "delito", "genero", "situacion_juridica"])
    .dropDuplicates()
)

print("=== DATOS LIMPIOS ===")
df_limpieza.printSchema()
df_limpieza.show(15)

print("Total registros originales:", df.count())
print("Total registros limpios:", df_limpieza.count())

#=================================================
#    5. EDA
#=================================================
print("\n========================================")
print("   ANALISIS_EXPLORATORIO_DE_DATOS       ")
print("========================================")

# 1. País con más detenidos
print("\n1. Pais con mas detenidos" )
df_limpieza.groupBy("pais_prision") \
    .count() \
    .orderBy("count", ascending=False) \
    .show()

# 2. Delitos más frecuentes
print("\n2. Delitos mas frecuentes")
df_limpieza.groupBy("delito") \
     .count() \
    .orderBy("count", ascending=False) \
    .show()

# 3. Distribución por género
print("\n3. Distribucion por genero")
df_limpieza.groupBy("genero") \
    .count() \
    .show()

# 4. Edad más común
print("\n4. Edad mas comun")
df_limpieza.groupBy("grupo_edad") \
    .count() \
    .orderBy("count", ascending=False) \
    .show()

# 5. Situación jurídica
print("\n5. Sitiacion juridica")
df_limpieza.groupBy("situacion_juridica") \
    .count() \
    .orderBy("count", ascending=False) \
    .show()

# 6. Número de detenidos masculinos por país
print("\n6. Número de detenidos masculinos por país")
df_limpieza.filter(df_limpieza.genero == "MASCULINO") \
    .groupBy("pais_prision") \
    .agg(F.count("*").alias("total_masculino")) \
    .orderBy("total_masculino", ascending=False) \
    .show()


# 7. Países con pocos casos
print("\n7. Paises con pocos casos")
df_limpieza.groupBy("pais_prision") \
    .count() \
    .filter("count < 10") \
    .show()

# 8. Porcentaje por género
print("\n8. Porcentaje por género")
total = df_limpieza.count()

df_limpieza.groupBy("genero") \
    .count() \
    .withColumn("porcentaje", (F.col("count") / total) * 100) \
    .show()

# =================================================
# 6. CONSULTAS CON RDD
# =================================================
# 9. Primedio de detenidos por pais
print("\n--- CONSULTA RDD: Promedio de detenidos por país ---")

rdd_promedio = df_limpieza.rdd \
    .map(lambda x: (x["pais_prision"], (x["cantidad"], 1))) \
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
    .mapValues(lambda x: x[0] / x[1]) \
    .sortBy(lambda x: -x[1])

for pais, promedio in rdd_promedio.take(10):
    print(f"{pais}: {round(promedio, 2)}")

# 10. Detenidos por año
print("\n--- CONSULTA RDD: Detenidos por año ---")


rdd_anio = df_limpieza.rdd \
    .filter(lambda x: x["fecha_publicacion"] is not None) \
    .map(lambda x: (x["fecha_publicacion"].year, 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .sortBy(lambda x: x[1], ascending=False)

for anio, total in rdd_anio.take(10):
    print(f"{anio}: {total}")

# 11. Total registros analizados
print("\n--- CONSULTA RDD: Total de registros ---")

total_registros = df_limpieza.rdd.count()

print(f"Total de registros: {total_registros}")


