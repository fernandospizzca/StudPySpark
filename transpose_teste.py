from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = SparkSession.builder \
    .appName("ExemploTranspose") \
    .getOrCreate()

# Supondo que seu DataFrame se chame 'df' com a estrutura fornecida
# Vamos criar um DataFrame de exemplo caso não exista
data = [
    (1, 10, 20, 30, 40, 1, 0, 1, 0),
    (2, 15, 25, 35, 45, 1, 0, 0, 1),
    (3, 20, 30, 40, 50, 0, 1, 1, 0)
]
columns = ["ID", "valor1", "valor2", "valor3", "valor4", 
           "valor1_OK", "valor1_NOK", "valor2_OK", "valor2_NOK"]
df = spark.createDataFrame(data, columns)

# Adicionando as colunas valor3_OK, valor3_NOK, valor4_OK, valor4_NOK que estavam faltando
df = df.withColumn("valor3_OK", F.lit(1)) \
       .withColumn("valor3_NOK", F.lit(0)) \
       .withColumn("valor4_OK", F.lit(1)) \
       .withColumn("valor4_NOK", F.lit(0))

# Realizando o transpose
# df_transposed = df.select(
#     "ID",
#     F.explode(F.array(
#         F.struct(
#             F.lit("valor1").alias("tipo"),
#             F.col("valor1").alias("valor"),
#             F.col("valor1_OK").alias("OK"),
#             F.col("valor1_NOK").alias("NOK")
#         ),
#         F.struct(
#             F.lit("valor2").alias("tipo"),
#             F.col("valor2").alias("valor"),
#             F.col("valor2_OK").alias("OK"),
#             F.col("valor2_NOK").alias("NOK")
#         ),
#         F.struct(
#             F.lit("valor3").alias("tipo"),
#             F.col("valor3").alias("valor"),
#             F.col("valor3_OK").alias("OK"),
#             F.col("valor3_NOK").alias("NOK")
#         ),
#         F.struct(
#             F.lit("valor4").alias("tipo"),
#             F.col("valor4").alias("valor"),
#             F.col("valor4_OK").alias("OK"),
#             F.col("valor4_NOK").alias("NOK")
#         )
#     )).alias("valores")
# ).select("ID", "valores.*")

# Mostrando o resultado
# df_transposed.show()

# df_transposed.groupBy("tipo", "valor", "OK", "NOK").count().orderBy("tipo").show()

# from itertools import chain

# Lista de valores base (sem sufixos)
valores_base = [f"valor{i}" for i in range(1, 5)]  # Ajuste o range conforme necessário

print(valores_base)

# Criar a lista de structs dinamicamente
structs = []
for vb in valores_base:
    structs.append(F.struct(
        F.lit(vb).alias("tipo"),
        F.col(vb).alias("valor"),
        F.col(f"{vb}_OK").alias("OK"),
        F.col(f"{vb}_NOK").alias("NOK")
    ))

# Aplicar o transpose
df_transposed = df.select(
    "ID",
    F.explode(F.array(*structs)).alias("valores")
).select("ID", "valores.*")


df_transposed.show()
