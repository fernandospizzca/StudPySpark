from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window

# Inicializar a SparkSession
spark = SparkSession.builder \
    .appName("ExemploDataFrame") \
    .getOrCreate()

# Criar dados de exemplo
# data = [
#     (10, 20, 30, 1, 0, 1, 0, 0, 1),
#     (15, 25, 35, 1, 0, 0, 1, 1, 0),
#     (20, 30, 40, 0, 1, 1, 0, 1, 0),
#     (25, 35, 45, 1, 0, 1, 0, 0, 1)
# ]

data = [
    (1, 10, 20, 10, 20),
    (2, 15, 25, 15, 25),
    (3, 20, 30, 20, 23),
    (4, 25, 35, 45, 12)
]

# Definir schema
# columns = ["valor1", "valor2", "valor3", 
#            "valor1_OK", "valor1_NOK", 
#            "valor2_OK", "valor2_NOK", 
#            "valor3_OK", "valor3_NOK"]

columns = ["ID", "valor1", "valor2", "valor3", "valor4"]

# Criar DataFrame
df = spark.createDataFrame(data, columns)

# windowSpec = Window.partitionBy("ID")

df = df.withColumn('valor1_OK', F.when(df.valor1 != df.valor3, 0).otherwise(1))\
       .withColumn('valor1_NOK', F.when(df.valor1 != df.valor3, 1).otherwise(0))\
       .withColumn('valor2_OK', F.when(df.valor2 != df.valor4, 0).otherwise(1))\
       .withColumn('valor2_NOK', F.when(df.valor2 != df.valor4, 1).otherwise(0))

qtdReg = df.count()

# df = df.withColumn('valor1_Qtd_OK', F.sum('valor1_OK'))\
#        .withColumn('valor1_Qtd_NOK', F.sum('valor1_NOK'))\
#        .withColumn('valor2_Qtd_OK', F.sum('valor2_OK'))\
#        .withColumn('valor2_Qtd_NOK', F.sum('valor2_NOK'))

columns = ["valor1", "valor2"]

# df2 = df.select(columns)

# Mostrar o schema e os dados
print("Schema do DataFrame:")
df.printSchema()

print("\nDados do DataFrame:")
# df.show()
# df2.show()

df2 = df.select(F.sum("valor1_OK").alias('valor1_Qtd_OK')\
         ,F.sum("valor1_NOK").alias('valor1_Qtd_NOK')\
         ,F.sum("valor2_OK").alias('valor2_Qtd_OK')\
         ,F.sum("valor2_NOK").alias('valor2_Qtd_NOK'))


df2.show()

df2 = df2.transpose()

df2.printSchema()

df2.show()

# pdf = df2.toPandas(

# df1_transposed = pdf.T.sort_index()  

# df1_transposed = pdf.transpose()

# from IPython.display import display

# display(df1_transposed)