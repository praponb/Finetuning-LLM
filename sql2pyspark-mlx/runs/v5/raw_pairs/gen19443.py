from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .agg(F.count("*").alias("cnt"))
)
