from pyspark.sql import functions as F

flattened = df_app_logs.select("id", F.explode("context").alias("k", "value"))

result = (
    flattened
    .select("id", "k", "value")
    .orderBy(F.col("k").desc_nulls_last())
)
