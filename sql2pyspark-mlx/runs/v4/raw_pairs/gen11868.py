from pyspark.sql import functions as F

flattened = (
    df_carts
    .select("id", "customer_id", F.explode("attributes").alias("attr_key", "value"))
)

result = (
    flattened
    .select("id", "customer_id", "attr_key", "value")
    .orderBy(F.col("id").asc_nulls_last())
)
