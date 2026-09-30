from pyspark.sql import functions as F

flattened = (
    df_carts
    .select("id", "created_at", F.explode("attributes").alias("attr_key", "attr_value"))
)

result = (
    flattened
    .select("id", "attr_key")
    .orderBy(F.col("attr_key").desc(), "id")
    .limit(5)
)
