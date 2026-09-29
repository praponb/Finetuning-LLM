from pyspark.sql import functions as F

lines = (
    df_carts
    .select("id", "customer_id", F.explode("attributes").alias("key", "attr_value"))
)

result = (
    lines
    .select("id", "customer_id", "key", "attr_value")
    .orderBy(F.col("customer_id").desc())
)
