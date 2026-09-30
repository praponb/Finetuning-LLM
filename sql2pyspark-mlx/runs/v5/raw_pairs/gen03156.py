from pyspark.sql import functions as F

result = (
    df_carts
    .select("id", "created_at", F.explode("attributes").alias("k", "attr_value"))
)
