from pyspark.sql import functions as F

exploded = (
    df_carts
    .select("id", "customer_id", F.explode_outer("attributes").alias("k", "attr_value"))
)

result = (
    exploded
    .select("id", "customer_id", "k")
    .orderBy(F.col("k").desc_nulls_first(), "id")
)
