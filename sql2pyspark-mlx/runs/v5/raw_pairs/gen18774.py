from pyspark.sql import functions as F

exploded = (
    df_carts
    .select("id", "coupon_codes", F.explode_outer("attributes").alias("key", "value"))
)

result = (
    exploded
    .select("id", "key")
    .orderBy(F.col("key").desc())
)
