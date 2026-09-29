from pyspark.sql import functions as F

exploded = df_carts.select("id", "created_at", "coupon_codes", F.inline("items"))

result = (
    exploded
    .select("id", "created_at", "coupon_codes", "qty")
    .orderBy(F.col("qty").asc_nulls_first())
)
