from pyspark.sql import functions as F

flattened = (
    df_carts
    .select("id", "customer_id", "created_at", F.explode("coupon_codes").alias("value"))
)

result = (
    flattened
    .select("customer_id")
    .orderBy(F.col("customer_id").asc_nulls_last())
)
