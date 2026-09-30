from pyspark.sql import functions as F

lines = (
    df_carts
    .select("id", "customer_id", "created_at", F.explode("coupon_codes").alias("code"))
)

result = (
    lines
    .select("id", "customer_id", "created_at")
    .orderBy(F.col("customer_id").asc_nulls_first(), "created_at")
)
