from pyspark.sql import functions as F

lines = df_carts.select("id", "customer_id", F.explode_outer("coupon_codes").alias("tag"))

result = (
    lines
    .select("customer_id", "tag")
    .orderBy(F.col("tag").asc_nulls_last())
)
