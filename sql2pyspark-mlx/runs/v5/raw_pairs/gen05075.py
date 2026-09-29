from pyspark.sql import functions as F

flattened = df_carts.select("id", "customer_id", F.explode_outer("coupon_codes").alias("tag"))

result = (
    flattened
    .select("customer_id", "tag")
    .orderBy(F.col("customer_id").desc_nulls_first())
)
