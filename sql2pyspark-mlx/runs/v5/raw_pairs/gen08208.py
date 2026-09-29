from pyspark.sql import functions as F

flattened = (
    df_carts
    .select("id", "customer_id", F.explode_outer("coupon_codes").alias("element"))
)

result = (
    flattened
    .select("element")
    .orderBy(F.col("element").asc_nulls_last())
)
