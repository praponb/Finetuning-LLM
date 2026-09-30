from pyspark.sql import functions as F

result = (
    df_carts
    .select("id", "coupon_codes", F.explode_outer("attributes").alias("key", "v"))
)
