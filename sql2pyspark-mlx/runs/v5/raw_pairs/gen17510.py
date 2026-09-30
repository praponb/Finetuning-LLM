from pyspark.sql import functions as F

exploded = df_carts.select("id", "created_at", F.explode_outer("coupon_codes").alias("code"))

result = (
    exploded
    .select("id")
    .orderBy("id")
)
