from pyspark.sql import functions as F

exploded = df_carts.select("id", F.explode("coupon_codes").alias("tag"))

result = (
    exploded
    .select("id")
    .orderBy("id")
)
