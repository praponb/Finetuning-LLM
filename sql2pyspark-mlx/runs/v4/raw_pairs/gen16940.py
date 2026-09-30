from pyspark.sql import functions as F

flattened = df_carts.select("id", "created_at", F.explode("coupon_codes").alias("value"))

result = (
    flattened
    .select("id", "value")
    .orderBy("value")
)
