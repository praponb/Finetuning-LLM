from pyspark.sql import functions as F

lines = df_carts.select("id", "created_at", F.explode_outer("coupon_codes").alias("value"))

result = lines.select("id")
