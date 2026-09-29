from pyspark.sql import functions as F

flattened = df_carts.select("id", "created_at", F.explode_outer("coupon_codes").alias("tag"))

result = flattened.select("id", "created_at", "tag")
