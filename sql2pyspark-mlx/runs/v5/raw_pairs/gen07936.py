from pyspark.sql import functions as F

result = df_carts.select("id", F.explode("coupon_codes").alias("code"))
