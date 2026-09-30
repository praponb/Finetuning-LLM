from pyspark.sql import functions as F

result = df_carts.select("id", "coupon_codes", F.inline("items"))
