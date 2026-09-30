from pyspark.sql import functions as F

lines = df_carts.select("id", "created_at", "coupon_codes", F.inline("items"))

result = lines.select("created_at", "coupon_codes", "product_id", "qty", "price", "promo")
