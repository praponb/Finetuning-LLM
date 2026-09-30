from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("code")
w2 = Window.orderBy("product_id")

lines = df_carts.select("id", "customer_id", "coupon_codes", F.inline("items"))

flattened = (
    lines
    .select("id", "product_id", "qty", F.explode_outer("coupon_codes").alias("code"))
)

with_windows = (
    flattened
    .withColumn("product_id_share", F.round((F.col("product_id") / F.sum("product_id").over(w)), 4))
    .withColumn("code_sum_product_id", F.sum("product_id").over(w))
    .withColumn("product_id_cume_dist", F.cume_dist().over(w2))
)

result = (
    with_windows
    .select("qty", "code", "product_id_share", "code_sum_product_id")
    .orderBy("qty")
)
