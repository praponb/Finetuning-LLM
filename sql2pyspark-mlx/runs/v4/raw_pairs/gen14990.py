from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo")
w_part = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w3 = Window.orderBy(F.col("product_id").desc_nulls_first())
w4 = Window.partitionBy("promo").orderBy(F.col("qty").desc_nulls_last())

lines = df_carts.select("id", "created_at", F.inline("items"))

result = (
    lines
    .withColumn("product_id_share", F.round((F.col("product_id") / F.sum("product_id").over(w)), 4))
    .withColumn("qty_sum_2h", F.sum("qty").over(w_part))
    .withColumn("product_id_rank", F.rank().over(w3))
    .withColumn("promo_min_product_id", F.min("product_id").over(w))
    .withColumn("qty_dense_rank", F.dense_rank().over(w4))
)
