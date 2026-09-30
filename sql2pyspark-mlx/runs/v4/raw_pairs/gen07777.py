from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc_nulls_last())
w2 = Window.partitionBy("product_id", "order_id")
w3 = Window.partitionBy("product_id", "order_id").orderBy(F.col("quantity").desc_nulls_first())

filtered = (
    df_order_items
    .filter(F.col("product_id") == 39)
    .withColumn("unit_price_adjusted", F.round((F.col("unit_price") * 0.9), 2))
)

result = (
    filtered
    .select(
        "*",
        F.cume_dist().over(w).alias("unit_price_cume_dist"),
        F.sum("unit_price").over(w2).alias("product_id_sum_unit_price"),
        F.rank().over(w3).alias("quantity_rank"),
    )
)
