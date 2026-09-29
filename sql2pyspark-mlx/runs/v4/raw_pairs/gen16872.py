from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("line_total_2", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.orderBy(F.col("line_total").desc())
w3 = Window.partitionBy("order_id", "product_id").orderBy("line_total_2", "id").rowsBetween(-5, -1)

filtered = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .withColumn("line_total", (F.col("quantity") + F.col("unit_price")))
    .withColumn("line_total_2", (F.col("quantity") * F.col("unit_price")))
)

result = (
    filtered
    .select(
        "*",
        F.avg("unit_price").over(w).alias("unit_price_avg_5"),
        F.cume_dist().over(w2).alias("line_total_cume_dist"),
        F.avg("unit_price").over(w3).alias("unit_price_avg_5_prior"),
    )
)
