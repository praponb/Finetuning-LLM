from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("product_id", "order_id").orderBy("unit_price", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.orderBy(F.col("unit_price").desc(), "id")

metrics = (
    df_order_items
    .select(
        "*",
        F.count("unit_price").over(w_running).alias("last4_count_unit_price"),
        F.avg("unit_price").over(w_running).alias("last4_avg_unit_price"),
        F.max("unit_price").over(w_running).alias("last4_max_unit_price"),
        F.ntile(5).over(w2).alias("unit_price_bucket"),
    )
)

result = (
    metrics
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        "last4_avg_unit_price",
        "last4_max_unit_price",
    )
    .orderBy("product_id")
)
