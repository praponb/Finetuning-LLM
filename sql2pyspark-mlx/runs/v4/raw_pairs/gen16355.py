from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.partitionBy("order_id").orderBy("quantity", "id").rowsBetween(-3, -1)
w3 = Window.partitionBy("order_id")
w4 = Window.partitionBy("product_id", "order_id").orderBy(F.col("quantity").desc(), "id")
w_running = Window.partitionBy("product_id").orderBy("unit_price", "id")

result = (
    df_order_items
    .select(
        "*",
        F.corr("quantity", "unit_price").over(w).alias("corr_quantity_unit_price"),
        F.avg("unit_price").over(w2).alias("unit_price_avg_3_prior"),
        F.max("unit_price").over(w3).alias("order_id_max_unit_price"),
        F.ntile(3).over(w4).alias("quantity_bucket"),
        F.ntile(4).over(w_running).alias("unit_price_bucket"),
    )
)
