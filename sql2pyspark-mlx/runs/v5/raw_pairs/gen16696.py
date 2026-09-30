from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")
w2 = Window.partitionBy("product_id", "order_id")

result = (
    df_order_items
    .select(
        "id",
        "order_id",
        "quantity",
        "product_id",
        "unit_price",
        F.avg("quantity").over(w).alias("order_id_avg_quantity"),
        F.round((F.col("unit_price") / F.sum("unit_price").over(w2)), 3).alias("unit_price_share"),
    )
)
