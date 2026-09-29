from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id").orderBy("unit_price", "id").rowsBetween(-3, -1)
w2 = Window.partitionBy("product_id", "order_id")

result = (
    df_order_items
    .select(
        "*",
        F.avg("quantity").over(w).alias("quantity_avg_3_prior"),
        F.min("unit_price").over(w2).alias("product_id_min_unit_price"),
    )
)
