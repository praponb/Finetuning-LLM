from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("unit_price", "id")
w2 = Window.partitionBy("order_id").orderBy(F.col("quantity").asc_nulls_last())

result = (
    df_order_items
    .select(
        "id",
        "product_id",
        "unit_price",
        F.lag("quantity", 2).over(w).alias("prev_quantity"),
        F.cume_dist().over(w2).alias("quantity_cume_dist"),
    )
)
