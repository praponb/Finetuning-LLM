from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id").orderBy(F.col("quantity").desc(), "id")
w2 = Window.orderBy(F.col("unit_price").asc_nulls_last())
w3 = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_order_items
    .select(
        "id",
        "order_id",
        "unit_price",
        F.row_number().over(w).alias("event_seq"),
        F.cume_dist().over(w2).alias("unit_price_cume_dist"),
        F.sum("quantity").over(w3).alias("cumulative_quantity"),
    )
)
