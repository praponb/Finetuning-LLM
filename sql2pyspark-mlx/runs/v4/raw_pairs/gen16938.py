from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("quantity").desc(), "id")
w2 = Window.partitionBy("order_id")
w3 = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("order_id", "product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_order_items
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.round((F.col("unit_price") / F.sum("unit_price").over(w2)), 4).alias("unit_price_share"),
        F.last("quantity", ignorenulls=True).over(w3).alias("last_known_quantity"),
        F.sum("unit_price").over(w4).alias("cumulative_unit_price"),
        F.last("unit_price", ignorenulls=True).over(w5).alias("last_known_unit_price"),
    )
)
