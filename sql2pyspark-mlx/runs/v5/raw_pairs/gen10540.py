from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("line_total_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy(F.col("line_total_2").desc(), "id")
w_running = Window.partitionBy("product_id").orderBy("line_total", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "*",
        F.round((F.col("unit_price") / F.col("quantity")), 2).alias("line_total"),
        (F.col("unit_price") + F.col("quantity")).alias("line_total_2"),
    )
)

result = (
    base
    .select(
        "id",
        "product_id",
        "quantity",
        F.sum("unit_price").over(w).alias("cumulative_unit_price"),
        F.ntile(3).over(w2).alias("line_total_2_bucket"),
        F.first("unit_price", ignorenulls=True).over(w_running).alias("first_known_unit_price"),
    )
)
