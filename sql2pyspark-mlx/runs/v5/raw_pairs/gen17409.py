from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_order_items
    .select(
        "id",
        "order_id",
        "quantity",
        F.sum("quantity").over(w).alias("running_sum_quantity"),
        F.count("quantity").over(w).alias("running_count_quantity"),
        F.max("quantity").over(w).alias("running_max_quantity"),
        F.min("quantity").over(w).alias("running_min_quantity"),
        F.first("unit_price", ignorenulls=True).over(w2).alias("first_known_unit_price"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("quantity") >= 9, "LARGE").otherwise("SMALL").alias("quantity_tier"),
    )
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.round(F.avg("running_sum_quantity"), 2).alias("avg_running_sum_quantity"),
    )
    .orderBy(F.col("quantity_tier").desc())
)
