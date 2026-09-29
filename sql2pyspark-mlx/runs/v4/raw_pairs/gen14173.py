from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("quantity", "id")
w2 = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_order_items
    .select(
        "*",
        F.ntile(3).over(w).alias("quantity_bucket"),
        F.sum("unit_price").over(w2).alias("cumulative_unit_price"),
    )
)

result = (
    scored
    .groupBy("order_id")
    .agg(
        F.percentile_approx("cumulative_unit_price", [0.25, 0.5, 0.75]).alias("cumulative_unit_price_quartiles"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.max("quantity_bucket").alias("max_quantity_bucket"),
    )
)
