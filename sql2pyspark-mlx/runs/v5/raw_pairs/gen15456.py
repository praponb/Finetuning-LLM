from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("product_id").orderBy("unit_price", "id")
w2 = Window.partitionBy("product_id").orderBy("quantity_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("quantity_rounded", "id").rowsBetween(-3, -1)

filtered = (
    df_order_items
    .filter(F.col("unit_price").isNotNull())
    .select(
        "id",
        "product_id",
        "unit_price",
        F.round("quantity", 1).alias("quantity_rounded"),
    )
)

result = (
    filtered
    .filter(F.col("unit_price") < 280)
    .select(
        "id",
        "product_id",
        "unit_price",
        F.ntile(4).over(w_part).alias("unit_price_bucket"),
        F.sum("quantity_rounded").over(w2).alias("cumulative_quantity_rounded"),
        F.avg("quantity_rounded").over(w3).alias("quantity_rounded_avg_3_prior"),
    )
)
