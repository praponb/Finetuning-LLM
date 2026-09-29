from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("order_id", "product_id").orderBy("quantity_scaled", "id").rowsBetween(-3, Window.currentRow)

cleaned = (
    df_order_items
    .filter(F.col("order_id").isin([5, 12, 15]))
    .withColumn("quantity_scaled", F.round((F.col("quantity") * 100), 2))
    .withColumn("quantity_rounded", F.round("quantity", 1))
)

result = (
    cleaned
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        "quantity_scaled",
        "quantity_rounded",
        F.avg("quantity_rounded").over(w_recent).alias("last4_avg_quantity_rounded"),
        F.count("quantity_rounded").over(w_recent).alias("last4_count_quantity_rounded"),
        F.min("quantity_rounded").over(w_recent).alias("last4_min_quantity_rounded"),
    )
)
