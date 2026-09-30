from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("quantity", "id")
w2 = Window.orderBy("last_restock", "id")

with_windows = (
    df_inventory
    .filter(F.col("product_id") == 6)
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        F.lead("quantity", 1).over(w).alias("next_quantity"),
        F.lag("quantity", 1, 0).over(w2).alias("prev_quantity"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("prev_quantity") >= 247, "HIGH").otherwise("LOW").alias("prev_quantity_tier"),
    )
    .agg(
        F.percentile_approx("next_quantity", [0.25, 0.5, 0.75]).alias("next_quantity_quartiles"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.max("prev_quantity").alias("max_prev_quantity"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("event_count"),
    )
)
