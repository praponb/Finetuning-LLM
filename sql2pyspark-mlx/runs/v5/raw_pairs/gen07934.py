from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id")
w2 = Window.partitionBy("store_id")
w3 = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")

with_windows = (
    df_inventory
    .filter(F.col("store_id") == 5)
    .select(
        "*",
        F.lag("quantity", 1).over(w).alias("prev_quantity"),
        F.count("quantity").over(w2).alias("store_id_count_quantity"),
        F.ntile(10).over(w3).alias("quantity_bucket"),
    )
)

result = (
    with_windows
    .groupBy(F.date_trunc("QUARTER", "last_restock").alias("quarter_start"), "store_id")
    .agg(
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.round(F.avg("quantity_bucket"), 2).alias("avg_quantity_bucket"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.bit_or("quantity").alias("quantity_bit_or_2"),
    )
)
