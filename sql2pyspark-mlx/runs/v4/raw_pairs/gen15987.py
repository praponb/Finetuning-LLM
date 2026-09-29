from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id")
w_part = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(-5, Window.currentRow)

with_windows = (
    df_inventory
    .select(
        "*",
        F.first("quantity", ignorenulls=True).over(w).alias("first_known_quantity"),
        F.count("quantity").over(w2).alias("product_id_count_quantity"),
        F.avg("quantity").over(w_part).alias("quantity_avg_5"),
    )
)

result = (
    with_windows
    .groupBy("store_id")
    .agg(
        F.count("*").alias("num_rows"),
        F.count("*").alias("row_count"),
        F.percentile_approx("quantity", 0.9).alias("quantity_p90"),
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.bit_or("quantity").alias("quantity_bit_or_2"),
    )
    .orderBy(F.col("quantity_bit_or_2").desc())
)
