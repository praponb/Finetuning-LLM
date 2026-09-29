from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity").asc_nulls_first())
w2 = Window.partitionBy("store_id").orderBy(F.col("quantity").desc(), "id")
w_ordered = Window.partitionBy("product_id").orderBy("quantity", "id")

with_windows = (
    df_inventory
    .filter(F.col("quantity") <= 180)
    .select(
        "*",
        F.rank().over(w).alias("quantity_rank"),
        F.ntile(5).over(w2).alias("quantity_bucket"),
        F.lead("quantity", 1).over(w_ordered).alias("next_quantity"),
    )
)

result = (
    with_windows
    .groupBy("product_id", "store_id")
    .agg(
        F.min("quantity_rank").alias("min_quantity_rank"),
        F.round(F.avg("quantity_bucket"), 2).alias("avg_quantity_bucket"),
        F.percentile_approx("quantity_bucket", 0.9).alias("quantity_bucket_p90"),
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.percentile_approx("quantity_bucket", 0.5).alias("quantity_bucket_p50"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
    )
    .filter(F.col("quantity_bucket_p50") < 1000000)
    .orderBy(F.col("quantity_bucket_p90").desc())
)
