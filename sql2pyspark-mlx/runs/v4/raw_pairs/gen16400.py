from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy(F.col("quantity").desc_nulls_first(), "id")
w2 = Window.partitionBy("store_id").orderBy(F.col("last_restock").asc_nulls_last(), "id")
w3 = Window.partitionBy("store_id").orderBy("quantity_rounded", "id")
w4 = Window.partitionBy("store_id")
w5 = Window.partitionBy("store_id").orderBy(F.col("quantity_rounded").desc_nulls_last())
w6 = Window.partitionBy("product_id").orderBy("quantity_rounded", "id")
w7 = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("store_id").orderBy("quantity_rounded_percent_rank", "id").rowsBetween(-3, Window.currentRow)
w9 = Window.partitionBy("store_id", "product_id").orderBy(F.col("cumulative_quantity").desc(), "id")

base = (
    df_inventory
    .filter((F.col("last_restock") >= F.to_date(F.lit("2023-03-09"))) & F.col("store_id").isin([1, 2, 4]))
    .withColumn("quantity_rounded", F.round("quantity", 2))
)

deduped = (
    base
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

latest = (
    deduped
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_windows = (
    latest
    .select(
        "*",
        F.ntile(10).over(w3).alias("quantity_rounded_bucket"),
        F.min("quantity_rounded").over(w4).alias("store_id_min_quantity_rounded"),
        F.percent_rank().over(w5).alias("quantity_rounded_percent_rank"),
        F.ntile(4).over(w6).alias("quantity_rounded_bucket_2"),
        F.sum("quantity").over(w7).alias("cumulative_quantity"),
    )
)

result = (
    with_windows
    .withColumn("cumulative_quantity_avg_3", F.avg("cumulative_quantity").over(w_part))
    .withColumn("cumulative_quantity_bucket", F.ntile(3).over(w9))
)
